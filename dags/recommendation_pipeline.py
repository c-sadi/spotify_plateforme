"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins


"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 1. Requête SQL pour récupérer l'historique des 7 derniers jours
        sql = """
            SELECT user_id, track_id, COUNT(*) as play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id;
        """
        
        records = pg_hook.get_records(sql)
        
        # 2. Construction du dictionnaire {user_id: {track_id: play_count}}
        raw_matrix = defaultdict(dict)
        for user_id, track_id, play_count in records:
            raw_matrix[str(user_id)][str(track_id)] = int(play_count)
            
        # 3. Filtrer : Ne garder que les utilisateurs avec >= 3 écoutes de morceaux distincts
        filtered_matrix = {
            user_id: tracks 
            for user_id, tracks in raw_matrix.items() 
            if len(tracks) >= 3
        }
        
        logging.info(f"Matrice construite : {len(filtered_matrix)} utilisateurs actifs retenus.")
        return filtered_matrix 

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus customisée.
        """
        if not matrix_data:
            logging.warning("Matrice d'écoutes vide. Impossible de générer des recommandations.")
            return {}

        # Fonction interne pour calculer la similarité cosinus entre deux utilisateurs
        def calculate_cosine(user_a_tracks, user_b_tracks):
            intersecting_tracks = set(user_a_tracks.keys()) & set(user_b_tracks.keys())
            if not intersecting_tracks:
                return 0.0
            
            dot_product = sum(user_a_tracks[t] * user_b_tracks[t] for t in intersecting_tracks)
            norm_a = math.sqrt(sum(v**2 for v in user_a_tracks.values()))
            norm_b = math.sqrt(sum(v**2 for v in user_b_tracks.values()))
            
            return dot_product / (norm_a * norm_b) if (norm_a * norm_b) > 0 else 0.0

        recommendations = {}
        users = list(matrix_data.keys())

        # Calcul des recommandations pour chaque utilisateur actif
        for target_user in users:
            target_tracks = matrix_data[target_user]
            scores = defaultdict(float)
            
            # Comparaison avec tous les autres utilisateurs (voisins)
            for neighbor_user in users:
                if target_user == neighbor_user:
                    continue
                
                sim = calculate_cosine(target_tracks, matrix_data[neighbor_user])
                if sim <= 0.1: # On ignore les profils trop éloignés
                    continue
                
                # Proposer les morceaux du voisin que la cible n'a pas encore écoutés
                for track_id, play_count in matrix_data[neighbor_user].items():
                    if track_id not in target_tracks:
                        scores[track_id] += sim * play_count

            # Tri et extraction du Top 10 des morceaux recommandés
            sorted_tracks = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:TOP_N_RECO]
            if sorted_tracks:
                recommendations[target_user] = [track_id for track_id, score in sorted_tracks]

        logging.info(f"Recommandations calculées pour {len(recommendations)} utilisateurs.")
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.
        """
        if not recommendations:
            logging.info("Aucune recommandation à stocker.")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # 1. Connexion à Redis
        r_client = redis.Redis.from_url(REDIS_URL)
        
        # 2. Connexion à PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        total_recos_inserted = 0
        formatted_rows = []
        generated_at = datetime.now()

        for user_id, track_ids in recommendations.items():
            # Stockage Redis (Clé avec TTL de 24h au format JSON)
            redis_key = f"reco:{user_id}"
            r_client.setex(redis_key, RECO_TTL_SECONDS, json.dumps(track_ids))
            
            # Préparation pour le batch PostgreSQL
            for rank, track_id in enumerate(track_ids, start=1):
                # Le score est calculé selon le rang d'affichage (ex: 10 pour le top 1, 1 pour le top 10)
                score = float(11 - rank) 
                formatted_rows.append((user_id, track_id, score, generated_at))
                total_recos_inserted += 1

        # 3. UPSERT de masse dans PostgreSQL
        if formatted_rows:
            upsert_sql = """
                INSERT INTO recommendations (user_id, track_id, score, generated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, track_id) 
                DO UPDATE SET 
                    score = EXCLUDED.score, 
                    generated_at = EXCLUDED.generated_at;
            """
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.executemany(upsert_sql, formatted_rows)
            conn.commit()
            cursor.close()
            conn.close()

        logging.info(f"Recommandations sauvegardées : {len(recommendations)} utilisateurs mis à jour.")
        return {
            "users_with_recos": len(recommendations), 
            "total_recommendations": total_recos_inserted 

        }

    # ── Orchestration ─────────────────────────────────────────
    matrix        = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
