"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook 

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...


"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.
        """
        # 1. Récupération de la date d'exécution incrémentale
        execution_date = context["data_interval_start"].date()
        logging.info(f"Calcul du Top Tracks pour la date : {execution_date}")
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 2. Requête SQL demandée pour extraire le Top 50
        sql = """
            SELECT track_id,
                   COUNT(*) as total_streams,
                   COUNT(DISTINCT user_id) as unique_listeners,
                   SUM(duration_ms) as total_duration_ms,
                   ARRAY_AGG(DISTINCT geo_country) as countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50;
        """
        
        # Exécution et récupération des résultats
        records = pg_hook.get_records(sql, parameters={"date": execution_date})
        logging.info(f"{len(records)} tracks récupérés pour le Top 50.")
        return records 

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        """
        execution_date = context["data_interval_start"].date()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Jointure complète pour regrouper par artiste + calcul du top_track de l'artiste via un système de sous-requête
        sql = """
            WITH base_stats AS (
                SELECT t.artist_id,
                       COUNT(le.id) as total_streams,
                       COUNT(DISTINCT le.user_id) as unique_listeners
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s AND le.completed = TRUE
                GROUP BY t.artist_id
            ),
            top_track_per_artist AS (
                SELECT DISTINCT ON (t.artist_id) 
                       t.artist_id, 
                       le.track_id
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s AND le.completed = TRUE
                GROUP BY t.artist_id, le.track_id
                ORDER BY t.artist_id, COUNT(*) DESC
            )
            SELECT b.artist_id, 
                   b.total_streams, 
                   b.unique_listeners, 
                   t.track_id as top_track_id
            FROM base_stats b
            JOIN top_track_per_artist t ON b.artist_id = t.artist_id;
        """
        
        records = pg_hook.get_records(sql, parameters={"date": execution_date})
        logging.info(f"Stats calculées pour {len(records)} artistes.")
        return records

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.
        """
        execution_date = context["data_interval_start"].date()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Extraction des KPIs globaux du réseau P2P
        sql = """
            SELECT 
                COALESCE(ROUND(COUNT(CASE WHEN event_source = 'cache' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2), 0) as cache_hit_rate,
                COALESCE(ROUND(AVG(latency_ms), 2), 0) as avg_latency_ms,
                COUNT(DISTINCT user_id) as active_peers
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s;
        """
        
        record = pg_hook.get_first(sql, parameters={"date": execution_date})
        
        metrics = {
            "date": str(execution_date),
            "cache_hit_rate": float(record[0]) if record else 0.0,
            "avg_latency_ms": float(record[1]) if record else 0.0,
            "active_peers": int(record[2]) if record else 0
        }
        
        logging.info(f"Métriques P2P calculées : {metrics}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.
        """
        execution_date = context["data_interval_start"].date()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 1. UPSERT dans la table daily_streams
        if top_tracks:
            insert_tracks_sql = """
                INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id, date) 
                DO UPDATE SET 
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries = EXCLUDED.countries;
            """
            # Préparation des paramètres pour l'insertion par lot
            formatted_tracks = [
                (row[0], execution_date, row[1], row[2], row[3], row[4]) for row in top_tracks
            ]
            pg_hook.insert_rows(table="daily_streams", rows=formatted_tracks, target_fields=["track_id", "date", "total_streams", "unique_listeners", "total_duration_ms", "countries"])
            logging.info(f"Top 50 inséré/mis à jour dans daily_streams pour le {execution_date}.")
            
            # Log de démo du meilleur track pour la console
            top_one = top_tracks[0]
            logging.info(f"🔥 Top track du jour : ID {top_one[0]} avec {top_one[1]} streams !")

        # 2. UPSERT dans la table artist_stats
        if artist_stats:
            formatted_artists = [
                (row[0], execution_date, row[1], row[2], row[3]) for row in artist_stats
            ]
            # Utilisation d'une requête SQL brute pour gérer proprement le ON CONFLICT de l'ID composé
            insert_artists_sql = """
                INSERT INTO artist_stats (artist_id, date, total_streams, unique_listeners, top_track_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id, date) 
                DO UPDATE SET 
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id = EXCLUDED.top_track_id;
            """
            # On passe par la connexion directe pour exécuter l'excutemany de l'upsert complet
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.executemany(insert_artists_sql, formatted_artists)
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"Stats artistes insérées/mises à jour dans artist_stats pour le {execution_date}.") 

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics) 
