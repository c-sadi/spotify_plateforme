"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()

TODO :
    [ ] Implémenter extract_from_minio() — lire les JSONs depuis MinIO
    [ ] Implémenter validate_schema() — vérifier les champs obligatoires
    [ ] Implémenter transform_catalog() — normaliser les noms d'artistes, déduplication
    [ ] Implémenter load_to_postgres() — upsert avec gestion des conflits
    [ ] Configurer retry_delay et retries sur les tâches réseau
    [ ] Ajouter un on_failure_callback pour alerting
    [ ] Activer le doc_md sur ce DAG (voir variable DAG_DOC ci-dessous)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        """
        import boto3
        import json
        import logging

        logger = logging.getLogger("airflow.task")
        raw_catalogs = []

        # 1. Configuration de la connexion à MinIO depuis le réseau Docker
        s3_client = boto3.client('s3',
            endpoint_url='http://minio:9000', 
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )

        # 2. Pour chaque fichier dans LABEL_FILES, télécharger et parser le JSON
        for filename in LABEL_FILES:
            try:
                logger.info(f" Tentative de lecture de s3://{MINIO_BUCKET}/{filename}...")
                
                # Récupération de l'objet sans le sauvegarder sur le disque
                response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=filename)
                
                # Lecture et parsing du JSON
                file_content = response['Body'].read().decode('utf-8')
                catalog_data = json.loads(file_content)
                
                raw_catalogs.append(catalog_data)
                logger.info(f" Fichier {filename} chargé avec succès ({len(catalog_data.get('tracks', []))} tracks).")

            # 4. Si un fichier est manquant : logger un warning et continuer
            except s3_client.exceptions.NoSuchKey:
                logger.warning(f" Le fichier {filename} est introuvable dans MinIO. On continue sans lui.")
            except Exception as e:
                logger.warning(f" Erreur inattendue lors de la lecture de {filename} : {e}")

        # 3. Retourner la liste de catalogues (Airflow gère le passage via XCom)
        return raw_catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import json
        import logging

        logger = logging.getLogger("airflow.task")
        
        valid_data = {"artists": [], "albums": [], "tracks": []}
        invalid_events = []
        errors_count = 0

        # 1 & 2. Parcourir et vérifier les champs obligatoires
        for catalog in raw_catalogs:
            # Vérification des Artistes
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ("id", "name", "label")):
                    valid_data["artists"].append(artist)
                else:
                    invalid_events.append(("schema_validation", json.dumps(artist)))
                    errors_count += 1
            
            # Vérification des Albums
            for album in catalog.get("albums", []):
                if all(k in album for k in ("id", "artist_id", "title")):
                    valid_data["albums"].append(album)
                else:
                    invalid_events.append(("schema_validation", json.dumps(album)))
                    errors_count += 1

            # Vérification des Tracks
            for track in catalog.get("tracks", []):
                if all(k in track for k in ("id", "artist_id", "title", "duration_ms")):
                    valid_data["tracks"].append(track)
                else:
                    invalid_events.append(("schema_validation", json.dumps(track)))
                    errors_count += 1

        # 3. Insérer les invalides en DLQ (Dead Letter Queue)
        if invalid_events:
            logger.warning(f" {errors_count} éléments invalides détectés. Envoi en DLQ.")
            try:
                # POSTGRES_CONN_ID est défini en haut de ton fichier DAG
                pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                conn = pg_hook.get_conn()
                cursor = conn.cursor()
                cursor.executemany(
                    "INSERT INTO dead_letter_events (error_type, payload) VALUES (%s, %s)", 
                    invalid_events
                )
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(" Éléments invalides insérés dans la table dead_letter_events.")
            except Exception as e:
                logger.error(f" Erreur lors de l'insertion en DLQ : {e}")

        # 4. Retourner le dictionnaire avec les données valides
        return {"valid": valid_data, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.
        """
        valid_data = validated.get("valid", {"artists": [], "albums": [], "tracks": []})
        
        # 1. Normaliser les artistes et déduplication (on garde le dernier vu pour un même ID)
        artists_dict = {}
        for artist in valid_data.get("artists", []):
            # Normalisation : on enlève les espaces inutiles et on met des majuscules (Title Case)
            artist["name"] = artist["name"].strip().title()
            
            # Déduplication basée sur l'ID de l'artiste
            artists_dict[artist["id"]] = artist
            
        transformed_artists = list(artists_dict.values())

        # 2. Albums (pas de transformation complexe demandée, on les passe directement)
        transformed_albums = valid_data.get("albums", [])

        # 3. Valider les durées de tracks et normaliser les genres si présents
        transformed_tracks = []
        for track in valid_data.get("tracks", []):
            duration = track.get("duration_ms", 0)
            
            # Vérification de la durée (entre 0 et 3 600 000 ms, soit 1 heure)
            if 0 < duration < 3600000:
                # Si un genre est présent, on le normalise (ex: " ROCK " -> "Rock")
                if "genre" in track and isinstance(track["genre"], str):
                    track["genre"] = track["genre"].strip().title()
                    
                transformed_tracks.append(track)

        # 4. Construire et retourner le dictionnaire final
        return {
            "artists": transformed_artists,
            "albums": transformed_albums,
            "tracks": transformed_tracks,
            "errors_count": validated.get("errors_count", 0)
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        logger = logging.getLogger("airflow.task")

        # Récupération des données transformées
        artists = transformed.get("artists", [])
        albums = transformed.get("albums", [])
        tracks = transformed.get("tracks", [])
        errors_count = transformed.get("errors_count", 0)

        # 1. Utiliser PostgresHook pour obtenir une connexion
        # POSTGRES_CONN_ID est la variable définie au début de ton fichier
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # 2. Artists UPSERT
            if artists:
                artist_tuples = [(a["id"], a["name"], a["label"]) for a in artists]
                cursor.executemany("""
                    INSERT INTO artists (id, name, label) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name, label) DO UPDATE 
                    SET id = EXCLUDED.id;
                """, artist_tuples)
                logger.info(f" {len(artists)} artistes traités.")

            # 3. Albums UPSERT
            if albums:
                album_tuples = [(a["id"], a["artist_id"], a["title"]) for a in albums]
                cursor.executemany("""
                    INSERT INTO albums (id, artist_id, title) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE 
                    SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id;
                """, album_tuples)
                logger.info(f" {len(albums)} albums traités.")

            # 4. Tracks UPSERT (avec updated_at=NOW())
            if tracks:
                track_tuples = [(t["id"], t["artist_id"], t["title"], t["duration_ms"]) for t in tracks]
                cursor.executemany("""
                    INSERT INTO tracks (id, artist_id, title, duration_ms) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE 
                    SET title = EXCLUDED.title, 
                        duration_ms = EXCLUDED.duration_ms,
                        updated_at = NOW();
                """, track_tuples)
                logger.info(f" {len(tracks)} tracks traitées.")

            # Commit des transactions si tout s'est bien passé
            conn.commit()

        except Exception as e:
            # En cas d'erreur, on annule tout pour ne pas corrompre la base
            conn.rollback()
            logger.error(f" Erreur lors de l'insertion : {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

        # 5 & 6. Commit et retourner les stats + Pousser dans XCom
        stats = {
            "tracks_inserted": len(tracks),
            "artists_inserted": len(artists),
            "albums_inserted": len(albums),
            "errors_count": errors_count
        }

        # Pousser les stats spécifiques dans XCom pour le monitoring Airflow
        context["ti"].xcom_push(key="tracks_inserted", value=len(tracks))
        context["ti"].xcom_push(key="errors_count", value=errors_count)

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion et simulation d'alerte.
        """
        import logging

        logger = logging.getLogger("airflow.task")
        dag_run = context["dag_run"]
        
        # Construction d'un rapport de log bien propre et lisible
        report = f"""
         catalog_ingestion_pipeline terminé avec succès !
        ─────────────────────────────────────────────────
        DAGRun           : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Albums insérés   : {stats.get('albums_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        ─────────────────────────────────────────────────
        """
        
        # Écriture dans les logs officiels de la tâche Airflow
        logger.info(report)
        
        # Simulation du webhook Slack (comme demandé en option dans ton sujet)
        logger.info(" [Simulation Slack Webhook] Envoi du rapport au canal #spotify-pipeline-alerts...")
    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)
