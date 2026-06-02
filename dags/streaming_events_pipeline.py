"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import os
import redis
import uuid
import os
from io import BytesIO
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### TODO
Compléter les 5 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements Redis publiés pendant la fenêtre de 5 minutes.

        TODO :
            1. Se connecter à Redis (REDIS_URL depuis les env vars)
            2. Utiliser un pattern subscriber ou lire depuis une liste Redis
               (le simulateur publie sur les channels REDIS_CHANNELS)
            3. Accumuler tous les messages de la fenêtre temporelle
            4. Retourner {"listening": [...], "p2p_network": [...]}

        Hint : avec redis pub/sub, les messages ne sont pas persistés.
        Une alternative : le simulateur peut aussi écrire dans une Redis LIST
        (lpush) que le DAG consomme avec rpop/lrange.
        Discutez avec l'équipe Infra & P2P de la stratégie choisie.
        """
        r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

        result = {"listening": [], "p2p_network": []}

        channel_map = {
            "listening_events":  "listening",
            "p2p_network_events": "p2p_network",
        }

        for redis_key, bucket in channel_map.items():
            # Lecture + suppression atomique via pipeline
            pipe = r.pipeline()
            pipe.lrange(redis_key, 0, 49_999)      
            pipe.ltrim(redis_key, 50_000, -1)       
            raw_messages, _ = pipe.execute()
        
        parsed = []
        for msg in raw_messages:
            try:
                parsed.append(json.loads(msg))
            except json.JSONDecodeError:
                pass  

        result[bucket] = parsed

        return result

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et isole les invalides en DLQ.

        Champs obligatoires pour un listening_event :
            event_id, user_id, track_id, timestamp, duration_ms

        TODO :
            1. Parcourir raw_events["listening"] et raw_events["p2p_network"]
            2. Valider les champs obligatoires
            3. Valider les types (timestamp parseable, duration_ms > 0)
            4. Invalides → INSERT dans dead_letter_events avec error_type="validation"
            5. Retourner {"valid_listening": [...], "valid_p2p": [...], "errors": N}
        """
        REQUIRED_LISTENING = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
        REQUIRED_P2P       = {"event_id", "peer_id", "timestamp", "event_type"}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        valid_listening, valid_p2p, invalid = [], [], []

        # --- Validation listening ---
        for evt in raw_events.get("listening", []):
            missing = REQUIRED_LISTENING - evt.keys()
            if missing:
                evt["_error"] = f"Champs manquants : {missing}"
                invalid.append(evt)
                continue

            try:
                datetime.fromisoformat(str(evt["timestamp"]))
            except (ValueError, TypeError):
                evt["_error"] = f"timestamp invalide : {evt['timestamp']}"
                invalid.append(evt)
                continue
            if not isinstance(evt["duration_ms"], (int, float)) or evt["duration_ms"] <= 0:
                evt["_error"] = f"duration_ms invalide : {evt['duration_ms']}"
                invalid.append(evt)
                continue

            valid_listening.append(evt)

        # --- Validation p2p ---
        for evt in raw_events.get("p2p_network", []):
            missing = REQUIRED_P2P - evt.keys()
            if missing:
                evt["_error"] = f"Champs manquants : {missing}"
                invalid.append(evt)
                continue

            try:
                datetime.fromisoformat(str(evt["timestamp"]))
            except (ValueError, TypeError):
                evt["_error"] = f"timestamp invalide : {evt['timestamp']}"
                invalid.append(evt)
                continue

            valid_p2p.append(evt)
        
        # --- Envoi en DLQ ---
        if invalid:
            rows = [
                (str(uuid.uuid4()), json.dumps(evt), "validation", evt.get("_error", ""))
                for evt in invalid
            ]
            hook.run(
                """
                INSERT INTO dead_letter_events (id, raw_payload, error_type, error_message)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                parameters=rows,
            )
    
        return {
        "valid_listening": valid_listening,
        "valid_p2p":       valid_p2p,
        "errors":          len(invalid),
    }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.

        TODO :
            1. Charger les tracks depuis PostgreSQL (batch query par track_id)
               SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%(ids)s)
            2. Pour chaque listening_event, ajouter : genre, artist_id, track_title
            3. Les track_id inconnus → DLQ avec error_type="unknown_track"
            4. Retourner la liste des events enrichis

        Hint : faire une seule requête PostgreSQL avec IN clause plutôt qu'une par event.
        """
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        events = validated.get("valid_listening", [])

        if not events:
            return []

        # 1. Collecte des track_ids distincts
        track_ids = list({evt["track_id"] for evt in events})

        # 2. Une seule requête pour tout le batch
        rows = hook.get_records(
            "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s)",
            parameters=(track_ids,),
        )

        # 3. Dictionnaire pour lookup O(1)
        catalogue = {
            row[0]: {
                "track_title": row[1],
                "artist_id":   row[2],
                "genre":       row[3],
            }
            for row in rows
        }

        # 4. Enrichissement + détection des track_id inconnus
        enriched, unknown = [], []

        for evt in events:
            meta = catalogue.get(evt["track_id"])

            if meta is None:
                evt["_error"] = f"track_id inconnu : {evt['track_id']}"
                unknown.append(evt)
            else:
                enriched.append({**evt, **meta})  # fusion des deux dicts
    
     # 5. Inconnus → DLQ
        if unknown:
            rows_dlq = [
                (str(uuid.uuid4()), json.dumps(evt), "unknown_track", evt["_error"])
                for evt in unknown
            ]
            hook.run(
                """
                INSERT INTO dead_letter_events (id, raw_payload, error_type, error_message)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                parameters=rows_dlq,
            )

        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.

        Partitionnement : date + heure (pour la parallélisation Phase 1, seq 3.1)

        TODO :
            1. Convertir la liste d'events en DataFrame pandas
            2. Partitionner par date et heure du timestamp
            3. Écrire en Parquet sur MinIO via boto3 ou pyarrow
               Chemin : s3://spotify-parquet/listening_events/date={date}/hour={hour}/part-{run_id}.parquet
            4. Retourner le chemin du fichier écrit

        Hint : pyarrow.parquet.write_table() + boto3 pour l'upload
        """
        if not enriched_events:
            return ""

        # 1. Calcul de la partition depuis la fin de la fenêtre temporelle
        exec_dt  = context["data_interval_end"]
        date_str = exec_dt.strftime("%Y-%m-%d")
        hour_str = exec_dt.strftime("%H")
        run_id   = context["run_id"].replace(":", "-").replace("+", "-")

        # 2. DataFrame → Table PyArrow
        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        table = pa.Table.from_pandas(df, preserve_index=False)

        # 3. Sérialisation en mémoire (pas de fichier tmp sur disque)
        buf = BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        # 4. Upload sur MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )

        s3_key = (
            f"listening_events/"
            f"date={date_str}/"
            f"hour={hour_str}/"
            f"part-{run_id}.parquet"
        )

        s3.upload_fileobj(buf, os.getenv("MINIO_BUCKET", "spotify-parquet"), s3_key)

        return f"s3://spotify-parquet/{s3_key}"

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.

        TODO :
            1. Utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            2. INSERT INTO listening_events (...) VALUES ...
               ON CONFLICT (id) DO NOTHING
            3. Retourner {"inserted": N, "skipped": M}

        Hint : utiliser executemany() avec des tuples pour les performances.
        """
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO listening_events
                (id, user_id, track_id, artist_id, genre, track_title, timestamp, duration_ms, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        rows = [
            (
                evt["event_id"],
                evt["user_id"],
                evt["track_id"],
                evt.get("artist_id"),
                evt.get("genre"),
                evt.get("track_title"),
                evt["timestamp"],
                evt["duration_ms"],
                json.dumps(evt),
            )
            for evt in enriched_events
        ]

        cursor.executemany(sql, rows)
        inserted = cursor.rowcount if cursor.rowcount >= 0 else len(rows)
        conn.commit()
        cursor.close()

        return {"inserted": inserted, "skipped": len(rows) - inserted}

    @task.branch(task_id="branch_by_event_type")
    def branch_by_event_type(raw_events: dict) -> list:
        branches = []

        if raw_events.get("listening"):
            branches.append("validate_events")

        if raw_events.get("p2p_network"):
            branches.append("handle_p2p_events")

        if not branches:
            branches = ["skip_pipeline"]

        return branches


    @task(task_id="handle_p2p_events")
    def handle_p2p_events(raw_events: dict) -> None:
        # Phase 2 — stockage dédié p2p, pour l'instant on logue juste
        import logging
        log = logging.getLogger(__name__)
        count = len(raw_events.get("p2p_network", []))
        log.info("handle_p2p_events : %d events reçus, traitement Phase 2 à venir.", count)


    @task(task_id="skip_pipeline")
    def skip_pipeline() -> None:
        import logging
        logging.getLogger(__name__).info("Fenêtre vide — rien à traiter.")

    # ── Orchestration ─────────────────────────────────────────
    raw    = consume_from_redis()
    branch = branch_by_event_type(raw)

    # Branche listening
    validated = validate_events(raw)
    enriched  = enrich_events(validated)
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)

    # Branche p2p
    handle_p2p_events(raw)

    # Branche vide
    skip_pipeline()

    # Dépendances de branchement
    branch >> validated
    branch >> handle_p2p_events(raw)
    branch >> skip_pipeline()

if __name__ == "__main__":
    dag.test()