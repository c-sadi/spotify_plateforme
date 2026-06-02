"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement (Statut 'pending').
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        sql = """
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %(max_retries)s
            ORDER BY created_at ASC
            LIMIT %(batch_size)s;
        """
        
        records = pg_hook.get_records(sql, parameters={"max_retries": MAX_RETRIES, "batch_size": BATCH_SIZE})
        logging.info(f" {len(records)} événements pending trouvés dans la DLQ.")
        return records

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        """
        if not pending_events:
            logging.info("Aucun événement à retraiter.")
            return {"reprocessed": [], "failed": []}
            
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        reprocessed = []
        failed = []
        
        for record in pending_events:
            event_id, payload_raw, error_type, retry_count, original_topic = record
            
            try:
                # 1. Parsing du payload JSON (sécurité si le type en base est du texte ou du JSON brut)
                if isinstance(payload_raw, str):
                    payload = json.loads(payload_raw)
                else:
                    payload = payload_raw
                
                # 2. RÈGLE 1 : user_id manquant -> impossible à réparer -> échec / abandonné
                if not payload.get("user_id"):
                    logging.warning(f" Événement {event_id} rejeté d'office : 'user_id' absent.")
                    failed.append(event_id)
                    continue
                
                # 3. RÈGLE 2 : timestamp invalide ou absent -> Utilisation de l'heure actuelle comme fallback
                if not payload.get("timestamp"):
                    payload["timestamp"] = datetime.utcnow().isoformat()
                    logging.info(f" Événement {event_id} réparé : insertion d'un timestamp de secours.")
                
                # 4. RÈGLE 3 : track_id inconnu -> On va vérifier s'il existe dans la table 'tracks'
                track_id = payload.get("track_id")
                if track_id:
                    track_check = pg_hook.get_first("SELECT 1 FROM tracks WHERE id = %s;", parameters=(track_id,))
                    if not track_check:
                        logging.warning(f" Événement {event_id} rejeté : track_id '{track_id}' introuvable dans le catalogue.")
                        failed.append(event_id)
                        continue
                else:
                    failed.append(event_id)
                    continue
                
                # Si l'événement passe toutes les étapes de nettoyage, il passe en succès
                reprocessed.append({"id": event_id, "payload": payload})
                
            except Exception as e:
                logging.error(f" Erreur critique lors du parsing de l'événement {event_id}: {str(e)}")
                failed.append(event_id)
                
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements traités et injecte les succès dans PostgreSQL.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])
        
        # 1. Gestion des événements corrigés avec succès
        for item in reprocessed:
            event_id = item["id"]
            p = item["payload"]
            
            # Injection propre dans la table cible finalisée 'listening_events'
            insert_sql = """
                INSERT INTO listening_events (user_id, track_id, timestamp, completed, duration_ms, geo_country, event_source, latency_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """
            pg_hook.run(insert_sql, parameters=(
                p.get("user_id"),
                p.get("track_id"),
                p.get("timestamp"),
                p.get("completed", True),
                p.get("duration_ms", 0),
                p.get("geo_country", "FR"),
                p.get("event_source", "server"),
                p.get("latency_ms", 0)
            ))
            
            # Changement de statut dans la DLQ pour clore l'incident
            pg_hook.run(
                "UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW() WHERE id = %s;",
                parameters=(event_id,)
            )
            
        # 2. Gestion des événements en échec (Incrémentation du compteur de tentatives)
        for event_id in failed:
            update_fail_sql = """
                UPDATE dead_letter_events
                SET retry_count = retry_count + 1,
                    last_retry_at = NOW(),
                    status = CASE WHEN retry_count + 1 >= %s THEN 'abandoned' ELSE 'pending' END
                WHERE id = %s;
            """
            pg_hook.run(update_fail_sql, parameters=(MAX_RETRIES, event_id))
            
        logging.info(f" Bilan Final de la DLQ : {len(reprocessed)} retraités avec succès, {len(failed)} échoués ou abandonnés.")
        return {"reprocessed_count": len(reprocessed), "failed_count": len(failed)}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
