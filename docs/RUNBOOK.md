# RUNBOOK SPOTIFY — Procédures incidents

> Ce document doit être complété par votre groupe au fur et à mesure de la semaine.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.

---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en "running" depuis > 30 minutes

**Symptômes :** Une tâche reste en état `running` dans l'UI Airflow.

**Diagnostic :**
```bash
# Voir les logs de la tâche
docker compose logs airflow-worker -f

# Lister les tâches actives
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

**Résolution :**
```bash
# Marquer la tâche comme failed manuellement
docker exec airflow-scheduler airflow tasks clear <dag_id> -t <task_id> --yes

# Ou tuer le worker et le relancer
docker compose restart airflow-worker
```

**Cause probable :** Cause probable : Désynchronisation temporelle sur l'opérateur ExternalTaskSensor (dans aggregation_pipeline). Le capteur cherche une exécution réussie du pipeline streaming_events_pipeline à une heure de calendrier strictement identique. Si l'un des DAGs a été déclenché manuellement (execution_date différente), le capteur tourne dans le vide en attendant un run qui n'existe pas.

INC-01B — Erreur SQL : Conflit de types UUID vs TEXT
Symptômes : Les tâches d'insertion échouent brutalement avec l'erreur :
psycopg2.errors.UndefinedFunction: operator does not exist: uuid = text

Résolution :
Ajouter un transtypage explicite (Type Casting) dans la requête SQL du DAG pour forcer PostgreSQL à comparer des chaînes de caractères : WHERE track_id::text = %s
Relancer ensuite le scheduler pour vider le cache : docker compose restart airflow-scheduler

---

### INC-02 — PostgreSQL : `too many connections`

**Symptômes :** Les tâches Airflow échouent avec `FATAL: too many connections`.

**Diagnostic :**
```sql
SELECT count(*), state FROM pg_stat_activity GROUP BY state;
SELECT max_conn FROM pg_settings WHERE name='max_connections';
```

**Résolution :**
```bash
# Court terme : killer les connexions idle pour libérer de la place
# Ouvrir psql : docker compose exec postgres psql -U airflow -d spotify
# SQL : SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';

**Prévention :** Augmenter max_connections à 200 dans les variables d'environnement du service postgres dans le docker-compose.yml et configurer un Pool Airflow limité à 10 slots dans l'UI Airflow pour brider l'ORM.

---

### INC-03 — MinIO inaccessible depuis Airflow

**Symptômes :** Les tâches de lecture/écriture Parquet échouent avec `Connection refused`.

**Diagnostic :**
```bash
docker compose ps minio
curl http://localhost:9000/minio/health/live
```

**Résolution :**
```bash
docker compose restart minio
# Attendre 10s puis relancer la tâche en échec sur Airflow
```
### INC-03B — Saturation de la base Dead Letter Queue (DLQ)
Symptômes : La table dead_letter_events accumule des milliers de lignes de bugs et ralentit Postgres.

Résolution :

Corriger la cause racine dans le simulateur P2P (champs manquants ou durées négatives).

Déclencher manuellement le DAG de secours dlq_reprocessing_pipeline depuis l'UI Airflow pour nettoyer, corriger et réinjecter automatiquement les messages vers le circuit normal.

---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec minio mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spark-master | grep "checkpoint"
```

**Résolution :**
→ À compléter par votre groupe

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...
