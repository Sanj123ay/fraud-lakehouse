# 07 — Step 6: Real-time fraud scoring

Until now the model from Step 5 only ran in a nightly batch. In this step the streaming job
loads the **champion model** and scores every new transaction within seconds of arriving.
The results go to two places:

- **PostgreSQL** `fraud_ops.predictions`: the full history of scores, for analysts.
- **Redis** `fraud:card:<card_id>`: the latest score per card, for lookups in milliseconds.

---

## 1. The big picture

```text
Kafka raw_transactions
   │  (10-second micro-batch)
   ▼
parse → validate → enrich with CDC dimensions
   │
   ├─► MERGE raw.transactions           (Iceberg, history)
   ├─► MERGE features.txn_features_1h   (Iceberg, rolling features: count_5min, amount_sum_1h)
   │
   ├─► SCORE with champion model        ◄── loaded from s3a://models/fraud_rf/latest
   │       fraud_score = P(fraud), 0..1
   │       is_fraud_pred = fraud_score >= 0.5
   │
   ├─► UPDATE raw.* score columns       (Iceberg, only rows not scored yet)
   ├─► INSERT fraud_ops.predictions     (Postgres, ON CONFLICT DO NOTHING)
   └─► SET fraud:card:<card_id>         (Redis, newest event per card wins)
```

### Words you need

| Word | Meaning |
|---|---|
| **Scoring / inference** | Running a trained model on a new example to get a prediction. |
| **Fraud score** | A number from 0 to 1. The model's estimate that a transaction is fraud. |
| **Decision threshold** | The score above which we flag a transaction. Here it is 0.5. |
| **Online store** | A fast key-value store (Redis) for "what is the latest value?" questions. |
| **Idempotent** | Running the same work twice gives the same final result. Replays after a crash are safe. |
| **Model cache** | Keeping the model in memory between batches instead of reloading it every 10 seconds. |

---

## 2. Files changed in this step

| File | What changed |
|---|---|
| `spark_jobs/common/model_registry.py` | **New.** Loads the champion from MinIO, caches it, and checks for a newer version every 5 minutes. |
| `spark_jobs/common/storage.py` | **New.** The small JSON read and write helpers, moved here so both training and streaming can use them. |
| `spark_jobs/streaming/job1_streaming_features.py` | Scores each batch, updates the raw table, and writes to Postgres and Redis. |
| `spark_jobs/batch/job2_train_model.py` | Imports the helpers from `common/storage.py` (no behaviour change). |
| `spark_jobs/common/config.py` | Postgres host and port, model reload interval, Redis key lifetime. |
| `docker/spark/Dockerfile` | Adds the `psycopg2` Postgres driver for the Spark workers. |
| `docs/07_step6_realtime_scoring.md` | This page. |

---

## 3. Why the scores are computed from the same features as training

The scoring code does not compute its own features. It takes the rolling features
(`count_5min`, `amount_sum_1h`) that the streaming job has just computed, and passes them
through the **saved pipeline from Step 5**. That pipeline contains the exact SQL from
`common/model_features.py`. Training and live scoring therefore use identical logic, which
avoids training/serving skew.

---

## 4. What happens to a single transaction

1. It arrives in Kafka, and within 10 seconds it is parsed and validated.
2. It is enriched with the customer's home country and the merchant's risk flag (Step 4).
3. Its rolling features are computed (Step 3).
4. The champion model returns `fraud_score`, for example `0.93`.
5. Because `0.93 >= 0.5`, `is_fraud_pred = true`.
6. The score is written to the raw table, to Postgres, and to Redis under the card's key.

---

## 5. Rules that keep the system safe

| Rule | Why |
|---|---|
| Only transactions in the current batch are scored. | Old transactions were already scored when they arrived. |
| A raw row is scored at most once (`fraud_score IS NULL` check). | A crash and replay cannot change a score that was already stored. |
| Postgres uses `ON CONFLICT DO NOTHING`. | A replayed batch cannot create duplicate predictions. |
| Redis keeps the newest `event_time` per card. | A late, out-of-order event cannot overwrite a newer score. |
| If no champion exists yet, scoring is skipped with a message. | The stream keeps running. Scores are filled in later for those rows if needed. |
| If a new champion cannot be loaded, the previous one stays in use. | A bad file in MinIO does not stop fraud scoring. |

---

## 6. Run it

### Step A: rebuild the Spark images (once, because the Dockerfile changed)

```powershell
docker compose build spark-master spark-worker
docker compose up -d spark-master spark-worker
```

### Step B: make sure a champion exists

```powershell
make inspect-model
```

You should see a `CURRENT CHAMPION` block. If not, run `make train` first (Step 5).

### Step C: start the stream

Stop the old stream (Ctrl+C in its window), then:

```powershell
make stream
```

Look for these lines after a few batches:

```text
[model] loaded champion model version 20261022T172044Z-da1bc4
[batch 12] raw input=48, feature rows inserted/recomputed=52, scored=48, ...
[batch 12] scored=48 flagged_fraud=3
```

`scored=` should match the number of new transactions in that batch.

---

## 7. Check the results

### In PostgreSQL (analysts' view)

```powershell
make psql-ops
```

Then at the `fraud_ops=#` prompt:

```sql
SELECT count(*) AS scored, sum(is_fraud_pred::int) AS flagged FROM predictions;

SELECT txn_id, card_id, amount, fraud_score, is_fraud_pred, model_version
FROM predictions
ORDER BY scored_at DESC
LIMIT 10;

\q
```

### In Redis (online store)

```powershell
docker compose exec redis redis-cli --scan --pattern "fraud:card:*" | Select-Object -First 5
docker compose exec redis redis-cli GET "fraud:card:<paste-a-card-id-here>"
```

The value is JSON with the latest `txn_id`, `fraud_score`, `is_fraud_pred`, `event_time`, and
`model_version`.

### In the raw table (Iceberg)

`make inspect-step3` now shows the score columns filled in for new rows.

---

## 8. Settings

| Setting | Default | Meaning |
|---|---|---|
| `FRAUD_DECISION_THRESHOLD` | 0.5 | Score at or above which a transaction is flagged. |
| `MODEL_RELOAD_SECONDS` | 300 | How often the stream checks MinIO for a newer champion. |
| `REDIS_TTL_SECONDS` | 604800 (7 days) | How long a card's latest score stays in Redis. |
| `PG_HOST`, `PG_PORT`, `PG_DATABASE` | `postgres`, `5432`, `fraud_ops` | Where predictions are written. |

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[scoring] no champion model published yet` | Step 5 has not published a model. | Run `make train`. |
| `ModuleNotFoundError: pandas` or `numpy` on the cluster | The Spark image is missing a Python library. | See E-006 in `docs/TROUBLESHOOTING_LOG.md`. |
| `psycopg2` import error on a worker | The Spark image was not rebuilt after the Dockerfile change. | `docker compose build spark-master spark-worker`, then restart them. |
| `connection refused` to `postgres:5432` | Postgres is not running, or the host or credentials are wrong. | `docker compose ps`, then check `PG_*` in `.env`. |
| Redis keys are missing | Redis is not reachable from the job. | `docker compose exec redis redis-cli PING` should answer `PONG`. |
| Scores are all 0.0 or all 1.0 | The model is trivially separating the simulated data (see E-003). | Expected for now. |

---

## 10. Concepts checklist

- **Real-time inference**: a model scores data as it arrives, inside a streaming job.
- **Model registry pattern**: the streaming job always uses the champion, and reloads it when it changes.
- **Dual sinks**: the same result goes to a history store (Postgres) and an online store (Redis).
- **Idempotent writes**: safe to replay after a failure.
- **Out-of-order handling**: the newest event wins in the online store.
- **Training/serving consistency**: one feature definition, shared by both.

Next: **Step 7** adds time travel, snapshot maintenance, and the Iceberg housekeeping DAG.
