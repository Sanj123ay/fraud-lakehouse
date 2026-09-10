# 01 — Architecture Explained (service by service)

## The moving parts

| Container | Tool | Its one job | You touch it at |
|---|---|---|---|
| `kafka` | Kafka 7.6 (KRaft) | Hold streams of messages safely | topics via CLI |
| `connect` | Debezium 2.5 | CDC: bank DB changes -> Kafka | REST API :8083 |
| `nifi` | NiFi 1.25 | Generate fake transactions -> Kafka | UI :8090 |
| `minio` | MinIO | S3 storage for Iceberg tables + models | UI :9001 |
| `createbuckets` | mc | One-shot: create S3 buckets, then exits | — |
| `spark-master` | Spark 3.5 + jars | Accept jobs, split work | UI :8080 |
| `spark-worker` | Spark 3.5 + jars | Actually crunch the data | — |
| `postgres` | Postgres 15 | Airflow metadata + `fraud_ops.predictions` | port 5432 |
| `postgres-source` | Postgres 15 | Pretend bank core DB (CDC source) | port 5433 |
| `airflow` | Airflow 2.8 | Schedule nightly training + maintenance | UI :8085 |
| `redis` | Redis 7 | Online store: card -> latest fraud score | port 6379 |

## Port cheat sheet (open in your browser)

| URL | What | Login |
|---|---|---|
| http://localhost:8090/nifi | NiFi canvas | anonymous (local mode) |
| http://localhost:8080 | Spark master UI | — |
| http://localhost:9001 | MinIO console | see `.env` (minioadmin / minioadmin123) |
| http://localhost:8085 | Airflow | admin / admin (from `.env`) |

## The life of one transaction (follow a single swipe)

1. **Born in NiFi** — a Groovy script inside a `GenerateFlowFile` processor
   fabricates a JSON transaction every ~second:
   `txn_id, card_id, customer_id, merchant_id, amount, country, txn_ts, is_fraud(label)`.
2. **Kafka** — NiFi's `PublishKafka` processor drops it onto topic
   `raw_transactions`. Kafka durably stores it. Even if Spark crashes for an
   hour, the data waits.
3. **Spark job1** reads the topic continuously (10-second micro-batches):
   - parses JSON into typed columns using `common/schemas.py`
   - sets an **event-time watermark** (10 minutes late tolerance)
   - computes **rolling features**: `count_5min`, `amount_sum_1h` per card
   - **broadcast-joins** with Iceberg dims `customers` + `merchants`
   - loads the latest ML model from MinIO and **scores** the batch
4. **Written everywhere it matters**:
   - Iceberg `lakehouse.raw.transactions` — full enriched + scored record (history)
   - Iceberg `lakehouse.features.txn_features_1h` — the offline feature store
   - Postgres `fraud_ops.predictions` — analysts query this
   - Redis — `fraud:card:<card_id>` = latest score (online feature store)
5. **Meanwhile, CDC**: someone edits a customer in the bank DB
   (`make psql-source`, run an UPDATE) → Debezium sees it in the WAL →
   event lands in Kafka topic `cdc.public.customers` → Spark job3
   **MERGEs** it into Iceberg `lakehouse.dims.customers` → job1's next join
   already uses the fresh data. No re-export, no nightly bulk sync.
6. **Every night, Airflow** runs `dag_01_daily_training`:
   Spark job2 re-reads history from Iceberg, trains a new RandomForest,
   evaluates it, and if it beats the current model, publishes it to
   `s3a://models/fraud_rf/latest`. The streaming job picks it up.
7. **Iceberg maintenance DAG** expires old snapshots so MinIO doesn't grow
   forever, and demos **time travel** (`SELECT ... TIMESTAMP AS OF`).

## Buckets & tables (what lives in MinIO)

```
s3a://warehouse/iceberg/
├── lakehouse/raw/transactions          <- every enriched transaction + score
├── lakehouse/features/txn_features_1h  <- offline feature store
├── lakehouse/dims/customers            <- CDC-synced dimension
└── lakehouse/dims/merchants            <- CDC-synced dimension
s3a://models/fraud_rf/latest            <- current champion model
s3a://checkpoints/job1, job3/           <- streaming fault-tolerance state
```

## Why each choice (one line each)

- **Kafka, not direct NiFi→Spark**: decoupling. Producers and consumers never block each other; replay is possible.
- **Iceberg, not parquet folders**: ACID MERGE for CDC, time travel, safe concurrent writes, schema evolution.
- **MinIO, not real S3**: same API, zero cost, runs offline.
- **Spark MLlib, not sklearn**: trains distributed on the same cluster; model artifacts loadable inside the streaming job.
- **Postgres + Redis for predictions**: Postgres = queryable history; Redis = the hot "latest risk per card" cache a real checkout API would hit.
- **Debezium for dims**: real production pattern — lakehouse dimensions stay in sync with the OLTP system in seconds instead of nightly dumps.
