# Project 1 — Real-Time Fraud Detection Lakehouse

> Batch + Streaming + Machine Learning + Open Table Format (Iceberg) + Orchestration + CDC

This repo is a **complete, local, production-style data platform** that ingests live
credit-card transactions, enriches them, computes rolling features, stores everything
in an **Apache Iceberg lakehouse on MinIO (S3)**, trains a fraud model nightly with
**Airflow + Spark MLlib**, and scores transactions **in real time** back into the stream.

It also includes **CDC (Change Data Capture) with Debezium** so that changes in the
bank's customer/merchant database flow into the lakehouse dimensions automatically.

Everything runs on your own machine with **Docker Compose**. No cloud account needed.

---

## 1. What this project does (plain English)

Imagine you work at a bank:

1. Every second, people swipe credit cards. Those swipes are **transactions**.
2. A generator (**Apache NiFi**) pretends to be the outside world and fires fake
   transactions into the platform, in JSON format.
3. A message bus (**Apache Kafka**) catches them safely, so nothing is lost.
4. A streaming engine (**Apache Spark Structured Streaming**) reads them continuously,
   cleans them, computes rolling statistics (e.g. "how much did this card spend in the
   last hour?", "how many transactions in the last 5 minutes?"), joins them with
   customer data, and **scores each one with a machine-learning model**:
   *"how likely is this transaction to be fraud?"*
5. Everything lands in **MinIO** (a local S3) as **Apache Iceberg tables** — a modern
   "lakehouse" format that gives you ACID, time travel, and schema evolution on files.
6. A separate "bank core database" (PostgreSQL) holds customers and merchants.
   **Debezium (CDC)** watches every INSERT/UPDATE/DELETE there and streams those
   changes into Kafka, and Spark keeps the lakehouse dimension tables always fresh.
7. Every night, **Apache Airflow** kicks off a batch Spark job that re-reads all
   historical transactions from Iceberg and re-trains the fraud model
   (RandomForest in Spark MLlib), saving the model artifact back to MinIO.
8. Predictions are pushed to **PostgreSQL** (queryable by analysts) and **Redis**
   (latest risk score per card, for millisecond lookups).

## 2. Architecture

```
                        +----------------------- CDC (Debezium) ---------------------+
                        |                                                          |
[ Bank core DB ]        |   [ NiFi: fake card swipes ]                             |
  postgres-source  -----+          |                                               |
  (customers,          |           v  JSON                                           v
   merchants)    WAL -->   [ Kafka: raw_transactions ]          [ Kafka: cdc.public.* ]
                        |           |                                               |
                        |           v  Spark Structured Streaming                   v
                        |   [ Spark job1: parse -> watermark -> windowed features    |
                        |       -> join dims -> ML score -> write everywhere ]       |
                        |           |                                  [ Spark job3:
                        |           v                                    dims MERGE ]
                        |   [ MinIO Iceberg tables ]  <------------------------------+
                        |     raw.transactions   features.txn_features   dims.customers
                        |           |
                        |           v  nightly, orchestrated
                        |   [ Airflow DAG -> Spark job2: train RandomForest ]
                        |           |
                        |           v  model artifact
                        |   [ MinIO: models/fraud_rf/... ]  ---> loaded by job1 (streaming)
                        |
[ Predictions ] <-- job1 writes --> [ Postgres fraud_ops.predictions ] + [ Redis latest score ]
```

## 3. What you will learn (checklist mapped to code)

| Concept | Where it happens |
|---|---|
| Batch ETL | `spark_jobs/batch/job2_train_model.py` + Airflow DAG |
| Streaming ETL | `spark_jobs/streaming/job1_streaming_features.py` |
| Orchestration | `airflow/dags/` (training + Iceberg maintenance) |
| No-code ingestion | `nifi/` (NiFi flow: GenerateFlowFile → PublishKafka) |
| Lakehouse / open table format | Iceberg tables on MinIO (`s3a://warehouse/iceberg`) |
| Feature store | `lakehouse.features.*` Iceberg tables + Redis online store |
| ML training & serving | Spark MLlib training (batch) + `foreachBatch` scoring (stream) |
| CDC | Debezium connector + `spark_jobs/streaming/job3_cdc_customers_sync.py` |
| Data versioning & time travel | Iceberg snapshots (demoed in docs + maintenance DAG) |
| Infrastructure as Code | `docker-compose.yml`, `docker/` images, `Makefile` |
| CI / DevOps | `.github/workflows/ci.yml` |

## 4. Repo layout (what lives where — details in docs/01)

```
project1_fraud/
├── README.md                      <- you are here
├── LICENSE
├── .gitignore                     <- files git must never upload (secrets, caches)
├── .env.example                   <- template for all passwords/settings (copy to .env)
├── Makefile                       <- shortcuts: `make up`, `make stream`, `make train`...
├── docker-compose.yml             <- THE WHOLE PLATFORM described as code
│
├── docs/                          <- read these in order, they assume zero knowledge
│   ├── 00_start_here.md           <- what Docker/git/Kafka even ARE + installs
│   ├── 01_architecture.md         <- every service explained + port map
│   ├── 02_github_guide.md         <- git & GitHub from zero: first push walkthrough
│   └── 03_cdc_explained.md        <- what CDC is and how Debezium fits in here
|   └── 04_step3_spark_streaming.md <- Kafka → watermark → features → Iceberg runbook
│
├── docker/                        <- custom Docker images we build ourselves
│   ├── spark/Dockerfile           <- Spark 3.5 + Iceberg + Kafka + S3 + JDBC jars
│   ├── airflow/Dockerfile         <- Airflow 2.8 + spark-submit client
│   ├── postgres/init/             <- creates airflow_meta + fraud_ops databases
│   └── postgres-source/init/      <- the "bank" DB schema + seed data + CDC publication
│
├── nifi/
│   ├── scripts/transaction_generator.groovy   <- fake transaction JSON generator
│   └── flow/README.md             <- how we wire the NiFi flow (Step 2)
│
├── kafka/scripts/create_topics.sh             <- creates all Kafka topics
│
├── cdc/                           <- Change Data Capture (the extra you asked about)
│   ├── debezium/customers-connector.json      <- Debezium connector config
│   └── register_connector.sh                  <- one command to activate CDC
│
├── spark_jobs/                    <- all PySpark code (the heart of the project)
│   ├── common/config.py           <- ONE place for all endpoints/credentials
│   ├── common/schemas.py          <- data contracts (transaction schema, CDC schema)
│   ├── streaming/job1_streaming_features.py   <- main streaming pipeline (Step 3)
│   ├── streaming/job3_cdc_customers_sync.py   <- CDC -> Iceberg dims MERGE (Step 4)
│   └── batch/job2_train_model.py              <- nightly model training (Step 5)
│
├── airflow/dags/                  <- orchestration
│   ├── dag_01_daily_training.py               <- retrains the model every night
│   └── dag_02_iceberg_maintenance.py          <- snapshot expiry / housekeeping
│
├── scripts/
│   └── seed_customers.py          <- loads/mutates bank DB rows (triggers CDC!)
│
├── ml/MODEL_CARD.md               <- documents the model like a real ML team would
└── .github/workflows/ci.yml       <- free automated checks on every git push
```

## 5. Prerequisites

- A laptop with **8 GB RAM free** (16 GB total is comfortable).
- **Docker Desktop** (Windows/macOS) or **Docker Engine + Compose plugin** (Linux).
- **git** installed, a free **GitHub** account.
- Step-by-step installs for every OS: **docs/00_start_here.md**.

## 6. Quickstart (works fully once all steps are built — we do it together)

```bash
cp .env.example .env          # one-time: create your local settings file
make up                       # build images + start the whole platform
make topics                   # create Kafka topics
make register-cdc             # turn on Debezium CDC
make stream                   # start the main Spark streaming job
make cdc-sync                 # start the CDC dimension sync job
# open the UIs (all with logins in .env):
#   NiFi    http://localhost:8090/nifi
#   Spark   http://localhost:8080
#   MinIO   http://localhost:9001
#   Airflow http://localhost:8085
make train                    # (or wait for the nightly Airflow DAG)
```

Then read **docs/01_architecture.md** while it runs and click around each UI.

## 7. Build order (how we will proceed, step by step)

1. **Step 1 — Platform up**: install Docker/git, `docker compose up`, verify every UI.
2. **Step 2 — Ingestion**: NiFi flow + Groovy generator → Kafka `raw_transactions`.
3. **Step 3 — Streaming core**: job1 → Iceberg raw + feature tables (watermarks, windows).
4. **Step 4 — CDC**: Debezium on the bank DB → job3 MERGE into Iceberg dimensions.
5. **Step 5 — Training**: job2 (RandomForest) + Airflow DAG scheduling.
6. **Step 6 — Real-time inference**: load model in job1, write predictions → Postgres + Redis.
7. **Step 7 — Lakehouse mastery**: time-travel queries, schema evolution, maintenance DAG.
8. **Step 8 — Ship it**: push to GitHub, CI checks green, write-up.

Each step builds on the previous one and explains *why* before *how*.

## 8. FAQ (newbie edition)

- **Saw `pull access denied for minio/minio`?** MinIO stopped publishing free
  community images to Docker Hub in October 2025. This repo uses maintained,
  public **Chainguard MinIO** server/client images from `cgr.dev`, pinned by
  immutable multi-architecture digests. Docker Hub login cannot fix the old URL.
- **Saw an error about `bitnami/spark` not found?** That was fixed in the repo —
  Bitnami removed their free versioned images from Docker Hub in 2025, so we use
  the **official `apache/spark` image** (see `docker/spark/Dockerfile`). Always
  `git pull` the latest code before building.
- **Is this a real bank system?** No — it's a realistic *simulation* you run locally,
  using the exact same tools real data teams use in production.
- **Why so many services?** Because real platforms separate concerns: ingestion (NiFi),
  transport (Kafka), compute (Spark), storage (MinIO/Iceberg), orchestration (Airflow),
  ops data (Postgres/Redis). Small machines, each doing one job well.
- **Can I break things?** Yes, and you should. `make down && make up` resets the world.
- **Where is the data, physically?** In Docker named volumes (`minio_data`, `pg_ops_data`,
  `pg_source_data`) on your disk. Nothing leaves your machine.
