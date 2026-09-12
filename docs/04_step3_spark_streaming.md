# Step 3 — Spark Structured Streaming → Iceberg feature store
This step connects Spark to the live Kafka topic from Step 2. Spark runs forever
in 10-second micro-batches, validates each message, stores clean history in
Iceberg, and calculates rolling card features.
## What is now implemented
```text
Kafka: raw_transactions
        |
        v
Spark: parse JSON against shared schema
        |
        +---- invalid ---> Iceberg raw.transaction_quarantine
        |
        v
valid + event-time watermark (10 min) + txn-id deduplication
        |
        v
foreachBatch every 10 sec
        |
        +--> left join Iceberg customer/merchant dimensions
        +--> MERGE raw.transactions               (immutable, no duplicates)
        +--> count_5min per card                  (includes current txn)
        +--> amount_sum_1h per card               (includes current txn)
        +--> MERGE features.txn_features_1h       (late-data corrections)
Checkpoints: s3a://checkpoints/job1/{main,quarantine}
Tables:      s3a://warehouse/iceberg/...
```
The customer and merchant dimension tables are created now but are empty until
Step 4 starts Debezium CDC. Therefore enrichment fields such as
`customer_risk_segment` are initially null. This is expected—not an error.
---
## 1. Synchronize the Step 3 files
Update your local repo before running anything:
```bash
git pull
```
Important changed files:
- `spark_jobs/streaming/job1_streaming_features.py`
- `spark_jobs/common/config.py`
- `spark_jobs/tools/inspect_step3.py`
- `Makefile`
- `.env.example`
If your `.env` predates Step 3, you do **not** need to replace it. Every new
setting has a default. You may add these if you want them visible:
```dotenv
KAFKA_STARTING_OFFSETS=earliest
KAFKA_MAX_OFFSETS_PER_TRIGGER=5000
STREAM_TRIGGER=10 seconds
EVENT_WATERMARK=10 minutes
FEATURE_VERSION=v1
```
---
## 2. Pre-flight check
From `project1_fraud`:
```bash
docker compose ps
```
Required services should be `Up`:
- `kafka`
- `nifi`
- `minio`
- `spark-master`
- `spark-worker`
`createbuckets` showing `Exited (0)` is normal.
Confirm Step 2 is still producing valid data:
```bash
make verify-ingestion
```
Expected:
```text
PASS: validated 10 schema-compatible Kafka transactions.
```
Confirm the worker is visible at **http://localhost:8080**.
---
## 3. Start the streaming job
Use a dedicated terminal and leave it open:
```bash
make stream
```
Without `make`:
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  /opt/spark/jobs/streaming/job1_streaming_features.py
```
The job intentionally uses one of the worker's two cores. The other remains
available for `make inspect-step3` and, later, Airflow/CDC work.
Expected startup summary:
```text
Fraud transaction stream is running.
  Kafka source : kafka:29092/raw_transactions
  Raw table    : lakehouse.raw.transactions
  Feature table: lakehouse.features.txn_features_1h
  Quarantine   : lakehouse.raw.transaction_quarantine
  Trigger      : 10 seconds
  Watermark    : 10 minutes
```
Then every non-empty micro-batch prints something similar to:
```text
[batch 3] raw input=10, feature rows inserted/recomputed=12
```
The feature count can exceed the raw input count. A late event legitimately
changes the rolling features of later transactions, so those rows are
recomputed and upserted.
> The first batch may be much larger because `startingOffsets=earliest` replays
> transactions NiFi generated before Spark started. This option matters only
> when a checkpoint is first created; after that, the checkpoint decides where
> to resume.
---
## 4. Watch Spark process it
Open **http://localhost:8080**.
Under **Running Applications**, you should see `fraud-streaming-features`.
Click it to inspect jobs, stages, executor memory, and task durations.
A Structured Streaming query is still a series of small Spark batch jobs. Every
10 seconds, the next set of Kafka offsets becomes one micro-batch.
---
## 5. Inspect the Iceberg tables
Open a second terminal in `project1_fraud` while streaming continues:
```bash
make inspect-step3
```
Expected report:
```text
valid raw transactions      : greater than 0
offline feature rows        : greater than 0
quarantined messages        : usually 0
customer dimension rows     : 0 until Step 4
merchant dimension rows     : 0 until Step 4
```
It also prints ten latest features and Iceberg snapshot IDs.
Sanity rules:
- `count_5min >= 1` because the current transaction counts itself.
- `amount_sum_1h >= amount` because the current amount is included.
- Counts/sums rise when the same card transacts repeatedly.
- Raw and feature row counts are normally equal after all corrections settle.
Open MinIO at **http://localhost:9001**, then browse:
```text
warehouse / iceberg / raw / transactions
warehouse / iceberg / raw / transaction_quarantine
warehouse / iceberg / features / txn_features_1h
warehouse / iceberg / dims / customers
warehouse / iceberg / dims / merchants
checkpoints / job1 / main
checkpoints / job1 / quarantine
```
Inside each Iceberg table, `metadata/` contains snapshots/manifests and `data/`
contains compressed Parquet files. Do not edit these objects manually.
---
## 6. Prove quarantine works
Publish one deliberately bad message from a second terminal:
```bash
printf '%s\n' '{"txn_id":"not-a-uuid","amount":-10,"channel":"carrier-pigeon"}' \
  | docker compose exec -T kafka kafka-console-producer \
      --bootstrap-server kafka:29092 \
      --topic raw_transactions
```
Wait 10–20 seconds, then:
```bash
make inspect-step3
```
`quarantined messages` should be at least 1, and the report shows validation
errors. The bad row must **not** appear in `raw.transactions` or the feature
store. This is the data-quality boundary.
---
## 7. What the watermark actually does
A transaction has two notions of time:
- **event time** (`txn_ts`): when the card swipe occurred;
- **processing time**: when Spark happened to receive/process it.
Networks retry, devices go offline, and messages arrive late. The job uses:
```python
withWatermark("event_time", "10 minutes")
dropDuplicatesWithinWatermark(["txn_id"])
```
Spark remembers transaction IDs in its state store long enough to suppress
normal retries, but eventually removes old IDs so memory does not grow forever.
Our NiFi generator intentionally makes some events 1–8 minutes late and a tiny
number over 10 minutes late so this behavior is observable.
The Iceberg `MERGE` is a second idempotency layer: even if Spark crashes after
writing Iceberg but before committing its checkpoint, replaying that micro-batch
cannot create duplicate `txn_id` rows.
---
## 8. How rolling features are calculated
For every transaction T on a card:
- `count_5min`: count of this card's events in `[T - 5 minutes, T]`;
- `amount_sum_1h`: amount sum for this card in `[T - 1 hour, T]`.
Spark uses event time converted to epoch seconds and SQL range windows. This is
not merely a micro-batch total: each calculation reads the required one-hour
history from Iceberg.
When a late event at time T arrives, it may alter already-stored later features.
The job recomputes the affected horizon through `T + 1 hour` and Iceberg
`MERGE`s corrected rows. This is why the feature table is an upsert table while
raw history remains immutable.
---
## 9. Stop and restart safely
In the streaming terminal, press **Ctrl+C**. The query stops cleanly.
Start again:
```bash
make stream
```
It resumes from the Kafka offsets and deduplication state stored under
`s3a://checkpoints/job1`. It does not replay the whole topic.
Never delete checkpoints for a production stream. For this local learning
project, delete them only when intentionally starting a brand-new query after an
incompatible schema/query change. Deleting a checkpoint causes Kafka replay;
Iceberg MERGE still protects raw `txn_id` uniqueness.
---
## 10. Troubleshooting
### `Failed to find data source: kafka`
The custom Spark image was not rebuilt with its Kafka JARs:
```bash
docker compose build --no-cache spark-master spark-worker
docker compose up -d spark-master spark-worker
```
### `ClassNotFoundException: S3AFileSystem`
The custom image is missing/reusing an old Hadoop AWS layer. Use the same
no-cache rebuild command above.
### `Connection refused: minio:9000`
```bash
docker compose ps minio
docker compose logs --tail=100 minio
docker compose up -d minio createbuckets
```
### `UnknownHostException: kafka` or Kafka timeout
```bash
docker compose ps kafka
docker compose exec spark-master getent hosts kafka
docker compose logs --tail=100 kafka
```
### No records enter Spark
- Confirm all NiFi processors are green.
- Run `make consume-raw`.
- Check that job startup says topic `raw_transactions`.
- Remember: existing checkpoints override `KAFKA_STARTING_OFFSETS`.
### `Table already exists` with an incompatible schema
This should not happen on a fresh Step 3 run. Do not manually delete only part
of an Iceberg table. If this is disposable local data and you intentionally want
a total reset:
```bash
# WARNING: deletes every Postgres/MinIO/NiFi volume in this project.
make destroy
make up
make topics
```
You must recreate the NiFi canvas after a total volume reset.
### Stream terminal exits unexpectedly
Read the **first `Caused by:`** line, not only the final stack trace:
```bash
docker compose logs --tail=150 spark-master
docker compose logs --tail=150 spark-worker
```
The driver itself runs inside `docker compose exec`, so its most useful error is
usually printed directly in the terminal where `make stream` ran.
---
## Step 3 completion checklist
- [ ] `make stream` stays running.
- [ ] Spark UI lists `fraud-streaming-features`.
- [ ] Micro-batch logs appear about every 10 seconds.
- [ ] `make inspect-step3` reports raw and feature rows greater than zero.
- [ ] Latest features satisfy count/sum sanity rules.
- [ ] Deliberately malformed JSON lands only in quarantine.
- [ ] MinIO contains Iceberg `metadata/` and `data/` objects.
- [ ] Stop/restart resumes from checkpoint without duplicate raw transaction IDs.
When all are true, Step 3 is complete. Step 4 will activate Debezium CDC and
populate the two dimension tables that this job already refreshes and joins on
every micro-batch.