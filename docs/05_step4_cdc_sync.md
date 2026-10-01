# Step 4 — Change Data Capture (CDC) with Debezium & Iceberg ACID Merges
In this step, we activate the **Change Data Capture (CDC)** pipeline. When customer or merchant master records are added or updated in the bank's operational database (`postgres-source`), Debezium captures those row-level changes from Postgres's Write-Ahead Log (WAL) and publishes them to Kafka in real time. Spark Structured Streaming consumes the changelog and executes an ACID `MERGE INTO` on Apache Iceberg dimension tables.
---
## 1. What happens under the hood
```text
[ Bank Core Database ]  (postgres-source: shopdb)
  tables: customers, merchants
       │
       │  PostgreSQL Logical Replication (WAL stream)
       ▼
[ Debezium Connector ]  (connect:8083)
       │
       │  Emits JSON changelog events (Unwrapped with __deleted flag)
       ▼
[ Apache Kafka ]
  topics: cdc.public.customers, cdc.public.merchants
       │
       │  Spark Structured Streaming (job3)
       ▼
[ Spark CDC Sync Job ]  (job3_cdc_customers_sync.py)
       │
       │  ACID MERGE INTO (MATCHED -> UPDATE/DELETE, NOT MATCHED -> INSERT)
       ▼
[ Apache Iceberg Dimension Tables ]  (MinIO: warehouse/iceberg/dims/...)
  lakehouse.dims.customers
  lakehouse.dims.merchants
       │
       ▲  Broadcast join on each 10s microbatch
       │
[ Spark Transaction Stream ]  (job1_streaming_features.py)
       │
       ▼
[ Enriched Raw & Feature Tables ]  (lakehouse.raw.transactions with customer_age, risk_segment, etc.)
```
---
## 2. Step-by-step Execution
### Step 4.1: Register the Debezium CDC Connector
Make sure your containers are running (`docker compose ps`), then register the connector:
```bash
make register-cdc
```
*(Without `make`: `bash cdc/register_connector.sh`)*
**What this does:**
Sends the connector configuration (`cdc/debezium/customers-connector.json`) to Kafka Connect on port `8083`. Debezium immediately connects to `postgres-source`, takes an initial snapshot of the `customers` and `merchants` tables, and creates replication slot `debezium_slot`.
**Expected output:**
```text
Connector 'shopdb-connector' registered.
=== Connector Status ===
{
    "name": "shopdb-connector",
    "connector": {
        "state": "RUNNING",
        "worker_id": "connect:8083"
    },
    "tasks": [
        {
            "id": 0,
            "state": "RUNNING",
            "worker_id": "connect:8083"
        }
    ]
}
```
---
### Step 4.2: Verify CDC Events in Kafka
Verify that initial snapshot records were published to Kafka:
```bash
make consume-cdc-customers
```
*(Without `make`: `docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic cdc.public.customers --from-beginning --max-messages 5`)*
You will see the JSON records for `CUST-001` through `CUST-005` with field `__deleted: "false"`.
---
### Step 4.3: Start the Spark CDC Sync Stream (`job3`)
Open a new terminal window in `project1_fraud` and run:
```bash
make cdc-sync
```
*(Without `make`: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-cores 1 /opt/spark/jobs/streaming/job3_cdc_customers_sync.py`)*
**Expected output:**
```text
CDC Dimensions Sync stream is running.
  Kafka brokers      : kafka:29092
  Customers topic    : cdc.public.customers -> lakehouse.dims.customers
  Merchants topic    : cdc.public.merchants -> lakehouse.dims.merchants
  Trigger interval   : 5 seconds
[CDC batch 0] Synced 5 customer change event(s) to Iceberg dims
[CDC batch 0] Synced 5 merchant change event(s) to Iceberg dims
```
Keep this terminal running!
---
### Step 4.4: Trigger Live Database Mutations (`make seed`)
Open another terminal and run the mutation generator:
```bash
make seed
```
*(Without `make`: `python3 scripts/seed_customers.py`)*
**What you will see:**
1. It seeds 10 customers and 8 merchants into Postgres.
2. Every 10 seconds, it performs a realistic mutation (e.g., updates `CUST-001`'s risk segment to `'high'`, or flags a merchant as `is_high_risk = TRUE`).
3. You will see live logs like:
   ```text
   [12:34:56] 🔄 UPDATED customer CUST-001 (Asha Rao) -> risk_segment='high'  --> [Captured by Debezium WAL -> Kafka -> Spark Iceberg MERGE]
   ```
4. Look at the `make cdc-sync` terminal: you will see Spark instantly execute the micro-batch and perform the Iceberg `MERGE INTO`.
---
### Step 4.5: Inspect Dimension Tables & Streaming Joins
In another terminal, run:
```bash
make inspect-step4
```
**What you will see in the report:**
1. **Dimension counts**: `Customer dimension rows: 10`, `Merchant dimension rows: 8`.
2. **Dimension records**: Shows the synced attributes (`full_name`, `age`, `risk_segment`, `updated_at`, `synced_at`).
3. **Live Streaming Joins**: Look at `lakehouse.raw.transactions` — whereas in Step 3 `customer_risk_segment`, `merchant_is_high_risk`, and `country_mismatch` were null, they are now **fully populated** because `job1` dynamically joins with the freshly synced Iceberg dimensions!
4. **Snapshot history**: Displays the Iceberg snapshots showing individual commit IDs and ACID merge operations.
---
## 3. Why this demonstrates Open Table Format (Iceberg) Superpowers
In traditional data lakes (raw Parquet files on S3), doing row-level updates or deletes requires rewriting entire partitions or whole datasets.
With **Apache Iceberg**, Spark executes:
```sql
MERGE INTO lakehouse.dims.customers t
USING customer_cdc_updates s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.__deleted = 'true' THEN DELETE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```
- Only affected data files are rewritten or recorded with positional deletes.
- The operation is atomic: readers (like `job1` streaming joins or Airflow batch training) never see partial updates.
- Time travel remains intact: you can query `lakehouse.dims.customers FOR SYSTEM_VERSION AS OF <snapshot_id>` to see the dimension table as it existed before a mutation.
---
## 4. Troubleshooting Step 4
### Kafka Connect returns 500 or Connection Refused
- Check that connect container is running:
  ```bash
  docker compose ps connect
  ```
- View connect logs:
  ```bash
  docker compose logs --tail=100 connect
  ```
### Debezium fails with "replication slot already exists"
- If resetting the database, delete the connector and restart connect:
  ```bash
  curl -s -X DELETE http://localhost:8083/connectors/shopdb-connector
  ```
- Re-run `make register-cdc`.
---
## Step 4 Completion Checklist
- [ ] `make register-cdc` returns state `RUNNING`.
- [ ] `make consume-cdc-customers` shows JSON change events with `__deleted: false`.
- [ ] `make cdc-sync` is running and syncing micro-batches.
- [ ] `make seed` executes live mutations against `postgres-source`.
- [ ] `make inspect-step4` shows populated dimension rows and non-null enriched fields in `lakehouse.raw.transactions`.
Once verified, commit your changes:
```bash
git add -A
git commit -m "feat: complete Step 4 CDC pipeline with Debezium and Iceberg MERGE"
git push
```
