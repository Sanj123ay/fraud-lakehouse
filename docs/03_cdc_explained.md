# 03 — CDC Explained (and why we added it)

> You asked: "check if CDC can be added to this project."
> **Yes.** It's a natural fit, and this page shows exactly where it plugs in.

## What is CDC?

**Change Data Capture** = capturing every INSERT / UPDATE / DELETE that happens
in a database, as an ordered stream of events, in near real time.

### The old way (what CDC replaces)

Nightly bulk export:
```
23:00  pg_dump customers -> csv -> copy to lake -> reload dim table
```
Problems: up to 24h stale, heavy on the source DB, breaks if anything changes schema.

### The CDC way

Postgres already writes every change into its **WAL** (Write-Ahead Log) — an
internal transaction journal used for crash recovery and replication.
**Debezium** simply *reads that journal* and converts each change into a
Kafka event, within a second or two. The source database feels zero extra load.

## What one CDC event looks like

You run in the bank DB:
```sql
UPDATE customers SET risk_segment = 'high' WHERE customer_id = 'CUST-003';
```

Seconds later, Kafka topic `cdc.public.customers` contains:

```json
{
  "customer_id": "CUST-003",
  "full_name": "Chen Wei",
  "risk_segment": "high",
  "updated_at": "2026-01-15T02:31:10Z",
  "__deleted": "false"
}
```

(Debezium's `ExtractNewRecordState` transform gives us just the *new row* plus
a `__deleted` flag — deletes arrive as the row with `__deleted=true`.)

## Where CDC plugs into THIS project

```
 postgres-source (bank DB)            Kafka                    Iceberg
 ┌──────────────────────┐   WAL   ┌───────────────┐  MERGE  ┌────────────────────┐
 │ customers  merchants │ ──────► │ cdc.public.*  │ ──────► │ lakehouse.dims.*   │
 └──────────────────────┘ Debezium└───────────────┘  job3   └─────────┬──────────┘
        ▲                                                            │ broadcast join
        │  scripts/seed_customers.py edits rows                      ▼
        │  so you can watch the whole loop live          job1 streaming joins
        │                                                  ALWAYS see fresh dims
```

Three new pieces make this work (all already in the repo):

| Piece | File | What it does |
|---|---|---|
| Source DB config | `docker-compose.yml` (`postgres-source`) | enables `wal_level=logical` + publication |
| Connector | `cdc/debezium/customers-connector.json` | tells Debezium which DB + tables to watch |
| Sync job | `spark_jobs/streaming/job3_cdc_customers_sync.py` | reads CDC topics and `MERGE INTO` the Iceberg dims |

## Why this is genuinely useful here (not just a checkbox)

1. **The streaming join stays correct.** Fraud features depend on customer
   attributes like `risk_segment` and merchant `is_high_risk`. With CDC, when
   the bank flags a merchant as high-risk, the *next* micro-batch of
   transactions is already joined against that truth.
2. **It demos Iceberg's killer feature**: CDC needs UPDATEs and DELETEs on
   data-lake files. Plain parquet folders can't do that. Iceberg's
   `MERGE INTO` handles it with ACID guarantees.
3. **It's the pattern real companies use** to keep lakehouse dimensions in
   sync with operational systems (banks, e-commerce, everywhere).

## Try it (after the platform is up — built in Steps 1–4)

```bash
make register-cdc          # activate the Debezium connector once
make cdc-sync              # start the Spark MERGE job

# in another terminal, edit the "bank":
make psql-source
UPDATE customers SET risk_segment='high', updated_at=now()
  WHERE customer_id='CUST-001';

# then query the lakehouse dim (via spark-sql) a few seconds later —
# the row has already changed there too. That round trip IS CDC.
```
