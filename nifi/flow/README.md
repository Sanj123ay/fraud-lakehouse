# NiFi flow — built in Step 2 (documented here so the repo is complete)

We will build this flow **together, click by click, in the NiFi UI**
(http://localhost:8090/nifi — the UI *is* the point of NiFi: no-code ingestion).

## Processors we will drag onto the canvas

```
┌───────────────────────┐      success       ┌──────────────────────┐
│  GenerateFlowFile     │ ─────────────────► │  PublishKafka_2_6    │
│  (runs every 1 sec)   │                    │  brokers: kafka:29092│
│  Custom Text: {}      │                    │  topic: raw_transactions
└───────────────────────┘                    └──────────────────────┘
          │
          │  (alternative, closer to real life)
          ▼
┌───────────────────────┐
│  ExecuteScript        │   Engine: Groovy
│  Script file: /opt/nifi/nifi-current/scripts/transaction_generator.groovy
└───────────────────────┘
```

## Settings that matter (will be filled in during Step 2)

| Processor | Property | Value |
|---|---|---|
| ExecuteScript | Script Engine | `Groovy` |
| ExecuteScript | Script File | `/opt/nifi/nifi-current/scripts/transaction_generator.groovy` |
| GenerateFlowFile (scheduler) | Run Schedule | `1 sec` |
| PublishKafka_2_6 | Kafka Brokers | `kafka:29092` |
| PublishKafka_2_6 | Topic Name | `raw_transactions` |
| PublishKafka_2_6 | Delivery Guarantee | `Guarantee Replicated Delivery` |

## How to verify it works (Step 2 checklist)

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic raw_transactions \
  --from-beginning --max-messages 5
```
You should see one JSON transaction per second scrolling by.
