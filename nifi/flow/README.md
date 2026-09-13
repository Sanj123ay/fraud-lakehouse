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



# Step 2 — NiFi transaction generator → Kafka
By the end of this step, NiFi creates one realistic credit-card transaction per
second and Kafka durably stores it in `raw_transactions`.
> **Why no InvokeHTTP?** NiFi is already inside the same Docker network as Kafka,
> and `PublishKafka_2_6` is the correct direct producer. `InvokeHTTP` is useful
> when an external HTTP API is the source. Adding it between NiFi and Kafka here
> would create an unnecessary service and failure point.
## 0. Files involved
| File | Purpose |
|---|---|
| `nifi/scripts/transaction_generator.groovy` | Generates one typed JSON transaction |
| `spark_jobs/common/schemas.py` | Spark-side contract with exactly the same fields |
| `kafka/scripts/create_topics.sh` | Creates `raw_transactions` with 3 partitions |
| `scripts/validate_transactions.py` | Tests Kafka records against the contract |
The host folder `./nifi/scripts` is mounted read-only inside NiFi at
`/opt/nifi/nifi-current/scripts`, so the UI refers to the container path—not a
Windows/macOS path.
---
## 1. Refresh NiFi and create the Kafka topic
Run from the `project1_fraud` folder:
```bash
# Recreates only NiFi so it receives SIMULATED_FRAUD_RATE from .env.
docker compose up -d --force-recreate nifi
# Explicit topic creation: 3 partitions, replication factor 1 for our one broker.
make topics
# Confirm both services are up.
docker compose ps nifi kafka
```
If you do not have `make`, use:
```bash
wsl -d Ubuntu bash kafka/scripts/create_topics.sh raw_transactions 3
```
Wait until this returns HTTP 200 (NiFi can take 60–90 seconds after startup):
```bash
curl -I http://localhost:8090/nifi/
```
Open **http://localhost:8090/nifi**. You should see the empty NiFi canvas.
---
## 2. Add processor 1: GenerateFlowFile
1. Drag the **Processor** icon from the top toolbar onto the canvas.
2. Search for `GenerateFlowFile`, select it, click **Add**.
3. Right-click it → **Configure**.
4. On **Settings**, set name to `01 - Transaction Trigger`.
5. On **Properties**, set:
| Property | Value |
|---|---|
| File Size | `0 B` |
| Batch Size | `1` |
| Data Format | `Text` |
| Unique FlowFiles | `false` |
| Custom Text | `{}` |
6. On **Scheduling**, set:
| Property | Value |
|---|---|
| Scheduling Strategy | `Timer driven` |
| Run Schedule | `1 sec` |
| Concurrent Tasks | `1` |
Do **not** start it yet.
**What this does:** creates one tiny trigger FlowFile each second. It does not
make the final JSON; the next processor replaces `{}` with the transaction.
---
## 3. Add processor 2: ExecuteScript
1. Add another processor and search for `ExecuteScript`.
2. Name it `02 - Build Transaction JSON`.
3. On **Properties**, set:
| Property | Value |
|---|---|
| Script Engine | `Groovy` |
| Script File | `/opt/nifi/nifi-current/scripts/transaction_generator.groovy` |
| Script Body | leave blank |
| Module Directory | leave blank |
4. Click **Apply**.
5. Hover over `01 - Transaction Trigger`, drag its connection arrow to this
   processor, select relationship **success**, and click **Add**.
**What the script does:**
- chooses one of the seeded `CUST-001`…`CUST-005` customers;
- creates a card, merchant, country, channel, amount, UUID and UTC event time;
- marks about 6% as simulated fraud with deliberately learnable patterns;
- makes 4% of records 1–8 minutes late and 0.5% over 10 minutes late for the
  watermark lesson in Step 3;
- sets the FlowFile attribute `kafka.key` to `card_id`, preserving per-card
  ordering within a Kafka partition;
- outputs exactly the schema declared in `spark_jobs/common/schemas.py`.
---
## 4. Add processor 3: PublishKafka_2_6
1. Add a processor and search for **exactly** `PublishKafka_2_6`.
2. Name it `03 - Publish raw_transactions`.
3. On **Properties**, change these values; leave unrelated security fields blank:
| Property | Value | Why |
|---|---|---|
| Kafka Brokers | `kafka:29092` | Internal Docker address, not localhost |
| Security Protocol | `PLAINTEXT` | Local learning environment |
| Topic Name | `raw_transactions` | Streaming source topic |
| Delivery Guarantee | `Guarantee Replicated Delivery` | Wait for Kafka acknowledgment |
| Use Transactions | `false` | One-broker local cluster; avoids transaction-log replication requirements |
| Kafka Key | leave blank | Processor automatically uses `kafka.key` attribute |
| Message Demarcator | leave blank | Entire FlowFile = exactly one Kafka message |
| Compression Type | `none` | Tiny demo messages do not need compression |
| Acknowledgment Wait Time | `10 secs` | Comfortable first-start timeout |
| Max Metadata Wait Time | `10 sec` | Comfortable first-start timeout |
4. On **Settings** → **Automatically Terminate Relationships**, check only
   **success**. A successfully published FlowFile is no longer needed in NiFi.
5. Click **Apply**.
6. Connect `02 - Build Transaction JSON` → `03 - Publish raw_transactions`
   using the **success** relationship.
> `localhost:9092` is correct from your laptop, but **wrong inside NiFi**.
> Each container has its own localhost. Containers use `kafka:29092`.
---
## 5. Add processor 4: LogAttribute (failure path)
Never hide failures in a data pipeline.
1. Add `LogAttribute` and name it `99 - Log Failed FlowFiles`.
2. Set property **Log Level** to `error`.
3. On **Settings**, automatically terminate its **success** relationship.
4. Connect both failure paths to it:
   - `02 - Build Transaction JSON` → `99 - Log Failed FlowFiles`: **failure**
   - `03 - Publish raw_transactions` → `99 - Log Failed FlowFiles`: **failure**
Final canvas:
```text
01 Transaction Trigger
        |
        | success
        v
02 Build Transaction JSON ------ failure ------+
        |                                       |
        | success                               v
        v                              99 Log Failed FlowFiles
03 Publish raw_transactions ------ failure -----+
        |
        | success (auto-terminated)
        v
      Kafka topic raw_transactions (3 partitions)
```
---
## 6. Start the flow
1. Hold Shift and click all four processors (or drag a selection box around them).
2. Click the **Start** button (triangle) in the Operate panel.
3. All processors should show a green play icon.
4. After 10 seconds, the connection counters should increase and then drain.
Expected signs:
- `01` shows Out around 1 record/sec.
- `02` shows In and Out at roughly the same rate.
- `03` shows In; its success is auto-terminated after Kafka acknowledges.
- Connections should not grow continuously.
- The top-right bulletin icon should show no red errors.
---
## 7. Prove the data reached Kafka
### Human-readable check
Run:
```bash
make consume-raw
```
Without `make`:
```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic raw_transactions --from-beginning --max-messages 10 --timeout-ms 20000
```
You should see ten one-line JSON objects. The command exits after ten; it does
not stop NiFi or delete Kafka messages.
### Automated schema check
Run:
```bash
make verify-ingestion
```
Expected result starts with:
```text
PASS: validated 10 schema-compatible Kafka transactions.
```
A random ten-row sample may contain zero fraud records. At 6%, that is normal.
### Check topic partitions and offsets
```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic raw_transactions
docker compose exec kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic raw_transactions
```
The topic should have 3 partitions, and offsets should continue increasing while
NiFi runs.
---
## 8. Inspect one FlowFile before Kafka (optional learning exercise)
To see the generated JSON inside NiFi:
1. Stop `03 - Publish raw_transactions` only.
2. Let the queue before it reach a few records.
3. Right-click that connection → **List queue**.
4. Click the details icon on a row → **View** → **Formatted**.
5. Start processor `03` again; the queue drains into Kafka.
Do not leave the publisher stopped for hours: NiFi will intentionally retain the
queue, consuming disk space.
---
## 9. Troubleshooting
### PublishKafka says it cannot connect
- Confirm Kafka is up: `docker compose ps kafka`.
- Confirm Kafka address is **`kafka:29092`**, not localhost.
- Test DNS/network from NiFi:
```bash
docker compose exec nifi bash -lc 'getent hosts kafka && timeout 3 bash -c "</dev/tcp/kafka/29092" && echo CONNECTED'
```
### ExecuteScript says the script file is missing
Run:
```bash
docker compose exec nifi ls -l /opt/nifi/nifi-current/scripts/
```
You should see `transaction_generator.groovy`. If not, make sure you ran Docker
Compose from the folder containing `docker-compose.yml`.
### Groovy is not in the Script Engine dropdown
Make sure you chose `ExecuteScript` from bundle `nifi-scripting-nar`, not a
similarly named processor. Check NiFi logs:
```bash
docker compose logs --tail=150 nifi
```
### Processor has a yellow warning triangle / cannot start
Hover over the triangle. NiFi tells you exactly which relationship or required
property is missing. The usual cause is forgetting to connect/auto-terminate a
relationship:
- PublishKafka: auto-terminate `success`, connect `failure`.
- ExecuteScript: connect both `success` and `failure`.
- LogAttribute: auto-terminate `success`.
### Verification says Kafka returned no records
Confirm all processors are running and check NiFi bulletins. Then run:
```bash
docker compose logs --tail=100 kafka
docker compose logs --tail=150 nifi
```
---
## Step 2 completion checklist
- [ ] `raw_transactions` exists with 3 partitions.
- [ ] All four NiFi processors are green/running.
- [ ] No connection queue grows continuously.
- [ ] `make consume-raw` prints JSON transactions.
- [ ] `make verify-ingestion` prints `PASS`.
- [ ] Kafka offsets increase while NiFi runs.
Once all six are true, Step 2 is complete. Keep NiFi running: Step 3 will attach
Spark Structured Streaming to this live topic.

