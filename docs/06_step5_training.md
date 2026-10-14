# 06 — Step 5: Nightly fraud-model training

Up to now the platform **collects and cleans** transactions (Steps 1–4). In this step it
**learns** from them. Every night the platform reads the history, trains a fraud model,
checks whether the new model is actually better, and saves it for the streaming job to use.

---

## 1. The big picture (plain English)

```text
Iceberg: features.txn_features_1h  ──┐
                                     ├─ join on txn_id ─► labelled history
Iceberg: raw.transactions (is_fraud) ┘                        │
                                                               ▼
                                              split by TIME: oldest 80% / newest 20%
                                                               │
                                                               ▼
                       RandomForest pipeline trains on the oldest 80%
                                                               │
                                                               ▼
                       score the newest 20% (data the model never saw)
                                                               │
                             ┌─────────────────────────────────┴──────────────────────┐
                             ▼                                                        ▼
                  AUC below 0.80 floor?                              better than current champion?
                  yes ─► REJECTED, red task                          no ─► KEPT, champion stays
                                                                       yes ─► PUBLISHED
                                                                                │
                                                                                ▼
                                             s3a://models/fraud_rf/latest  (Step 6 loads this)
```

### Words you need

| Word | Meaning |
|---|---|
| **Model** | A function that turns a transaction's features into a fraud score between 0 and 1. |
| **Training** | Letting the algorithm find patterns in historical examples where the answer is known. |
| **Label** | The known answer for each example. Here it is `is_fraud` (0 or 1). |
| **Feature** | An input the model looks at (amount, transactions in the last 5 minutes, and so on). |
| **Random Forest** | Many small decision trees that vote. Robust and easy to explain. |
| **Pipeline** | A saved chain of steps: prepare features → model. Saved as one object. |
| **AUC** | Score from 0 to 1 for how well the model separates fraud from legit. 0.5 is coin-flip, 1.0 is perfect. |
| **Champion/challenger** | The current model is the champion. A new model (challenger) must beat it to take over. |
| **Overfitting** | The model memorises the training data and fails on new data. Testing on unseen data detects it. |

---

## 2. Files in this step

| File | Role |
|---|---|
| `spark_jobs/common/model_features.py` | **Shared feature definition.** Used by training now and by live scoring in Step 6. |
| `spark_jobs/batch/job2_train_model.py` | **The training job.** Reads data, trains, evaluates, gates, publishes. |
| `spark_jobs/batch/submit_training.sh` | Sends the training job to the Spark cluster from inside Airflow. |
| `airflow/dags/dag_01_daily_training.py` | **The schedule.** Runs the job every night at 02:00 IST. |
| `spark_jobs/tools/inspect_model.py` | Shows the current champion, its metrics, and the run archive. |
| `spark_jobs/common/config.py` | Training settings (minimum rows, AUC floor, tree count, and so on). |
| `ml/MODEL_CARD.md` | Documentation of the model, written the way real teams document it. |
| `docs/06_step5_training.md` | This page. |

---

## 3. Why the features live in one place

A classic production bug is **training/serving skew**. The model is trained on one version
of a feature and scored on a slightly different one, so its accuracy quietly drops in
production.

To prevent this, `model_features.py` defines the feature SQL once, as a stage inside the
Spark ML pipeline. The trained model file *contains* that SQL. Step 6 will load the same file
and run it on live data, so the logic cannot drift.

---

## 4. The training job, step by step

1. **Check the data exists.** If the tables are not there yet, it exits cleanly.
2. **Build the labelled history.** Feature rows joined to `is_fraud` on `txn_id`.
3. **Skip if there is too little data.** Defaults: at least 2,000 rows and 50 fraud rows.
   Early on this is normal: it prints `SKIPPED` and exits with success.
4. **Split by time.** Training uses the oldest 80%. The newest 20% are the exam.
5. **Weight the fraud rows.** Fraud is rare (about 6%), so each fraud row counts as much as
   about 15 legit rows. Without this the model could ignore fraud and still look accurate.
6. **Train** the pipeline: shared feature stages, then the RandomForest.
7. **Evaluate** on the newest 20%: AUC-ROC, AUC-PR, confusion matrix, feature importances.
8. **Gate:**
   - AUC below 0.80 → `rejected_below_floor`, the task turns red, the champion is untouched.
   - AUC not higher than the champion → `kept_champion`, nothing is replaced.
   - Otherwise → `published`.
9. **Save.** A champion goes to `latest` (what Step 6 loads), plus an immutable copy under
   `versions/<run_id>`. Metrics go to `latest.metrics.json`.
10. **Archive.** Every run, including rejected ones, is written to `runs/<run_id>.json`. This
    is your audit trail.

---

## 5. Run it

### Option A: manually (for learning)

The stream from Step 3 must have run for a while first, so there is data to learn from.

```bash
make stream        # keep this running in one terminal (Ctrl+C stops it)
# in a second terminal, after a few minutes of data:
make train         # trains the model once
make inspect-model # shows the champion and the run archive
```

What a healthy run prints looks roughly like this:

```text
Labelled rows: 4815 (legit=4512, fraud=303)
Time cutoff (epoch s): ...
Train rows: {'legit': 3610, 'fraud': 240}  Test rows: {'legit': 902, 'fraud': 63}
Training RandomForest pipeline...
{ "auc_roc": 0.9x, "auc_pr": 0.8x, "confusion_matrix": {...} }
Decision: published
```

If you see `SKIPPED: need >= 2000 rows`, wait longer or lower `TRAIN_MIN_ROWS` in `.env`.
The generator makes about one transaction per second, so 2,000 rows takes roughly 35 minutes.

### Option B: automatically with Airflow (the real setup)

1. Open **http://localhost:8085** and log in (`admin` / `admin` by default).
2. Find **dag_01_daily_training** and switch the toggle on.
3. Click the name, then **Graph**, to see the one task. Use **Trigger DAG** to run it now.
4. Click the green or red box, then **Log**, to read the output.

Airflow only runs what is scheduled. Turning the DAG on does not train the model straight
away. The first night it runs at 02:00 IST, or whenever you trigger it.

### Option C: look at the results in MinIO

Open **http://localhost:9001** (login from `.env`), then the `models` bucket:

```text
models/fraud_rf/
├── latest/                    <- the champion model (Step 6 loads this)
├── latest.metrics.json        <- champion metrics
├── versions/<run_id>/         <- every published model, kept forever
└── runs/<run_id>.json         <- every run, including rejected ones
```

---

## 6. Settings you can change (in `.env`)

| Setting | Default | What it controls |
|---|---|---|
| `TRAIN_MIN_ROWS` | 2000 | Minimum labelled transactions before training is allowed |
| `TRAIN_MIN_FRAUD_ROWS` | 50 | Minimum fraud examples before training is allowed |
| `TRAIN_HOLDOUT_FRACTION` | 0.2 | Share of the newest data kept for testing |
| `MODEL_MIN_AUC` | 0.80 | Quality floor. Models below this are never published |
| `RF_NUM_TREES` | 100 | Trees in the forest (more = slower and steadier) |
| `RF_MAX_DEPTH` | 8 | How deep each tree may grow (more = risk of overfitting) |
| `FRAUD_DECISION_THRESHOLD` | 0.5 | Score above which a transaction is flagged (used in Step 6) |

Changes take effect on the next run. Restart the Airflow container if you change `.env`
and want the nightly DAG to pick it up.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `SKIPPED: feature/raw tables do not exist yet` | The stream has never run. | Run `make stream` for a few minutes first. |
| `SKIPPED: need >= 2000 rows` | Not enough data yet. | Wait, or lower `TRAIN_MIN_ROWS`. |
| `rejected_below_floor` | Model is weaker than 0.80 AUC. | Check `make inspect-model`, then look at the feature importances and data volume. |
| `rejected_not_enough_test_fraud` | Too few fraud rows in the newest 20%. | Let more data accumulate. |
| `kept_champion` | The new model was not better. This is correct behaviour. | Nothing to fix. |
| Airflow task fails with a Spark or connection error | Airflow cannot reach `spark-master:7077`, or `--packages` could not download jars. | Check `docker compose ps`, then read the task log. The first run needs internet access to download jars. |
| `spark-submit: not found` in the Airflow log | The pip-installed pyspark is missing from the Airflow image. | Rebuild with `docker compose build airflow`. |

---

## 8. Concepts checklist (what you learned)

- **Batch ETL**: a scheduled job reads a big historical table and produces a result.
- **Time-based train/test split**: why the future must never leak into training.
- **Class imbalance**: why rare events need weighting.
- **ML pipelines**: feature steps and a model saved as one artifact.
- **Model registry pattern**: versions, a champion pointer, and an audit log.
- **Quality gates**: bad models are rejected automatically.
- **Orchestration**: Airflow schedules the job, retries it, and shows its history.

Next: **Step 6** loads this champion model inside the streaming job and scores each
transaction as it arrives, writing the results to Postgres and Redis.
