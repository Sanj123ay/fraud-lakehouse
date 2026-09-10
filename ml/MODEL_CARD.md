# Model Card — fraud_rf (filled in properly during Step 5)

- **Task**: binary classification — is this transaction fraudulent?
- **Algorithm**: Spark MLlib RandomForestClassifier (numTrees=100, maxDepth=8)
- **Trained by**: `spark_jobs/batch/job2_train_model.py`, nightly via Airflow DAG `dag_01_daily_training`
- **Served by**: `spark_jobs/streaming/job1_streaming_features.py` (`PipelineModel.load` from `s3a://models/fraud_rf/latest` inside `foreachBatch`)

## Features (offline store: `lakehouse.features.txn_features_1h`)

| Feature | Source | Window |
|---|---|---|
| amount | transaction | — |
| count_5min | per-card rolling count | 5 min |
| amount_sum_1h | per-card rolling sum | 1 hour |
| country_mismatch | txn country != customer home_country | — |
| merchant_is_high_risk | dims.merchants (CDC-fresh) | — |
| channel, hour_of_day | txn timestamp | — |

## Governance notes

- Champion/challenger publish gate: new model must beat stored AUC.
- Metrics sidecar: `s3a://models/fraud_rf/latest.metrics.json`
- Labels come from the generator's planted fraud patterns — documented so
  nobody mistakes simulated ground truth for real labels.
- Upgrade path noted for real MLflow Model Registry (tracked as optional).
