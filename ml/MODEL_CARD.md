# Model Card — `fraud_rf` (fraud-detection classifier)

A model card is the "label on the box" for a machine-learning model: what it does,
how it was trained, how well it works, and what it must not be used for.

## 1. Summary

- **Task**: binary classification. Is this card transaction fraudulent? (`1` = fraud)
- **Algorithm**: Spark MLlib `RandomForestClassifier` (100 trees, max depth 8, seed 42)
- **Packaging**: a Spark `PipelineModel`. The feature stages and the classifier are saved
  together, so serving code cannot compute features differently from training.
- **Owner**: fraud data engineering (learning project)
- **Status**: learning/demo model trained on simulated data. Not for real card decisions.

## 2. Where things live

| Artifact | Location (MinIO) | Purpose |
|---|---|---|
| Champion model (what Step 6 loads) | `s3a://models/fraud_rf/latest` | Current production-style model |
| Champion metrics | `s3a://models/fraud_rf/latest.metrics.json` | AUC used by the champion/challenger gate |
| Immutable version | `s3a://models/fraud_rf/versions/<run_id>/` | Roll back to any earlier model |
| Audit trail | `s3a://models/fraud_rf/runs/<run_id>.json` | Metrics of every run, including rejected ones |

- Training code: `spark_jobs/batch/job2_train_model.py`
- Shared feature definition: `spark_jobs/common/model_features.py`
- Schedule: Airflow `dag_01_daily_training`, every day at 02:00 IST (20:30 UTC)
- Inspect: `make inspect-model`
- Served by: `spark_jobs/streaming/job1_streaming_features.py` (Step 6). The champion is loaded through `common/model_registry.py`, checked every 5 minutes, and applied to every micro-batch.
- Outputs: `fraud_score` (P(fraud), 0..1), `is_fraud_pred` (score >= 0.5), `model_version`. Written to `lakehouse.raw.transactions`, `fraud_ops.predictions` (Postgres), and `fraud:card:<card_id>` (Redis).

## 3. Inputs (features)

All features are computed by the same SQL in `common/model_features.py`.

| Model input | Meaning | Source |
|---|---|---|
| `amount_f` | Transaction amount | `txn_features_1h.amount` |
| `count_5min_f` | Number of this card's transactions in the last 5 minutes | rolling window, Step 3 |
| `amount_sum_1h_f` | Total spend on this card in the last hour | rolling window, Step 3 |
| `country_mismatch_f` | 1 if the swipe country differs from the customer's home country | CDC-fed `dims.customers` (Step 4) |
| `merchant_high_risk_f` | 1 if the merchant is flagged high risk | CDC-fed `dims.merchants` (Step 4) |
| `hour_of_day_f` | Hour of the transaction (UTC) | `event_time` |
| `channel_idx` | Index of `pos` / `ecom` / `atm` (unknown or null becomes its own index) | `channel` |
| `risk_segment_idx` | Index of the customer's risk segment (unknown or null becomes its own index) | `customer_risk_segment` |

**Label**: `lakehouse.raw.transactions.is_fraud`, joined on `txn_id`.

**Important caveat**: the label comes from the data generator's *planted* fraud patterns
(large e-commerce or ATM swipes, abroad, at high-risk merchants). The model is therefore
learning those patterns, which is a simulation. Real labels would come from chargebacks
and investigations.

## 4. Training method

1. **Time-based split.** The oldest 80% of transactions train the model and the newest 20%
   test it. A random split would let the model see the future.
2. **Class weights.** Fraud is about 6% of rows, so each fraud row gets a weight equal to
   the legit:fraud ratio in the training set.
3. **Champion/challenger gate.** A new model is published only if:
   - its test AUC-ROC is at least `MODEL_MIN_AUC` (default 0.80), and
   - it has more test AUC-ROC than the current champion.
   Otherwise the champion is kept, and the run is still archived.
4. **Minimum data.** Training is skipped until there are at least 2,000 labelled rows and 50
   fraud rows (`TRAIN_MIN_ROWS`, `TRAIN_MIN_FRAUD_ROWS`).

## 5. Evaluation

Metrics are computed on the newest 20% of data, which the model never trained on.

| Metric | Meaning | Where to see it |
|---|---|---|
| AUC-ROC | How well the model ranks fraud above legit (1.0 is perfect, 0.5 is random) | metrics JSON, `make inspect-model` |
| AUC-PR | Precision/recall quality, more honest than AUC-ROC on rare fraud | metrics JSON |
| Confusion matrix at threshold 0.5 | tp / fp / fn / tn, plus precision and recall | metrics JSON |
| Feature importance | Which inputs the forest relied on most | metrics JSON |

Results of the latest run are written to `latest.metrics.json`. They are not copied into this
file, because they change every night.

## 6. Decision threshold

Scores are probabilities from 0 to 1. A transaction is flagged as fraud when its score is at
least `FRAUD_DECISION_THRESHOLD` (default 0.5). Because of class weighting, scores run higher
than the true fraud rate. Tune the threshold with the precision/recall trade-off before using
it for anything real.

## 7. Known limitations

- Trained on simulated data. Real fraud is more varied and adversarial.
- No customer history beyond the rolling 1-hour and 5-minute windows.
- Labels arrive instantly in this simulation. In production they arrive weeks later.
- The model is retrained from scratch each night, not updated incrementally.

## 8. Intended and prohibited use

- **Intended**: learning the ML lifecycle (train, evaluate, gate, deploy, monitor) on a
  realistic local platform.
- **Prohibited**: making real credit decisions, sharing customer data, or treating the
  simulated labels as ground truth.

## 9. Upgrade path

- Replace the JSON sidecar with MLflow tracking and the Model Registry.
- Add drift monitoring on the feature distributions.
- Add a second model type (for example gradient-boosted trees) and compare them in the gate.
