#!/usr/bin/env python3
"""
Step 5 — nightly fraud-model training (Spark MLlib RandomForest).

What it does, in order
----------------------
1. READ      labelled history: lakehouse.features.txn_features_1h (model inputs)
             joined to lakehouse.raw.transactions (is_fraud = the label).
2. SKIP      if there is not enough history yet (exit 0, nothing published).
3. SPLIT     time-based: the oldest 80% trains, the newest 20% tests.
             A random split would leak the future into training.
4. WEIGHT    fraud is rare (~6%), so fraud rows get a higher training weight.
5. TRAIN     Pipeline = shared feature stages (common/model_features.py)
             + RandomForestClassifier.
6. EVALUATE  AUC-ROC, AUC-PR, confusion matrix, feature importances.
7. GATE      champion/challenger:
               - AUC below MODEL_MIN_AUC           -> REJECTED   (exit 2, red task)
               - AUC not better than the champion  -> KEPT       (exit 0)
               - otherwise                         -> PUBLISHED  (exit 0)
8. PUBLISH   save the PipelineModel to s3a://models/fraud_rf/latest (what the
             streaming job loads in Step 6) plus an immutable copy under versions/.
             Metrics go to latest.metrics.json. Every run is archived in runs/.

Run it:
    make train                        (manual, from your laptop)
    Airflow DAG dag_01_daily_training (nightly, automatic)

Exit codes: 0 = success (published, kept, or skipped), 2 = quality gate failed.
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# spark-submit runs this file from batch/. Add spark_jobs/ so `common` imports work.
SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_ROOT))

from pyspark.ml import Pipeline, PipelineModel  # noqa: E402
from pyspark.ml.classification import RandomForestClassifier  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import Settings, build_spark  # noqa: E402
from common.model_features import MODEL_FEATURE_COLUMNS, build_feature_stages  # noqa: E402

EXIT_OK = 0
EXIT_QUALITY_GATE_FAILED = 2
MIN_TEST_FRAUD_ROWS = 10  # below this, AUC is too noisy to trust

FEATURE_SOURCE_COLUMNS = [
    "txn_id",
    "event_time",
    "amount",
    "count_5min",
    "amount_sum_1h",
    "country_mismatch",
    "merchant_is_high_risk",
    "channel",
    "customer_risk_segment",
]


# ---------------------------------------------------------------------------
# Hadoop file helpers: tiny JSON read/write on MinIO (s3a://) via the JVM.
# ---------------------------------------------------------------------------
def _hadoop_handles(spark: SparkSession, uri: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return path, fs


def read_json(spark: SparkSession, uri: str) -> dict | None:
    """Return the parsed JSON at `uri`, or None if it does not exist yet."""
    path, fs = _hadoop_handles(spark, uri)
    if not fs.exists(path):
        return None
    text = "\n".join(row.value for row in spark.read.text(uri).collect())
    return json.loads(text)


def write_json(spark: SparkSession, uri: str, payload: dict) -> None:
    """Overwrite `uri` with pretty JSON."""
    path, fs = _hadoop_handles(spark, uri)
    out = fs.create(path, True)
    try:
        out.write(bytearray((json.dumps(payload, indent=2) + "\n").encode("utf-8")))
    finally:
        out.close()


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------
def load_labelled_history(spark: SparkSession, settings: Settings) -> DataFrame:
    """Feature rows joined to the ground-truth label from the raw table."""
    features = spark.table(settings.T_FEATURES).select(*FEATURE_SOURCE_COLUMNS)
    labels = (
        spark.table(settings.T_RAW)
        .where(F.col("is_fraud").isNotNull())
        .select("txn_id", F.col("is_fraud").cast("double").alias("label"))
    )
    return features.join(labels, "txn_id", "inner")


def time_split(df: DataFrame, holdout_fraction: float):
    """Oldest rows train, newest rows test. Never a random split for fraud."""
    epoch = F.col("event_time").cast("long")
    cutoff = df.select(epoch.alias("t")).approxQuantile("t", [1.0 - holdout_fraction], 0.001)[0]
    return df.where(epoch < cutoff), df.where(epoch >= cutoff), cutoff


def label_counts(df: DataFrame) -> dict:
    rows = df.groupBy("label").count().collect()
    counts = {0.0: 0, 1.0: 0}
    for row in rows:
        counts[float(row["label"])] = int(row["count"])
    return {"legit": counts[0.0], "fraud": counts[1.0]}


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------
def confusion_matrix(predictions: DataFrame) -> dict:
    cm = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    for row in predictions.groupBy("label", "prediction").count().collect():
        actual_fraud = row["label"] == 1.0
        predicted_fraud = row["prediction"] == 1.0
        key = (
            "tp" if actual_fraud and predicted_fraud
            else "fp" if predicted_fraud
            else "fn" if actual_fraud
            else "tn"
        )
        cm[key] += int(row["count"])
    precision = cm["tp"] / (cm["tp"] + cm["fp"]) if (cm["tp"] + cm["fp"]) else 0.0
    recall = cm["tp"] / (cm["tp"] + cm["fn"]) if (cm["tp"] + cm["fn"]) else 0.0
    cm.update({"precision": round(precision, 4), "recall": round(recall, 4)})
    return cm


def feature_importance(model: PipelineModel) -> dict:
    forest = model.stages[-1]
    scores = forest.featureImportances.toArray()
    ranked = sorted(zip(MODEL_FEATURE_COLUMNS, scores), key=lambda pair: -pair[1])
    return {name: round(float(score), 4) for name, score in ranked}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    settings = Settings()
    spark = build_spark("fraud-model-training")
    spark.sparkContext.setLogLevel("WARN")

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    print(f"\n=== Fraud model training run {run_id} ===", flush=True)

    if not (spark.catalog.tableExists(settings.T_FEATURES) and spark.catalog.tableExists(settings.T_RAW)):
        print("SKIPPED: feature/raw tables do not exist yet. Start the stream first (make stream).")
        return EXIT_OK

    data = load_labelled_history(spark, settings).cache()
    total_rows = data.count()
    counts = label_counts(data)
    print(f"Labelled rows: {total_rows} (legit={counts['legit']}, fraud={counts['fraud']})", flush=True)

    if total_rows < settings.TRAIN_MIN_ROWS or counts["fraud"] < settings.TRAIN_MIN_FRAUD_ROWS:
        print(
            f"SKIPPED: need >= {settings.TRAIN_MIN_ROWS} rows and >= "
            f"{settings.TRAIN_MIN_FRAUD_ROWS} fraud rows. Let the generator run longer.",
            flush=True,
        )
        data.unpersist()
        return EXIT_OK

    train_df, test_df, cutoff = time_split(data, settings.TRAIN_HOLDOUT_FRACTION)
    train_counts = label_counts(train_df)
    test_counts = label_counts(test_df)
    print(f"Time cutoff (epoch s): {int(cutoff)}", flush=True)
    print(f"Train rows: {train_counts}  Test rows: {test_counts}", flush=True)

    # Class weights: each fraud row counts as much as `ratio` legit rows.
    ratio = train_counts["legit"] / max(train_counts["fraud"], 1)
    weighted_train = train_df.withColumn(
        "weight", F.when(F.col("label") == 1.0, F.lit(ratio)).otherwise(F.lit(1.0))
    )

    classifier = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="weight",
        numTrees=settings.RF_NUM_TREES,
        maxDepth=settings.RF_MAX_DEPTH,
        seed=42,
    )
    print("Training RandomForest pipeline...", flush=True)
    model = Pipeline(stages=build_feature_stages() + [classifier]).fit(weighted_train)

    # ---- Evaluate on the newest, unseen data --------------------------------
    predictions = model.transform(test_df).cache()
    auc_roc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(predictions)
    cm = confusion_matrix(predictions)
    predictions.unpersist()

    metrics = {
        "run_id": run_id,
        "model_version": run_id,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "algorithm": "RandomForestClassifier",
        "num_trees": settings.RF_NUM_TREES,
        "max_depth": settings.RF_MAX_DEPTH,
        "feature_version": settings.FEATURE_VERSION,
        "decision_threshold": settings.FRAUD_DECISION_THRESHOLD,
        "train_rows": train_counts,
        "test_rows": test_counts,
        "time_cutoff_epoch": int(cutoff),
        "auc_roc": round(float(auc_roc), 4),
        "auc_pr": round(float(auc_pr), 4),
        "confusion_matrix": cm,
        "feature_importance": feature_importance(model),
    }
    print(json.dumps({k: metrics[k] for k in ("auc_roc", "auc_pr", "confusion_matrix")}, indent=2), flush=True)

    # ---- Quality gate and champion/challenger decision ----------------------
    champion = read_json(spark, settings.MODEL_METRICS_PATH)
    champion_auc = champion.get("auc_roc") if champion else None

    if test_counts["fraud"] < MIN_TEST_FRAUD_ROWS:
        status, exit_code = "rejected_not_enough_test_fraud", EXIT_QUALITY_GATE_FAILED
    elif auc_roc < settings.MODEL_MIN_AUC:
        status, exit_code = "rejected_below_floor", EXIT_QUALITY_GATE_FAILED
    elif champion_auc is not None and auc_roc <= champion_auc:
        status, exit_code = "kept_champion", EXIT_OK
    else:
        status, exit_code = "published", EXIT_OK

    metrics["status"] = status
    metrics["champion_auc_before"] = champion_auc

    if status == "published":
        model.write().overwrite().save(f"{settings.MODEL_ROOT}/versions/{run_id}")
        model.write().overwrite().save(settings.MODEL_PATH)
        write_json(spark, settings.MODEL_METRICS_PATH, metrics)

    # Every run is archived, even rejected ones: this is the audit trail.
    write_json(spark, f"{settings.MODEL_ROOT}/runs/{run_id}.json", metrics)

    print(f"\nDecision: {status}", flush=True)
    if status == "published":
        print(f"New champion saved to {settings.MODEL_PATH}", flush=True)
    elif status == "kept_champion":
        print(f"New AUC {auc_roc:.4f} did not beat champion {champion_auc}. Keeping current model.", flush=True)
    else:
        print(f"Quality gate FAILED ({status}). Current champion left untouched.", flush=True)

    data.unpersist()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
