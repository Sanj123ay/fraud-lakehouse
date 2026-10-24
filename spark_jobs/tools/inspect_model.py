#!/usr/bin/env python3
"""
Show the current fraud champion and the archive of training runs (Step 5).

Run:  make inspect-model
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_ROOT))

from common.config import Settings, build_spark  # noqa: E402
from common.storage import read_json  # noqa: E402


def list_run_files(spark, settings: Settings) -> list[str]:
    jvm = spark.sparkContext._jvm
    runs_dir = jvm.org.apache.hadoop.fs.Path(f"{settings.MODEL_ROOT}/runs")
    fs = runs_dir.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(runs_dir):
        return []
    return sorted(status.getPath().getName() for status in fs.listStatus(runs_dir))


def main() -> int:
    settings = Settings()
    spark = build_spark("inspect-fraud-model")
    spark.sparkContext.setLogLevel("WARN")

    champion = read_json(spark, settings.MODEL_METRICS_PATH)
    print("\n=== CURRENT CHAMPION ===")
    if champion is None:
        print("No model published yet. Run `make train` after the stream has data.")
    else:
        summary = {
            key: champion.get(key)
            for key in (
                "model_version",
                "trained_at",
                "status",
                "auc_roc",
                "auc_pr",
                "decision_threshold",
                "train_rows",
                "test_rows",
                "confusion_matrix",
            )
        }
        print(json.dumps(summary, indent=2))
        print("\nFeature importance:")
        for name, score in (champion.get("feature_importance") or {}).items():
            print(f"  {name:<24} {score:.4f}")

    print("\n=== TRAINING RUN ARCHIVE ===")
    runs = list_run_files(spark, settings)
    if not runs:
        print("(no runs yet)")
    for name in runs:
        run = read_json(spark, f"{settings.MODEL_ROOT}/runs/{name}")
        print(f"  {name:<32} status={run.get('status'):<32} auc_roc={run.get('auc_roc')}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
