#!/usr/bin/env python3
"""Read-only Step 3 health report for the local Iceberg lakehouse."""
from __future__ import annotations
import sys
from pathlib import Path
SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_ROOT))
from pyspark.sql import functions as F
from common.config import Settings, build_spark
def main() -> None:
    settings = Settings()
    spark = build_spark("inspect-fraud-step3")
    spark.sparkContext.setLogLevel("ERROR")
    print("\n================ STEP 3 LAKEHOUSE REPORT ================")
    for label, table in (
        ("valid raw transactions", settings.T_RAW),
        ("offline feature rows", settings.T_FEATURES),
        ("quarantined messages", settings.T_QUARANTINE),
        ("customer dimension rows", settings.T_DIM_CUSTOMERS),
        ("merchant dimension rows", settings.T_DIM_MERCHANTS),
    ):
        count = spark.table(table).count()
        print(f"{label:28s}: {count}")
    features = spark.table(settings.T_FEATURES)
    if not features.isEmpty():
        print("\nLatest rolling features (count includes current transaction):")
        (
            features.select(
                "event_time",
                "txn_id",
                "card_id",
                "amount",
                "count_5min",
                "amount_sum_1h",
                "country_mismatch",
                "merchant_is_high_risk",
            )
            .orderBy(F.col("event_time").desc())
            .show(10, truncate=False)
        )
        print("Feature sanity summary:")
        features.agg(
            F.min("count_5min").alias("min_count_5min"),
            F.max("count_5min").alias("max_count_5min"),
            F.min("amount_sum_1h").alias("min_amount_sum_1h"),
            F.max("amount_sum_1h").alias("max_amount_sum_1h"),
        ).show(truncate=False)
    quarantine = spark.table(settings.T_QUARANTINE)
    if not quarantine.isEmpty():
        print("\nLatest quarantined records:")
        quarantine.orderBy(F.col("quarantined_at").desc()).show(5, truncate=False)
    print("\nLatest Iceberg snapshots for raw transactions:")
    spark.sql(
        f"""
        SELECT committed_at, snapshot_id, operation
        FROM {settings.T_RAW}.snapshots
        ORDER BY committed_at DESC
        LIMIT 5
        """
    ).show(truncate=False)
    print("=========================================================\n")
    spark.stop()
if __name__ == "__main__":
    main()
