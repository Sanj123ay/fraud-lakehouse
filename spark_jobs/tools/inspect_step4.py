#!/usr/bin/env python3
"""Inspect Step 4: Iceberg Dimension Tables, CDC Sync Status & Streaming Enrichment."""
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
    spark = build_spark("inspect-fraud-step4")
    spark.sparkContext.setLogLevel("ERROR")
    print("\n================ STEP 4 CDC & DIMENSIONS REPORT ================")
    # 1. Dimension Tables Summary
    for label, table in (
        ("Customer dimension rows", settings.T_DIM_CUSTOMERS),
        ("Merchant dimension rows", settings.T_DIM_MERCHANTS),
        ("Raw transactions count", settings.T_RAW),
        ("Feature store count", settings.T_FEATURES),
    ):
        try:
            count = spark.table(table).count()
            print(f"{label:28s}: {count}")
        except Exception as e:
            print(f"{label:28s}: (table not initialized yet: {e})")
    # 2. Customers Dimension Sample
    try:
        customers = spark.table(settings.T_DIM_CUSTOMERS)
        if not customers.isEmpty():
            print("\nLatest Customer Dimensions (synced from bank DB via CDC):")
            (
                customers.select(
                    "customer_id",
                    "full_name",
                    "age",
                    "home_country",
                    "risk_segment",
                    "updated_at",
                    "synced_at",
                )
                .orderBy(F.col("synced_at").desc())
                .show(10, truncate=False)
            )
    except Exception:
        pass
    # 3. Merchants Dimension Sample
    try:
        merchants = spark.table(settings.T_DIM_MERCHANTS)
        if not merchants.isEmpty():
            print("\nLatest Merchant Dimensions (synced from bank DB via CDC):")
            (
                merchants.select(
                    "merchant_id",
                    "merchant_name",
                    "category",
                    "country",
                    "is_high_risk",
                    "updated_at",
                    "synced_at",
                )
                .orderBy(F.col("synced_at").desc())
                .show(10, truncate=False)
            )
    except Exception:
        pass
    # 4. Check Enriched Transactions (Stream + CDC Dimension Join)
    try:
        raw = spark.table(settings.T_RAW)
        if not raw.isEmpty():
            enriched_sample = raw.select(
                "event_time",
                "txn_id",
                "card_id",
                "customer_id",
                "customer_risk_segment",
                "merchant_id",
                "merchant_is_high_risk",
                "country_mismatch",
            ).orderBy(F.col("event_time").desc())
            print("\nEnriched Streaming Transactions (Live Join with Fresh Dimensions):")
            enriched_sample.show(10, truncate=False)
            enriched_count = raw.filter(F.col("customer_risk_segment").isNotNull()).count()
            total_raw = raw.count()
            print(f"Transactions with non-null enriched dimension values: {enriched_count}/{total_raw}")
    except Exception:
        pass
    # 5. Snapshots for Dimensions
    try:
        print("\nIceberg Snapshots for lakehouse.dims.customers:")
        spark.sql(
            f"""
            SELECT committed_at, snapshot_id, operation
            FROM {settings.T_DIM_CUSTOMERS}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
            """
        ).show(truncate=False)
    except Exception:
        pass
    print("=================================================================\n")
    spark.stop()
if __name__ == "__main__":
    main()