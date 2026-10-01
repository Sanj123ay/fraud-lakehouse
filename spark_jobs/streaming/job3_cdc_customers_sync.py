#!/usr/bin/env python3
"""CDC Stream: Debezium Kafka Topics -> Iceberg Dimension Tables (MERGE INTO).
Step 4 responsibilities
-----------------------
* Continuously stream from `cdc.public.customers` and `cdc.public.merchants`.
* Parse Debezium unwrapped JSON messages with schema contracts.
* Handle initial DB snapshot events + incremental INSERT/UPDATE/DELETE.
* Deduplicate within each micro-batch to the latest change per entity.
* Execute transactional Iceberg MERGE INTO:
    - On MATCHED and __deleted = 'true': DELETE row from Iceberg dim.
    - On MATCHED: UPDATE all dimension attributes.
    - On NOT MATCHED and not deleted: INSERT new dimension record.
* Maintain checkpoints under `s3a://checkpoints/job3/`.
"""
from __future__ import annotations
import sys
from pathlib import Path
# Add spark_jobs directory to path for driver and executors
SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_ROOT))
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from common.config import Settings, build_spark
from common.schemas import CDC_CUSTOMERS_SCHEMA, CDC_MERCHANTS_SCHEMA
def ensure_dim_tables(spark: SparkSession, settings: Settings) -> None:
    """Ensure Iceberg catalog namespace and dimension tables exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.CATALOG}.dims")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.T_DIM_CUSTOMERS} (
            customer_id STRING,
            full_name STRING,
            email STRING,
            age INT,
            home_country STRING,
            risk_segment STRING,
            updated_at TIMESTAMP,
            source_operation STRING,
            source_kafka_offset BIGINT,
            synced_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'commit.retry.num-retries'='10'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.T_DIM_MERCHANTS} (
            merchant_id STRING,
            merchant_name STRING,
            category STRING,
            country STRING,
            is_high_risk BOOLEAN,
            updated_at TIMESTAMP,
            source_operation STRING,
            source_kafka_offset BIGINT,
            synced_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'commit.retry.num-retries'='10'
        )
        """
    )
def robust_timestamp(col_name: str):
    """Parse ISO timestamp or epoch integer or fall back to current timestamp."""
    return F.coalesce(
        F.to_timestamp(col_name),
        F.to_timestamp(F.from_unixtime(F.col(col_name).cast("double") / 1000000.0)),
        F.to_timestamp(F.from_unixtime(F.col(col_name).cast("double") / 1000.0)),
        F.current_timestamp(),
    )
def process_customers_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of customer CDC events and MERGE into Iceberg."""
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession
    settings = Settings()
    # Filter out null payloads (tombstones) and parse
    valid_records = (
        batch_df.filter(F.col("value").isNotNull())
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), CDC_CUSTOMERS_SCHEMA).alias("payload"),
        )
        .select("kafka_offset", "kafka_timestamp", "payload.*")
        .filter(F.col("customer_id").isNotNull())
    )
    if valid_records.isEmpty():
        return
    # Deduplicate within this micro-batch: keep the highest Kafka offset per customer_id
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("kafka_offset").desc())
    deduped = (
        valid_records.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("parsed_updated_at", robust_timestamp("updated_at"))
        .withColumn(
            "source_operation",
            F.when(F.col("__deleted") == "true", F.lit("DELETE")).otherwise(F.lit("UPSERT")),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    count = deduped.count()
    if count == 0:
        deduped.unpersist()
        return
    deduped.createOrReplaceTempView("customer_cdc_updates")
    merge_sql = f"""
    MERGE INTO {settings.T_DIM_CUSTOMERS} t
    USING customer_cdc_updates s
    ON t.customer_id = s.customer_id
    WHEN MATCHED AND s.__deleted = 'true' THEN
        DELETE
    WHEN MATCHED THEN
        UPDATE SET
            t.full_name = s.full_name,
            t.email = s.email,
            t.age = s.age,
            t.home_country = s.home_country,
            t.risk_segment = s.risk_segment,
            t.updated_at = s.parsed_updated_at,
            t.source_operation = s.source_operation,
            t.source_kafka_offset = s.kafka_offset,
            t.synced_at = current_timestamp()
    WHEN NOT MATCHED AND (s.__deleted IS NULL OR s.__deleted != 'true') THEN
        INSERT (
            customer_id, full_name, email, age, home_country,
            risk_segment, updated_at, source_operation, source_kafka_offset, synced_at
        ) VALUES (
            s.customer_id, s.full_name, s.email, s.age, s.home_country,
            s.risk_segment, s.parsed_updated_at, s.source_operation, s.kafka_offset, current_timestamp()
        )
    """
    spark.sql(merge_sql)
    deduped.unpersist()
    print(f"[CDC batch {batch_id}] Synced {count} customer change event(s) to Iceberg dims", flush=True)
def process_merchants_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch of merchant CDC events and MERGE into Iceberg."""
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession
    settings = Settings()
    valid_records = (
        batch_df.filter(F.col("value").isNotNull())
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), CDC_MERCHANTS_SCHEMA).alias("payload"),
        )
        .select("kafka_offset", "kafka_timestamp", "payload.*")
        .filter(F.col("merchant_id").isNotNull())
    )
    if valid_records.isEmpty():
        return
    window_spec = Window.partitionBy("merchant_id").orderBy(F.col("kafka_offset").desc())
    deduped = (
        valid_records.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("parsed_updated_at", robust_timestamp("updated_at"))
        .withColumn(
            "source_operation",
            F.when(F.col("__deleted") == "true", F.lit("DELETE")).otherwise(F.lit("UPSERT")),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    count = deduped.count()
    if count == 0:
        deduped.unpersist()
        return
    deduped.createOrReplaceTempView("merchant_cdc_updates")
    merge_sql = f"""
    MERGE INTO {settings.T_DIM_MERCHANTS} t
    USING merchant_cdc_updates s
    ON t.merchant_id = s.merchant_id
    WHEN MATCHED AND s.__deleted = 'true' THEN
        DELETE
    WHEN MATCHED THEN
        UPDATE SET
            t.merchant_name = s.merchant_name,
            t.category = s.category,
            t.country = s.country,
            t.is_high_risk = s.is_high_risk,
            t.updated_at = s.parsed_updated_at,
            t.source_operation = s.source_operation,
            t.source_kafka_offset = s.kafka_offset,
            t.synced_at = current_timestamp()
    WHEN NOT MATCHED AND (s.__deleted IS NULL OR s.__deleted != 'true') THEN
        INSERT (
            merchant_id, merchant_name, category, country,
            is_high_risk, updated_at, source_operation, source_kafka_offset, synced_at
        ) VALUES (
            s.merchant_id, s.merchant_name, s.category, s.country,
            s.is_high_risk, s.parsed_updated_at, s.source_operation, s.kafka_offset, current_timestamp()
        )
    """
    spark.sql(merge_sql)
    deduped.unpersist()
    print(f"[CDC batch {batch_id}] Synced {count} merchant change event(s) to Iceberg dims", flush=True)
def main() -> None:
    settings = Settings()
    spark = build_spark("cdc-dimensions-sync")
    spark.sparkContext.setLogLevel("WARN")
    ensure_dim_tables(spark, settings)
    checkpoint_root = settings.CHECKPOINTS.rstrip("/")
    # Stream 1: Customers CDC
    customers_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("subscribe", settings.TOPIC_CDC_CUSTOMERS)
        .option("startingOffsets", settings.KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .writeStream.queryName("cdc-sync-customers")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{checkpoint_root}/job3/customers")
        .foreachBatch(process_customers_batch)
        .start()
    )
    # Stream 2: Merchants CDC
    merchants_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("subscribe", settings.TOPIC_CDC_MERCHANTS)
        .option("startingOffsets", settings.KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .writeStream.queryName("cdc-sync-merchants")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{checkpoint_root}/job3/merchants")
        .foreachBatch(process_merchants_batch)
        .start()
    )
    print("\nCDC Dimensions Sync stream is running.", flush=True)
    print(f"  Kafka brokers      : {settings.KAFKA_BOOTSTRAP}", flush=True)
    print(f"  Customers topic    : {settings.TOPIC_CDC_CUSTOMERS} -> {settings.T_DIM_CUSTOMERS}", flush=True)
    print(f"  Merchants topic    : {settings.TOPIC_CDC_MERCHANTS} -> {settings.T_DIM_MERCHANTS}", flush=True)
    print(f"  Trigger interval   : 5 seconds\n", flush=True)
    queries = [customers_stream, merchants_stream]
    try:
        spark.streams.awaitAnyTermination()
        failed = next((q for q in queries if q.exception()), None)
        if failed is not None:
            raise RuntimeError(f"Streaming query {failed.name} terminated: {failed.exception()}")
    except KeyboardInterrupt:
        print("Stopping CDC streaming queries...", flush=True)
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
if __name__ == "__main__":
    main()