# ============================================================================
#  STEP 3 + STEP 6 FILE — the main streaming pipeline (the star of the show)
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL DO (we build it in Step 3, extend with ML in Step 6):
#
#    1. READ     Kafka topic `raw_transactions` as a streaming DataFrame
#                (spark.readStream, subscribe, 10-second trigger)
#    2. PARSE    JSON -> typed columns via common/schemas.TRANSACTION_SCHEMA
#                cast txn_ts -> event_time (timestamp), drop malformed rows
#    3. WATERMARK withWatermark("event_time", "10 minutes")
#                => Spark tolerates 10 min of lateness, then forgets old state
#    4. FEATURES rolling aggregates per card_id:
#                 - count_5min   : window("event_time", "5 minutes")
#                 - amount_sum_1h: window("event_time", "1 hour")
#                (we'll discuss sliding vs tumbling windows while writing it)
#    5. ENRICH   broadcast-join with lakehouse.dims.customers / dims.merchants
#                (kept fresh by the CDC job — Step 4)
#    6. SCORE    (Step 6) load PipelineModel from s3a://models/fraud_rf/latest
#                inside foreachBatch, call model.transform(batchDF)
#    7. WRITE    foreachBatch fan-out:
#                 - Iceberg lakehouse.raw.transactions      (full history)
#                 - Iceberg lakehouse.features.txn_features_1h (feature store)
#                 - Postgres fraud_ops.predictions          (JDBC, overwrite by key)
#                 - Redis  fraud:card:<id> = score          (online store)
#    8. FAULT-TOLERANCE  checkpointLocation under s3a://checkpoints/job1
#
#  RUN (once built):   make stream
# ============================================================================

#!/usr/bin/env python3
"""Kafka -> validated Iceberg transactions + rolling feature store.
Step 3 responsibilities
-----------------------
* subscribe continuously to Kafka topic ``raw_transactions``;
* parse the NiFi JSON using the shared data contract;
* route malformed records to an Iceberg quarantine table;
* apply a 10-minute event-time watermark and bounded txn-id deduplication;
* enrich each micro-batch from the latest Iceberg customer/merchant dimensions;
* idempotently MERGE raw transactions into Iceberg;
* calculate exact per-transaction ``count_5min`` and ``amount_sum_1h``;
* recompute later affected rows when an out-of-order transaction arrives;
* checkpoint Kafka offsets and streaming state in MinIO.
Step 6 will extend the same ``process_valid_batch`` function with model scoring
and Postgres/Redis prediction sinks.
"""
from __future__ import annotations
import sys
from datetime import timedelta
from pathlib import Path
# spark-submit runs this file from streaming/. Add spark_jobs/ so `common`
# imports work on both the driver and mounted worker filesystem.
SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_ROOT))
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from common.config import Settings, build_spark
from common.schemas import TRANSACTION_SCHEMA
RAW_COLUMNS = [
    "txn_id",
    "card_id",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "channel",
    "event_time",
    "is_fraud",
    "customer_age",
    "customer_home_country",
    "customer_risk_segment",
    "merchant_category",
    "merchant_country",
    "merchant_is_high_risk",
    "country_mismatch",
    "fraud_score",
    "is_fraud_pred",
    "model_version",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "processed_at",
]
FEATURE_COLUMNS = [
    "txn_id",
    "card_id",
    "customer_id",
    "merchant_id",
    "event_time",
    "amount",
    "currency",
    "country",
    "channel",
    "count_5min",
    "amount_sum_1h",
    "country_mismatch",
    "merchant_is_high_risk",
    "customer_risk_segment",
    "feature_version",
    "computed_at",
    "source_batch_id",
]
def create_iceberg_objects(spark: SparkSession, settings: Settings) -> None:
    """Create namespaces and empty tables before starting streaming queries."""
    for namespace in ("raw", "features", "dims"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {settings.CATALOG}.{namespace}")
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
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.T_RAW} (
            txn_id STRING,
            card_id STRING,
            customer_id STRING,
            merchant_id STRING,
            amount DOUBLE,
            currency STRING,
            country STRING,
            channel STRING,
            event_time TIMESTAMP,
            is_fraud INT,
            customer_age INT,
            customer_home_country STRING,
            customer_risk_segment STRING,
            merchant_category STRING,
            merchant_country STRING,
            merchant_is_high_risk BOOLEAN,
            country_mismatch BOOLEAN,
            fraud_score DOUBLE,
            is_fraud_pred BOOLEAN,
            model_version STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP,
            processed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'write.distribution-mode'='hash',
            'commit.retry.num-retries'='10'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.T_FEATURES} (
            txn_id STRING,
            card_id STRING,
            customer_id STRING,
            merchant_id STRING,
            event_time TIMESTAMP,
            amount DOUBLE,
            currency STRING,
            country STRING,
            channel STRING,
            count_5min BIGINT,
            amount_sum_1h DOUBLE,
            country_mismatch BOOLEAN,
            merchant_is_high_risk BOOLEAN,
            customer_risk_segment STRING,
            feature_version STRING,
            computed_at TIMESTAMP,
            source_batch_id BIGINT
        ) USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'write.distribution-mode'='hash',
            'commit.retry.num-retries'='10'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.T_QUARANTINE} (
            raw_payload STRING,
            validation_error STRING,
            parsed_txn_id STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            quarantined_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='zstd',
            'write.spark.fanout.enabled'='true',
            'commit.retry.num-retries'='10'
        )
        """
    )
def read_and_parse_kafka(spark: SparkSession, settings: Settings) -> DataFrame:
    """Return a streaming DataFrame with parsed fields and validation errors."""
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("subscribe", settings.TOPIC_RAW)
        .option("startingOffsets", settings.KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", settings.KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        kafka.select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", F.from_json("raw_payload", TRANSACTION_SCHEMA))
        .select("raw_payload", "data.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp("txn_ts"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    uuid_pattern = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
        r"[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )
    checks = [
        F.when(F.col("txn_id").isNull(), F.lit("missing or malformed JSON/txn_id")),
        F.when(
            F.col("txn_id").isNotNull() & ~F.col("txn_id").rlike(uuid_pattern),
            F.lit("txn_id is not a UUID"),
        ),
        F.when(F.col("card_id").isNull(), F.lit("missing card_id")),
        F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
        F.when(F.col("merchant_id").isNull(), F.lit("missing merchant_id")),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("amount must be positive")),
        F.when(F.col("currency").isNull(), F.lit("missing currency")),
         F.when(F.col("country").isNull(), F.lit("missing country")),
        F.when(
            F.col("channel").isNull() | ~F.col("channel").isin("pos", "ecom", "atm"),
            F.lit("channel must be pos, ecom, or atm"),
        ),
        F.when(F.col("event_time").isNull(), F.lit("txn_ts is not a timestamp")),
        F.when(
            F.col("event_time") > F.expr("current_timestamp() + INTERVAL 5 MINUTES"),
            F.lit("txn_ts is more than 5 minutes in the future"),
        ),
        F.when(
            F.col("is_fraud").isNull() | ~F.col("is_fraud").isin(0, 1),
            F.lit("is_fraud must be 0 or 1"),
        ),
    ]
    return parsed.withColumn("validation_error", F.concat_ws(" | ", *checks))
def enrich_with_dimensions(batch: DataFrame, spark: SparkSession, settings: Settings) -> DataFrame:
    """Left-join the latest Iceberg dimensions; empty dims remain safe/null."""
    spark.catalog.refreshTable(settings.T_DIM_CUSTOMERS)
    spark.catalog.refreshTable(settings.T_DIM_MERCHANTS)
    customers = F.broadcast(
        spark.table(settings.T_DIM_CUSTOMERS).select(
            "customer_id",
            F.col("age").alias("customer_age"),
            F.col("home_country").alias("customer_home_country"),
            F.col("risk_segment").alias("customer_risk_segment"),
        )
    )
    merchants = F.broadcast(
        spark.table(settings.T_DIM_MERCHANTS).select(
            "merchant_id",
            F.col("category").alias("merchant_category"),
            F.col("country").alias("merchant_country"),
            F.col("is_high_risk").alias("merchant_is_high_risk"),
        )
    )
    return (
        batch.join(customers, "customer_id", "left")
        .join(merchants, "merchant_id", "left")
        .withColumn(
            "country_mismatch",
            F.when(
                F.col("country").isNull() | F.col("customer_home_country").isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("country") != F.col("customer_home_country")),
        )
        # Step 6 fills these columns; defining them now makes schema evolution unnecessary.
        .withColumn("fraud_score", F.lit(None).cast("double"))
        .withColumn("is_fraud_pred", F.lit(None).cast("boolean"))
        .withColumn("model_version", F.lit(None).cast("string"))
        .withColumn("processed_at", F.current_timestamp())
        .select(*RAW_COLUMNS)
    )
def merge_raw(spark: SparkSession, updates: DataFrame, settings: Settings) -> None:
    """Insert unseen transaction IDs; retries/replays cannot duplicate history."""
    updates.createOrReplaceTempView("raw_microbatch_updates")
    insert_columns = ", ".join(RAW_COLUMNS)
    insert_values = ", ".join(f"s.{column}" for column in RAW_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {settings.T_RAW} t
        USING raw_microbatch_updates s
        ON t.txn_id = s.txn_id
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )
def calculate_affected_features(
    spark: SparkSession,
    incoming: DataFrame,
    settings: Settings,
    batch_id: int,
) -> DataFrame:
    """Calculate rolling features and correct rows affected by late events.
    A newly arrived event at time T can change a later event's 5-minute count
    until T+5m and its 1-hour amount sum until T+1h. We therefore recompute the
    one-hour affected horizon for only cards present in this micro-batch.
    """
    bounds = incoming.agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("max_event_time"),
    ).first()
    lower_bound = bounds["min_event_time"] - timedelta(hours=1)
    upper_bound = bounds["max_event_time"] + timedelta(hours=1)
    active_cards = F.broadcast(incoming.select("card_id").distinct())
    history = (
        spark.table(settings.T_RAW)
        .where(
            (F.col("event_time") >= F.lit(lower_bound))
            & (F.col("event_time") <= F.lit(upper_bound))
        )
        .join(active_cards, "card_id", "inner")
        .dropDuplicates(["txn_id"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    new_events = F.broadcast(
        incoming.select(
            F.col("card_id").alias("new_card_id"),
            F.col("event_time").alias("new_event_time"),
        ).distinct()
    )
    affected_keys = (
        history.alias("h")
        .join(
            new_events.alias("n"),
            (F.col("h.card_id") == F.col("n.new_card_id"))
            & (F.col("h.event_time") >= F.col("n.new_event_time"))
            & (F.col("h.event_time") <= F.col("n.new_event_time") + F.expr("INTERVAL 1 HOUR")),
            "inner",
        )
        .select(F.col("h.txn_id").alias("txn_id"))
        .distinct()
    )
    ordered = history.withColumn("_event_epoch", F.col("event_time").cast("long"))
    five_minutes = (
        Window.partitionBy("card_id")
        .orderBy(F.col("_event_epoch"))
        .rangeBetween(-5 * 60, 0)
    )
    one_hour = (
        Window.partitionBy("card_id")
        .orderBy(F.col("_event_epoch"))
        .rangeBetween(-60 * 60, 0)
    )
    features = (
        ordered.withColumn("count_5min", F.count(F.lit(1)).over(five_minutes))
        .withColumn("amount_sum_1h", F.round(F.sum("amount").over(one_hour), 2))
        .join(affected_keys, "txn_id", "inner")
        .withColumn("feature_version", F.lit(settings.FEATURE_VERSION))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("source_batch_id", F.lit(batch_id).cast("long"))
        .select(*FEATURE_COLUMNS)
    )
    # The result still depends on cached history. Materialize it before cleanup.
    features = features.persist(StorageLevel.MEMORY_AND_DISK)
    features.count()
    history.unpersist()
    return features
def merge_features(spark: SparkSession, updates: DataFrame, settings: Settings) -> None:
    """Upsert features because late events can legitimately revise prior rows."""
    updates.createOrReplaceTempView("feature_microbatch_updates")
    update_assignments = ",\n            ".join(
        f"t.{column} = s.{column}" for column in FEATURE_COLUMNS if column != "txn_id"
    )
    insert_columns = ", ".join(FEATURE_COLUMNS)
    insert_values = ", ".join(f"s.{column}" for column in FEATURE_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {settings.T_FEATURES} t
        USING feature_microbatch_updates s
        ON t.txn_id = s.txn_id
        WHEN MATCHED THEN UPDATE SET
            {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )
def process_valid_batch(batch: DataFrame, batch_id: int) -> None:
    """Idempotently commit one valid Kafka micro-batch and its features."""
    if batch.isEmpty():
        print(f"[batch {batch_id}] no valid records", flush=True)
        return
    spark = batch.sparkSession
    settings = Settings()
    incoming = enrich_with_dimensions(
        batch.dropDuplicates(["txn_id"]), spark, settings
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        input_count = incoming.count()  # fully materialize stateful batch output
        merge_raw(spark, incoming, settings)
        spark.catalog.refreshTable(settings.T_RAW)
        features = calculate_affected_features(spark, incoming, settings, batch_id)
        try:
            feature_count = features.count()
            merge_features(spark, features, settings)
        finally:
            features.unpersist()
        print(
            f"[batch {batch_id}] raw input={input_count}, "
            f"feature rows inserted/recomputed={feature_count}",
            flush=True,
        )
    finally:
        incoming.unpersist()
def main() -> None:
    settings = Settings()
    spark = build_spark("fraud-streaming-features")
    spark.sparkContext.setLogLevel("WARN")
    create_iceberg_objects(spark, settings)
    parsed = read_and_parse_kafka(spark, settings)
    valid = (
        parsed.where(F.length("validation_error") == 0)
        .select(
            "txn_id",
            "card_id",
            "customer_id",
            "merchant_id",
            "amount",
            "currency",
            "country",
            "channel",
            "event_time",
            "is_fraud",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
        )
        .withWatermark("event_time", settings.EVENT_WATERMARK)
        .dropDuplicatesWithinWatermark(["txn_id"])
    )
    invalid = parsed.where(F.length("validation_error") > 0).select(
        "raw_payload",
        "validation_error",
        F.col("txn_id").alias("parsed_txn_id"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("quarantined_at"),
    )
    checkpoint_root = settings.CHECKPOINTS.rstrip("/")
    quarantine_query = (
        invalid.writeStream.queryName("fraud-transaction-quarantine")
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=settings.STREAM_TRIGGER)
        .option("checkpointLocation", f"{checkpoint_root}/job1/quarantine")
        .option("fanout-enabled", "true")
        .toTable(settings.T_QUARANTINE)
    )
    main_query = (
        valid.writeStream.queryName("fraud-raw-and-features")
        .outputMode("append")
        .trigger(processingTime=settings.STREAM_TRIGGER)
        .option("checkpointLocation", f"{checkpoint_root}/job1/main")
        .foreachBatch(process_valid_batch)
        .start()
    )
    print("\nFraud transaction stream is running.", flush=True)
    print(f"  Kafka source : {settings.KAFKA_BOOTSTRAP}/{settings.TOPIC_RAW}", flush=True)
    print(f"  Raw table    : {settings.T_RAW}", flush=True)
    print(f"  Feature table: {settings.T_FEATURES}", flush=True)
    print(f"  Quarantine   : {settings.T_QUARANTINE}", flush=True)
    print(f"  Trigger      : {settings.STREAM_TRIGGER}", flush=True)
    print(f"  Watermark    : {settings.EVENT_WATERMARK}\n", flush=True)
    queries = [main_query, quarantine_query]
    try:
        spark.streams.awaitAnyTermination()
        failed = next((query for query in queries if query.exception()), None)
        if failed is not None:
            raise RuntimeError(
                f"Streaming query {failed.name} terminated: {failed.exception()}"
            )
    except KeyboardInterrupt:
        print("Stopping streaming queries...", flush=True)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
if __name__ == "__main__":
    main()