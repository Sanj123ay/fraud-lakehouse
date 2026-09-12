"""Shared runtime configuration for every Spark job.
All values have local Docker defaults and can be overridden with environment
variables. Keeping endpoints and table names here prevents configuration drift
between streaming, CDC, training, and maintenance jobs.
"""
from __future__ import annotations
import os
class Settings:
    # Kafka -------------------------------------------------------------------
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw_transactions")
    TOPIC_CDC_CUSTOMERS = "cdc.public.customers"
    TOPIC_CDC_MERCHANTS = "cdc.public.merchants"
    KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    KAFKA_MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")
    # Streaming behavior ------------------------------------------------------
    STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "10 seconds")
    EVENT_WATERMARK = os.getenv("EVENT_WATERMARK", "10 minutes")
    FEATURE_VERSION = os.getenv("FEATURE_VERSION", "v1")
    # MinIO / S3A -------------------------------------------------------------
    S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
    S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")
    CHECKPOINTS = os.getenv("CHECKPOINTS_ROOT", "s3a://checkpoints")
    MODEL_PATH = os.getenv("MODEL_PATH", "s3a://models/fraud_rf/latest")
    # Iceberg catalog and table names ----------------------------------------
    CATALOG = "lakehouse"
    T_RAW = "lakehouse.raw.transactions"
    T_QUARANTINE = "lakehouse.raw.transaction_quarantine"
    T_FEATURES = "lakehouse.features.txn_features_1h"
    T_DIM_CUSTOMERS = "lakehouse.dims.customers"
    T_DIM_MERCHANTS = "lakehouse.dims.merchants"
    # PostgreSQL operations database -----------------------------------------
    PG_JDBC_URL = os.getenv(
        "PG_JDBC_URL",
        "jdbc:postgresql://postgres:5432/fraud_ops",
    )
    PG_USER = os.getenv("POSTGRES_USER", "airflow")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
    PG_PREDICTIONS_TABLE = "predictions"
    # Redis online store ------------------------------------------------------
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
def build_spark(app_name: str):
    """Build a SparkSession configured for Iceberg tables on local MinIO."""
    
    from pyspark.sql import SparkSession
    settings = Settings()
    return (
        SparkSession.builder.appName(app_name)
        # Iceberg catalog named `lakehouse`.
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", settings.ICEBERG_WAREHOUSE)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Deterministic timestamps across developer laptops and containers.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        # S3A points at MinIO rather than AWS.
        .config("spark.hadoop.fs.s3a.endpoint", settings.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
