"""
common/config.py — ONE place for every endpoint, credential and path.

Why: beginners (and teams) break pipelines by hardcoding "minio:9000" in five
files. Here every job reads the same settings, overridable via environment
variables (docker-compose already injects them from .env).
"""
import os


class Settings:
    # --- Kafka ---
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw_transactions")
    TOPIC_CDC_CUSTOMERS = "cdc.public.customers"
    TOPIC_CDC_MERCHANTS = "cdc.public.merchants"

    # --- MinIO / S3A (Iceberg warehouse, model artifacts, checkpoints) ---
    S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
    S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")
    CHECKPOINTS = os.getenv("CHECKPOINTS_ROOT", "s3a://checkpoints")
    MODEL_PATH = os.getenv("MODEL_PATH", "s3a://models/fraud_rf/latest")

    # --- Iceberg catalog and table names (dot-notation: catalog.db.table) ---
    CATALOG = "lakehouse"
    T_RAW = "lakehouse.raw.transactions"
    T_FEATURES = "lakehouse.features.txn_features_1h"
    T_DIM_CUSTOMERS = "lakehouse.dims.customers"
    T_DIM_MERCHANTS = "lakehouse.dims.merchants"

    # --- Postgres (ops DB) : predictions sink ---
    PG_JDBC_URL = os.getenv(
        "PG_JDBC_URL",
        "jdbc:postgresql://postgres:5432/fraud_ops",
    )
    PG_USER = os.getenv("POSTGRES_USER", "airflow")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
    PG_PREDICTIONS_TABLE = "predictions"

    # --- Redis (online feature store): key pattern fraud:card:<card_id> ---
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def build_spark(app_name: str):
    """
    Create a SparkSession pre-wired for: Iceberg catalog on MinIO (S3A),
    Kafka source, and our job code. Every job calls this — config lives once,
    in code, where you can read exactly what's happening.
    """
    from pyspark.sql import SparkSession

    s = Settings()
    return (
        SparkSession.builder.appName(app_name)
        # ---- Iceberg catalog named "lakehouse", files stored in MinIO ----
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", s.ICEBERG_WAREHOUSE)
        # Iceberg extension gives us: MERGE INTO, time travel SQL, CALL maintenance
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # ---- S3A filesystem pointed at MinIO (not real AWS) ----
        .config("spark.hadoop.fs.s3a.endpoint", s.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", s.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", s.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   # MinIO needs this
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Spark jobs run on the worker; make our common/ package importable there
        .config("spark.submit.pyFiles", "/opt/spark/jobs")
        .getOrCreate()
    )
