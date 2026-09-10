"""
common/schemas.py — the DATA CONTRACTS.

Every JSON that flows through this platform is parsed against a schema here.
When producer (NiFi) and consumers (Spark jobs) agree on these shapes,
nothing silently breaks — this is what "schema on write" feels like.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType,
)

# --- A raw transaction, exactly as the NiFi generator emits it ---
TRANSACTION_SCHEMA = StructType([
    StructField("txn_id", StringType(), False),          # unique id (UUID)
    StructField("card_id", StringType(), False),         # the physical card
    StructField("customer_id", StringType(), True),      # joins dims.customers
    StructField("merchant_id", StringType(), True),      # joins dims.merchants
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),          # where the swipe happened
    StructField("channel", StringType(), True),          # pos | ecom | atm
    StructField("txn_ts", StringType(), False),          # ISO-8601 event time
    StructField("is_fraud", IntegerType(), True),        # ground-truth label (training only)
])

# --- A Debezium CDC event (after ExtractNewRecordState unwrap) ---
# Same shape as the source table + __deleted flag added by the transform.
CDC_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("home_country", StringType(), True),
    StructField("risk_segment", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("__deleted", StringType(), True),        # "true" => row was deleted
])

CDC_MERCHANTS_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_high_risk", BooleanType(), True),
    StructField("updated_at", StringType(), True),
    StructField("__deleted", StringType(), True),
])
