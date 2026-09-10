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
