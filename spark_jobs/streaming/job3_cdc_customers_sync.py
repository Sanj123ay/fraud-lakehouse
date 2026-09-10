# ============================================================================
#  STEP 4 FILE — CDC sync: Kafka (Debezium) -> Iceberg dimension tables
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL DO (we build it in Step 4, after CDC is live):
#
#    1. READ   Kafka topics `cdc.public.customers` and `cdc.public.merchants`
#              (events produced by Debezium whenever the bank DB changes)
#    2. PARSE  JSON via common/schemas.CDC_* schemas, honour __deleted flag
#    3. MERGE  foreachBatch -> for each micro-batch run Iceberg SQL:
#
#         MERGE INTO lakehouse.dims.customers t
#         USING updates s ON t.customer_id = s.customer_id
#         WHEN MATCHED AND s.__deleted = 'true' THEN DELETE
#         WHEN MATCHED THEN UPDATE SET *
#         WHEN NOT MATCHED AND s.__deleted = 'false' THEN INSERT *
#
#      => this is the moment you see WHY we use Iceberg and not raw parquet:
#         row-level UPDATE/DELETE on the data lake, with ACID.
#    4. BOOTSTRAP  on first run, snapshot the existing rows too
#                  (Debezium emits an initial snapshot automatically —
#                   we'll watch it happen in the Kafka console consumer)
#
#  RUN (once built):   make cdc-sync
# ============================================================================
