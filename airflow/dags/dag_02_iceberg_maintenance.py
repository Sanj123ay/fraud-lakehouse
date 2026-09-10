# ============================================================================
#  STEP 7 FILE — Airflow DAG: Iceberg housekeeping + time-travel demo
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL CONTAIN (we build it in Step 7):
#
#    Every 6 hours:
#      CALL lakehouse.system.expire_snapshots('lakehouse.raw.transactions', ...)
#      CALL lakehouse.system.rewrite_manifests(...)      -- compaction hygiene
#
#    Plus a "show off" task that runs a TIME TRAVEL query and logs the result:
#      SELECT count(*) FROM lakehouse.raw.transactions
#      TIMESTAMP AS OF '<yesterday>'      -- the past, queryable!
#    and FOR SYSTEM_VERSION AS OF <snapshot_id> equivalent via the
#      `lakehouse.raw.transactions.history` metadata table.
#
#  Why it matters: every commit in Iceberg = a snapshot = a restorable,
#  queryable version of your table. But snapshots cost storage, so a real
#  platform expires them on a schedule. That schedule is this DAG.
# ============================================================================
