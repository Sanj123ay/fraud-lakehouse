# ============================================================================
#  STEP 5 FILE — Airflow DAG: nightly model training
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL CONTAIN (we build it in Step 5):
#
#    from airflow import DAG
#    schedule = "0 2 * * *"          # 02:00 every night
#
#    task spark-submit -> /opt/airflow/spark_jobs/batch/job2_train_model.py
#         (BashOperator or SparkSubmitOperator against spark://spark-master:7077)
#
#    task quality_gate  -> fail loudly if AUC dropped vs yesterday
#    task notify        -> print a summary / placeholder for Slack webhook
#
#  You'll see it as a graph (boxes + arrows) in http://localhost:8085,
#  toggle it ON, watch it run green, and read task logs by clicking them.
# ============================================================================
