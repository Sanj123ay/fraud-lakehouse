"""
Step 5 — Airflow DAG: nightly fraud-model training.

Schedule : every day at 02:00 India time (IST = UTC+5:30), which is 20:30 UTC.
           Airflow always schedules in UTC, so the cron below is "30 20 * * *".
Graph    : train_fraud_model
           The task submits spark_jobs/batch/job2_train_model.py to the Spark
           cluster. The quality gate (AUC floor and champion/challenger) lives
           inside the job, so a bad model makes this task fail and the
           champion model stays in place.

Watch it: http://localhost:8085  ->  DAGs -> dag_01_daily_training -> Graph.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "fraud-data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_01_daily_training",
    description="Nightly RandomForest fraud model training (Spark MLlib) with champion/challenger gate",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 20 * * *",  # 02:00 IST
    catchup=False,
    max_active_runs=1,  # never train two models at the same time
    tags=["ml", "training", "step5"],
) as dag:
    train_fraud_model = BashOperator(
        task_id="train_fraud_model",
        # NOTE: the command must NOT end in ".sh". Airflow's BashOperator treats a
        # string ending in .sh as a template *file name* and fails with
        # "TemplateNotFound". The trailing newline below avoids that (see docs/TROUBLESHOOTING_LOG.md, E-001).
        bash_command="set -e\nbash /opt/airflow/spark_jobs/batch/submit_training.sh\n",
    )
