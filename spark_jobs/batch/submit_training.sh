#!/usr/bin/env bash
# ============================================================================
#  Submits job2 (model training) to the Spark cluster FROM THE AIRFLOW container.
#
#  Why this script exists:
#    The Airflow container has a Spark *client* (pip package pyspark), not the
#    Iceberg/S3 jars. So we tell spark-submit to fetch those jars with
#    --packages (same versions as docker/spark/Dockerfile) and to run the driver
#    inside the Airflow container while the work runs on spark-master/worker.
#
#  spark.driver.host=airflow makes the Spark workers call back to this container
#  by its Docker network name. Without it they would try an unreachable address.
#
#  Exit code = spark-submit exit code, so Airflow turns red when the job fails.
# ============================================================================
set -euo pipefail

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-airflow}"
# Keep these versions in sync with docker/spark/Dockerfile.
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.767}"

# The pip-installed pyspark ships its own spark-submit. Find it without guessing.
PYSPARK_BIN="$(python3 -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "bin"))')"

echo "Submitting fraud model training to ${SPARK_MASTER_URL} (driver host: ${SPARK_DRIVER_HOST})"

exec "${PYSPARK_BIN}/spark-submit" \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --name fraud-model-training \
  --packages "${SPARK_PACKAGES}" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --total-executor-cores 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  /opt/airflow/spark_jobs/batch/job2_train_model.py
