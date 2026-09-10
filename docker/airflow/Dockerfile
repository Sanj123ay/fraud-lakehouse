# ============================================================================
#  Custom Airflow image: official Airflow 2.8 + a Spark *client*.
#
#  Airflow's job is orchestration only — it doesn't crunch data itself.
#  Installing the `pyspark` pip package gives Airflow a `spark-submit`
#  binary it can use (via BashOperator / SparkSubmitOperator) to send
#  training jobs to the real Spark cluster (spark-master:7077).
# ============================================================================
FROM apache/airflow:2.8.0

USER root
# spark-submit is a Java program -> Airflow needs a Java runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
      pyspark==3.5.1 \
      apache-airflow-providers-apache-spark==4.7.1 \
      psycopg2-binary==2.9.9
