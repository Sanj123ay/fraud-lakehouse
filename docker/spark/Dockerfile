# ============================================================================
#  Custom Spark image: OFFICIAL Apache Spark 3.5.1 + external JARs our jobs need.
#
#  Why a custom image? Spark out-of-the-box cannot read Kafka, Iceberg or S3.
#  Those live in separate Java libraries ("jars") that we bake into the image
#  once, instead of downloading them on every single job run.
#
#  Why apache/spark and not bitnami/spark?
#  Bitnami removed their free versioned images from Docker Hub in 2025.
#  apache/spark is published by the Spark project itself = most future-proof.
#  Difference: there is no SPARK_MODE env var; docker-compose starts the
#  master/worker daemons with explicit `command:` entries instead.
# ============================================================================
FROM apache/spark:3.5.1

USER root

ARG MVN=https://repo1.maven.org/maven2
ARG JARS=/opt/spark/jars

# Tools the plain official image doesn't ship.
# python3-venv is important: Ubuntu marks its system Python as externally
# managed (PEP 668), so packages must be installed in an isolated environment.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates python3-venv \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create one Python environment baked into both the Spark master and worker.
# Setting these variables makes PySpark use the same interpreter for its
# driver and executors, preventing package/version differences across nodes.
ENV SPARK_PYTHON_ENV=/opt/spark/venv
ENV PATH="${SPARK_PYTHON_ENV}/bin:${PATH}"
ENV PYSPARK_PYTHON=${SPARK_PYTHON_ENV}/bin/python3
ENV PYSPARK_DRIVER_PYTHON=${SPARK_PYTHON_ENV}/bin/python3

# Python libraries available on driver AND executors (worker nodes):
#  - redis    : streaming job writes latest-score-per-card into Redis
#  - pandas   : used by windowed/feature code paths
RUN python3 -m venv ${SPARK_PYTHON_ENV} \
 && ${SPARK_PYTHON_ENV}/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && ${SPARK_PYTHON_ENV}/bin/pip install --no-cache-dir redis==5.0.8 pandas==2.2.2 \
 && ${SPARK_PYTHON_ENV}/bin/python -c "import redis, pandas; print('Python dependencies installed successfully')"

# Java libraries (exact, pinned versions, downloaded from Maven Central)
#  - iceberg-spark-runtime : the lakehouse table format
#  - hadoop-aws + aws-bundle : lets Spark read/write S3 (MinIO) via s3a://
#  - spark-sql-kafka + kafka-clients + token-provider + commons-pool2
#                          : the Kafka streaming connector and its dependencies
#  - postgresql-jdbc       : write predictions into Postgres
RUN curl -fsSL -o $JARS/iceberg-spark-runtime.jar \
      $MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
 && curl -fsSL -o $JARS/hadoop-aws.jar \
      $MVN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o $JARS/aws-java-sdk-bundle.jar \
      $MVN/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar \
 && curl -fsSL -o $JARS/spark-sql-kafka.jar \
      $MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o $JARS/kafka-clients.jar \
      $MVN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSL -o $JARS/spark-token-provider-kafka.jar \
      $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o $JARS/commons-pool2.jar \
      $MVN/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && curl -fsSL -o $JARS/postgresql-jdbc.jar \
      $MVN/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Run as root for zero permission surprises in this LOCAL learning platform.
# (In a real production Kubernetes deployment you'd run as the spark UID, 185.)
USER root
