"""
common/storage.py — read and write small files on MinIO (s3a://) from Spark jobs.

Used for the model registry: the champion's metrics JSON and the run archive.
Works through the JVM's Hadoop FileSystem, so the same code also works on local
paths (file://) for testing.
"""
from __future__ import annotations

import json

from pyspark.sql import SparkSession


def _hadoop_handles(spark: SparkSession, uri: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return path, fs


def path_exists(spark: SparkSession, uri: str) -> bool:
    path, fs = _hadoop_handles(spark, uri)
    return bool(fs.exists(path))


def read_json(spark: SparkSession, uri: str) -> dict | None:
    """Return the parsed JSON at `uri`, or None if it does not exist yet."""
    if not path_exists(spark, uri):
        return None
    text = "\n".join(row.value for row in spark.read.text(uri).collect())
    return json.loads(text)


def write_json(spark: SparkSession, uri: str, payload: dict) -> None:
    """Overwrite `uri` with pretty JSON."""
    path, fs = _hadoop_handles(spark, uri)
    out = fs.create(path, True)
    try:
        out.write(bytearray((json.dumps(payload, indent=2) + "\n").encode("utf-8")))
    finally:
        out.close()
