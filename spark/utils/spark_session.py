"""
spark/utils/spark_session.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
Centralised SparkSession factory with Delta Lake, S3, and environment-aware config.
All Spark jobs import get_spark() — never create their own session.
"""
from __future__ import annotations

import os
from enum import Enum

from pyspark.sql import SparkSession


class Environment(str, Enum):
    LOCAL = "local"
    DEV   = "dev"
    PROD  = "prod"


# ── Environment configs ───────────────────────────────────────────────────────

_CONFIGS: dict[str, dict] = {
    "local": {
        "spark.master":                          "local[*]",
        "spark.sql.shuffle.partitions":          "8",
        "spark.sql.adaptive.enabled":            "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.sql.extensions":                  "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Local Delta Lake warehouse
        "spark.sql.warehouse.dir":               "/tmp/banking_warehouse",
    },
    "dev": {
        "spark.master":                          "yarn",
        "spark.executor.instances":              "4",
        "spark.executor.memory":                 "8g",
        "spark.executor.cores":                  "4",
        "spark.driver.memory":                   "4g",
        "spark.sql.shuffle.partitions":          "200",
        "spark.sql.adaptive.enabled":            "true",
        "spark.sql.adaptive.skewJoin.enabled":   "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.extensions":                  "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.sql.warehouse.dir":               "s3a://banking-data-lake-dev/warehouse",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    },
    "prod": {
        "spark.master":                          "yarn",
        "spark.executor.instances":              "20",
        "spark.executor.memory":                 "16g",
        "spark.executor.cores":                  "5",
        "spark.driver.memory":                   "8g",
        "spark.sql.shuffle.partitions":          "800",
        "spark.sql.adaptive.enabled":            "true",
        "spark.sql.adaptive.skewJoin.enabled":   "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.extensions":                  "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.sql.warehouse.dir":               "s3a://banking-data-lake-prod/warehouse",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.optimize.maxFileSize":      str(128 * 1024 * 1024),
    },
}


def get_spark(
    app_name: str,
    env: str | Environment = Environment.LOCAL,
    extra_config: dict | None = None,
) -> SparkSession:
    """
    Return (or create) a SparkSession for the given environment.

    Args:
        app_name:     Descriptive name shown in Spark UI / logs.
        env:          One of 'local', 'dev', 'prod'.
        extra_config: Any job-specific overrides merged on top.
    """
    env_str = str(env)
    base = _CONFIGS.get(env_str, _CONFIGS["local"]).copy()
    if extra_config:
        base.update(extra_config)

    builder = SparkSession.builder.appName(app_name)
    for k, v in base.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_s3_path(layer: str, table: str, env: str = "local") -> str:
    """Return the canonical S3 / local path for a Delta table."""
    if env == "local":
        return f"/tmp/banking_warehouse/{layer}/{table}"
    bucket = f"banking-data-lake-{env}"
    return f"s3a://{bucket}/{layer}/{table}"
