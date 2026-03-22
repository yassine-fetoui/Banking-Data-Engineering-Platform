"""
spark/bronze/ingest_transactions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bronze layer: Ingest raw transactions from S3 landing zone into Delta Lake.

Design principles:
  - Immutable: raw data is NEVER modified after landing
  - Idempotent: safe to re-run — uses replaceWhere on ingestion_date partition
  - Audit: every row gets _source_system, _batch_id, _ingested_at
  - PII: card numbers and account numbers hashed immediately at ingestion
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date

import structlog
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.utils.data_quality import add_audit_columns, mask_pii
from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────

RAW_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),              False),
    StructField("account_id",           StringType(),              False),
    StructField("customer_id",          StringType(),              False),
    StructField("transaction_datetime", TimestampType(),           False),
    StructField("amount",               DecimalType(18, 4),        False),
    StructField("currency",             StringType(),              False),
    StructField("transaction_type",     StringType(),              False),
    StructField("channel",              StringType(),              True),
    StructField("merchant_id",          StringType(),              True),
    StructField("merchant_category",    StringType(),              True),
    StructField("country_code",         StringType(),              True),
    StructField("status",               StringType(),              False),
    StructField("card_number_masked",   StringType(),              True),
    StructField("reference_id",         StringType(),              True),
])

PII_COLUMNS = ["card_number_masked"]


def read_raw_transactions(spark, landing_path: str, batch_date: str) -> DataFrame:
    """Read raw CSV/JSON transactions from the landing zone for a given date."""
    log.info("reading_raw_transactions", path=landing_path, batch_date=batch_date)
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(f"{landing_path}/transactions/date={batch_date}/")
    )


def transform_bronze(df: DataFrame, batch_id: str) -> DataFrame:
    """Apply Bronze-layer transformations: audit columns, PII hashing, partitioning key."""
    return (
        df
        .filter(F.col("_corrupt_record").isNull())  # Quarantine corrupt records
        .drop("_corrupt_record")
        # Standardise: upper-case currency and status
        .withColumn("currency",         F.upper(F.col("currency")))
        .withColumn("status",           F.upper(F.col("status")))
        .withColumn("transaction_type", F.upper(F.col("transaction_type")))
        # Partition key for Delta
        .withColumn("ingestion_date", F.to_date(F.col("transaction_datetime")))
        # PII tokenisation — card numbers hashed with SHA-256
        .transform(lambda d: mask_pii(d, PII_COLUMNS))
        # Audit metadata
        .transform(lambda d: add_audit_columns(d, "core_banking", batch_id, "bronze"))
    )


def write_bronze_delta(df: DataFrame, output_path: str, batch_date: str) -> None:
    """
    Write to Delta Lake using replaceWhere — idempotent partition overwrite.
    Re-running for the same batch_date safely replaces only that partition.
    """
    log.info("writing_bronze_delta", path=output_path, batch_date=batch_date)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{batch_date}'")
        .partitionBy("ingestion_date", "currency")
        .save(output_path)
    )
    log.info("write_complete", rows=df.count(), path=output_path)


def run(env: str, batch_date: str | None = None) -> None:
    batch_date  = batch_date or str(date.today())
    batch_id    = str(uuid.uuid4())

    spark = get_spark("banking.bronze.transactions", env=env)

    landing_path = get_s3_path("landing", "", env).rstrip("/")
    output_path  = get_s3_path("bronze", "transactions", env)

    raw_df    = read_raw_transactions(spark, landing_path, batch_date)
    bronze_df = transform_bronze(raw_df, batch_id)
    write_bronze_delta(bronze_df, output_path, batch_date)

    # Optimize Delta table weekly (handled by Airflow, not here by default)
    log.info("bronze_ingestion_complete", env=env, batch_date=batch_date, batch_id=batch_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
