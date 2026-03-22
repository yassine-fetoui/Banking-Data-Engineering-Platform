"""
spark/bronze/ingest_customers.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bronze layer: Ingest raw customer master data (KYC) into Delta Lake.

- Schema enforced on read (reject malformed records to quarantine)
- PII (national_id, full_name) hashed immediately at Bronze
- Idempotent: replaceWhere on ingestion_date partition
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.utils.data_quality import DataQualityChecker, add_audit_columns, mask_pii
from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",          StringType(),    False),
    StructField("full_name",            StringType(),    True),
    StructField("national_id",          StringType(),    True),
    StructField("date_of_birth",        DateType(),      True),
    StructField("nationality",          StringType(),    True),
    StructField("risk_rating",          StringType(),    True),
    StructField("kyc_status",           StringType(),    False),
    StructField("kyc_review_date",      DateType(),      True),
    StructField("customer_segment",     StringType(),    True),
    StructField("relationship_manager", StringType(),    True),
    StructField("onboarding_date",      DateType(),      True),
    StructField("last_updated",         TimestampType(), True),
    StructField("_corrupt_record",      StringType(),    True),
])

PII_COLS = ["full_name", "national_id"]


def transform_bronze_customers(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df
        .filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("kyc_status",       F.upper(F.col("kyc_status")))
        .withColumn("risk_rating",      F.upper(F.col("risk_rating")))
        .withColumn("customer_segment", F.upper(F.col("customer_segment")))
        .withColumn("ingestion_date",   F.current_date())
        .transform(lambda d: mask_pii(d, PII_COLS))
        .transform(lambda d: add_audit_columns(d, "kyc_portal", batch_id, "bronze"))
    )


def run(env: str, batch_date: str | None = None) -> None:
    batch_date = batch_date or str(date.today())
    batch_id   = str(uuid.uuid4())
    spark      = get_spark("banking.bronze.customers", env=env)

    landing = get_s3_path("landing", "", env).rstrip("/")
    output  = get_s3_path("bronze", "customers", env)

    raw = (
        spark.read
        .schema(CUSTOMER_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(f"{landing}/customers/date={batch_date}/")
    )

    bronze = transform_bronze_customers(raw, batch_id)

    DataQualityChecker(bronze, "bronze.customers") \
        .not_null(["customer_id", "kyc_status"]) \
        .value_in("kyc_status", ["PENDING", "APPROVED", "SUSPENDED", "CLOSED"]) \
        .row_count_at_least(1) \
        .run()

    (
        bronze.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{batch_date}'")
        .partitionBy("ingestion_date", "kyc_status")
        .save(output)
    )
    log.info("bronze_customers_complete", env=env, batch_date=batch_date, rows=bronze.count())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
