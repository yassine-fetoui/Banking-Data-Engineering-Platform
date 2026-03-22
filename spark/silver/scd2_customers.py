"""
spark/silver/scd2_customers.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Silver layer: SCD Type 2 — track full KYC history for every customer.

Every KYC status change (PENDING → APPROVED → SUSPENDED → CLOSED) produces
a new history record. Regulators can query the exact KYC state at any point
in time using valid_from / valid_to / is_current.

Implemented with Delta Lake MERGE for ACID correctness.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime

import structlog
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
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
    StructField("customer_id",          StringType(),   False),
    StructField("full_name",            StringType(),   True),
    StructField("national_id",          StringType(),   True),  # Will be hashed
    StructField("date_of_birth",        DateType(),     True),
    StructField("nationality",          StringType(),   True),
    StructField("risk_rating",          StringType(),   True),  # LOW/MEDIUM/HIGH
    StructField("kyc_status",           StringType(),   False), # PENDING/APPROVED/SUSPENDED/CLOSED
    StructField("kyc_review_date",      DateType(),     True),
    StructField("customer_segment",     StringType(),   True),  # RETAIL/SME/CORPORATE
    StructField("relationship_manager", StringType(),   True),
    StructField("onboarding_date",      DateType(),     True),
    StructField("last_updated",         TimestampType(),True),
])

PII_COLS = ["full_name", "national_id"]

SCD2_SCHEMA = StructType([
    *CUSTOMER_SCHEMA.fields,
    StructField("valid_from",   TimestampType(), False),
    StructField("valid_to",     TimestampType(), True),
    StructField("is_current",   BooleanType(),   False),
    StructField("_record_hash", StringType(),    False),
    StructField("_batch_id",    StringType(),    False),
    StructField("_ingested_at", TimestampType(), False),
])

# Columns that define a "change" — any change triggers a new SCD2 record
TRACKED_COLUMNS = ["kyc_status", "risk_rating", "customer_segment", "relationship_manager"]


def _record_hash(df: DataFrame) -> DataFrame:
    """MD5 hash of tracked columns — change detection shortcut."""
    concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("NULL")) for c in TRACKED_COLUMNS])
    return df.withColumn("_record_hash", F.md5(concat_expr))


def upsert_scd2(
    spark: SparkSession,
    incoming: DataFrame,
    target_path: str,
    batch_id: str,
    effective_date: str,
) -> None:
    """
    Apply SCD Type 2 logic using Delta Lake MERGE.

    For each incoming customer:
      - If customer is new → INSERT as is_current=True
      - If tracked columns changed → expire current record + INSERT new record
      - If no change → no-op
    """
    now = F.lit(datetime.utcnow()).cast(TimestampType())
    far_future = F.lit("9999-12-31").cast(TimestampType())

    incoming = (
        incoming
        .transform(_record_hash)
        .withColumn("valid_from",  now)
        .withColumn("valid_to",    F.lit(None).cast(TimestampType()))
        .withColumn("is_current",  F.lit(True))
        .withColumn("_batch_id",   F.lit(batch_id))
        .withColumn("_ingested_at", now)
    )
    incoming.createOrReplaceTempView("incoming_customers")

    # Bootstrap table if it doesn't exist yet
    if not DeltaTable.isDeltaTable(spark, target_path):
        log.info("creating_scd2_table", path=target_path)
        incoming.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)

    # Step 1: Expire changed current records
    target.alias("tgt").merge(
        incoming.alias("src"),
        "tgt.customer_id = src.customer_id AND tgt.is_current = true AND tgt._record_hash != src._record_hash"
    ).whenMatchedUpdate(set={
        "is_current": F.lit(False),
        "valid_to":   F.current_timestamp(),
    }).execute()

    # Step 2: Insert new records for new customers OR changed customers
    new_and_changed = spark.sql("""
        SELECT src.*
        FROM incoming_customers src
        LEFT JOIN (
            SELECT customer_id FROM delta.`{path}` WHERE is_current = true
        ) existing ON src.customer_id = existing.customer_id
        WHERE existing.customer_id IS NULL
        UNION ALL
        SELECT src.*
        FROM incoming_customers src
        JOIN (
            SELECT customer_id, _record_hash FROM delta.`{path}` WHERE is_current = true
        ) existing ON src.customer_id = existing.customer_id
        WHERE src._record_hash != existing._record_hash
    """.replace("{path}", target_path))

    if new_and_changed.count() > 0:
        new_and_changed.write.format("delta").mode("append").save(target_path)
        log.info("scd2_records_inserted", count=new_and_changed.count())
    else:
        log.info("scd2_no_changes_detected")


def run(env: str, batch_date: str | None = None) -> None:
    batch_date = batch_date or str(date.today())
    batch_id   = str(uuid.uuid4())

    spark = get_spark("banking.silver.customers_scd2", env=env)

    bronze_path = get_s3_path("bronze", "customers", env)
    silver_path = get_s3_path("silver", "dim_customers_scd2", env)

    # Read today's snapshot from Bronze
    raw = (
        spark.read
        .schema(CUSTOMER_SCHEMA)
        .format("delta")
        .load(bronze_path)
        .filter(F.col("ingestion_date") == batch_date)
    )

    # Data quality before promotion
    DataQualityChecker(raw, "bronze.customers")  \
        .not_null(["customer_id", "kyc_status"])  \
        .value_in("kyc_status", ["PENDING", "APPROVED", "SUSPENDED", "CLOSED"]) \
        .value_in("risk_rating", ["LOW", "MEDIUM", "HIGH"]) \
        .row_count_at_least(1) \
        .run()

    # PII tokenisation before Silver write
    clean = mask_pii(raw, PII_COLS)

    upsert_scd2(spark, clean, silver_path, batch_id, batch_date)
    log.info("scd2_complete", env=env, batch_date=batch_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
