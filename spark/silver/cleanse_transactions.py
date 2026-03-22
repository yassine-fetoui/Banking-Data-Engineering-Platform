"""
spark/silver/cleanse_transactions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Silver layer: Cleanse, deduplicate, and enrich Bronze transactions.

Steps:
  1. Read Bronze Delta partition for batch_date
  2. Deduplicate on transaction_id (keep latest ingested)
  3. Convert amounts to AED base currency via FX rates
  4. Compute AML flags (fast rule-based — ML scoring done in Gold)
  5. MERGE into Silver Delta table (idempotent upsert)
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date

import structlog
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.utils.data_quality import DataQualityChecker, add_audit_columns
from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)

CTR_THRESHOLD_AED = 40_000.00


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest ingested record per transaction_id."""
    return (
        df.withColumn(
            "_rn",
            F.row_number().over(
                F.Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_fx_conversion(df: DataFrame, fx_df: DataFrame) -> DataFrame:
    """Join FX rates and compute amount_aed."""
    return (
        df.join(
            fx_df.select("from_currency", "rate_to_aed", "rate_date"),
            on=(df.currency == fx_df.from_currency) &
               (F.to_date(df.transaction_datetime) == fx_df.rate_date),
            how="left",
        )
        .withColumn("fx_rate",    F.coalesce(F.col("rate_to_aed"), F.lit(1.0)))
        .withColumn("amount_aed", F.round(F.col("amount") * F.col("fx_rate"), 4))
        .drop("from_currency", "rate_to_aed", "rate_date")
    )


def compute_aml_flags(df: DataFrame) -> DataFrame:
    """Fast rule-based AML flags — no ML needed at Silver."""
    return (
        df
        .withColumn("exceeds_ctr",        F.col("amount_aed") > CTR_THRESHOLD_AED)
        .withColumn("is_round_amount",    (F.col("amount_aed") % 1000 == 0) & (F.col("amount_aed") > 0))
        .withColumn("transaction_date",   F.to_date(F.col("transaction_datetime")))
        .withColumn("transaction_hour",   F.hour(F.col("transaction_datetime")))
        .withColumn("is_weekend",         F.dayofweek(F.col("transaction_datetime")).isin(1, 7))
        .withColumn("is_international",   F.col("transaction_type") == "INTERNATIONAL")
    )


def upsert_silver(spark: SparkSession, df: DataFrame, path: str) -> None:
    """MERGE into Silver Delta — idempotent on transaction_id."""
    if not DeltaTable.isDeltaTable(spark, path):
        log.info("bootstrapping_silver_table", path=path)
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("transaction_date", "currency")
            .save(path)
        )
        return

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("tgt")
        .merge(df.alias("src"), "tgt.transaction_id = src.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("silver_merge_complete", rows=df.count())


def run(env: str, batch_date: str | None = None) -> None:
    batch_date = batch_date or str(date.today())
    batch_id   = str(uuid.uuid4())
    spark      = get_spark("banking.silver.transactions", env=env)

    bronze_path = get_s3_path("bronze", "transactions", env)
    fx_path     = get_s3_path("bronze", "fx_rates",     env)
    silver_path = get_s3_path("silver", "fct_transactions", env)

    bronze = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("ingestion_date") == batch_date)
    )

    fx = spark.read.format("delta").load(fx_path)

    silver = (
        bronze
        .transform(deduplicate)
        .transform(lambda d: apply_fx_conversion(d, fx))
        .transform(compute_aml_flags)
        .transform(lambda d: add_audit_columns(d, "silver_pipeline", batch_id, "silver"))
    )

    DataQualityChecker(silver, "silver.fct_transactions") \
        .not_null(["transaction_id", "account_id", "amount_aed"]) \
        .unique(["transaction_id"]) \
        .between("amount_aed", 0, 200_000_000) \
        .row_count_at_least(1) \
        .run()

    upsert_silver(spark, silver, silver_path)
    log.info("silver_cleanse_complete", env=env, batch_date=batch_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
