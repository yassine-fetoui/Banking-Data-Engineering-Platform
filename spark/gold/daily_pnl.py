"""
spark/gold/daily_pnl.py
━━━━━━━━━━━━━━━━━━━━━━
Gold layer: Daily P&L and balance sheet aggregations.

Outputs:
  - Revenue by product_type + currency
  - Transaction volumes and counts
  - Net position per currency (FX exposure)

Consumed by: Redshift (BI dashboards), CFO reporting, Basel III LCR inputs.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)


def compute_daily_pnl(txn: DataFrame, accounts: DataFrame, batch_date: str) -> DataFrame:
    """Aggregate daily P&L from transactions joined to account product types."""
    daily_txn = (
        txn
        .filter(
            (F.col("transaction_date") == batch_date) &
            (F.col("status") == "COMPLETED")
        )
        .join(
            accounts.select("account_id", "product_type", "account_manager"),
            on="account_id",
            how="left",
        )
    )

    return (
        daily_txn
        .groupBy("transaction_date", "product_type", "currency")
        .agg(
            F.count("*")                                        .alias("txn_count"),
            F.sum("amount")                                     .alias("txn_volume"),
            F.sum("amount_aed")                                 .alias("txn_volume_aed"),
            F.avg("amount_aed")                                 .alias("avg_txn_aed"),
            F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount_aed")).otherwise(0))
                                                                .alias("credit_volume_aed"),
            F.sum(F.when(F.col("transaction_type") == "DEBIT",  F.col("amount_aed")).otherwise(0))
                                                                .alias("debit_volume_aed"),
            F.sum(F.when(F.col("exceeds_ctr"), F.col("amount_aed")).otherwise(0))
                                                                .alias("ctr_volume_aed"),
            F.count(F.when(F.col("exceeds_ctr"), True))        .alias("ctr_txn_count"),
            F.countDistinct("customer_id")                      .alias("unique_customers"),
            F.countDistinct("account_id")                       .alias("unique_accounts"),
        )
        .withColumn("net_position_aed",
            F.col("credit_volume_aed") - F.col("debit_volume_aed")
        )
        .withColumn("report_date",   F.lit(batch_date).cast("date"))
        .withColumn("_ingested_at",  F.current_timestamp())
    )


def run(env: str, batch_date: str | None = None) -> None:
    batch_date = batch_date or str(date.today())
    batch_id   = str(uuid.uuid4())
    spark      = get_spark("banking.gold.daily_pnl", env=env)

    silver_txn_path  = get_s3_path("silver", "fct_transactions", env)
    silver_acc_path  = get_s3_path("silver", "dim_accounts",     env)
    output_path      = get_s3_path("gold",   "fct_daily_pnl",    env)

    txn      = spark.read.format("delta").load(silver_txn_path)
    accounts = spark.read.format("delta").load(silver_acc_path)

    pnl = compute_daily_pnl(txn, accounts, batch_date)

    (
        pnl.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"report_date = '{batch_date}'")
        .partitionBy("report_date", "product_type")
        .save(output_path)
    )

    total_volume = pnl.agg(F.sum("txn_volume_aed")).collect()[0][0]
    log.info("daily_pnl_complete",
             env=env,
             batch_date=batch_date,
             total_volume_aed=float(total_volume or 0))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
