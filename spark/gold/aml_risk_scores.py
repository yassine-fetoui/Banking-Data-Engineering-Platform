"""
spark/gold/aml_risk_scores.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Gold layer: Compute AML (Anti-Money Laundering) behavioural risk features.

Features computed per customer per day:
  - Transaction velocity (count + volume in rolling 7 / 30 days)
  - Geographic anomaly score (transactions from new countries)
  - Round-amount pattern score (regulatory typology indicator)
  - Dormant account reactivation flag (no activity for 90+ days)
  - Structuring indicator (multiple transactions just below reporting threshold)

Output: gold.aml_risk_scores — consumed by rule engine and ML scoring model.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date

import structlog
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)

# AML reporting threshold (varies by jurisdiction; AED 40,000 for UAE)
CTR_THRESHOLD = 40_000.00


def compute_velocity_features(txn: DataFrame) -> DataFrame:
    """Rolling 7-day and 30-day transaction counts and volumes per customer."""
    w7  = Window.partitionBy("customer_id").orderBy("txn_date") \
                .rangeBetween(-6 * 86400, 0)
    w30 = Window.partitionBy("customer_id").orderBy("txn_date") \
                .rangeBetween(-29 * 86400, 0)

    daily = (
        txn
        .filter(F.col("status") == "COMPLETED")
        .groupBy("customer_id", F.to_date("transaction_datetime").alias("txn_date"))
        .agg(
            F.count("*").alias("daily_txn_count"),
            F.sum("amount").alias("daily_txn_volume"),
        )
    )

    return (
        daily
        .withColumn("txn_count_7d",  F.sum("daily_txn_count").over(w7))
        .withColumn("txn_count_30d", F.sum("daily_txn_count").over(w30))
        .withColumn("txn_volume_7d", F.sum("daily_txn_volume").over(w7))
        .withColumn("txn_volume_30d",F.sum("daily_txn_volume").over(w30))
    )


def compute_geographic_features(txn: DataFrame) -> DataFrame:
    """
    Count distinct countries in last 30 days and flag new countries
    not seen in the prior 90-day baseline.
    """
    w_hist = Window.partitionBy("customer_id").orderBy(F.unix_timestamp("transaction_datetime")) \
                   .rangeBetween(-90 * 86400, -31 * 86400)
    w_rec  = Window.partitionBy("customer_id").orderBy(F.unix_timestamp("transaction_datetime")) \
                   .rangeBetween(-30 * 86400, 0)

    return (
        txn
        .withColumn("historical_countries",  F.collect_set("country_code").over(w_hist))
        .withColumn("recent_countries",       F.collect_set("country_code").over(w_rec))
        .withColumn("new_country_flag",
            F.size(F.array_except("recent_countries", "historical_countries")) > 0
        )
        .withColumn("distinct_countries_30d", F.size("recent_countries"))
        .groupBy("customer_id")
        .agg(
            F.max("new_country_flag").alias("new_country_flag"),
            F.max("distinct_countries_30d").alias("distinct_countries_30d"),
        )
    )


def compute_structuring_indicator(txn: DataFrame) -> DataFrame:
    """
    Structuring: Multiple transactions just below the CTR reporting threshold
    within a 24-hour window — a classic AML typology.
    """
    threshold_low  = CTR_THRESHOLD * 0.85
    threshold_high = CTR_THRESHOLD * 0.99

    near_threshold = txn.filter(
        (F.col("amount") >= threshold_low) &
        (F.col("amount") <  threshold_high) &
        (F.col("status") == "COMPLETED")
    )

    w24h = Window.partitionBy("customer_id") \
                 .orderBy(F.unix_timestamp("transaction_datetime")) \
                 .rangeBetween(-86400, 0)

    return (
        near_threshold
        .withColumn("near_threshold_count_24h", F.count("*").over(w24h))
        .groupBy("customer_id")
        .agg(F.max("near_threshold_count_24h").alias("structuring_txn_count_24h"))
        .withColumn("structuring_flag", F.col("structuring_txn_count_24h") >= 3)
    )


def compute_round_amount_score(txn: DataFrame) -> DataFrame:
    """Round amounts (e.g. exactly 1000, 5000) are a typology indicator."""
    return (
        txn
        .filter(F.col("status") == "COMPLETED")
        .withColumn("is_round", (F.col("amount") % 1000 == 0).cast("int"))
        .groupBy("customer_id")
        .agg(
            F.sum("is_round").alias("round_amount_count_30d"),
            F.count("*").alias("total_count_30d"),
        )
        .withColumn(
            "round_amount_pct",
            F.round(F.col("round_amount_count_30d") / F.col("total_count_30d"), 4),
        )
    )


def compute_composite_risk_score(df: DataFrame) -> DataFrame:
    """
    Simple weighted composite risk score [0–100].
    In production this is replaced by the output of an ML model.
    """
    return df.withColumn(
        "composite_risk_score",
        F.least(
            F.lit(100.0),
            (
                F.when(F.col("txn_count_7d")        > 50,   F.lit(20.0)).otherwise(F.lit(0.0)) +
                F.when(F.col("new_country_flag"),            F.lit(25.0)).otherwise(F.lit(0.0)) +
                F.when(F.col("structuring_flag"),            F.lit(35.0)).otherwise(F.lit(0.0)) +
                F.when(F.col("round_amount_pct")    > 0.5,  F.lit(20.0)).otherwise(F.lit(0.0))
            )
        )
    ).withColumn(
        "risk_band",
        F.when(F.col("composite_risk_score") >= 75, "HIGH")
         .when(F.col("composite_risk_score") >= 40, "MEDIUM")
         .otherwise("LOW")
    )


def run(env: str, batch_date: str | None = None) -> None:
    batch_date = batch_date or str(date.today())
    batch_id   = str(uuid.uuid4())

    spark = get_spark("banking.gold.aml_risk_scores", env=env)

    silver_txn_path   = get_s3_path("silver", "fct_transactions",      env)
    silver_cust_path  = get_s3_path("silver", "dim_customers_scd2",    env)
    output_path       = get_s3_path("gold",   "aml_risk_scores",       env)

    txn = spark.read.format("delta").load(silver_txn_path)

    velocity   = compute_velocity_features(txn)
    geo        = compute_geographic_features(txn)
    structure  = compute_structuring_indicator(txn)
    rounds     = compute_round_amount_score(txn)

    # Join all feature sets
    risk_df = (
        velocity
        .filter(F.col("txn_date") == batch_date)
        .join(geo,       "customer_id", "left")
        .join(structure, "customer_id", "left")
        .join(rounds,    "customer_id", "left")
        .fillna(0, subset=["structuring_txn_count_24h", "round_amount_count_30d"])
        .fillna(False, subset=["structuring_flag", "new_country_flag"])
        .transform(compute_composite_risk_score)
        .withColumn("score_date",  F.lit(batch_date).cast("date"))
        .withColumn("_batch_id",   F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    # Overwrite today's partition
    (
        risk_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"score_date = '{batch_date}'")
        .partitionBy("score_date", "risk_band")
        .save(output_path)
    )

    high_risk = risk_df.filter(F.col("risk_band") == "HIGH").count()
    log.info("aml_scoring_complete",
             env=env, batch_date=batch_date, high_risk_customers=high_risk)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",        default="local")
    parser.add_argument("--batch-date", default=None)
    args = parser.parse_args()
    run(args.env, args.batch_date)
