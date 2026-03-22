"""
tests/unit/test_aml_risk_scoring.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Unit tests for AML behavioural risk feature computation.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType
)

from spark.gold.aml_risk_scores import (
    compute_round_amount_score,
    compute_structuring_indicator,
    compute_composite_risk_score,
    CTR_THRESHOLD,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("aml-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


TXN_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),       False),
    StructField("customer_id",          StringType(),       False),
    StructField("transaction_datetime", TimestampType(),     False),
    StructField("amount",               DecimalType(18, 4), False),
    StructField("currency",             StringType(),       False),
    StructField("country_code",         StringType(),       True),
    StructField("status",               StringType(),       False),
    StructField("transaction_type",     StringType(),       False),
])


def make_txn(spark, customer_id, amount, status="COMPLETED", country="AE",
             dt: datetime | None = None):
    dt = dt or datetime.utcnow()
    return spark.createDataFrame(
        [(f"txn_{id(amount)}", customer_id, dt, float(amount), "AED", country, status, "DEBIT")],
        TXN_SCHEMA,
    )


class TestRoundAmountScore:

    def test_round_amounts_detected(self, spark):
        txns = [
            ("txn_1", "cust_1", datetime.utcnow(), 1000.0, "AED", "AE", "COMPLETED", "DEBIT"),
            ("txn_2", "cust_1", datetime.utcnow(), 2000.0, "AED", "AE", "COMPLETED", "DEBIT"),
            ("txn_3", "cust_1", datetime.utcnow(),  750.5, "AED", "AE", "COMPLETED", "DEBIT"),
        ]
        df = spark.createDataFrame(txns, TXN_SCHEMA)
        result = compute_round_amount_score(df)
        row = result.filter(F.col("customer_id") == "cust_1").first()

        # 2 out of 3 transactions are round multiples of 1000
        assert row["round_amount_count_30d"] == 2
        assert abs(row["round_amount_pct"] - 0.6667) < 0.001

    def test_non_round_amounts_not_flagged(self, spark):
        txns = [
            ("txn_1", "cust_2", datetime.utcnow(), 123.45, "AED", "AE", "COMPLETED", "DEBIT"),
            ("txn_2", "cust_2", datetime.utcnow(), 678.90, "AED", "AE", "COMPLETED", "DEBIT"),
        ]
        df = spark.createDataFrame(txns, TXN_SCHEMA)
        result = compute_round_amount_score(df)
        row = result.filter(F.col("customer_id") == "cust_2").first()

        assert row["round_amount_count_30d"] == 0
        assert row["round_amount_pct"] == 0.0


class TestStructuringIndicator:

    def test_structuring_flag_raised_on_3_near_threshold_txns(self, spark):
        """3+ transactions just below CTR threshold in 24h → structuring flag."""
        near = CTR_THRESHOLD * 0.90   # 90% of threshold
        base_time = datetime.utcnow()
        txns = [
            (f"txn_{i}", "cust_struct", base_time + timedelta(hours=i),
             near, "AED", "AE", "COMPLETED", "DEBIT")
            for i in range(4)  # 4 transactions
        ]
        df = spark.createDataFrame(txns, TXN_SCHEMA)
        result = compute_structuring_indicator(df)
        row = result.filter(F.col("customer_id") == "cust_struct").first()

        assert row["structuring_flag"] is True
        assert row["structuring_txn_count_24h"] >= 3

    def test_no_structuring_flag_on_2_transactions(self, spark):
        """Only 2 near-threshold transactions — below flag threshold."""
        near = CTR_THRESHOLD * 0.90
        txns = [
            (f"txn_{i}", "cust_clean", datetime.utcnow() + timedelta(hours=i),
             near, "AED", "AE", "COMPLETED", "DEBIT")
            for i in range(2)
        ]
        df = spark.createDataFrame(txns, TXN_SCHEMA)
        result = compute_structuring_indicator(df)
        row = result.filter(F.col("customer_id") == "cust_clean").first()
        assert row["structuring_flag"] is False

    def test_high_amount_above_threshold_not_flagged(self, spark):
        """Transactions ABOVE the CTR threshold are not structuring signals."""
        above = CTR_THRESHOLD * 1.10
        txns = [
            (f"txn_{i}", "cust_legit", datetime.utcnow() + timedelta(hours=i),
             above, "AED", "AE", "COMPLETED", "DEBIT")
            for i in range(5)
        ]
        df = spark.createDataFrame(txns, TXN_SCHEMA)
        result = compute_structuring_indicator(df)
        # No rows — no near-threshold transactions
        assert result.count() == 0


class TestCompositeRiskScore:

    def test_high_risk_customer_scores_above_75(self, spark):
        data = [(
            "cust_high",
            "2026-01-01",
            60,     # txn_count_7d > 50 → +20
            True,   # new_country_flag → +25
            True,   # structuring_flag → +35
            0.8,    # round_amount_pct > 0.5 → +20
        )]
        schema = ["customer_id", "txn_date", "txn_count_7d", "new_country_flag",
                  "structuring_flag", "round_amount_pct"]
        df = spark.createDataFrame(data, schema)
        result = compute_composite_risk_score(df)
        row = result.first()

        assert row["composite_risk_score"] >= 75.0
        assert row["risk_band"] == "HIGH"

    def test_low_risk_customer_scores_below_40(self, spark):
        data = [("cust_low", "2026-01-01", 5, False, False, 0.1)]
        schema = ["customer_id", "txn_date", "txn_count_7d", "new_country_flag",
                  "structuring_flag", "round_amount_pct"]
        df = spark.createDataFrame(data, schema)
        result = compute_composite_risk_score(df)
        row = result.first()

        assert row["composite_risk_score"] < 40.0
        assert row["risk_band"] == "LOW"

    def test_risk_score_capped_at_100(self, spark):
        """Score should never exceed 100 regardless of how many flags are set."""
        data = [("cust_extreme", "2026-01-01", 999, True, True, 0.99)]
        schema = ["customer_id", "txn_date", "txn_count_7d", "new_country_flag",
                  "structuring_flag", "round_amount_pct"]
        df = spark.createDataFrame(data, schema)
        result = compute_composite_risk_score(df)
        assert result.first()["composite_risk_score"] <= 100.0
