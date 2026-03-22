"""
tests/unit/test_data_quality.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Unit tests for data quality utilities using a local SparkSession.
Uses chispa for DataFrame equality assertions.
"""
from __future__ import annotations

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from spark.utils.data_quality import DataQualityChecker, DataQualityError, add_audit_columns, mask_pii


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("banking-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_transactions(spark):
    data = [
        ("txn_001", "acc_1", "cust_1", 1500.00, "USD", "DEBIT",   "COMPLETED"),
        ("txn_002", "acc_2", "cust_2", 5000.00, "AED", "CREDIT",  "COMPLETED"),
        ("txn_003", "acc_3", "cust_3",  250.50, "EUR", "TRANSFER","PENDING"),
    ]
    schema = StructType([
        StructField("transaction_id",   StringType(),       False),
        StructField("account_id",       StringType(),       False),
        StructField("customer_id",      StringType(),       False),
        StructField("amount",           DecimalType(18, 4), False),
        StructField("currency",         StringType(),       False),
        StructField("transaction_type", StringType(),       False),
        StructField("status",           StringType(),       False),
    ])
    return spark.createDataFrame(data, schema)


# ── DataQualityChecker tests ──────────────────────────────────────────────────

class TestDataQualityChecker:

    def test_not_null_passes_on_clean_data(self, sample_transactions):
        checker = DataQualityChecker(sample_transactions, "test.transactions")
        result = checker.not_null(["transaction_id", "account_id"]).run()
        assert result is not None

    def test_not_null_fails_when_null_present(self, spark):
        data = [("txn_001", None), ("txn_002", "acc_1")]
        df = spark.createDataFrame(data, ["transaction_id", "account_id"])
        checker = DataQualityChecker(df, "test.transactions", fail_fast=True)
        with pytest.raises(DataQualityError, match="NOT_NULL:account_id"):
            checker.not_null(["account_id"]).run()

    def test_unique_passes_on_distinct_keys(self, sample_transactions):
        checker = DataQualityChecker(sample_transactions, "test.transactions")
        checker.unique(["transaction_id"]).run()

    def test_unique_fails_on_duplicates(self, spark):
        data = [("txn_001", 100.0), ("txn_001", 200.0)]  # Duplicate transaction_id
        df = spark.createDataFrame(data, ["transaction_id", "amount"])
        checker = DataQualityChecker(df, "test.transactions")
        with pytest.raises(DataQualityError, match="UNIQUE"):
            checker.unique(["transaction_id"]).run()

    def test_value_in_passes_valid_currencies(self, sample_transactions):
        checker = DataQualityChecker(sample_transactions, "test.transactions")
        checker.value_in("currency", ["USD", "EUR", "AED", "GBP"]).run()

    def test_value_in_fails_invalid_currency(self, spark):
        data = [("txn_001", "INVALID_CURRENCY")]
        df = spark.createDataFrame(data, ["transaction_id", "currency"])
        checker = DataQualityChecker(df, "test.transactions")
        with pytest.raises(DataQualityError, match="VALUE_IN:currency"):
            checker.value_in("currency", ["USD", "EUR", "AED"]).run()

    def test_between_passes_valid_amounts(self, sample_transactions):
        checker = DataQualityChecker(sample_transactions, "test.transactions")
        checker.between("amount", 0.01, 10_000_000).run()

    def test_between_fails_negative_amount(self, spark):
        data = [("txn_001", -500.0)]
        df = spark.createDataFrame(data, ["transaction_id", "amount"])
        checker = DataQualityChecker(df, "test.transactions")
        with pytest.raises(DataQualityError, match="BETWEEN:amount"):
            checker.between("amount", 0.01, 10_000_000).run()

    def test_no_fail_fast_collects_all_errors(self, spark):
        """With fail_fast=False, all failing checks are collected before raising."""
        data = [(None, -999.0)]
        df = spark.createDataFrame(data, ["transaction_id", "amount"])
        checker = DataQualityChecker(df, "test.transactions", fail_fast=False)
        # Does not raise — returns the DF
        result = checker.not_null(["transaction_id"]).between("amount", 0, 1000).run()
        assert result is not None


# ── Audit columns ─────────────────────────────────────────────────────────────

class TestAuditColumns:

    def test_add_audit_columns_adds_required_fields(self, sample_transactions):
        result = add_audit_columns(sample_transactions, "core_banking", "batch_123", "bronze")

        assert "_source_system" in result.columns
        assert "_batch_id"      in result.columns
        assert "_layer"         in result.columns
        assert "_ingested_at"   in result.columns
        assert "_ingestion_date" in result.columns

    def test_audit_column_values(self, sample_transactions, spark):
        result = add_audit_columns(sample_transactions, "core_banking", "batch_123", "bronze")
        row = result.select("_source_system", "_batch_id", "_layer").first()

        assert row["_source_system"] == "core_banking"
        assert row["_batch_id"]      == "batch_123"
        assert row["_layer"]         == "bronze"


# ── PII masking ───────────────────────────────────────────────────────────────

class TestPIIMasking:

    def test_mask_pii_replaces_values_with_hash(self, spark):
        data = [("txn_001", "1234-5678-9012-3456")]
        df = spark.createDataFrame(data, ["transaction_id", "card_number"])
        masked = mask_pii(df, ["card_number"])
        row = masked.first()

        # SHA-256 produces 64-char hex string
        assert len(row["card_number"]) == 64
        assert row["card_number"] != "1234-5678-9012-3456"

    def test_mask_pii_is_deterministic(self, spark):
        """Same input → same hash (deterministic tokenisation)."""
        data = [("txn_001", "4111111111111111"), ("txn_002", "4111111111111111")]
        df = spark.createDataFrame(data, ["transaction_id", "card_number"])
        masked = mask_pii(df, ["card_number"])
        hashes = [row["card_number"] for row in masked.collect()]
        assert hashes[0] == hashes[1]

    def test_mask_pii_different_values_different_hashes(self, spark):
        data = [("txn_001", "card_A"), ("txn_002", "card_B")]
        df = spark.createDataFrame(data, ["transaction_id", "card_number"])
        masked = mask_pii(df, ["card_number"])
        hashes = [row["card_number"] for row in masked.collect()]
        assert hashes[0] != hashes[1]
