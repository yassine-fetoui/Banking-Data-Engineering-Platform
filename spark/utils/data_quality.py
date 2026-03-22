"""
spark/utils/data_quality.py
━━━━━━━━━━━━━━━━━━━━━━━━━
Reusable PySpark data quality utilities.
Used in Silver layer before promoting data to Gold.
"""
from __future__ import annotations

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

log = structlog.get_logger(__name__)


class DataQualityError(Exception):
    """Raised when a hard data quality rule is violated."""


class DataQualityChecker:
    """
    Fluent data quality checker for PySpark DataFrames.

    Usage:
        checker = DataQualityChecker(df, table="silver.transactions")
        checker.not_null(["transaction_id", "amount"])
               .unique(["transaction_id"])
               .value_in("currency", ["USD", "EUR", "AED", "GBP"])
               .between("amount", 0.01, 10_000_000)
               .run()
    """

    def __init__(self, df: DataFrame, table: str, fail_fast: bool = True) -> None:
        self._df         = df
        self._table      = table
        self._fail_fast  = fail_fast
        self._checks: list[tuple[str, callable]] = []

    # ── Check builders ────────────────────────────────────────────────────────

    def not_null(self, columns: list[str]) -> "DataQualityChecker":
        for col in columns:
            self._checks.append((
                f"NOT_NULL:{col}",
                lambda df, c=col: df.filter(F.col(c).isNull()).count() == 0,
            ))
        return self

    def unique(self, columns: list[str]) -> "DataQualityChecker":
        key = ", ".join(columns)
        self._checks.append((
            f"UNIQUE:[{key}]",
            lambda df, cols=columns: (
                df.count() == df.dropDuplicates(cols).count()
            ),
        ))
        return self

    def value_in(self, column: str, allowed: list) -> "DataQualityChecker":
        self._checks.append((
            f"VALUE_IN:{column}",
            lambda df, c=column, a=allowed: (
                df.filter(~F.col(c).isin(a)).count() == 0
            ),
        ))
        return self

    def between(self, column: str, min_val: float, max_val: float) -> "DataQualityChecker":
        self._checks.append((
            f"BETWEEN:{column}:[{min_val},{max_val}]",
            lambda df, c=column, lo=min_val, hi=max_val: (
                df.filter((F.col(c) < lo) | (F.col(c) > hi)).count() == 0
            ),
        ))
        return self

    def row_count_at_least(self, min_rows: int) -> "DataQualityChecker":
        self._checks.append((
            f"MIN_ROWS:{min_rows}",
            lambda df, n=min_rows: df.count() >= n,
        ))
        return self

    def no_future_dates(self, column: str) -> "DataQualityChecker":
        self._checks.append((
            f"NO_FUTURE_DATES:{column}",
            lambda df, c=column: (
                df.filter(F.col(c) > F.current_timestamp()).count() == 0
            ),
        ))
        return self

    # ── Runner ────────────────────────────────────────────────────────────────

    def run(self) -> DataFrame:
        """Execute all checks. Raises DataQualityError on failure if fail_fast=True."""
        failures = []
        for name, check_fn in self._checks:
            passed = check_fn(self._df)
            status = "✅ PASS" if passed else "❌ FAIL"
            log.info("data_quality_check", table=self._table, check=name, status=status)
            if not passed:
                failures.append(name)

        if failures and self._fail_fast:
            raise DataQualityError(
                f"Data quality failed for table '{self._table}': {failures}"
            )

        return self._df


def add_audit_columns(
    df: DataFrame,
    source_system: str,
    batch_id: str,
    layer: str,
) -> DataFrame:
    """Append standard audit metadata columns to any DataFrame."""
    return df.withColumn("_source_system", F.lit(source_system)) \
             .withColumn("_batch_id",      F.lit(batch_id)) \
             .withColumn("_layer",         F.lit(layer)) \
             .withColumn("_ingested_at",   F.current_timestamp()) \
             .withColumn("_ingestion_date", F.current_date())


def mask_pii(df: DataFrame, columns: list[str]) -> DataFrame:
    """Replace PII columns with SHA-256 hashes (tokenisation)."""
    for col in columns:
        df = df.withColumn(
            col,
            F.sha2(F.col(col).cast("string"), 256),
        )
    return df
