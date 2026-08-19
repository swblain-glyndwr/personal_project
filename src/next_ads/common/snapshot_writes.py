from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    KeyValidationSummary,
    replace_scope_by_name,
    replace_table_by_name,
    validate_unique_non_null_keys,
)
from dsutils.logtools import get_logger

logger = get_logger(__name__)

__all__ = [
    "SnapshotPublicationResult",
    "ValidatedSnapshotResult",
    "capture_run_date",
    "publish_history_and_latest",
    "replace_validated_scope",
    "replace_validated_snapshot",
    "with_run_date",
]


@dataclass(frozen=True)
class SnapshotPublicationResult:
    run_date: date
    validation: KeyValidationSummary
    history_write: DeltaWriteReceipt
    latest_write: DeltaWriteReceipt


@dataclass(frozen=True)
class ValidatedSnapshotResult:
    validation: KeyValidationSummary
    write: DeltaWriteReceipt


def capture_run_date(spark: Any) -> date:
    """Capture Spark's current date once for every output in a logical run."""
    return spark.sql("SELECT current_date() AS run_date").first()["run_date"]


def with_run_date(
    df: DataFrame,
    run_date: date | str,
    *,
    column: str = "rundate",
) -> DataFrame:
    """Set one explicit run date, replacing any inherited run-date column."""
    return df.withColumn(column, F.lit(run_date).cast("date"))


def _resolve_run_date(spark: Any, run_date: date | str | None) -> date:
    if run_date is None:
        return capture_run_date(spark)
    if isinstance(run_date, str):
        return date.fromisoformat(run_date)
    return run_date


def _validation_keys(
    key_columns: Sequence[str],
    *,
    scope_columns: Sequence[str] = (),
) -> list[str]:
    keys = [*key_columns]
    for column in scope_columns:
        if column not in keys:
            keys.append(column)
    return keys


def replace_validated_snapshot(
    spark: Any,
    df: DataFrame,
    *,
    table: str,
    key_columns: Sequence[str],
    columns: Sequence[str] | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
) -> ValidatedSnapshotResult:
    """Validate, materialise and atomically replace a complete snapshot."""
    selected_columns = list(columns or df.columns)
    prepared = df.select(*selected_columns).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        validation = validate_unique_non_null_keys(prepared, key_columns)
        write = replace_table_by_name(
            prepared,
            table,
            selected_columns,
            spark=spark,
            build_id=build_id,
            attempt_id=attempt_id,
            git_commit=git_commit,
        )
        return ValidatedSnapshotResult(validation=validation, write=write)
    finally:
        prepared.unpersist()


def replace_validated_scope(
    spark: Any,
    df: DataFrame,
    *,
    table: str,
    scope: dict[str, Any],
    key_columns: Sequence[str],
    columns: Sequence[str] | None = None,
) -> KeyValidationSummary:
    """Validate, materialise and atomically replace one structured scope."""
    selected_columns = list(columns or df.columns)
    prepared = df.select(*selected_columns).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        validation = validate_unique_non_null_keys(
            prepared,
            _validation_keys(key_columns, scope_columns=list(scope)),
        )
        replace_scope_by_name(
            prepared,
            table,
            scope,
            selected_columns,
            spark=spark,
        )
        return validation
    finally:
        prepared.unpersist()


def publish_history_and_latest(
    spark: Any,
    df: DataFrame,
    *,
    history_table: str,
    latest_table: str,
    key_columns: Sequence[str],
    run_date: date | str | None = None,
    run_date_column: str = "rundate",
    columns: Sequence[str] | None = None,
    scope: dict | None = None,
) -> SnapshotPublicationResult:
    """Publish an idempotent date slice, then its serving snapshot.
    Can publish only latest by providing an empty string for history table
    """
    resolved_run_date = _resolve_run_date(spark, run_date)
    prepared_with_date = with_run_date(
        df,
        resolved_run_date,
        column=run_date_column,
    )
    selected_columns = list(columns or prepared_with_date.columns)
    prepared = prepared_with_date.select(*selected_columns).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        if not scope:
            _scope = {run_date_column: resolved_run_date}
        else:
            _scope = scope
            _scope.update({run_date_column: resolved_run_date})

        validation = validate_unique_non_null_keys(
            prepared,
            _validation_keys(
                key_columns,
                scope_columns=list(_scope.keys()),
            ),
        )
        if history_table:
            logger.info("Writing history table")
            history_write = replace_scope_by_name(
                prepared,
                history_table,
                _scope,
                selected_columns,
                spark=spark,
            )

        else:
            history_write = None
            logger.info("No history table provided-skipping")
        logger.info("Writing latest table")
        if scope:
            latest_write = replace_scope_by_name(
                prepared,
                latest_table,
                _scope,
                selected_columns,
                spark=spark,
            )
        else:
            latest_write = replace_table_by_name(
                prepared,
                latest_table,
                selected_columns,
                spark=spark,
            )
        return SnapshotPublicationResult(
            run_date=resolved_run_date,
            validation=validation,
            history_write=history_write,
            latest_write=latest_write,
        )
    finally:
        prepared.unpersist()
