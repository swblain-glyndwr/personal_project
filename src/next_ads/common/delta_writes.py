from __future__ import annotations

import math
import hashlib
import json
import random
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import reduce
from operator import and_, or_
from typing import Any, Tuple

from delta.exceptions import DeltaConcurrentModificationException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dsutils.logtools import get_logger

logger = get_logger(__name__)

__all__ = [
    "DeltaRetryPolicy",
    "DeltaWriteReceipt",
    "find_delta_write_receipt",
    "KeyValidationSummary",
    "atomic_append_by_name",
    "atomic_replace_where_by_name",
    "replace_scope_by_name",
    "replace_table_by_name",
    "typed_table_frame",
    "validate_typed_table_schema",
    "validate_target_columns",
    "validate_replace_source_scope",
    "validate_unique_non_null_keys",
    "quote_qualified_identifier",
    "schema_checksum",
]


@dataclass(frozen=True)
class KeyValidationSummary:
    row_count: int
    distinct_key_count: int
    null_key_count: int


@dataclass(frozen=True)
class DeltaRetryPolicy:
    max_attempts: int = 5
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0
    jitter_seconds: float = 1.0

    def __post_init__(self) -> None:
        """Validate retry bounds before a write starts."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds cannot be negative")
        if self.max_backoff_seconds < 0:
            raise ValueError("max_backoff_seconds cannot be negative")
        if self.jitter_seconds < 0:
            raise ValueError("jitter_seconds cannot be negative")


@dataclass(frozen=True)
class DeltaWriteReceipt:
    """The exact Delta transaction produced by one atomic writer."""

    statement: str
    attempts: int
    receipt_id: str = ""
    target_table: str = ""
    delta_version: int | None = None
    row_count: int | None = None
    schema_checksum: str | None = None
    build_id: str | None = None
    attempt_id: str | None = None
    git_commit: str | None = None
    committed_at: datetime | None = None
    write_duration_ms: int = 0

    def as_binding(self) -> dict[str, Any]:
        """Return the small, serialisable proof stored in a READY manifest."""
        if self.delta_version is None:
            raise ValueError("Delta receipt is missing its committed version")
        if self.row_count is None:
            raise ValueError("Delta receipt is missing its output row count")
        if (
            not self.receipt_id
            or not self.target_table
            or not self.schema_checksum
        ):
            raise ValueError("Delta receipt is incomplete")
        return {
            "table": self.target_table,
            "delta_version": self.delta_version,
            "row_count": self.row_count,
            "schema_checksum": self.schema_checksum,
            "write_receipt_id": self.receipt_id,
            "write_duration_ms": self.write_duration_ms,
            "retry_count": max(0, self.attempts - 1),
        }


@dataclass(frozen=True)
class _ReceiptMetadata:
    receipt_id: str
    target_table: str
    build_id: str | None
    attempt_id: str | None
    git_commit: str | None
    details: Mapping[str, Any] | None = None

    def as_json(self) -> str:
        payload = {
            "nextads_receipt_id": self.receipt_id,
            "target_table": self.target_table,
            "build_id": self.build_id,
            "attempt_id": self.attempt_id,
            "git_commit": self.git_commit,
        }
        if self.details:
            payload["details"] = dict(self.details)
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        )


_DELTA_COMMIT_METADATA_KEY = "spark.databricks.delta.commitInfo.userMetadata"
_DATA_WRITE_OPERATIONS = frozenset(
    {
        "WRITE",
        "MERGE",
        "CREATE TABLE AS SELECT",
        "REPLACE TABLE AS SELECT",
        "COPY INTO",
        "STREAMING UPDATE",
    }
)
_OUTPUT_ROW_METRICS = (
    "numOutputRows",
    "numTargetRowsInserted",
    "numInsertedRows",
    "numSourceRows",
)


def _is_data_write_history_row(row: Any) -> bool:
    """Exclude maintenance commits that can inherit write metadata."""
    return row["operation"] in _DATA_WRITE_OPERATIONS


def _operation_row_count(
    operation_metrics: Mapping[str, Any] | None,
) -> int | None:
    metrics = operation_metrics or {}
    raw_row_count = next(
        (
            metrics[name]
            for name in _OUTPUT_ROW_METRICS
            if metrics.get(name) is not None
        ),
        None,
    )
    return int(raw_row_count) if raw_row_count is not None else None


_COMMIT_METADATA_LOCK = threading.Lock()


SqlLiteral = str | int | float | bool | date | datetime | None


def typed_table_frame(
    spark: Any,
    table: str,
    rows: Sequence[Mapping[str, Any]],
) -> DataFrame:
    """Create manifest rows with the repo-owned target schema, never inference."""
    if not rows:
        raise ValueError("At least one typed table row is required")
    target_schema = spark.table(table).schema
    target_names = {field.name for field in target_schema}
    for index, row in enumerate(rows):
        unknown = sorted(set(row).difference(target_names))
        if unknown:
            raise ValueError(
                f"Row {index} contains columns absent from {table}: "
                + ", ".join(unknown)
            )
        missing_required = sorted(
            field.name
            for field in target_schema
            if not field.nullable and field.name not in row
        )
        if missing_required:
            raise ValueError(
                f"Row {index} is missing required columns for {table}: "
                + ", ".join(missing_required)
            )
    return spark.createDataFrame(list(rows), schema=target_schema)


def validate_typed_table_schema(
    spark: Any,
    table: str,
    expected_columns: Sequence[str],
    *,
    nullable_columns: Sequence[str] = (),
) -> None:
    """Validate a small typed-table contract without scanning any rows."""
    if not spark.catalog.tableExists(table):
        raise ValueError(f"Required typed table is missing: {table}")
    fields = {field.name: field for field in spark.table(table).schema}
    expected = list(expected_columns)
    missing = sorted(set(expected).difference(fields))
    unexpected = sorted(set(fields).difference(expected))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        raise ValueError(
            f"Typed table {table} does not match its v2 contract: "
            + "; ".join(details)
        )
    invalid_nullable = sorted(
        column
        for column in nullable_columns
        if column not in fields or not fields[column].nullable
    )
    if invalid_nullable:
        raise ValueError(
            f"Typed table {table} must allow null values for: "
            + ", ".join(invalid_nullable)
        )


def find_delta_write_receipt(
    spark: Any,
    *,
    target_table: str,
    build_id: str,
    attempt_id: str,
) -> DeltaWriteReceipt | None:
    """Find the exact tagged commit for an idempotent write repair."""
    history = spark.sql(
        f"DESCRIBE HISTORY {quote_qualified_identifier(target_table)}"
    )
    matching_rows = (
        history.where(
            (
                F.get_json_object("userMetadata", "$.target_table")
                == F.lit(target_table)
            )
            & (
                F.get_json_object("userMetadata", "$.build_id")
                == F.lit(build_id)
            )
            & (
                F.get_json_object("userMetadata", "$.attempt_id")
                == F.lit(attempt_id)
            )
        )
        .orderBy(F.col("version").desc())
        .limit(10)
        .collect()
    )
    write_rows = [
        row for row in matching_rows if _is_data_write_history_row(row)
    ]
    if not write_rows:
        return None
    if len(write_rows) != 1:
        raise RuntimeError(
            "Multiple Delta commits share one NextAds build attempt for "
            f"{target_table}"
        )
    row = write_rows[0]
    metadata = json.loads(row["userMetadata"])
    return _receipt_from_history_row(
        spark,
        row=row,
        statement="",
        attempts=1,
        metadata=_ReceiptMetadata(
            receipt_id=metadata["nextads_receipt_id"],
            target_table=target_table,
            build_id=build_id,
            attempt_id=attempt_id,
            git_commit=metadata.get("git_commit"),
        ),
        write_duration_ms=0,
    )


def validate_unique_non_null_keys(
    df: DataFrame,
    key_columns: Sequence[str],
) -> KeyValidationSummary:
    """Validate a dataframe key with one aggregate action."""
    keys = list(key_columns)
    if not keys:
        raise ValueError("At least one key column is required")
    if len(set(keys)) != len(keys):
        raise ValueError("Key columns must be unique")

    missing = [column for column in keys if column not in df.columns]
    if missing:
        raise ValueError(f"Missing key columns: {', '.join(missing)}")

    any_null = reduce(or_, (F.col(column).isNull() for column in keys))
    summary = df.agg(
        F.count(F.lit(1)).alias("_row_count"),
        F.countDistinct(F.struct(*[F.col(column) for column in keys])).alias(
            "_distinct_key_count"
        ),
        F.coalesce(
            F.sum(F.when(any_null, F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("_null_key_count"),
    ).first()

    result = KeyValidationSummary(
        row_count=int(summary["_row_count"]),
        distinct_key_count=int(summary["_distinct_key_count"]),
        null_key_count=int(summary["_null_key_count"]),
    )
    if result.null_key_count:
        raise ValueError(
            f"Null values found in key columns {keys}: "
            f"{result.null_key_count} row(s)"
        )
    if result.row_count != result.distinct_key_count:
        duplicate_count = result.row_count - result.distinct_key_count
        raise ValueError(
            f"Duplicate values found for key columns {keys}: "
            f"{duplicate_count} row(s)"
        )
    return result


def validate_replace_source_scope(
    df: DataFrame,
    filters: Mapping[str, SqlLiteral],
) -> None:
    """Reject source rows outside the slice an atomic write will replace."""
    if not filters:
        raise ValueError("At least one replacement filter is required")

    missing = [column for column in filters if column not in df.columns]
    if missing:
        raise ValueError(
            f"Missing replacement filter columns: {', '.join(missing)}"
        )

    predicates = []
    for column, value in filters.items():
        if value is None:
            predicates.append(F.col(column).isNull())
        else:
            predicates.append(F.col(column).eqNullSafe(F.lit(value)))

    in_scope = reduce(and_, predicates)
    summary = df.agg(
        F.coalesce(
            F.sum(F.when(~in_scope, F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("_out_of_scope_count")
    ).first()
    out_of_scope_count = int(summary["_out_of_scope_count"])
    if out_of_scope_count:
        raise ValueError(
            "Source contains "
            f"{out_of_scope_count} row(s) outside replacement scope"
        )


def validate_target_columns(
    spark: Any,
    target_table: str,
    source_columns: Sequence[str],
) -> list[str]:
    """Require exact names and return the existing target column order."""
    selected_columns = list(source_columns)
    if len(set(selected_columns)) != len(selected_columns):
        raise ValueError("Output columns must be unique")

    target_columns = list(spark.table(target_table).columns)
    missing = sorted(set(target_columns) - set(selected_columns))
    unexpected = sorted(set(selected_columns) - set(target_columns))
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing target columns: {', '.join(missing)}")
        if unexpected:
            details.append(
                f"unexpected source columns: {', '.join(unexpected)}"
            )
        raise ValueError(
            f"Source schema does not match {target_table}: "
            + "; ".join(details)
        )
    return target_columns


def quote_identifier(identifier: str) -> str:
    """Quote one SQL identifier."""
    value = identifier.strip()
    if not value:
        raise ValueError("SQL identifier cannot be empty")
    return f"`{value.replace('`', '``')}`"


def quote_qualified_identifier(identifier: str) -> str:
    """Quote every component of a qualified SQL identifier."""
    parts = [part.strip() for part in identifier.split(".")]
    if not parts or any(not part for part in parts):
        raise ValueError(f"Invalid qualified SQL identifier: {identifier!r}")
    return ".".join(quote_identifier(part) for part in parts)


def sql_literal(value: SqlLiteral) -> str:
    """Render a supported value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        escaped = value.isoformat(sep=" ").replace("'", "''")
        return f"TIMESTAMP '{escaped}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "Non-finite floats are not supported SQL literals"
            )
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported SQL literal type: {type(value).__name__}")


def build_equality_predicate(
    filters: Mapping[str, SqlLiteral], predicate_type: str = "="
) -> str:
    """Build an equality predicate from trusted column names and literal values."""
    if predicate_type not in ("=", "!="):
        raise ValueError("predicate_type must be '=' or '!='")
    if not filters:
        raise ValueError("At least one replacement filter is required")

    predicates = []
    for column, value in filters.items():
        quoted_column = quote_identifier(column)
        if value is None:
            predicates.append(f"{quoted_column} IS NULL")
        else:
            predicates.append(
                f"{quoted_column} {predicate_type} {sql_literal(value)}"
            )
    return " AND ".join(predicates)


def build_replace_where_statement(
    *,
    target_table: str,
    source_view: str,
    columns: Sequence[str],
    filters: Mapping[str, SqlLiteral] | None = None,
    replace_all: bool = False,
) -> str:
    """Build an atomic replacement after source columns are target-ordered."""
    selected_columns = list(columns)
    if not selected_columns:
        raise ValueError("At least one output column is required")
    if len(set(selected_columns)) != len(selected_columns):
        raise ValueError("Output columns must be unique")
    if replace_all == bool(filters):
        raise ValueError("Specify exactly one of filters or replace_all=True")

    predicate = (
        "TRUE" if replace_all else build_equality_predicate(filters or {})
    )
    column_sql = ", ".join(
        quote_identifier(column) for column in selected_columns
    )
    return (
        f"INSERT INTO {quote_qualified_identifier(target_table)}\n"
        f"REPLACE WHERE {predicate}\n"
        f"SELECT {column_sql}\n"
        f"FROM {quote_qualified_identifier(source_view)}"
    )


def build_append_statement(
    *,
    target_table: str,
    source_view: str,
    columns: Sequence[str],
) -> str:
    """Build a name-aligned append statement."""
    selected_columns = list(columns)
    if not selected_columns:
        raise ValueError("At least one output column is required")
    if len(set(selected_columns)) != len(selected_columns):
        raise ValueError("Output columns must be unique")

    column_sql = ", ".join(
        quote_identifier(column) for column in selected_columns
    )
    return (
        f"INSERT INTO {quote_qualified_identifier(target_table)} BY NAME\n"
        f"SELECT {column_sql}\n"
        f"FROM {quote_qualified_identifier(source_view)}"
    )


def build_delete_statement(*, target_table: str, delete_scope: str) -> str:
    """Build a name-aligned append statement."""
    if not delete_scope:
        raise ValueError("At least one deletion scope should be included")

    predicate = build_equality_predicate(
        delete_scope or {}, predicate_type="!="
    )
    return (
        f"DELETE FROM {quote_qualified_identifier(target_table)}\n"
        f"WHERE {predicate}"
    )


def atomic_replace_where_by_name(
    spark: Any,
    df: DataFrame,
    *,
    target_table: str,
    filters: Mapping[str, SqlLiteral] | None = None,
    replace_all: bool = False,
    columns: Sequence[str] | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
    commit_metadata: Mapping[str, Any] | None = None,
    capture_receipt: bool = True,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
    delete_scope: Mapping[str, SqlLiteral] | None = None,
) -> DeltaWriteReceipt:
    """Atomically replace a target slice, matching source columns by name."""
    if (replace_all + bool(filters) + bool(delete_scope)) != 1:
        raise ValueError(
            "Specify exactly one of filters delete_Scope or replace_all=True"
        )
    selected_columns = list(columns or df.columns)
    target_columns = validate_target_columns(
        spark,
        target_table,
        selected_columns,
    )
    if filters:
        omitted_filters = [
            column for column in filters if column not in selected_columns
        ]
        if omitted_filters:
            raise ValueError(
                "Replacement filter columns must be written: "
                f"{', '.join(omitted_filters)}"
            )
    if delete_scope:
        omitted_deletions = [
            column for column in delete_scope if column not in selected_columns
        ]
        if omitted_deletions:
            raise ValueError(
                "Replacement deletion columns must be written: "
                f"{', '.join(omitted_filters)}"
            )

    return _write_from_temporary_view(
        spark,
        df,
        target_table=target_table,
        statement_builder=lambda source_view, selected_columns: (
            build_delete_statement(
                target_table=target_table, delete_scope=delete_scope
            )
            if delete_scope
            else build_replace_where_statement(
                target_table=target_table,
                source_view=source_view,
                columns=selected_columns,
                filters=filters,
                replace_all=replace_all,
            )
        ),
        # DBR 15.4 cannot combine name-matched inserts with selective
        # replacement. Selecting the validated source by target schema order
        # retains name alignment and one atomic Delta statement.
        columns=target_columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=capture_receipt,
        sleep=sleep,
        jitter=jitter,
        delete_run=True if delete_scope else False,
    )


def replace_table_by_name(
    df: DataFrame,
    table: str,
    columns: Sequence[str] | None = None,
    *,
    spark: Any | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
    commit_metadata: Mapping[str, Any] | None = None,
    capture_receipt: bool = True,
) -> DeltaWriteReceipt:
    """Atomically replace a complete Delta table using name-aligned columns."""
    return atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        replace_all=True,
        columns=columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=capture_receipt,
    )


def replace_scope_by_name(
    df: DataFrame,
    table: str,
    scope: Mapping[str, SqlLiteral],
    columns: Sequence[str] | None = None,
    *,
    spark: Any | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
    commit_metadata: Mapping[str, Any] | None = None,
    capture_receipt: bool = True,
) -> DeltaWriteReceipt:
    """Atomically replace one structured equality/date scope by column name."""
    return atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        filters=scope,
        columns=columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=capture_receipt,
    )


def replace_and_update_scope_by_name(
    df: DataFrame,
    table: str,
    scope: Mapping[str, SqlLiteral],
    columns: Sequence[str] | None = None,
    *,
    spark: Any | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
    commit_metadata: Mapping[str, Any] | None = None,
    capture_receipt: bool = True,
    delete_scope: Mapping[str, SqlLiteral] | None = None,
) -> Tuple[DeltaWriteReceipt, DeltaWriteReceipt]:
    """Atomically replace structured equality/date scope by column name
    and remove all otherrecords not in scope columns
    """
    logger.info("Deleting records from table based on deletion scope")
    delete_receipt = atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        columns=columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=False,
        delete_scope=delete_scope,
    )
    logger.info("Updating records in table based on deletion scope")
    replace_reciept = atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        filters=scope,
        columns=columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=capture_receipt,
    )
    return (delete_receipt, replace_reciept)


def atomic_append_by_name(
    spark: Any,
    df: DataFrame,
    *,
    target_table: str,
    columns: Sequence[str] | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    build_id: str | None = None,
    attempt_id: str | None = None,
    git_commit: str | None = None,
    commit_metadata: Mapping[str, Any] | None = None,
    capture_receipt: bool = True,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> DeltaWriteReceipt:
    """Append to a target while matching source columns by name."""
    selected_columns = list(columns or df.columns)
    validate_target_columns(spark, target_table, selected_columns)
    return _write_from_temporary_view(
        spark,
        df,
        target_table=target_table,
        statement_builder=lambda source_view, selected_columns: (
            build_append_statement(
                target_table=target_table,
                source_view=source_view,
                columns=selected_columns,
            )
        ),
        columns=selected_columns,
        retry_policy=retry_policy,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata=commit_metadata,
        capture_receipt=capture_receipt,
        sleep=sleep,
        jitter=jitter,
    )


def _write_from_temporary_view(
    spark: Any,
    df: DataFrame,
    *,
    target_table: str,
    statement_builder: Callable[[str, Sequence[str]], str],
    columns: Sequence[str] | None,
    retry_policy: DeltaRetryPolicy | None,
    build_id: str | None,
    attempt_id: str | None,
    git_commit: str | None,
    commit_metadata: Mapping[str, Any] | None,
    capture_receipt: bool,
    sleep: Callable[[float], None],
    jitter: Callable[[], float],
    delete_run: bool = False,
) -> DeltaWriteReceipt:
    selected_columns = list(columns or df.columns) if not delete_run else []
    if not selected_columns and not delete_run:
        raise ValueError("At least one output column is required")
    missing = [
        column for column in selected_columns if column not in df.columns
    ]
    if missing and not delete_run:
        raise ValueError(f"Missing output columns: {', '.join(missing)}")

    if not delete_run:
        source_view = f"_nextads_delta_write_{uuid.uuid4().hex}"
        df.select(*selected_columns).createOrReplaceTempView(source_view)
    else:
        source_view = None
    statement = statement_builder(source_view, selected_columns)
    receipt_metadata = _ReceiptMetadata(
        receipt_id=uuid.uuid4().hex,
        target_table=target_table,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        details=commit_metadata,
    )
    started = time.monotonic()
    try:
        if _supports_commit_receipts(spark):
            with _COMMIT_METADATA_LOCK:
                previous_metadata = _set_commit_metadata(
                    spark, receipt_metadata.as_json()
                )
                try:
                    attempts = _execute_with_delta_retry(
                        spark,
                        statement,
                        retry_policy=retry_policy or DeltaRetryPolicy(),
                        sleep=sleep,
                        jitter=jitter,
                    )
                    if capture_receipt:
                        receipt = _read_commit_receipt(
                            spark,
                            statement=statement,
                            attempts=attempts,
                            metadata=receipt_metadata,
                            write_duration_ms=int(
                                (time.monotonic() - started) * 1000
                            ),
                        )
                    else:
                        receipt = DeltaWriteReceipt(
                            statement=statement,
                            attempts=attempts,
                            receipt_id=receipt_metadata.receipt_id,
                            target_table=target_table,
                            build_id=build_id,
                            attempt_id=attempt_id,
                            git_commit=git_commit,
                            write_duration_ms=int(
                                (time.monotonic() - started) * 1000
                            ),
                        )
                finally:
                    _restore_commit_metadata(spark, previous_metadata)
        else:
            attempts = _execute_with_delta_retry(
                spark,
                statement,
                retry_policy=retry_policy or DeltaRetryPolicy(),
                sleep=sleep,
                jitter=jitter,
            )
            receipt = DeltaWriteReceipt(
                statement=statement,
                attempts=attempts,
                receipt_id=receipt_metadata.receipt_id,
                target_table=target_table,
                build_id=build_id,
                attempt_id=attempt_id,
                git_commit=git_commit,
                write_duration_ms=int((time.monotonic() - started) * 1000),
            )
    finally:
        if not delete_run:
            spark.catalog.dropTempView(source_view)
    return receipt


def _supports_commit_receipts(spark: Any) -> bool:
    conf = getattr(spark, "conf", None)
    return all(hasattr(conf, name) for name in ("get", "set", "unset"))


def _set_commit_metadata(spark: Any, value: str) -> str | None:
    try:
        previous = spark.conf.get(_DELTA_COMMIT_METADATA_KEY)
    except Exception:
        previous = None
    spark.conf.set(_DELTA_COMMIT_METADATA_KEY, value)
    return previous


def _restore_commit_metadata(spark: Any, previous: str | None) -> None:
    if previous is None:
        spark.conf.unset(_DELTA_COMMIT_METADATA_KEY)
    else:
        spark.conf.set(_DELTA_COMMIT_METADATA_KEY, previous)


def schema_checksum(frame: Any) -> str:
    """Return a stable checksum for an ordered Spark schema."""
    schema = frame.schema
    signature = [
        (field.name, field.dataType.simpleString()) for field in schema
    ]
    return hashlib.sha256(
        json.dumps(signature, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _target_schema_checksum(spark: Any, target_table: str) -> str:
    return schema_checksum(spark.table(target_table))


def _read_commit_receipt(
    spark: Any,
    *,
    statement: str,
    attempts: int,
    metadata: _ReceiptMetadata,
    write_duration_ms: int,
) -> DeltaWriteReceipt:
    metadata_json = metadata.as_json()
    history = spark.sql(
        f"DESCRIBE HISTORY {quote_qualified_identifier(metadata.target_table)}"
    )
    matching_rows = (
        history.where(F.col("userMetadata") == F.lit(metadata_json))
        .orderBy(F.col("version").desc())
        .limit(10)
        .collect()
    )
    write_rows = [
        row for row in matching_rows if _is_data_write_history_row(row)
    ]
    if not write_rows:
        raise RuntimeError(
            "Delta write completed without a matching transaction receipt: "
            f"{metadata.receipt_id}"
        )
    if len(write_rows) != 1:
        raise RuntimeError(
            "Multiple Delta writes share one transaction receipt: "
            f"{metadata.receipt_id}"
        )
    return _receipt_from_history_row(
        spark,
        row=write_rows[0],
        statement=statement,
        attempts=attempts,
        metadata=metadata,
        write_duration_ms=write_duration_ms,
    )


def _receipt_from_history_row(
    spark: Any,
    *,
    row: Any,
    statement: str,
    attempts: int,
    metadata: _ReceiptMetadata,
    write_duration_ms: int,
) -> DeltaWriteReceipt:
    row_count = _operation_row_count(row["operationMetrics"])
    committed_at = row["timestamp"]
    if committed_at is not None and committed_at.tzinfo is None:
        committed_at = committed_at.replace(tzinfo=timezone.utc)
    return DeltaWriteReceipt(
        statement=statement,
        attempts=attempts,
        receipt_id=metadata.receipt_id,
        target_table=metadata.target_table,
        delta_version=int(row["version"]),
        row_count=row_count,
        schema_checksum=_target_schema_checksum(spark, metadata.target_table),
        build_id=metadata.build_id,
        attempt_id=metadata.attempt_id,
        git_commit=metadata.git_commit,
        committed_at=committed_at,
        write_duration_ms=write_duration_ms,
    )


def _execute_with_delta_retry(
    spark: Any,
    statement: str,
    *,
    retry_policy: DeltaRetryPolicy,
    sleep: Callable[[float], None],
    jitter: Callable[[], float],
) -> int:
    for attempt in range(1, retry_policy.max_attempts + 1):
        try:
            spark.sql(statement)
            return attempt
        except DeltaConcurrentModificationException:
            if attempt == retry_policy.max_attempts:
                raise
            base_delay = min(
                retry_policy.initial_backoff_seconds * (2 ** (attempt - 1)),
                retry_policy.max_backoff_seconds,
            )
            sleep(base_delay + (retry_policy.jitter_seconds * jitter()))

    raise RuntimeError("Delta retry loop ended unexpectedly")
