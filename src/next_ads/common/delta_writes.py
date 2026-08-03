from __future__ import annotations

import math
import random
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce
from operator import and_, or_
from typing import Any

from delta.exceptions import DeltaConcurrentModificationException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


__all__ = [
    "DeltaRetryPolicy",
    "DeltaWriteResult",
    "KeyValidationSummary",
    "atomic_append_by_name",
    "atomic_replace_where_by_name",
    "replace_scope_by_name",
    "replace_table_by_name",
    "validate_target_columns",
    "validate_replace_source_scope",
    "validate_unique_non_null_keys",
    "quote_qualified_identifier",
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
class DeltaWriteResult:
    statement: str
    attempts: int


SqlLiteral = str | int | float | bool | date | datetime | None


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
    summary = (
        df.agg(
            F.count(F.lit(1)).alias("_row_count"),
            F.countDistinct(
                F.struct(*[F.col(column) for column in keys])
            ).alias("_distinct_key_count"),
            F.coalesce(
                F.sum(F.when(any_null, F.lit(1)).otherwise(F.lit(0))),
                F.lit(0),
            ).alias("_null_key_count"),
        )
        .first()
    )

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
    summary = (
        df.agg(
            F.coalesce(
                F.sum(
                    F.when(~in_scope, F.lit(1)).otherwise(F.lit(0))
                ),
                F.lit(0),
            ).alias("_out_of_scope_count")
        )
        .first()
    )
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
            raise ValueError("Non-finite floats are not supported SQL literals")
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported SQL literal type: {type(value).__name__}")


def build_equality_predicate(filters: Mapping[str, SqlLiteral]) -> str:
    """Build an equality predicate from trusted column names and literal values."""
    if not filters:
        raise ValueError("At least one replacement filter is required")

    predicates = []
    for column, value in filters.items():
        quoted_column = quote_identifier(column)
        if value is None:
            predicates.append(f"{quoted_column} IS NULL")
        else:
            predicates.append(f"{quoted_column} = {sql_literal(value)}")
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

    predicate = "TRUE" if replace_all else build_equality_predicate(filters or {})
    column_sql = ", ".join(quote_identifier(column) for column in selected_columns)
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

    column_sql = ", ".join(quote_identifier(column) for column in selected_columns)
    return (
        f"INSERT INTO {quote_qualified_identifier(target_table)} BY NAME\n"
        f"SELECT {column_sql}\n"
        f"FROM {quote_qualified_identifier(source_view)}"
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
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> DeltaWriteResult:
    """Atomically replace a target slice, matching source columns by name."""
    if replace_all == bool(filters):
        raise ValueError("Specify exactly one of filters or replace_all=True")

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
        validate_replace_source_scope(df, filters)

    return _write_from_temporary_view(
        spark,
        df,
        statement_builder=lambda source_view, selected_columns: (
            build_replace_where_statement(
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
        sleep=sleep,
        jitter=jitter,
    )


def replace_table_by_name(
    df: DataFrame,
    table: str,
    columns: Sequence[str] | None = None,
    *,
    spark: Any | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
) -> DeltaWriteResult:
    """Atomically replace a complete Delta table using name-aligned columns."""
    return atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        replace_all=True,
        columns=columns,
        retry_policy=retry_policy,
    )


def replace_scope_by_name(
    df: DataFrame,
    table: str,
    scope: Mapping[str, SqlLiteral],
    columns: Sequence[str] | None = None,
    *,
    spark: Any | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
) -> DeltaWriteResult:
    """Atomically replace one structured equality/date scope by column name."""
    return atomic_replace_where_by_name(
        spark or df.sparkSession,
        df,
        target_table=table,
        filters=scope,
        columns=columns,
        retry_policy=retry_policy,
    )


def atomic_append_by_name(
    spark: Any,
    df: DataFrame,
    *,
    target_table: str,
    columns: Sequence[str] | None = None,
    retry_policy: DeltaRetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> DeltaWriteResult:
    """Append to a target while matching source columns by name."""
    selected_columns = list(columns or df.columns)
    validate_target_columns(spark, target_table, selected_columns)
    return _write_from_temporary_view(
        spark,
        df,
        statement_builder=lambda source_view, selected_columns: (
            build_append_statement(
                target_table=target_table,
                source_view=source_view,
                columns=selected_columns,
            )
        ),
        columns=selected_columns,
        retry_policy=retry_policy,
        sleep=sleep,
        jitter=jitter,
    )


def _write_from_temporary_view(
    spark: Any,
    df: DataFrame,
    *,
    statement_builder: Callable[[str, Sequence[str]], str],
    columns: Sequence[str] | None,
    retry_policy: DeltaRetryPolicy | None,
    sleep: Callable[[float], None],
    jitter: Callable[[], float],
) -> DeltaWriteResult:
    selected_columns = list(columns or df.columns)
    if not selected_columns:
        raise ValueError("At least one output column is required")
    missing = [column for column in selected_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing output columns: {', '.join(missing)}")

    source_view = f"_nextads_delta_write_{uuid.uuid4().hex}"
    statement = statement_builder(source_view, selected_columns)
    df.select(*selected_columns).createOrReplaceTempView(source_view)
    try:
        attempts = _execute_with_delta_retry(
            spark,
            statement,
            retry_policy=retry_policy or DeltaRetryPolicy(),
            sleep=sleep,
            jitter=jitter,
        )
    finally:
        spark.catalog.dropTempView(source_view)
    return DeltaWriteResult(statement=statement, attempts=attempts)


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
