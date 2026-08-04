from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    replace_scope_by_name,
)


DEFAULT_PUBLISH_TABLE_SUFFIXES = (
    "advanced_features",
    "customer_features",
    "customer_segments",
    "popularity_metrics",
)
FOUNDATION_OWNED_SUFFIXES = frozenset(("ranked", "complete", "build_marker"))

MAX_PUBLISH_WORKERS = 4


def parse_table_suffixes(table_suffixes: str | None) -> tuple[str, ...]:
    if not table_suffixes:
        return DEFAULT_PUBLISH_TABLE_SUFFIXES
    suffixes = tuple(
        suffix.strip()
        for suffix in table_suffixes.split(",")
        if suffix.strip()
    )
    forbidden = sorted(set(suffixes).intersection(FOUNDATION_OWNED_SUFFIXES))
    if forbidden:
        raise ValueError(
            "Foundation-owned outputs cannot use compatibility publication: "
            + ", ".join(forbidden)
        )
    return suffixes


def publish_theme_affinity_outputs(
    spark,
    *,
    source_namespace: str,
    target_namespace: str,
    table_prefix: str,
    target_table_prefix: str | None = None,
    table_suffixes: tuple[str, ...] = DEFAULT_PUBLISH_TABLE_SUFFIXES,
    run_date: str,
) -> list[str]:
    source_namespace = _normalise_namespace(source_namespace)
    target_namespace = _normalise_namespace(target_namespace)
    target_table_prefix = target_table_prefix or table_prefix
    if (
        source_namespace == target_namespace
        and table_prefix == target_table_prefix
    ):
        return []
    if not table_suffixes:
        return []

    published_tables: list[str | None] = [None] * len(table_suffixes)
    max_workers = min(MAX_PUBLISH_WORKERS, len(table_suffixes))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _publish_output,
                spark,
                source_namespace=source_namespace,
                target_namespace=target_namespace,
                table_prefix=table_prefix,
                target_table_prefix=target_table_prefix,
                suffix=suffix,
                run_date=run_date,
            ): index
            for index, suffix in enumerate(table_suffixes)
        }
        try:
            for future in as_completed(futures):
                published_tables[futures[future]] = future.result()
        except Exception:
            for future in futures:
                future.cancel()
            raise

    if any(table is None for table in published_tables):
        raise RuntimeError("Theme Affinity output publication was incomplete")
    return [table for table in published_tables if table is not None]


def _publish_output(
    spark,
    *,
    source_namespace: str,
    target_namespace: str,
    table_prefix: str,
    target_table_prefix: str,
    suffix: str,
    run_date: str,
) -> str:
    from pyspark.sql import functions as F

    source_table = f"{source_namespace}.{table_prefix}_{suffix}"
    target_table = f"{target_namespace}.{target_table_prefix}_{suffix}"
    source_df = _read_required_table(spark, source_table).where(
        F.col("reference_date") == F.lit(run_date).cast("date")
    )
    if source_df.limit(1).count() == 0:
        raise ValueError(
            "Required Theme Affinity compatibility output is empty for "
            f"{run_date}: {source_table}"
        )
    _ensure_target_table(spark, source_table, target_table)
    replace_scope_by_name(
        source_df,
        target_table,
        {"reference_date": run_date},
        source_df.columns,
        spark=spark,
    )
    return target_table


def _ensure_target_table(spark, source_table: str, target_table: str) -> None:
    """Bootstrap a missing publish target before its first atomic replacement."""
    if spark.catalog.tableExists(target_table):
        return
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
        f"{quote_qualified_identifier(target_table)} "
        f"LIKE {quote_qualified_identifier(source_table)}"
    )


def _read_required_table(spark, table_name: str):
    try:
        return spark.table(table_name)
    except Exception as exc:
        raise ValueError(
            f"Required Theme Affinity publish source table not found: {table_name}"
        ) from exc


def _normalise_namespace(namespace: str) -> str:
    value = (namespace or "").strip().strip(".")
    if value.count(".") != 1:
        raise ValueError(
            "Theme Affinity publish namespace must be catalog.schema: "
            f"{namespace!r}"
        )
    return value
