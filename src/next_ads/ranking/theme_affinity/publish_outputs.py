from __future__ import annotations

from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    replace_table_by_name,
)


DEFAULT_PUBLISH_TABLE_SUFFIXES = (
    "ranked",
    "complete",
    "advanced_features",
    "customer_features",
    "customer_segments",
    "popularity_metrics",
)


def parse_table_suffixes(table_suffixes: str | None) -> tuple[str, ...]:
    if not table_suffixes:
        return DEFAULT_PUBLISH_TABLE_SUFFIXES
    return tuple(
        suffix.strip()
        for suffix in table_suffixes.split(",")
        if suffix.strip()
    )


def publish_theme_affinity_outputs(
    spark,
    *,
    source_namespace: str,
    target_namespace: str,
    table_prefix: str,
    target_table_prefix: str | None = None,
    table_suffixes: tuple[str, ...] = DEFAULT_PUBLISH_TABLE_SUFFIXES,
) -> list[str]:
    source_namespace = _normalise_namespace(source_namespace)
    target_namespace = _normalise_namespace(target_namespace)
    target_table_prefix = target_table_prefix or table_prefix
    if source_namespace == target_namespace and table_prefix == target_table_prefix:
        return []

    published_tables = []
    for suffix in table_suffixes:
        source_table = f"{source_namespace}.{table_prefix}_{suffix}"
        target_table = f"{target_namespace}.{target_table_prefix}_{suffix}"
        source_df = _read_required_table(spark, source_table)
        _ensure_target_table(spark, source_table, target_table)
        replace_table_by_name(
            source_df,
            target_table,
            source_df.columns,
            spark=spark,
        )
        published_tables.append(target_table)
    return published_tables


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
