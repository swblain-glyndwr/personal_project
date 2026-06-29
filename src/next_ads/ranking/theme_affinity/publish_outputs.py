from __future__ import annotations


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
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        published_tables.append(target_table)
    return published_tables


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
