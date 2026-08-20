from __future__ import annotations

from collections.abc import Mapping

from pyspark import StorageLevel
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    replace_scope_by_name,
    replace_table_by_name,
)
from next_ads.ranking.scoring_inputs import latest_delta_version


def _config_value(config, name, default=None):
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _require_single_transaction(
    spark,
    table: str,
    previous_version: int,
) -> int:
    output_version = latest_delta_version(spark, table)
    if output_version != previous_version + 1:
        raise ValueError(f"Table {table} changed during provider publication")
    return output_version


def build_markov_compatibility_scores(signals):
    """Rebuild the legacy Markov shape from accepted canonical scores."""
    return signals.select(
        "AccountNumber",
        F.col("EntityID").alias("NextTheme"),
        F.col("RawScore").cast("float").alias("ProbAgg"),
        (F.col("RawScore") - F.col("Score"))
        .cast("float")
        .alias("ProbBase"),
        F.col("Score").cast("float").alias("ProbAggRebased"),
        F.col("RunDate").alias("rundate"),
    )


def publish_markov_compatibility_outputs(
    spark,
    config,
    context,
    signals,
    _completed_at,
):
    """Publish legacy Markov history/latest from one validated signal frame."""
    history_table = config.tables_write.next_theme_scores
    latest_table = config.tables_write.next_theme_scores_latest
    legacy = build_markov_compatibility_scores(signals).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        history_before = latest_delta_version(spark, history_table)
        replace_scope_by_name(
            legacy,
            history_table,
            {"rundate": context.run_date},
            legacy.columns,
            spark=spark,
        )
        history_version = _require_single_transaction(
            spark,
            history_table,
            history_before,
        )

        latest_before = latest_delta_version(spark, latest_table)
        replace_table_by_name(
            legacy,
            latest_table,
            legacy.columns,
            spark=spark,
        )
        latest_version = _require_single_transaction(
            spark,
            latest_table,
            latest_before,
        )
        return {
            "history": history_version,
            "latest": latest_version,
        }
    finally:
        legacy.unpersist()


def configured_compatibility_publisher(
    spark,
    *,
    config,
    context,
    provider_config,
):
    """Resolve an optional legacy publisher outside the canonical contract."""
    publisher_name = _config_value(
        provider_config,
        "compatibility_publisher",
        "none",
    )
    if publisher_name in {None, "", "none"}:
        return lambda _signals, _completed_at: {}
    if publisher_name == "markov_legacy":
        return lambda signals, completed_at: (
            publish_markov_compatibility_outputs(
                spark,
                config,
                context,
                signals,
                completed_at,
            )
        )
    if publisher_name == "theme_affinity_legacy":
        from next_ads.ranking.theme_affinity.clean_output import (
            publish_theme_affinity_compatibility_outputs,
        )

        return lambda signals, completed_at: (
            publish_theme_affinity_compatibility_outputs(
                spark,
                config,
                context,
                signals,
                completed_at,
            )
        )
    raise ValueError(
        f"Unsupported provider compatibility publisher: {publisher_name}"
    )


__all__ = [
    "build_markov_compatibility_scores",
    "configured_compatibility_publisher",
    "publish_markov_compatibility_outputs",
]
