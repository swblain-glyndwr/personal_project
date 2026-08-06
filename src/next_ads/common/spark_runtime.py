"""Lean Spark settings for the distributed NextAds critical path."""

from __future__ import annotations

from typing import Any


def configure_lean_spark(
    spark: Any,
    *,
    max_records_per_file: int = 1_000_000,
) -> None:
    """Let Databricks size shuffles while bounding individual Delta files."""
    if max_records_per_file < 1:
        raise ValueError("max_records_per_file must be positive")
    settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.partitions": "auto",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
        "spark.sql.files.maxRecordsPerFile": str(max_records_per_file),
    }
    for name, value in settings.items():
        spark.conf.set(name, value)


__all__ = ["configure_lean_spark"]
