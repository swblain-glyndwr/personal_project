"""Validation helpers for the v1/v2 Theme Mapping sheet copy contract."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


THEME_MAPPING_COMPARE_COLUMNS = [
    "Theme",
    "TargetingAttributes",
    "ThemeType",
    "ThemeTypeRank",
    "AdType",
    "AdTypeRank",
]


def canonicalise_theme_mapping_for_comparison(df: DataFrame) -> DataFrame:
    """Return the effective Theme Mapping values used by the parser."""
    select_exprs = []
    for column_name in THEME_MAPPING_COMPARE_COLUMNS:
        column = F.col(column_name)
        if column_name in {"ThemeTypeRank", "AdTypeRank"}:
            normalised = F.coalesce(
                column.cast("int").cast("string"), F.lit("")
            )
        else:
            normalised = F.coalesce(
                F.lower(F.trim(column.cast("string"))), F.lit("")
            )
        select_exprs.append(normalised.alias(column_name))

    return df.select(*select_exprs).where(F.col("Theme") != "").distinct()


def build_theme_mapping_differences(
    v1_theme_mapping: DataFrame,
    v2_theme_mapping: DataFrame,
) -> DataFrame:
    """Compare copied v1 Theme Mapping rows against the v2 source rows."""
    v1 = canonicalise_theme_mapping_for_comparison(v1_theme_mapping)
    v2 = canonicalise_theme_mapping_for_comparison(v2_theme_mapping)

    v1_only = v1.subtract(v2).withColumn("difference_type", F.lit("v1_only"))
    v2_only = v2.subtract(v1).withColumn("difference_type", F.lit("v2_only"))

    return v1_only.unionByName(v2_only).select(
        "difference_type",
        *THEME_MAPPING_COMPARE_COLUMNS,
    )
