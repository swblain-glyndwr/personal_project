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


def build_theme_mapping_differences(
    v1_mapping: DataFrame,
    v2_mapping: DataFrame,
) -> DataFrame:
    """Return normalized row-level differences between route mapping tabs."""
    compare_cols = [
        col
        for col in THEME_MAPPING_COMPARE_COLUMNS
        if col in v1_mapping.columns and col in v2_mapping.columns
    ]
    if not compare_cols:
        raise ValueError("No comparable Theme Mapping columns found")

    v1_rows = v1_mapping.select(*compare_cols).distinct()
    v2_rows = v2_mapping.select(*compare_cols).distinct()

    v1_only = (
        v1_rows.join(v2_rows, on=compare_cols, how="left_anti")
        .withColumn("difference_type", F.lit("v1_only"))
        .select("difference_type", *compare_cols)
    )
    v2_only = (
        v2_rows.join(v1_rows, on=compare_cols, how="left_anti")
        .withColumn("difference_type", F.lit("v2_only"))
        .select("difference_type", *compare_cols)
    )

    return v1_only.unionByName(v2_only)
