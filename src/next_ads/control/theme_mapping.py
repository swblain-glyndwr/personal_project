"""Reusable theme-mapping helpers for NextAds control metadata."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from next_ads.control.attributes import parse_ad_attributes


def normalise_theme_mapping(df_themes: DataFrame) -> DataFrame:
    """Apply legacy normalisation to raw theme-mapping rows."""
    return df_themes.withColumn("Theme", F.trim(F.lower(F.col("Theme"))))


def valid_theme_rank_condition():
    """Return the legacy positive-rank validation condition."""
    return (
        (F.col("ThemeTypeRank").cast("int").isNotNull())
        & (F.col("ThemeTypeRank").cast("int") > 0)
        & (F.col("AdTypeRank").cast("int").isNotNull())
        & (F.col("AdTypeRank").cast("int") > 0)
    )


def collect_invalid_theme_ranks(df_themes: DataFrame) -> list[str]:
    """Collect themes removed by the positive-rank validation."""
    invalid_condition = ~valid_theme_rank_condition()
    return [
        row[0]
        for row in (
            df_themes.filter(invalid_condition)
            .select("Theme")
            .distinct()
            .collect()
        )
    ]


def filter_valid_theme_ranks(df_themes: DataFrame) -> DataFrame:
    """Keep only themes with positive integer ranking fields."""
    return df_themes.filter(valid_theme_rank_condition())


def build_theme_attributes(df_themes: DataFrame) -> DataFrame:
    """Parse theme targeting attributes into theme-attribute rows."""
    return parse_ad_attributes(
        df=df_themes.select("Theme", "TargetingAttributes"),
        ad_id_col="Theme",
    ).distinct()


def build_item_themes(
    item_attributes: DataFrame,
    theme_attributes: DataFrame,
) -> DataFrame:
    """Map items to themes when all required theme attributes are present."""
    item_theme_joined = (
        item_attributes.alias("i")
        .join(theme_attributes.alias("t"), on="attribute", how="inner")
        .where(F.col("i.value") == F.col("t.value"))
    )
    matched_counts = item_theme_joined.groupBy("pid", "Theme").agg(
        F.countDistinct("attribute").alias("n_matched")
    )
    required_counts = item_theme_joined.groupBy("Theme").agg(
        F.countDistinct("attribute").alias("n_required")
    )
    return (
        matched_counts.join(required_counts, on="Theme", how="inner")
        .where(F.col("n_matched") == F.col("n_required"))
        .select(F.col("pid"), F.col("Theme").alias("theme"))
    )


def rank_item_themes(
    item_themes: DataFrame,
    df_themes: DataFrame,
    theme_ranking_mode: str,
) -> DataFrame:
    """Rank candidate themes per item using the configured legacy mode."""
    if theme_ranking_mode == "adtype-themefreq":
        theme_freq = item_themes.groupBy("theme").agg(
            F.count("pid").alias("MatchingItems")
        )
        return (
            item_themes.join(theme_freq, on="theme", how="left")
            .join(
                df_themes.select("Theme", "AdTypeRank").withColumnRenamed(
                    "Theme", "theme"
                ),
                on="theme",
                how="left",
            )
            .withColumn("AdTypeScore", F.lit(1.0) / F.col("AdTypeRank").cast("float"))
            .withColumn("FreqScore", F.lit(1.0) / F.col("MatchingItems").cast("float"))
            .fillna({"AdTypeScore": -1.0, "AdTypeRank": -1.0})
            .withColumn(
                "theme_rank",
                F.dense_rank().over(
                    Window.partitionBy("pid").orderBy(
                        F.desc(F.col("AdTypeScore")),
                        F.desc(F.col("FreqScore")),
                    )
                ),
            )
        )

    if theme_ranking_mode == "adtype-themetype":
        return (
            item_themes.join(
                df_themes.select(
                    "Theme", "AdTypeRank", "ThemeTypeRank"
                ).withColumnRenamed("Theme", "theme"),
                on="theme",
                how="left",
            )
            .withColumn("AdTypeScore", F.lit(1.0) / F.col("AdTypeRank").cast("float"))
            .withColumn(
                "ThemeTypeScore",
                F.lit(1.0) / F.col("ThemeTypeRank").cast("float"),
            )
            .fillna({"AdTypeScore": -1.0, "AdTypeRank": -1.0})
            .withColumn(
                "theme_rank",
                F.dense_rank().over(
                    Window.partitionBy("pid").orderBy(
                        F.desc(F.col("AdTypeScore")),
                        F.desc(F.col("ThemeTypeScore")),
                    )
                ),
            )
        )

    raise ValueError(f"Unknown THEME_RANKING_MODE: {theme_ranking_mode}")
