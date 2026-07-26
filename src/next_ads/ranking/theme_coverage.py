"""Validation helpers for ad-theme coverage against Theme Affinity output."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalised_non_blank(column_name: str):
    return F.lower(F.trim(F.col(column_name).cast("string")))


def route_ad_themes(control_ads: DataFrame, route: str) -> DataFrame:
    """Return distinct route ad themes with ad counts."""
    return (
        control_ads.where(F.coalesce(F.col("AudienceOnly").cast("int"), F.lit(0)) != 1)
        .withColumn("Theme", normalised_non_blank("Themes"))
        .where(F.col("Theme").isNotNull())
        .where(F.col("Theme") != "")
        .groupBy("Theme")
        .agg(F.countDistinct("UniqueAdID").alias("ad_count"))
        .withColumn("route", F.lit(route))
        .select("route", "Theme", "ad_count")
    )


def theme_affinity_themes(theme_affinity_scores: DataFrame) -> DataFrame:
    """Return distinct customer-theme model output themes."""
    return (
        theme_affinity_scores.withColumn("Theme", normalised_non_blank("NextTheme"))
        .where(F.col("Theme").isNotNull())
        .where(F.col("Theme") != "")
        .select("Theme")
        .distinct()
    )


def build_missing_theme_affinity_coverage(
    control_ads: DataFrame,
    theme_affinity_scores: DataFrame,
    route: str,
) -> DataFrame:
    """Find ad themes that cannot join to the shared Theme Affinity output."""
    route_themes = route_ad_themes(control_ads, route)
    model_themes = theme_affinity_themes(theme_affinity_scores)

    return route_themes.join(model_themes, on="Theme", how="left_anti").select(
        "route",
        "Theme",
        "ad_count",
    )
