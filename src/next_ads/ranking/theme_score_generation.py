from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F


def select_latest_view_themes(view_themes: DataFrame) -> DataFrame:
    """Select one deterministic latest viewed theme per account."""
    latest_window = Window.partitionBy("account_number").orderBy(
        F.col("date").desc_nulls_last(),
        F.col("theme").asc_nulls_last(),
    )
    return (
        view_themes.select("account_number", "theme", "date")
        .dropDuplicates(["account_number", "theme", "date"])
        .withColumn("_latest_rank", F.row_number().over(latest_window))
        .where(F.col("_latest_rank") == 1)
        .select("account_number", "theme")
    )


def select_global_top_themes(
    theme_sales: DataFrame,
    *,
    limit: int = 25,
) -> DataFrame:
    """Select globally popular fallback themes with a total order."""
    if limit <= 0:
        raise ValueError("limit must be greater than zero")
    return (
        theme_sales.orderBy(
            F.col("sales_count").desc(),
            F.col("theme").asc(),
        )
        .limit(limit)
        .select(F.col("theme").alias("next_theme"))
        .withColumn("prob_agg", F.lit(0.0))
        .withColumn("prob_base", F.lit(0.0))
        .withColumn("prob_agg_rebased", F.lit(-999.0))
    )


def merge_and_rank_theme_scores(
    scores: DataFrame,
    global_top_themes: DataFrame,
    *,
    limit: int = 100,
) -> DataFrame:
    """Add only missing fallback keys and deterministically rank each account."""
    if limit <= 0:
        raise ValueError("limit must be greater than zero")

    key_columns = ["account_number", "next_theme"]
    existing_pairs = scores.select(*key_columns).distinct()
    missing_fallbacks = (
        scores.select("account_number")
        .distinct()
        .crossJoin(F.broadcast(global_top_themes))
        .join(existing_pairs, on=key_columns, how="left_anti")
    )
    rank_window = Window.partitionBy("account_number").orderBy(
        F.col("prob_agg_rebased").desc_nulls_last(),
        F.col("prob_agg").desc_nulls_last(),
        F.col("next_theme").asc(),
    )
    return (
        scores.unionByName(missing_fallbacks)
        .withColumn("_score_rank", F.row_number().over(rank_window))
        .where(F.col("_score_rank") <= limit)
        .drop("_score_rank")
    )
