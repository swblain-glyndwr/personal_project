from pyspark.sql import Window
from pyspark.sql import functions as F

from next_ads.decisioning.assignment import generate_repeat_ad_sessions


def calculate_score_range(df_theme_scores, logger):
    bounds = df_theme_scores.agg(
        F.min("ProbAggRebased").alias("min_score"),
        F.max("ProbAggRebased").alias("max_score"),
    ).first()
    min_score = bounds["min_score"]
    max_score = bounds["max_score"]
    if min_score is None or max_score is None:
        raise ValueError("Provider theme scores are empty or all null")
    score_range = max_score - min_score
    if score_range == 0:
        logger.warning(
            "Provider theme scores are constant; using a neutral score range"
        )
        score_range = 1.0
    logger.info(f"Norm min/max/range: {min_score}/{max_score}/{score_range}")
    return min_score, score_range


def build_score_components(
    df_theme_scores, df_theme2ad, min_score, score_range
):
    return (
        df_theme_scores.withColumn(
            "RelevanceScore",
            ((F.col("ProbAggRebased") - F.lit(min_score)) / F.lit(score_range))
            + F.col("GreedyScore"),
        )
        .fillna(0, subset=["RelevanceScore"])
        .join(
            df_theme2ad,
            on=df_theme2ad["Themes"] == df_theme_scores["NextTheme"],
            how="inner",
        )
        .withColumn(
            "Score", F.col("RelevanceScore") * F.col("IncrementalScore")
        )
        .select(
            "AccountNumber",
            F.col("NextTheme").alias("Theme"),
            "UniqueAdID",
            "AdVariant",
            F.col("ProbAggRebased").alias("TriggerScore"),
            "RelevanceScore",
            "IncrementalScore",
            "Score",
        )
    )


def apply_multi_session_downweighting(
    df_score_components, sessions_table, actions_table
):
    multi_session_ad_df = generate_repeat_ad_sessions(
        sessions_table, actions_table
    )
    fm_window = Window.partitionBy("RowID").orderBy(
        F.col("StringDistance").asc_nulls_last(),
        F.col("AdSeen").asc_nulls_last(),
    )
    max_distance_threshold = 10

    return (
        df_score_components.join(
            multi_session_ad_df, on="AccountNumber", how="left"
        )
        .withColumn(
            "RowID",
            F.concat_ws("_", F.col("AccountNumber"), F.col("UniqueAdID")),
        )
        .withColumn(
            "StringDistance",
            F.levenshtein(F.col("AdSeen"), F.col("UniqueAdID")),
        )
        .withColumn("MatchRank", F.row_number().over(fm_window))
        .filter(F.col("MatchRank") == 1)
        .withColumn(
            "MultiSessionDownweightScore",
            F.when(
                F.col("StringDistance") <= max_distance_threshold,
                F.col("MultiSessionDownweightScore"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "Score", F.col("Score") * F.col("MultiSessionDownweightScore")
        )
        .drop(
            "MatchRank",
            "StringDistance",
            "AdSeen",
            "RowID",
            "sessions_seen_ad_in_last_7_days",
        )
    )


def rank_top_ads_per_adset(
    df_score_components,
    df_ad2adset,
    customer_prefs,
    age_order_map,
    top_ads_per_group: int | None = None,
    *,
    top_ads_per_location: int | None = None,
):
    if top_ads_per_group is None:
        top_ads_per_group = top_ads_per_location
    if top_ads_per_group is None:
        raise ValueError("top_ads_per_group must be provided")
    top_ads_per_group = int(top_ads_per_group)
    if top_ads_per_group <= 0:
        raise ValueError("top_ads_per_group must be greater than zero")

    return (
        df_score_components.join(customer_prefs, "AccountNumber", how="left")
        .withColumn("ad_age_order", age_order_map[F.col("AdVariant")])
        .withColumn(
            "age_diff",
            F.when(
                F.col("ad_age_order").isNotNull()
                & F.col("customer_age_order").isNotNull(),
                F.col("ad_age_order") - F.col("customer_age_order"),
            ),
        )
        .filter(
            F.col("ad_age_order").isNull()
            | F.col("customer_age_order").isNull()
            | ((F.col("age_diff") >= 0) & (F.col("age_diff") <= 1))
        )
        # Remains stable if autoscaling forces cached partitions to recompute.
        .withColumn(
            "ThemeTieBreaker",
            F.xxhash64(
                F.lit(13),
                F.col("AccountNumber"),
                F.col("Theme"),
                F.col("UniqueAdID"),
            ),
        )
        .withColumn(
            "AdPerThemeRank",
            F.row_number().over(
                Window.partitionBy("AccountNumber", "Theme").orderBy(
                    F.coalesce(F.col("age_diff"), F.lit(99)).asc(),
                    F.coalesce(F.col("customer_rank"), F.lit(999)).asc(),
                    F.col("ThemeTieBreaker").asc(),
                    F.col("UniqueAdID").asc(),
                )
            ),
        )
        .where(F.col("AdPerThemeRank") == 1)
        .select("AccountNumber", "UniqueAdID", "Score", "TriggerScore")
        .join(df_ad2adset, on="UniqueAdID", how="inner")
        # Keeps top-ad selection independent of Spark partition assignment.
        .withColumn(
            "AdSetTieBreaker",
            F.xxhash64(
                F.lit(17),
                F.col("AccountNumber"),
                F.col("AdSetID"),
                F.col("UniqueAdID"),
            ),
        )
        .withColumn(
            "Rank",
            F.row_number().over(
                Window.partitionBy("AccountNumber", "AdSetID").orderBy(
                    F.desc("Score"),
                    F.desc("AdSetTieBreaker"),
                    F.col("UniqueAdID").asc(),
                )
            ),
        )
        .where(F.col("Rank") <= top_ads_per_group)
    )


def map_ranked_ads_to_groups(df_adset_scores, df_adset2group, group_col: str):
    return df_adset_scores.join(
        df_adset2group, on="AdSetID", how="inner"
    ).select(
        "AccountNumber",
        "UniqueAdID",
        group_col,
        "Score",
        "TriggerScore",
        "Rank",
    )


def map_ranked_ads_to_locations(df_adset_scores, df_adset2loc):
    return map_ranked_ads_to_groups(
        df_adset_scores,
        df_adset2loc,
        group_col="Location",
    )
