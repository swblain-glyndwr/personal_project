from pyspark.sql import Window
from pyspark.sql import functions as F

from dsutils.etl import post_to_webhook
from next_ads.decisioning.assignment import get_ad_feedback_scores, greedy_assignment


def apply_auto_trading_filter(df_ads, spark, underperforming_ads: str, enabled: bool, logger):
    if not enabled:
        return df_ads

    count_df_ads = df_ads.select("UniqueAdID").distinct().count()
    filtered_df = (
        df_ads.join(
            spark.table(underperforming_ads),
            on=["UniqueAdID", "rundate"],
            how="left_anti",
        ).cache()
    )
    count_df_ads_pruned = filtered_df.select("UniqueAdID").distinct().count()
    logger.info(
        f"AutoTrading: removed {count_df_ads - count_df_ads_pruned:,} "
        "underperforming ads."
    )
    return filtered_df


def apply_greedy_theme_assignment(
    df_theme_scores,
    greedy_cfg: dict,
    job_env: str,
    webhook_url: str,
    logger,
):
    gcfg_isdict = isinstance(greedy_cfg.get("quotas", None), dict)
    if gcfg_isdict:
        gcfg_val_int = all(isinstance(k, int) for k in greedy_cfg["quotas"].values())
    else:
        gcfg_val_int = False

    if not all([gcfg_isdict, gcfg_val_int]):
        if greedy_cfg:
            bad_gcfg_msg = "Invalid greedy theme config, skipping greedy assignment"
            logger.warning(bad_gcfg_msg)
            if job_env == "prod":
                post_to_webhook(webhook_url, bad_gcfg_msg)
        logger.info("Greedy assignment not enabled")
        logger.info("Defaulting to greedy score of 0 for all themes")
        return df_theme_scores.withColumn("GreedyScore", F.lit(0))

    greedy_quotas = greedy_cfg.get("quotas")
    max_quota = max(greedy_quotas.values())
    logger.info(f"Greedy quotas: {greedy_quotas}")
    switch_tiles = greedy_cfg.get("switch_tiles", True)
    tiles = greedy_cfg.get("tiles", 1000)
    logger.info(f"Greedy tiles: {tiles} (switching: {switch_tiles})")
    switch_multiplier = -1 if switch_tiles else 1

    df_theme_order = (
        df_theme_scores.where(F.col("NextTheme").isin(list(greedy_quotas.keys())))
        .groupBy("NextTheme")
        .agg(F.first("ProbBase").alias("ProbBase"))
        .orderBy(F.col("ProbBase"))
        .withColumn("ThemeOrder", F.monotonically_increasing_id() + 1)
    )

    df_theme_scores_global_rank = (
        df_theme_scores.join(df_theme_order, on="NextTheme", how="inner")
        .withColumn(
            "RankInTheme",
            F.row_number().over(
                Window.partitionBy("NextTheme").orderBy(F.col("ProbAggRebased").desc())
            ),
        )
        .where(F.col("RankInTheme") <= (len(greedy_quotas.keys()) * max_quota))
        .withColumn(
            "nTile",
            F.ntile(1000).over(
                Window.partitionBy("NextTheme").orderBy(F.col("ProbAggRebased").desc())
            ),
        )
        .withColumn(
            "SwitchRank",
            F.when(
                F.col("nTile") % 2 == 0,
                F.col("ThemeOrder") * F.lit(switch_multiplier),
            ).otherwise(F.col("ThemeOrder")),
        )
        .orderBy(F.col("nTile"), F.col("SwitchRank"), F.col("ProbAggRebased").desc())
        .withColumn("GlobalRank", F.monotonically_increasing_id() + 1)
    )

    df_theme_scores_global_rank.cache()
    gr_records = df_theme_scores_global_rank.count()
    logger.info(f"{gr_records:,} records passed to greedy assignment")

    df_theme_scores_greedy = greedy_assignment(
        df_theme_scores_global_rank,
        greedy_quotas,
        item_col="NextTheme",
        user_col="AccountNumber",
        rank_col="GlobalRank",
    )

    return (
        df_theme_scores.join(
            df_theme_scores_greedy.withColumn("GreedyScore", F.lit(1)),
            on=["AccountNumber", "NextTheme"],
            how="left",
        ).fillna(0, subset=["GreedyScore"])
    )


def append_ad_feedback_scores(
    df_theme2ad,
    *,
    enabled: bool,
    ad_results_table: str,
    control_sheet_latest_table: str,
    ad_feedback_weight,
    sessions_threshold,
    lookback_period_days,
    logger,
):
    if not enabled:
        logger.info("Ad feedback loop not enabled")
        logger.info("Defaulting to incremental score of 1.0 for all ads")
        return df_theme2ad.withColumn("IncrementalScore", F.lit(1.0))

    logger.info(f"Getting ad feedback scores (weight: {ad_feedback_weight})")
    df_ad_feedback_scores = get_ad_feedback_scores(
        ad_results_table=ad_results_table,
        control_sheet_latest_table=control_sheet_latest_table,
        ad_feedback_weight=ad_feedback_weight,
        sessions_threshold=sessions_threshold,
        lookback_period_days=lookback_period_days,
    )

    if not df_ad_feedback_scores or df_ad_feedback_scores.isEmpty():
        logger.warning("No ad feedback scores returned")
        logger.info("Defaulting to incremental score of 1.0 for all ads")
        return df_theme2ad.withColumn("IncrementalScore", F.lit(1.0))

    n_afs = df_ad_feedback_scores.count()
    logger.info(f"{n_afs:,} ad feedback scores returned, appending")
    return (
        df_theme2ad.join(df_ad_feedback_scores, on="UniqueAdID", how="left")
        .withColumnRenamed("AdFeedbackScore", "IncrementalScore")
        .fillna(1.0, subset=["IncrementalScore"])
    )


def load_customer_age_preferences(spark, kids_age_groups: str):
    age_order_map = F.create_map(
        [
            F.lit(x)
            for pair in [
                ("newborn", 0),
                ("toddler", 1),
                ("younger", 2),
                ("older", 3),
                ("teen", 4),
            ]
            for x in pair
        ]
    )

    customer_prefs = (
        spark.table(kids_age_groups)
        .drop("rundate")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumnRenamed("rank", "customer_rank")
        .withColumn("customer_age_order", age_order_map[F.col("kids_age_group")])
        .select("AccountNumber", "customer_age_order", "customer_rank")
    )
    return customer_prefs, age_order_map


def assert_eligible_locations(df_ad_scores, df_ad2loc):
    df_violations = df_ad_scores.join(
        df_ad2loc,
        on=["Location", "UniqueAdID"],
        how="left_anti",
    )
    assert df_violations.count() == 0, "Ads assigned to ineligible locations"
