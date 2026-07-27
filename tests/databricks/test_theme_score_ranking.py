import pytest
from pyspark.sql import functions as F

from next_ads.ranking.theme_score_ranking import (
    map_ranked_ads_to_locations,
    rank_top_ads_per_adset,
)


pytestmark = pytest.mark.databricks


def _age_order_map():
    return F.create_map(
        F.lit("newborn"),
        F.lit(0),
        F.lit("toddler"),
        F.lit(1),
        F.lit("younger"),
        F.lit(2),
        F.lit("older"),
        F.lit(3),
        F.lit("teen"),
        F.lit(4),
    )


def _ranked_rows(df):
    return (
        df.select(
            "AccountNumber",
            "UniqueAdID",
            "AdSetID",
            "Score",
            "TriggerScore",
            "Rank",
        )
        .orderBy("AccountNumber", "AdSetID", "Rank", "UniqueAdID")
        .collect()
    )


def test_ranking_is_stable_across_repartitioning_and_duplicate_preferences(
    spark,
):
    score_components = spark.createDataFrame(
        [
            ("account-1", "theme-1", "ad-1", "generic", 0.95, 0.75),
            ("account-1", "theme-1", "ad-2", "generic", 0.95, 0.75),
            ("account-1", "theme-2", "ad-3", "generic", 0.90, 0.70),
            ("account-1", "theme-3", "ad-4", "generic", 0.85, 0.65),
        ],
        [
            "AccountNumber",
            "Theme",
            "UniqueAdID",
            "AdVariant",
            "Score",
            "TriggerScore",
        ],
    )
    customer_prefs = spark.createDataFrame(
        [
            ("account-1", 0, 1),
            ("account-1", 0, 1),
            ("account-1", 2, 2),
        ],
        ["AccountNumber", "customer_age_order", "customer_rank"],
    )
    ad_to_adset = spark.createDataFrame(
        [("ad-1", 1), ("ad-2", 1), ("ad-3", 1), ("ad-4", 1)],
        ["UniqueAdID", "AdSetID"],
    )

    ranked_from_one_partition = rank_top_ads_per_adset(
        score_components.repartition(1),
        ad_to_adset.repartition(1),
        customer_prefs.repartition(1),
        _age_order_map(),
        top_ads_per_location=2,
    )
    ranked_from_four_partitions = rank_top_ads_per_adset(
        score_components.repartition(4),
        ad_to_adset.repartition(4),
        customer_prefs.repartition(4),
        _age_order_map(),
        top_ads_per_location=2,
    )

    assert _ranked_rows(ranked_from_one_partition) == _ranked_rows(
        ranked_from_four_partitions
    )

    ranked_counts = ranked_from_four_partitions.groupBy(
        "AccountNumber",
        "AdSetID",
    ).count()
    assert ranked_counts.where(F.col("count") > 2).count() == 0
    assert ranked_from_four_partitions.count() == 2


def test_location_expansion_preserves_primary_key_uniqueness(spark):
    score_components = spark.createDataFrame(
        [
            ("account-1", "theme-1", "ad-1", "generic", 0.95, 0.75),
            ("account-1", "theme-1", "ad-2", "generic", 0.95, 0.75),
            ("account-1", "theme-2", "ad-3", "generic", 0.90, 0.70),
        ],
        [
            "AccountNumber",
            "Theme",
            "UniqueAdID",
            "AdVariant",
            "Score",
            "TriggerScore",
        ],
    )
    customer_prefs = spark.createDataFrame(
        [
            ("account-1", 0, 1),
            ("account-1", 0, 1),
            ("account-1", 2, 2),
        ],
        ["AccountNumber", "customer_age_order", "customer_rank"],
    )
    ad_to_adset = spark.createDataFrame(
        [("ad-1", 1), ("ad-2", 1), ("ad-3", 1)],
        ["UniqueAdID", "AdSetID"],
    )
    adset_to_location = spark.createDataFrame(
        [(1, "PL1"), (1, "PL2")],
        ["AdSetID", "Location"],
    )

    ranked = rank_top_ads_per_adset(
        score_components,
        ad_to_adset,
        customer_prefs,
        _age_order_map(),
        top_ads_per_location=2,
    )
    mapped = map_ranked_ads_to_locations(ranked, adset_to_location)

    duplicate_keys = (
        mapped.groupBy("AccountNumber", "UniqueAdID", "Location")
        .count()
        .where(F.col("count") > 1)
    )
    assert duplicate_keys.count() == 0
    assert mapped.count() == ranked.count() * 2


def test_age_and_customer_preferences_remain_first_ranking_priorities(spark):
    score_components = spark.createDataFrame(
        [
            ("account-1", "kids", "ad-newborn", "newborn", 0.90, 0.70),
            ("account-1", "kids", "ad-toddler", "toddler", 0.90, 0.70),
        ],
        [
            "AccountNumber",
            "Theme",
            "UniqueAdID",
            "AdVariant",
            "Score",
            "TriggerScore",
        ],
    )
    customer_prefs = spark.createDataFrame(
        [
            ("account-1", 1, 1),
            ("account-1", 0, 2),
        ],
        ["AccountNumber", "customer_age_order", "customer_rank"],
    )
    ad_to_adset = spark.createDataFrame(
        [("ad-newborn", 1), ("ad-toddler", 1)],
        ["UniqueAdID", "AdSetID"],
    )

    ranked = rank_top_ads_per_adset(
        score_components,
        ad_to_adset,
        customer_prefs,
        _age_order_map(),
        top_ads_per_location=2,
    )

    assert [
        row.UniqueAdID for row in ranked.select("UniqueAdID").collect()
    ] == ["ad-toddler"]
