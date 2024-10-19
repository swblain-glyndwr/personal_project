from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import assert_pk
from collections.abc import Callable


def assign_random_ads(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str) -> DataFrame:
    """
    Function assigns Ads randomly (and uniformly) within group.
    Arguments:
        df_ads - PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp - PySpark dataframe with cols ("AccountNumber", grp_col)
        grp_col - column reference to group (partition) by (e.g. "Division")
    Returns:
        Dataframe - Ads assigned randomly (uniform) to customers within-group
    """
    # TODO: Generalise function to assign_random_entity?

    w = Window().partitionBy(grp_col).orderBy("UniqueAdID")
    df_ads = df_ads.withColumn("RandomKey", F.row_number().over(w))

    # Dictionary of Ads per group (max RandomKey)
    # Number of Ads within group is arg for ntile
    df_ad_counts = (
        df_ads
        .groupBy(grp_col)
        .agg(F.max("RandomKey").alias("nAds"))
    )
    grp_ads = {row[grp_col]: row["nAds"] for row in df_ad_counts.collect()}

    # ntile customers into nAds groups by group to create RandomKey
    # orderBy first for deterministic output
    # TODO: Avoid for loop by passing nAds as arg to F.ntile()
    grp_cust_rdm_list = []
    w = Window().partitionBy(F.lit(1)).orderBy("RandomValue")
    for grp_k in grp_ads:
        df_cust_rdm_grp = (
            df_cust_grp
            .where(F.col(grp_col) == grp_k)
            .orderBy("AccountNumber")
            .withColumn("RandomValue", F.rand(seed=42))
            .withColumn("RandomKey", F.ntile(grp_ads[grp_k]).over(w))
            .drop("RandomValue")
        )
        grp_cust_rdm_list.append(df_cust_rdm_grp)

    df_cust_rdm = grp_cust_rdm_list.pop()
    for df_n in grp_cust_rdm_list:
        df_cust_rdm = df_cust_rdm.union(df_n)

    df_cust_rdm_ads = (
        df_cust_rdm
        .join(df_ads, on=["RandomKey", grp_col])
        .drop("RandomKey")
    )

    return df_cust_rdm_ads


def assign_best_ads(
        df_ads: DataFrame,
        targeting_scores_table: str,
        df_cust: DataFrame = None,
        score_scale_fn: Callable = None,
        score_scale_partition: list[str] = ["TargetingCriteria"],
        return_ranks: list = [1],
        tie_breaker: Callable = None
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on scores provided.
    Dev - Tie Breaker needed for one-to-many TargetingCriteria:Ad

    Arguments:
        df_ads - Dataframe with columns (UniqueAdID, TargetingCriteria)
        targeting_scores_table - Name of table containing TargetingScores
        df_cust - Filter customers (Dataframe with col: AccountNumber)
        score_scale_fn - Function for scaling the score
        score_scale_within - Partition for scaling
        return_ranks - Rankings to return (e.g. for 'second best ad' use [2])
        tie_breaker - TODO - How to break tie when one Targeting has many Ads
    """

    df_adscores = (
        df_ads
        .select("UniqueAdID", "TargetingCriteria")
        .join(get_spark().table(targeting_scores_table),
              on="TargetingCriteria",
              how="inner")
    )

    if df_cust:
        df_adscores = df_adscores.join(df_cust,
                                       on="AccountNumber",
                                       how="inner")

    df_adscores = (
        df_adscores
        .withColumn("TargetingScoreScaled",
                    score_scale_fn(F.col("TargetingScore"),
                                   partition_by=score_scale_partition))
        )

    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID", "TargetingCriteria"])

    w = (
        Window
        .partitionBy([F.col("AccountNumber")])
        .orderBy(F.col("TargetingScoreScaled").desc(),
                 F.col("UniqueAdID").desc())
    )
    # Will take last Ad ID alphabetically (proxy for newest) if scores are tied
    # TODO: Probably quite a rare occurence, but need a better method

    df_return = (
        df_adscores
        .withColumn("AdRank", F.rank().over(w))
        .where(F.col("AdRank").isin(return_ranks))
        .select("AccountNumber",
                "TargetingCriteria",
                "TargetingScoreScaled",
                "AdRank",
                "UniqueAdID")
    )

    if tie_breaker:
        # TODO: Tie breaker condition
        # e.g. ads with common TargetingCriteria
        # utilise assign_random_ads within TargetingCriteria
        pass

    return df_return
