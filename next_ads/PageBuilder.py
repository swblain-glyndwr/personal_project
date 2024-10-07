import json
from utils.dbcutils import get_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


with open("config/params.json") as f:
    prm = json.load(f)

with open("config/resources.json") as f:
    rsc = json.load(f)


def get_underperforming_ads(
        location: str,
        t_threshold: float = -1.64) -> DataFrame:
    """
    Function returns a dataframe containing UniqueAdIDs of Next Ads that are
    'underperforming', as defined by the Ad's T-value.
    args:
        location: A valid Nexts Ads location (MASID prefix, e.g. HN1)
        t_threshold: The T-value threshold for "underperforming"
    """
    df = (
        get_spark()
        .read.format("delta")
        .load(rsc["files"]["results"][location])
        .filter(F.col("t_RPS_targetted_div_random_ad") <= t_threshold)
        .select("ID", "Division")
        .withColumnRenamed("ID", "UniqueAdID")
        )

    return df


def get_live_ads(location: str) -> DataFrame:
    """
    Function gets live ads (via the "_latest" control sheet table)
    for a given location.
    args:
        location: A valid Nexts Ads location (MASID prefix, e.g. HN1)
    """
    df = (
        get_spark()
        .table(rsc["tables"]["control_sheet_latest"])
        .where(F.col("MASID").startswith(location))
        .withColumnRenamed("Video", "model")
        .withColumn(
            "model",
            F.when(F.col("model") == "N", None).otherwise(F.col("model")))
        .select(
            "UniqueAdID",
            "Division",
            "MASID",
            "BriefingDeck",
            "Ad",
            "Title",
            "URL",
            "model"
            )
    )

    return df


def get_pscores(
        division: str,
        col_subset: list = []) -> DataFrame:
    """
    Function gets propensity score columns for models relevant to
    Next Ads for a given division.
    args:
        division: e.g. "womens", "mens", "boys", "girls", "baby", "home"...
        cols: optional list to specify model columns to return
    """

    tbl = rsc["tables"]["pscores"][division]

    df = (
        get_spark()
        .table(tbl)
        .withColumn("division", F.lit(division))
    )

    if col_subset:
        df = df.select(*col_subset)

    return df
