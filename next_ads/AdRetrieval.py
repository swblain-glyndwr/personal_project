import json
from utils.dbcutils import get_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


with open("config/resources.json") as f:
    rsc = json.load(f)


def get_underperforming_ads(
        location: str,
        t_threshold: float = -1.64) -> DataFrame:
    """
    Gets underperforming Ads, as defined by the Ad's T-value.

    Args:
        location -- A valid Nexts Ads location (MASID prefix, e.g. HN1)
        t_threshold -- The T-value threshold for "underperforming"

    Returns:
        A dataframe with UniqueAdID and Division of underperforming Ads
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
    Gets Ads from `_latest` Control Sheet table for a given location.

    Args:
        location -- A valid Next Ads location (i.e. MASID prefix, e.g. HN1)
    """
    df = (
        get_spark()
        .table(rsc["tables"]["control_sheet_latest"])
        .where(F.col("MASID").startswith(location))
        .withColumnRenamed("Video", "Model")
        .withColumn(
            "Model",
            F.when(F.col("Model") == "N", None).otherwise(F.col("Model")))
        .select(
            "UniqueAdID",
            "Division",
            "MASID",
            "BriefingDeck",
            "Ad",
            "Title",
            "URL",
            "Model"
            )
    )

    return df
