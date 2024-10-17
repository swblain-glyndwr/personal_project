import json
from next_ads.utils.dbc import get_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re


with open("config/resources.json") as f:
    rsc = json.load(f)


def get_underperforming_ads(
        location: str,
        t_threshold: float = -1.64) -> DataFrame:
    """
    Gets 'underperforming' Ads, as defined by the Ad's t-value.

    Args:
        location - A valid Nexts Ads location (MASID prefix, e.g. HN1)
        t_threshold - The t-value threshold for "underperforming"

    Returns:
        Dataframe with UniqueAdID and Division of underperforming Ads
    """
    test_location = re.findall(r"^[a-zA-Z]{2,3}", location)[0]

    # UniqueAdID extended to include Division suffix to ensure uniqueness
    # from Oct 2024, therefore Ads from before this period need the suffix
    # adding for the join
    df = (
        get_spark()
        .read.format("delta")
        .load(rsc["files"]["results"][test_location])
        .filter(F.col("t_RPS_targetted_div_random_ad") <= t_threshold)
        .select("ID", "Division", "FirstShown_targeted_div")
        .withColumn("UniqueAdID",
                    F.when(F.col("FirstShown_targeted_div") <= "2024-09-30",
                           F.concat(F.col("ID"), F.lit("_"),
                           F.concat("Division")))
                    .otherwise(F.col("ID")),
                    )
        .select("UniqueAdID")
        .distinct()
        )

    return df


def get_latest_ads(location: str = "",
                   filter_underperforming: bool = False,
                   t_threshold: float = None) -> DataFrame:
    """
    Gets Ads from `_latest` Control Sheet table for a given location.
    Optional filter underperforming with kwargs for customer t_threshold.

    Args:
        location - A valid Next Ads location (i.e. MASID prefix, e.g. HN1)
        filter_underperforming - Remove 'underperforming' Ads
        t_threshold - Custom t_threshold to define 'underperforming' ads
    """
    df = get_spark().table(rsc["tables"]["control_sheet_latest"])

    if location:
        df = df.where(F.col("Location") == location)

    if filter_underperforming and t_threshold:
        return (df.join(get_underperforming_ads(location, t_threshold),
                        on="UniqueAdID", how="leftanti"))
    elif filter_underperforming:
        return (df.join(get_underperforming_ads(location),
                        on="UniqueAdID", how="leftanti"))
    else:
        return df
