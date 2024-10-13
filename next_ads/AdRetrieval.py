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
    Gets underperforming Ads, as defined by the Ad's T-value.

    Args:
        location -- A valid Nexts Ads location (MASID prefix, e.g. HN1)
        t_threshold -- The T-value threshold for "underperforming"

    Returns:
        A dataframe with UniqueAdID and Division of underperforming Ads
    """
    test_location = re.findall(r"^[a-zA-Z]{2,3}", location)[0]
    df = (
        get_spark()
        .read.format("delta")
        .load(rsc["files"]["results"][test_location])
        .filter(F.col("t_RPS_targetted_div_random_ad") <= t_threshold)
        .select("ID", "Division")
        .withColumnRenamed("ID", "UniqueAdID")
        .distinct()
        )
    return df


def get_live_ads(location: str = "",
                 cols: list = [],
                 filter_underperforming: bool = False,
                 **kwargs) -> DataFrame:
    """
    Gets Ads from `_latest` Control Sheet table for a given location.
    Optional filter underperforming with kwargs for customer t_threshold.

    Args:
        location -- A valid Next Ads location (i.e. MASID prefix, e.g. HN1)
        filter_underperforming -- Optional - Remove underperforming Ads.
        **kwargs -- Optional - Pass through custom t_threshold for removal.
    """
    df = get_spark().table(rsc["tables"]["control_sheet_latest"])

    if location:
        df = df.where(F.col("Location") == location)

    if filter_underperforming:
        df = (
            df.join(get_underperforming_ads(location, **kwargs),
                    on="UniqueAdID", how="leftanti")
        )

    if cols:
        df = df.select(*cols)

    return df
