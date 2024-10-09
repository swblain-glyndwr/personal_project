import json
from utils.dbcutils import get_spark
from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F


with open("config/params.json") as f:
    prm = json.load(f)

with open("config/resources.json") as f:
    rsc = json.load(f)


def standardise_col_to_mean(
        column: Column,
        window: Window) -> Column:
    """
    Standardises column values by subtracting the mean.

    Arguments:
        column -- PySpark `Column` to standardise
        window -- PySpark `Window` over which to standardise

    Returns:
        PySpark `Column` with standardised values
    """
    col_rtn = (column - F.mean(column).over(window))
    return col_rtn


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


def get_pscores(
        division: str,
        col_subset: list = [],
        melt_scores: bool = False) -> DataFrame:
    """
    Get propensity scores for models relevant to Next Ads for given division.

    Args:
        division -- e.g. "womens"
        cols -- (optional) Subset of models to return
        melt_scores -- (optional) If `True` returns scores in "long" format
    """

    tbl = rsc["tables"]["pscores"][division]

    df = (
        get_spark()
        .table(tbl)
        .withColumn("Division", F.lit(division))
        .withColumnRenamed("account_number", "AccountNumber")
    )

    # Remove rundate if present
    if "rundate" in df.columns:
        df = df.drop("rundate")

    if col_subset:
        df = (
            df
            .select("AccountNumber", "Division", *col_subset)
        )

    if melt_scores:
        df = (
            df
            .melt(
                ids=["AccountNumber", "Division"],
                values=None,
                variableColumnName="Model",
                valueColumnName="Score"
            )
        )

    return df


def assign_pscores_to_ads(
        df_ads: DataFrame,
        standardise: bool = False) -> DataFrame:
    """
    Assigns propensity scores to Ads.

    Args:
        df_ads -- Dataframe with columns
        (UniqueAdID, Division, Model)
        standardise -- If True, scores are standardised

    Returns:
        Dataframe with columns
        (AccountNumber, UniqueAdID, Division, TargetingGroup, Score)
    """
    # Split out model column by delimiter
    split_on_asterisk = F.split(F.col("Model"), r"\*")
    # TODO: Change to , or whitespace when moving to new control sheet

    # Calculate the maximum number of models assigned to a single Ad
    max_models_assigned = (
        df_ads
        .withColumn("ModelsAssigned", F.size(split_on_asterisk))
        .agg(F.max(F.col("ModelsAssigned")).alias("nModelsMax"))
        .collect()[0]["nModelsMax"]
    )

    # Dynamically spread models to separate columns
    for n in range(1, max_models_assigned+1):
        df_ads = (
            df_ads
            .withColumn(
                f"Model{str(n).zfill(2)}",
                F.get(split_on_asterisk, n - 1)
            )
        )

    # Model combination operator (and, or) added for old control sheet
    df_ads = df_ads.withColumn("ModelCombination", F.lit("and"))
    # TODO: Remove for new control sheet

    # List of models (with Combination operator) becomes model TargetingGroup
    df_ads = (
        df_ads
        .withColumn("TargetingGroup",
                    F.concat("ModelCombination", F.lit("|"), "Model"))
        .drop("ModelCombination", "Model")
    )

    # Melt assigned models down to one row per model for joining to scores
    df_ads = (
        df_ads
        .melt(
            ids=[c for c in df_ads.columns if not c.startswith("Model")],
            values=[c for c in df_ads.columns if c.startswith("Model")],
            variableColumnName="ModelN",
            valueColumnName="Model"
        )
        .drop("ModelN")
        .where(F.col("Model").isNotNull())
    )

    # Split model score column from schema to create column reference
    split_on_period = F.split(F.col("Model"), r"\.")
    df_adscore_lookup = (
        df_ads
        .select("UniqueAdID", "Division", "TargetingGroup", "Model")
        .withColumn("ModelSplit", split_on_period)
        .withColumn(
            "ModelCol",
            F.get(F.col("ModelSplit"), F.size(F.col("ModelSplit"))-1)
        )
        .drop("Model", "ModelSplit")
    )
    # TODO: This might be different in new control sheet, depending on
    # how models are referenced - unique ID for each model would be ideal

    # Loop through and union model scores by division
    df_pscores_div_list = []
    for div in list(rsc["tables"]["pscores"].keys()):
        # Find only specified models, to avoid pulling back all unnecessarily
        model_col_subset = [
            x[0] for x in (
                df_adscore_lookup
                .where(F.col("Division") == div)
                .select("ModelCol")
                .distinct()
                .collect()
                )
            ]

        # Get pscores for current div
        df_pscores_div = get_pscores(
            division=div,
            col_subset=model_col_subset,
            melt_scores=True
            )

        # Store df of div scores in list
        df_pscores_div_list.append(df_pscores_div)

    # Union div scores into single dataframe
    df_pscores = df_pscores_div_list.pop()
    for df_p in df_pscores_div_list:
        df_pscores = df_pscores.union(df_p)

    # Join scores to Ad lookup
    df_adscores_long = (
        df_pscores.join(
            (
                df_adscore_lookup
                .withColumnRenamed("ModelCol", "Model")
            ),
            on=["Division", "Model"]
        )
        .drop("ModelCol")
    )

    # Combine scores
    # TODO: Other cases to "and": F.product? "or", "max" etc.
    df_return = (
        df_adscores_long
        .groupBy([
            "AccountNumber",
            "UniqueAdID",
            "Division",
            "TargetingGroup"])
        .agg(F.product("Score").alias("Score"))
    )

    # Score Standardisation
    if standardise:
        w = Window.partitionBy([F.col("Division"), F.col("TargetingGroup")])
        df_return = (
            df_return
            .withColumn("Score", standardise_col_to_mean(F.col("Score"), w))
        )

    return df_return


def assign_best_ads(
        df_adscores: DataFrame,
        select_rank: int = 1,
        tie_breaker: str = ""
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on scores provided.
    Dev - Tie Breaker needed for one-to-many TargetingGroup:Ad

    Args:
        df_adscores -- Dataframe with columns
        (AccountNumber, UniqueAdID, Division, TargetingGroup, Score)
        select_rank -- Ranking to choose as 'best'
        tie_breaker -- String indicating method to use when multiple ads
            feature the same targeting criteria (arg in development)

    Returns:
        Dataframe with columns
        (AccountNumber, Division, UniqueAdID)
    """
    w = (
        Window
        .partitionBy([F.col("AccountNumber"), F.col("Division")])
        .orderBy(F.col("Score").desc())
    )

    df_return = (
        df_adscores
        .withColumn("ScoreRank", F.rank().over(w))
        .orderBy(F.col("AccountNumber"), F.col("Division"), F.col("ScoreRank"))
        .where(F.col("ScoreRank") == select_rank)
        .drop("ScoreRank")
    )

    if tie_breaker:
        # TODO: Tie breaker condition - e.g. two ads with common TargetingGroup
        pass

    return df_return


def assign_random_ads(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str) -> DataFrame:
    """
    Function assigns Ads randomly and uniformly within group
    args:
        df_ads: PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp: PySpark dataframe with cols ("AccountNumber", grp_col)
        grp_col: column reference to group (partition) by (e.g. "Division")
    returns:
        PySpark dataframe with Ads assigned randomly and uniformly
        to customers within-group
    """

    # Label each add with RandomKey
    # orderBy first for deterministic output
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
