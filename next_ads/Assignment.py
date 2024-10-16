import json
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import build_spark_schema, assert_pk
# from next_ads.utils.columnscalers import subtract_mean, z_score


with open("config/resources.json") as f:
    rsc = json.load(f)


def get_model_scores(
        model_score_table: str,
        models: list = [],
        melt_scores: bool = False) -> DataFrame:
    """
    Get propensity scores from model scores view.

    Args:
        cols -- (optional) Subset of models to return
        melt_scores -- (optional) If `True` returns scores in "long" format
    """

    df = (
        get_spark()
        .table(model_score_table)
        .withColumnRenamed("account_number", "AccountNumber")
    )

    # Remove rundate if present
    if "rundate" in df.columns:
        df = df.drop("rundate")

    if models:
        df = (
            df
            .select("AccountNumber", *models)
        )

    if melt_scores:
        df = (
            df
            .melt(
                ids=["AccountNumber"],
                values=None,
                variableColumnName="Model",
                valueColumnName="Score"
            )
        )

    df = df.where(F.col("Score").isNotNull())

    return df


def append_targeting_criteria(
        df: DataFrame,
        col_models: str = "Models",
        col_model_combination: str = "ModelCombination") -> DataFrame:
    """
    Combines col_models (model refs) and col_model_combination (operator
    e.g. "and", "or") to yeild TargetingCriteria column in standardised format.

    Returns:
        Dataframe with addidional TargetingCriteria column
    """
    df = (
        df
        .withColumn(
            "TargetingCriteria",
            F.concat(col_model_combination, F.lit("|"), col_models)
            )
    )

    return df


def aggregate_model_scores(
        df: DataFrame,
        model_score_table: str,
        patch_model_refs: bool = False) -> DataFrame:
    """
    Assigns each customer in `model_score_table` an aggregated score.
    Aggregated score is defined by combining Models and ModelCombination
    to form a TargetingCriteria. The TargetingScore for each customer, for
    each TargetingCriteria is calculated and returned.

    Arguments:
        df -- Spark dataframe with cols:
            - `Models` (comma separated list of models)
            - `ModelCombination` (operator for combining models)
                - N.B. Only "and" ModelCombination currently supported

        model_score_table -- Table containing individual model scores in
            wide format (cols: `account_number`, `model_ref_1`,
            `model_ref_2`...)

        patch_model_refs -- Patch verbose model refs in control sheet with
            new shorter refs

    Returns:
        Spark dataframe with cols:
            - AccountNumber
            - TargetingCriteria
            - TargetingScore
    """

    # TODO: ModelCombination forced to "and" until "or" functionality is built
    df = df.withColumn("ModelCombination", F.lit("and"))

    # Split column by comma (optional trailing whitespace)
    split_col_comma = F.split(F.col("Models"), r",+\s*")

    # Calculate the maximum number of models assigned to a single Ad
    max_models_assigned = (
        df
        .withColumn("ModelsAssigned", F.size(split_col_comma))
        .agg(F.max(F.col("ModelsAssigned")).alias("nModelsMax"))
        .collect()[0]["nModelsMax"]
    )

    # Dynamically spread models to separate columns
    for n in range(1, max_models_assigned+1):
        df = (
            df
            .withColumn(
                f"Model{str(n).zfill(2)}",
                F.get(split_col_comma, n - 1)
            )
        )

    # List of models (with combination operator) becomes TargetingCriteria
    df = append_targeting_criteria(df).drop("ModelCombination", "Models")

    # Melt assigned models down to one row per model for joining to scores
    df = (
        df
        .melt(
            ids=[c for c in df.columns if not c.startswith("Model")],
            values=[c for c in df.columns if c.startswith("Model")],
            variableColumnName="ModelN",
            valueColumnName="Model"
        )
        .drop("ModelN")
        .where(F.col("Model").isNotNull())
    )

    # TODO: vvv Patch start - Patch model references between old and new
    # Refactor model refs in the new control sheet and remove this patch
    if patch_model_refs:
        with open("config/patch_model_ref.json") as f:
            patch_model_ref = json.load(f)

        df_model_patch = (
            get_spark()
            .createDataFrame(
                list(patch_model_ref.items()),
                schema=build_spark_schema([
                    ["Model", "string", "not null"],
                    ["ModelRef", "string", "not null"]
                    ])
                )
        )
        assert_pk(df_model_patch, ["Model", "ModelRef"])

        df_score_lookup = (
            df
            .join(df_model_patch, on="Model", how="left")
            .drop("Model")
            .withColumnRenamed("ModelRef", "Model")
        )
    else:
        df_score_lookup = df

    assert_pk(df_score_lookup, ["TargetingCriteria", "Model"])
    # TODO: ^^^ Patch end

    # Loop through and union model scores by division
    # Find only specified models, to avoid pulling back all unnecessarily
    model_subset = [
        x[0] for x in (
            df_score_lookup.select("Model").distinct().collect()
            )
        ]

    # Get scores for relevant models
    df_scores = get_model_scores(
        model_score_table,
        models=model_subset,
        melt_scores=True
        )

    # Join scores to entity using model as a key
    df_scores_pre_agg = df_scores.join(df_score_lookup, on="Model")

    # Combine scores
    # TODO: Other cases than "and"/F.product ("or", "max" etc.)
    df_agg_scores = (
        df_scores_pre_agg
        .groupBy(["AccountNumber", "TargetingCriteria"])
        .agg(F.product("Score").alias("TargetingScore"))
    )

    return df_agg_scores


def assign_scores_to_entity(
        df: DataFrame,
        entity_col: str,
        model_score_table: str,
        patch_model_refs: bool = False  # ,
        # standardise_partition: list = ["TargetingCriteria"]
        ) -> DataFrame:
    """
    Assigns, combines and scales scores for a given entity.

    Arguments:
        df -- Dataframe with cols (entity_id, "Models", "ModelCombination")
            - Models is a string of form "model1, model2, ..."
            - ModelCombination is a string, only "and" is currently supported
        entity_col -- Column name of the Entity to be assigned scores
            - e.g. entity_col = "UniqueAdID" if supplied df has cols
            ("UniqueAdID", "Models", "ModelCombination")
        score_table -- table containing model scores (column per model)

    Returns:
        Dataframe with cols:
            - entity_id
            - "AccountNumber"
            - "TargetingCriteria"
            - *[raw and scaled scores])
    """
    # Rename column for processing
    df = df.withColumnRenamed(entity_col, "EntityID")

    # TODO: ModelCombination forced to "and" until "or" functionality is built
    df = df.withColumn("ModelCombination", F.lit("and"))

    # Split column by comma (optional trailing whitespace)
    split_col_comma = F.split(F.col("Models"), r",+\s*")

    # Calculate the maximum number of models assigned to a single Ad
    max_models_assigned = (
        df
        .withColumn("ModelsAssigned", F.size(split_col_comma))
        .agg(F.max(F.col("ModelsAssigned")).alias("nModelsMax"))
        .collect()[0]["nModelsMax"]
    )

    # Dynamically spread models to separate columns
    for n in range(1, max_models_assigned+1):
        df = (
            df
            .withColumn(
                f"Model{str(n).zfill(2)}",
                F.get(split_col_comma, n - 1)
            )
        )

    # List of models (with Combination operator) becomes TargetingCriteria
    df = (
        df
        .withColumn(
            "TargetingCriteria",
            F.concat("ModelCombination", F.lit("|"), "Models")
            )
        .drop("ModelCombination", "Models")
    )

    # Melt assigned models down to one row per model for joining to scores
    df = (
        df
        .melt(
            ids=[c for c in df.columns if not c.startswith("Model")],
            values=[c for c in df.columns if c.startswith("Model")],
            variableColumnName="ModelN",
            valueColumnName="Model"
        )
        .drop("ModelN")
        .where(F.col("Model").isNotNull())
    )

    # TODO: vvv Patch start - Patch model references between old and new
    # Refactor model refs in the new control sheet and remove this patch
    if patch_model_refs:
        with open("config/patch_model_ref.json") as f:
            patch_model_ref = json.load(f)

        df_model_patch = (
            get_spark()
            .createDataFrame(
                list(patch_model_ref.items()),
                schema=build_spark_schema([
                    ["Model", "string", "not null"],
                    ["ModelRef", "string", "not null"]
                    ])
                )
        )
        assert_pk(df_model_patch, ["Model", "ModelRef"])

        df_score_lookup = (
            df
            .join(df_model_patch, on="Model", how="left")
            .drop("Model")
            .withColumnRenamed("ModelRef", "Model")
        )
    else:
        df_score_lookup = df

    assert_pk(df_score_lookup, ["EntityID", "TargetingCriteria", "Model"])

    # TODO: ^^^ Patch end

    # Loop through and union model scores by division
    # Find only specified models, to avoid pulling back all unnecessarily
    model_subset = [
        x[0] for x in (
            df_score_lookup.select("Model").distinct().collect()
            )
        ]

    # Get scores for relevant models
    df_scores = get_model_scores(
        model_score_table,
        models=model_subset,
        melt_scores=True
        )

    # Join scores to entity using model as a key
    df_scores_pre_agg = df_scores.join(df_score_lookup, on="Model")

    # Combine scores
    # TODO: Other cases than "and"/F.product ("or", "max" etc.)
    df_ent_scores = (
        df_scores_pre_agg
        .groupBy(["AccountNumber", "EntityID", "TargetingCriteria"])
        .agg(F.product("Score").alias("TargetingScore"))
    )

    # Score Scaling/Normalisation/Standardisation
    # df_ent_scores_scl = (
    #     df_ent_scores
    #     .withColumn("ScoreSubMean",
    #                 subtract_mean(F.col("ScoreRaw"),
    #                               partition_by=standardise_partition))
    #     .withColumn("ScoreZ",
    #                 z_score(F.col("ScoreRaw"),
    #                         partition_by=standardise_partition))
    #     )
    # assert_pk(df_ent_scores_scl,
    #           ["AccountNumber", "EntityID", "TargetingCriteria"])

    # return df_ent_scores_scl.withColumnRenamed("EntityID", entity_col)
    return df_ent_scores


def assign_random_ads(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str) -> DataFrame:
    """
    Function assigns Ads randomly (and uniformly) within group.
    args:
        df_ads: PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp: PySpark dataframe with cols ("AccountNumber", grp_col)
        grp_col: column reference to group (partition) by (e.g. "Division")
    returns:
        PySpark dataframe with Ads assigned randomly and uniformly
        to customers within-group
    """
    # TODO: Generalise function to assign_random_entity?
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


def assign_best_ads(
        df_ads: DataFrame,
        targeting_scores_table: str,
        df_cust: DataFrame = None,
        scale_fn: callable = None,
        scale_partition: list[str] = [],
        return_ranks: list = [1],
        tie_breaker: str = ""
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on scores provided.
    Dev - Tie Breaker needed for one-to-many TargetingCriteria:Ad

    Args:
        df_adscores -- Dataframe with columns
        (AccountNumber, UniqueAdID, Division, TargetingCriteria, Score)
        select_ranks -- Rankings to return (for 'best': [1])
        tie_breaker -- String indicating method to use when multiple ads
            feature the same targeting criteria (arg in development)

    Returns:
        Dataframe with columns
        (AccountNumber, Division, UniqueAdID)
    """

    # Score Scaling/Normalisation/Standardisation
    df_adscores = (
        append_targeting_criteria(df_ads)
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
                    scale_fn(F.col("TargetingScore"),
                             partition_by=scale_partition))
        )
    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID", "TargetingCriteria"])

    # Will take last Ad ID alphabetically (proxy for newest) if Scores are tied
    w = (
        Window
        .partitionBy([F.col("AccountNumber"), F.col("Division")])
        .orderBy(F.col("Score").desc(), F.col("UniqueAdID").desc())
    )

    df_return = (
        df_adscores
        .withColumn("ScoreRank", F.rank().over(w))
        .orderBy(F.col("AccountNumber"), F.col("Division"), F.col("ScoreRank"))
        .where(F.col("ScoreRank").isin(return_ranks))
        .drop("ScoreRank")
    )

    if tie_breaker:
        # TODO: Tie breaker condition - e.g. ads with common TargetingCriteria
        pass

    return df_return
