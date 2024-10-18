import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import build_spark_schema, assert_pk


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


def get_model_scores(
        model_score_table: str,
        models: list = [],
        melt_scores: bool = False) -> DataFrame:
    """
    Get propensity scores from model scores view.

    Arguments:
        model_score_table - table name (rows:"AccountNumber", cols:model_refs)
        models - subset of model refs to retrieve
        melt_scores - If `True` returns scores in long format
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
        df - dataframe with cols:
            - `Models` (comma separated list of models)
            - `ModelCombination` (operator for combining models)
                - N.B. Only "and" ModelCombination currently supported

        model_score_table - Table containing individual model scores in
            wide format (cols: `account_number`, `model_ref_1`,
            `model_ref_2`...)

        patch_model_refs - Patch verbose model refs in control sheet with
            new shorter refs

    Returns:
        Dataframe with cols: AccountNumber, TargetingCriteria, TargetingScore
    """

    # TODO: ModelCombination forced to "and" until "or" functionality is built
    df = df.withColumn("ModelCombination", F.lit("and"))

    split_col_comma = F.split(F.col("Models"), r",+\s*")
    max_models_assigned = (
        df
        .withColumn("ModelsAssigned", F.size(split_col_comma))
        .agg(F.max(F.col("ModelsAssigned")).alias("nModelsMax"))
        .collect()[0]["nModelsMax"]
    )

    for n in range(1, max_models_assigned+1):
        df = (
            df
            .withColumn(
                f"Model{str(n).zfill(2)}",
                F.get(split_col_comma, n - 1)
            )
        )

    df = append_targeting_criteria(df).drop("ModelCombination", "Models")

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
