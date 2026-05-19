from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dsutils.dbc import get_spark
from dsutils.logtools import get_logger


logger = get_logger(__name__)


def append_targeting_criteria(
        df: DataFrame,
        col_models: str = "Models",
        col_model_combination: str = "ModelCombination",
        targeting: bool = True) -> DataFrame:
    """
    Combines col_models (model refs) and col_model_combination (operator
    e.g. "and", "or") to yeild TargetingCriteria column in standardised format.

    Returns:
        Dataframe with addidional TargetingCriteria column
    """
    if targeting:
        logger.debug('Appending standardised concatenation TargetingCriteria')
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
    logger.debug(f'Getting model scores from table: {model_score_table}')
    df = (
        get_spark()
        .table(model_score_table)
        .withColumnRenamed("account_number", "AccountNumber")
    )

    # Remove rundate if present
    if "rundate" in df.columns:
        logger.debug('Removing rundate column for processing')
        df = df.drop("rundate")

    if models:
        logger.debug(f'Filtering to model subset: {models}')
        df = (
            df
            .select("AccountNumber", *models)
        )

    if melt_scores:
        logger.debug('Melting model scores')
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
        model_score_table: str) -> DataFrame:
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

    Returns:
        Dataframe with cols: AccountNumber, TargetingCriteria, TargetingScore
    """
    logger.debug('Aggregating model scores')

    # TODO: ModelCombination forced to "and" until "or" functionality is built
    df = df.withColumn("ModelCombination", F.lit("and"))

    split_col_comma = F.split(F.col("Models"), r",+\s*")
    max_models_assigned = (
        df
        .withColumn("ModelsAssigned", F.size(split_col_comma))
        .agg(F.max(F.col("ModelsAssigned")).alias("nModelsMax"))
        .collect()[0]["nModelsMax"]
    )
    logger.debug(f'Max models assigned: {max_models_assigned}')

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

    # Find only specified models, to avoid pulling back all unnecessarily
    model_subset = [x[0] for x in df.select("Model").distinct().collect()]

    # Get scores for relevant models
    df_scores = get_model_scores(
        model_score_table,
        models=model_subset,
        melt_scores=True
    )

    # Join scores to entity using model as a key
    df_scores_pre_agg = df_scores.join(df, on="Model")

    # TODO: Other cases than "and"/F.product ("or", "max" etc.)
    logger.debug('Combining model scores')
    df_agg_scores = (
        df_scores_pre_agg
        .groupBy(["AccountNumber", "TargetingCriteria"])
        .agg(F.product("Score").alias("TargetingScore"))
    )

    return df_agg_scores
