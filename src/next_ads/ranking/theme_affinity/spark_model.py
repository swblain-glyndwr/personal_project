from pyspark.sql import Window
from pyspark.sql import functions as F


QID_COL = "account_number_qid"
VALIDATION_COL = "is_validation"
PREDICTION_COL = "prediction"


def build_spark_model_signature(feature_cols: list[str], categorical_cols: list[str]):
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    categorical = set(categorical_cols)
    inputs = Schema(
        [
            ColSpec("string" if column in categorical else "double", column)
            for column in feature_cols
        ]
    )
    outputs = Schema([ColSpec("double", PREDICTION_COL)])
    return ModelSignature(inputs=inputs, outputs=outputs)


def spark_feature_columns(feature_cols: list[str], categorical_cols: list[str]):
    categorical = set(categorical_cols)
    return [
        f"{column}__idx" if column in categorical else column
        for column in feature_cols
    ]


def prepare_spark_ranker_frame(df, feature_cols: list[str], categorical_cols: list[str]):
    categorical = set(categorical_cols)
    prepared = df
    for column in feature_cols:
        if column in categorical:
            prepared = prepared.withColumn(column, F.col(column).cast("string"))
        else:
            prepared = prepared.withColumn(column, F.col(column).cast("double"))

    return (
        prepared.withColumn("label", F.col("label").cast("double"))
        .withColumn(
            QID_COL,
            F.pmod(
                F.xxhash64(F.col("account_number")),
                F.lit(9223372036854775807),
            ).cast("long"),
        )
        .withColumn(VALIDATION_COL, F.col("split") == F.lit("validation"))
    )


def fit_spark_xgb_ranker(
    train_validation_df,
    feature_cols: list[str],
    categorical_cols: list[str],
    params: dict,
    num_boost_round: int,
    early_stopping_rounds: int,
    num_workers: int,
):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from xgboost.spark import SparkXGBRanker

    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=f"{column}__idx",
            handleInvalid="keep",
        )
        for column in categorical_cols
        if column in feature_cols
    ]
    ranker_params = dict(params)
    ranker_params.setdefault("device", "cpu")
    ranker_params.setdefault("tree_method", "hist")

    assembler = VectorAssembler(
        inputCols=spark_feature_columns(feature_cols, categorical_cols),
        outputCol="features",
        handleInvalid="keep",
    )
    ranker = SparkXGBRanker(
        features_col="features",
        label_col="label",
        qid_col=QID_COL,
        validation_indicator_col=VALIDATION_COL,
        prediction_col=PREDICTION_COL,
        num_workers=num_workers,
        n_estimators=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
        force_repartition=True,
        repartition_random_shuffle=True,
        **ranker_params,
    )
    return Pipeline(stages=[*indexers, assembler, ranker]).fit(train_validation_df)


def evaluate_ranked_predictions(predictions):
    ranked = predictions.withColumn(
        "predicted_rank",
        F.row_number().over(
            Window.partitionBy("account_number").orderBy(
                F.col(PREDICTION_COL).desc(),
                F.col("theme_clean").asc(),
            )
        ),
    )
    positives = (
        ranked.filter(F.col("label") > 0)
        .groupBy("account_number")
        .agg(
            F.min("predicted_rank").alias("first_positive_rank"),
            F.sum("label").alias("num_positive"),
            F.sum(
                F.when(
                    F.col("predicted_rank") <= 5,
                    1.0 / F.log2(F.col("predicted_rank") + F.lit(1.0)),
                ).otherwise(0.0)
            ).alias("dcg_5"),
            F.sum(
                F.when(
                    F.col("predicted_rank") <= 32,
                    1.0 / F.log2(F.col("predicted_rank") + F.lit(1.0)),
                ).otherwise(0.0)
            ).alias("dcg_32"),
        )
    )
    scored = (
        positives.withColumn(
            "mrr",
            F.lit(1.0) / F.col("first_positive_rank"),
        )
        .withColumn("hit_at_1", (F.col("first_positive_rank") <= 1).cast("double"))
        .withColumn("hit_at_3", (F.col("first_positive_rank") <= 3).cast("double"))
        .withColumn("hit_at_5", (F.col("first_positive_rank") <= 5).cast("double"))
        .withColumn("ideal_5", _ideal_dcg_expr(5))
        .withColumn("ideal_32", _ideal_dcg_expr(32))
        .withColumn("ndcg_5", F.col("dcg_5") / F.col("ideal_5"))
        .withColumn("ndcg_32", F.col("dcg_32") / F.col("ideal_32"))
    )
    row = scored.agg(
        F.avg("mrr").alias("mrr"),
        F.avg("ndcg_5").alias("ndcg_at_5"),
        F.avg("ndcg_32").alias("ndcg_at_32"),
        F.avg("hit_at_1").alias("hit_at_1"),
        F.avg("hit_at_3").alias("hit_at_3"),
        F.avg("hit_at_5").alias("hit_at_5"),
    ).first()
    if row is None:
        return {
            "mrr": 0.0,
            "ndcg_at_5": 0.0,
            "ndcg_at_32": 0.0,
            "hit_at_1": 0.0,
            "hit_at_3": 0.0,
            "hit_at_5": 0.0,
        }
    return {key: float(row[key] or 0.0) for key in row.asDict()}


def _ideal_dcg_expr(k: int):
    return F.expr(
        "aggregate("
        f"sequence(1, cast(least(num_positive, {k}) as int)), "
        "cast(0.0 as double), "
        "(acc, x) -> acc + 1.0 / log2(x + 1.0)"
        ")"
    )
