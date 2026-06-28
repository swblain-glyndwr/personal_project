from pyspark.sql import Window
from pyspark.sql import functions as F


QID_COL = "account_number_qid"
VALIDATION_COL = "is_validation"
PREDICTION_COL = "prediction"
TOP_K_VALUES = (1, 3, 5)


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
                F.xxhash64(*_ranking_group_exprs(prepared)),
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
    ranked = rank_predictions(predictions)
    group_cols = _ranking_group_columns(ranked)
    positives = (
        ranked.filter(F.col("label") > 0)
        .groupBy(*group_cols)
        .agg(
            F.min("predicted_rank").alias("first_positive_rank"),
            F.sum("label").alias("num_positive"),
            *[
                F.sum(
                    F.when(F.col("predicted_rank") <= k, F.col("label")).otherwise(
                        F.lit(0.0)
                    )
                ).alias(f"true_positive_at_{k}")
                for k in TOP_K_VALUES
            ],
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
        .withColumn("recall_at_1", F.col("true_positive_at_1") / F.col("num_positive"))
        .withColumn("recall_at_3", F.col("true_positive_at_3") / F.col("num_positive"))
        .withColumn("recall_at_5", F.col("true_positive_at_5") / F.col("num_positive"))
        .withColumn("precision_at_1", F.col("true_positive_at_1") / F.lit(1.0))
        .withColumn("precision_at_3", F.col("true_positive_at_3") / F.lit(3.0))
        .withColumn("precision_at_5", F.col("true_positive_at_5") / F.lit(5.0))
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
        F.avg("recall_at_1").alias("recall_at_1"),
        F.avg("recall_at_3").alias("recall_at_3"),
        F.avg("recall_at_5").alias("recall_at_5"),
        F.avg("precision_at_1").alias("precision_at_1"),
        F.avg("precision_at_3").alias("precision_at_3"),
        F.avg("precision_at_5").alias("precision_at_5"),
    ).first()
    if row is None:
        return {
            "mrr": 0.0,
            "ndcg_at_5": 0.0,
            "ndcg_at_32": 0.0,
            "hit_at_1": 0.0,
            "hit_at_3": 0.0,
            "hit_at_5": 0.0,
            "recall_at_1": 0.0,
            "recall_at_3": 0.0,
            "recall_at_5": 0.0,
            "precision_at_1": 0.0,
            "precision_at_3": 0.0,
            "precision_at_5": 0.0,
        }
    return {key: float(row[key] or 0.0) for key in row.asDict()}


def prediction_evidence_stats(predictions, top_k_values: tuple[int, ...] = TOP_K_VALUES):
    ranked = rank_predictions(predictions).withColumn(
        "actual_positive",
        (F.col("label") > F.lit(0)).cast("int"),
    )
    return {
        "top_k_confusion_matrices": _top_k_confusion_matrices(ranked, top_k_values),
        "score_distribution": _score_distribution(ranked),
        "lift_by_decile": _lift_by_decile(ranked),
    }


def rank_predictions(predictions):
    theme_col = _theme_column(predictions)
    order_cols = [F.col(PREDICTION_COL).desc()]
    if theme_col:
        order_cols.append(F.col(theme_col).asc())
    return predictions.withColumn(
        "predicted_rank",
        F.row_number().over(
            Window.partitionBy(*_ranking_group_columns(predictions)).orderBy(
                *order_cols
            )
        ),
    )


def _top_k_confusion_matrices(ranked, top_k_values: tuple[int, ...]):
    matrices = {}
    for k in top_k_values:
        row = (
            ranked.withColumn(
                "predicted_positive",
                (F.col("predicted_rank") <= F.lit(k)).cast("int"),
            )
            .agg(
                F.sum(
                    F.when(
                        (F.col("actual_positive") == 1)
                        & (F.col("predicted_positive") == 1),
                        1,
                    ).otherwise(0)
                ).alias("tp"),
                F.sum(
                    F.when(
                        (F.col("actual_positive") == 0)
                        & (F.col("predicted_positive") == 1),
                        1,
                    ).otherwise(0)
                ).alias("fp"),
                F.sum(
                    F.when(
                        (F.col("actual_positive") == 0)
                        & (F.col("predicted_positive") == 0),
                        1,
                    ).otherwise(0)
                ).alias("tn"),
                F.sum(
                    F.when(
                        (F.col("actual_positive") == 1)
                        & (F.col("predicted_positive") == 0),
                        1,
                    ).otherwise(0)
                ).alias("fn"),
            )
            .first()
        )
        matrices[str(k)] = {key: int(row[key] or 0) for key in ["tp", "fp", "tn", "fn"]}
    return matrices


def _score_distribution(ranked):
    scored = ranked.withColumn(
        "score_bin",
        F.ntile(20).over(Window.orderBy(F.col(PREDICTION_COL).asc_nulls_first())),
    ).withColumn(
        "label_bucket",
        F.when(F.col("actual_positive") == 1, F.lit("positive")).otherwise(
            F.lit("negative")
        ),
    )
    return [
        {
            "score_bin": int(row["score_bin"]),
            "label_bucket": row["label_bucket"],
            "count": int(row["count"] or 0),
        }
        for row in (
            scored.groupBy("score_bin", "label_bucket")
            .count()
            .orderBy("score_bin", "label_bucket")
            .collect()
        )
    ]


def _lift_by_decile(ranked):
    deciles = ranked.withColumn(
        "score_decile",
        F.ntile(10).over(Window.orderBy(F.col(PREDICTION_COL).desc_nulls_last())),
    )
    return [
        {
            "score_decile": int(row["score_decile"]),
            "row_count": int(row["row_count"] or 0),
            "positive_rows": int(row["positive_rows"] or 0),
            "positive_rate": float(row["positive_rate"] or 0.0),
        }
        for row in (
            deciles.groupBy("score_decile")
            .agg(
                F.count("*").alias("row_count"),
                F.sum(F.col("actual_positive")).alias("positive_rows"),
                F.avg(F.col("actual_positive").cast("double")).alias(
                    "positive_rate"
                ),
            )
            .orderBy("score_decile")
            .collect()
        )
    ]


def _theme_column(df):
    if "theme_clean" in df.columns:
        return "theme_clean"
    if "theme" in df.columns:
        return "theme"
    return None


def _ranking_group_columns(df):
    columns = ["account_number"]
    if "reference_date" in df.columns:
        columns.append("reference_date")
    return columns


def _ranking_group_exprs(df):
    return [F.col(column) for column in _ranking_group_columns(df)]


def _ideal_dcg_expr(k: int):
    return F.expr(
        "aggregate("
        f"sequence(1, cast(least(num_positive, {k}) as int)), "
        "cast(0.0 as double), "
        "(acc, x) -> acc + 1.0 / log2(x + 1.0)"
        ")"
    )
