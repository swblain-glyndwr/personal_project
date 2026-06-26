from next_ads.ml.lifecycle.registry import (
    configure_mlflow,
    copy_model_alias_to_registered_model,
    copy_model_version_to_registered_model,
    model_uri_for_alias,
    model_uri_for_version,
    set_model_alias,
)
from next_ads.ml.lifecycle.spec import (
    ModelLifecycleSpec as MlflowLifecycleConfig,
)
from next_ads.ranking.theme_affinity.lifecycle_spec import (
    resolve_lifecycle_config,
)

__all__ = [
    "MlflowLifecycleConfig",
    "configure_mlflow",
    "copy_model_alias_to_registered_model",
    "copy_model_version_to_registered_model",
    "model_uri_for_alias",
    "model_uri_for_version",
    "resolve_lifecycle_config",
    "set_model_alias",
    "train_and_register_model",
]


def train_and_register_model(
    spark,
    config,
    input_table: str | None = None,
    feature_cols: list[str] | None = None,
    params: dict | None = None,
    num_boost_round: int | None = None,
    early_stopping_rounds: int | None = None,
    mlflow_module=None,
):
    if mlflow_module is None:
        import mlflow as mlflow_module

    from pyspark.sql import functions as F

    from next_ads.ranking.theme_affinity.spark_model import (
        build_spark_model_signature,
        evaluate_ranked_predictions,
        fit_spark_xgb_ranker,
        prepare_spark_ranker_frame,
    )
    from next_ads.ranking.theme_affinity.training_data import (
        build_bounded_training_frame,
        resolve_training_frame_config,
        split_counts,
        split_training_frame,
        validate_split_counts,
    )

    configure_mlflow(mlflow_module)
    lifecycle_config = resolve_lifecycle_config(config)
    model_config = config.ranking_model
    table_name = input_table or lifecycle_config.train_table
    selected_features = feature_cols or list(model_config.model_input_cols)
    train_ratio = float(model_config.train_ratio)
    test_ratio = float(model_config.test_ratio)
    training_backend = str(
        getattr(model_config, "training_backend", "spark_xgb_ranker")
    )
    if training_backend != "spark_xgb_ranker":
        raise ValueError(
            f"Unsupported Theme Affinity training backend: {training_backend}"
        )
    train_end = train_ratio
    resolved_params = (
        params
        if params is not None
        else _to_plain_dict(model_config.xgb_params)
    )
    resolved_num_boost_round = int(
        num_boost_round or model_config.num_boost_round
    )
    resolved_early_stopping_rounds = int(
        early_stopping_rounds or model_config.early_stopping_rounds
    )
    categorical_cols = list(lifecycle_config.categorical_cols)
    spark_num_workers = int(getattr(model_config, "spark_num_workers", 4))
    alias = f"{lifecycle_config.job_env}_spark_xgboost"

    base = spark.table(table_name)
    training_frame, training_frame_stats = build_bounded_training_frame(
        base,
        resolve_training_frame_config(model_config),
    )
    base_with_split = split_training_frame(
        training_frame, train_end, test_ratio
    )

    train_validation_sdf = prepare_spark_ranker_frame(
        base_with_split.filter(F.col("split").isin("train", "validation")),
        selected_features,
        categorical_cols,
    )
    test_sdf = prepare_spark_ranker_frame(
        base_with_split.filter(F.col("split") == "test"),
        selected_features,
        categorical_cols,
    )
    counts_by_split = split_counts(base_with_split)
    validate_split_counts(counts_by_split)

    mlflow_module.set_experiment(lifecycle_config.experiment_path)
    with mlflow_module.start_run() as run:
        mlflow_module.log_params(resolved_params)
        mlflow_module.log_param("input_table", table_name)
        mlflow_module.log_param("training_backend", training_backend)
        mlflow_module.log_param("model_alias", alias)
        mlflow_module.log_params(training_frame_stats)
        mlflow_module.log_param("spark_num_workers", spark_num_workers)
        mlflow_module.log_param("num_boost_round", resolved_num_boost_round)
        mlflow_module.log_param(
            "early_stopping_rounds",
            resolved_early_stopping_rounds,
        )
        mlflow_module.log_param("feature_cols", selected_features)
        mlflow_module.log_param("categorical_cols", categorical_cols)
        mlflow_module.log_param("train_count", counts_by_split.get("train", 0))
        mlflow_module.log_param(
            "validation_count",
            counts_by_split.get("validation", 0),
        )
        mlflow_module.log_param("test_count", counts_by_split.get("test", 0))

        model = fit_spark_xgb_ranker(
            train_validation_sdf,
            feature_cols=selected_features,
            categorical_cols=categorical_cols,
            params=resolved_params,
            num_boost_round=resolved_num_boost_round,
            early_stopping_rounds=resolved_early_stopping_rounds,
            num_workers=spark_num_workers,
        )
        metrics = evaluate_ranked_predictions(model.transform(test_sdf))
        mlflow_module.log_metrics(
            {f"test_{key}": value for key, value in metrics.items()}
        )
        model_info = mlflow_module.spark.log_model(
            spark_model=model,
            artifact_path="model",
            signature=build_spark_model_signature(
                selected_features,
                categorical_cols,
            ),
        )
        registered_model = mlflow_module.register_model(
            model_uri=model_info.model_uri,
            name=lifecycle_config.registered_model_name,
        )
        client = mlflow_module.tracking.MlflowClient()
        set_model_alias(
            client,
            lifecycle_config.registered_model_name,
            registered_model.version,
            alias,
        )
        return {
            "run_id": run.info.run_id,
            "registered_model_name": lifecycle_config.registered_model_name,
            "version": registered_model.version,
            "alias": alias,
            "metrics": metrics,
        }


def _to_plain_dict(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return dict(value)
