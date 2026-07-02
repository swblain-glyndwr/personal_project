from next_ads.ml.lifecycle.registry import configure_mlflow, set_model_alias
from next_ads.ranking.theme_affinity.lifecycle_spec import resolve_lifecycle_config


def train_and_register_gpu_xgboost_model(
    spark,
    config,
    input_table: str | None = None,
    feature_cols: list[str] | None = None,
    params: dict | None = None,
    num_boost_round: int | None = None,
    early_stopping_rounds: int | None = None,
    alias_suffix: str = "gpu_xgboost",
    mlflow_module=None,
):
    if mlflow_module is None:
        import mlflow as mlflow_module

    from pyspark.sql import functions as F

    from next_ads.ranking.theme_affinity.model import XGBoostRankingModel
    from next_ads.ranking.theme_affinity.spark_model import (
        build_spark_model_signature,
    )
    from next_ads.ranking.theme_affinity.mlflow_evidence import (
        log_training_evidence_artifacts,
    )
    from next_ads.ranking.theme_affinity.training_data import (
        build_bounded_training_frame,
        resolve_training_frame_config,
        split_label_stats,
        split_counts,
        split_training_frame,
        validate_split_label_stats,
        validate_split_counts,
    )

    configure_mlflow(mlflow_module)
    lifecycle_config = resolve_lifecycle_config(config)
    model_config = config.ranking_model
    table_name = input_table or lifecycle_config.train_table
    selected_features = feature_cols or list(model_config.model_input_cols)
    train_ratio = float(model_config.train_ratio)
    test_ratio = float(model_config.test_ratio)
    train_end = train_ratio
    resolved_params = (
        params
        if params is not None
        else _to_plain_dict(model_config.xgb_params)
    )
    resolved_params = dict(resolved_params)
    resolved_params["device"] = "cuda"
    resolved_params.setdefault("tree_method", "hist")
    resolved_num_boost_round = int(
        num_boost_round or model_config.num_boost_round
    )
    resolved_early_stopping_rounds = int(
        early_stopping_rounds or model_config.early_stopping_rounds
    )

    base = spark.table(table_name)
    training_frame, training_frame_stats = build_bounded_training_frame(
        base,
        resolve_training_frame_config(model_config),
    )
    pandas_row_limit = int(
        getattr(
            model_config.training_frame,
            "max_pandas_rows",
            model_config.training_frame.max_rows,
        )
    )
    if training_frame_stats["training_frame_row_count"] > pandas_row_limit:
        raise ValueError(
            "Theme Affinity GPU XGBoost training frame exceeds configured "
            "max_pandas_rows: "
            f"{training_frame_stats['training_frame_row_count']:,} > "
            f"{pandas_row_limit:,}. Tighten ranking_model.training_frame "
            "before collecting to pandas."
        )
    base_with_split = split_training_frame(training_frame, train_end, test_ratio)
    counts_by_split = split_counts(base_with_split)
    validate_split_counts(counts_by_split)
    label_stats_by_split = split_label_stats(base_with_split)
    validate_split_label_stats(label_stats_by_split)

    train_df = base_with_split.filter(F.col("split") == "train").toPandas()
    validation_df = base_with_split.filter(
        F.col("split") == "validation"
    ).toPandas()
    test_df = base_with_split.filter(F.col("split") == "test").toPandas()
    alias = f"{lifecycle_config.job_env}_{alias_suffix}"

    mlflow_module.set_experiment(lifecycle_config.experiment_path)
    with mlflow_module.start_run() as run:
        mlflow_module.log_params(resolved_params)
        mlflow_module.log_param("input_table", table_name)
        mlflow_module.log_param("training_backend", "local_xgboost_gpu")
        mlflow_module.log_params(_scalar_params(training_frame_stats))
        mlflow_module.log_param("max_pandas_rows", pandas_row_limit)
        mlflow_module.log_param("num_boost_round", resolved_num_boost_round)
        mlflow_module.log_param(
            "early_stopping_rounds",
            resolved_early_stopping_rounds,
        )
        mlflow_module.log_param("feature_cols", selected_features)
        mlflow_module.log_param("train_count", len(train_df))
        mlflow_module.log_param(
            "train_positive_rows",
            label_stats_by_split.get("train", {}).get("positive_rows", 0),
        )
        mlflow_module.log_param("validation_count", len(validation_df))
        mlflow_module.log_param(
            "validation_positive_rows",
            label_stats_by_split.get("validation", {}).get("positive_rows", 0),
        )
        mlflow_module.log_param("test_count", len(test_df))
        mlflow_module.log_param(
            "test_positive_rows",
            label_stats_by_split.get("test", {}).get("positive_rows", 0),
        )

        model = XGBoostRankingModel(feature_cols=selected_features).fit(
            df_train=train_df,
            df_val=validation_df,
            params=resolved_params,
            num_boost_round=resolved_num_boost_round,
            early_stopping_rounds=resolved_early_stopping_rounds,
        )
        metrics = model.evaluate(test_df)
        mlflow_module.log_metrics(
            {f"test_{key}": value for key, value in metrics.items()}
        )
        log_training_evidence_artifacts(
            mlflow_module,
            training_frame_stats.get("training_frame_sample_profile", {}),
            label_stats_by_split,
        )
        model_info = mlflow_module.pyfunc.log_model(
            artifact_path="model",
            python_model=model,
            artifacts=model.artifacts("tmp/theme_affinity_gpu_xgboost_model"),
            signature=build_spark_model_signature(
                selected_features,
                list(lifecycle_config.categorical_cols),
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


def _scalar_params(values: dict):
    return {
        key: value
        for key, value in values.items()
        if isinstance(value, (str, int, float, bool)) or value is None
    }
