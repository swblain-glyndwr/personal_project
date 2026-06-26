from next_ads.ml.lifecycle.spec import (
    DriftThresholds,
    ModelLifecycleSpec,
    qualified_model_name,
)


def resolve_lifecycle_config(config) -> ModelLifecycleSpec:
    model_config = config.ranking_model
    monitoring = model_config.monitoring
    return ModelLifecycleSpec(
        job_env=str(config.job_env),
        catalog=str(config.catalog_write),
        schema=str(config.schema_write),
        model_name=str(model_config.model_name),
        registered_model_name=qualified_model_name(
            str(config.catalog_write),
            str(config.schema_write),
            str(model_config.registered_model_name),
        ),
        experiment_path=str(model_config.experiment_path),
        train_table=str(config.ranking_model_tables.model_train_input_table),
        feature_cols=tuple(model_config.model_input_cols),
        label_col="label",
        entity_col="account_number",
        prediction_col=None,
        categorical_cols=tuple(monitoring.categorical_cols),
        monitoring_sample_limit=int(monitoring.sample_limit),
        drift_thresholds=DriftThresholds(
            numeric_psi_warn=float(monitoring.numeric_psi_warn_threshold),
            numeric_psi_fail=float(monitoring.numeric_psi_fail_threshold),
            categorical_warn=float(monitoring.categorical_warn_threshold),
            categorical_fail=float(monitoring.categorical_fail_threshold),
        ),
    )
