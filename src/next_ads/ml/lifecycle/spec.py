from dataclasses import dataclass, field


@dataclass(frozen=True)
class DriftThresholds:
    numeric_psi_warn: float = 0.1
    numeric_psi_fail: float = 0.25
    categorical_warn: float = 0.1
    categorical_fail: float = 0.25


@dataclass(frozen=True)
class ModelLifecycleSpec:
    job_env: str
    catalog: str
    schema: str
    model_name: str
    registered_model_name: str
    experiment_path: str
    train_table: str
    feature_cols: tuple[str, ...]
    label_col: str = "label"
    entity_col: str = "account_number"
    prediction_col: str | None = None
    categorical_cols: tuple[str, ...] = ()
    monitoring_sample_limit: int = 100000
    drift_thresholds: DriftThresholds = field(default_factory=DriftThresholds)


def qualified_model_name(catalog: str, schema: str, model_name: str) -> str:
    if model_name.count(".") >= 2:
        return model_name
    return f"{catalog}.{schema}.{model_name}"
