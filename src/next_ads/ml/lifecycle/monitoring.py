from __future__ import annotations

from next_ads.ml.lifecycle.drift import (
    assess_drift,
    drift_metrics,
    to_mlflow_metrics,
)
from next_ads.ml.lifecycle.registry import configure_mlflow
from next_ads.ml.lifecycle.spec import DriftThresholds


def table_drift_metrics(
    spark,
    baseline_table: str,
    candidate_table: str,
    feature_cols: list[str],
    categorical_cols: list[str] | None = None,
    prediction_col: str | None = None,
    sample_limit: int = 100000,
):
    categorical_cols = categorical_cols or []
    numeric_cols = [column for column in feature_cols if column not in categorical_cols]
    selected_cols = sorted(set(feature_cols + categorical_cols + _optional(prediction_col)))

    baseline = (
        spark.table(baseline_table)
        .select(*selected_cols)
        .limit(sample_limit)
        .toPandas()
    )
    candidate = (
        spark.table(candidate_table)
        .select(*selected_cols)
        .limit(sample_limit)
        .toPandas()
    )
    return drift_metrics(
        baseline=baseline,
        candidate=candidate,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        prediction_col=prediction_col,
    )


def log_table_drift_to_mlflow(
    spark,
    mlflow_module,
    experiment_path: str,
    baseline_table: str,
    candidate_table: str,
    feature_cols: list[str],
    categorical_cols: list[str] | None = None,
    prediction_col: str | None = None,
    sample_limit: int = 100000,
    thresholds: DriftThresholds | None = None,
    tags: dict[str, str] | None = None,
):
    thresholds = thresholds or DriftThresholds()
    configure_mlflow(mlflow_module)
    mlflow_module.set_experiment(experiment_path)
    metrics = table_drift_metrics(
        spark=spark,
        baseline_table=baseline_table,
        candidate_table=candidate_table,
        feature_cols=feature_cols,
        categorical_cols=categorical_cols,
        prediction_col=prediction_col,
        sample_limit=sample_limit,
    )
    assessment = assess_drift(
        metrics,
        numeric_psi_warn_threshold=thresholds.numeric_psi_warn,
        numeric_psi_fail_threshold=thresholds.numeric_psi_fail,
        categorical_warn_threshold=thresholds.categorical_warn,
        categorical_fail_threshold=thresholds.categorical_fail,
    )
    with mlflow_module.start_run(run_name="model_drift_monitor") as run:
        mlflow_module.log_param("baseline_table", baseline_table)
        mlflow_module.log_param("candidate_table", candidate_table)
        mlflow_module.log_param("sample_limit", sample_limit)
        mlflow_module.log_param("numeric_psi_warn", thresholds.numeric_psi_warn)
        mlflow_module.log_param("numeric_psi_fail", thresholds.numeric_psi_fail)
        mlflow_module.log_param("categorical_warn", thresholds.categorical_warn)
        mlflow_module.log_param("categorical_fail", thresholds.categorical_fail)
        if tags:
            for key, value in tags.items():
                mlflow_module.set_tag(key, value)
        mlflow_module.set_tag("drift.status", assessment.status)
        mlflow_module.set_tag(
            "drift.retrain_recommended",
            str(assessment.retrain_recommended).lower(),
        )
        mlflow_module.set_tag(
            "drift.promotion_blocked",
            str(assessment.promotion_blocked).lower(),
        )
        if assessment.reasons:
            mlflow_module.set_tag("drift.reasons", "; ".join(assessment.reasons))
        mlflow_module.log_metrics(to_mlflow_metrics(metrics))
        return {
            "run_id": run.info.run_id,
            "metrics": metrics,
            "assessment": assessment,
        }


def _optional(value: str | None) -> list[str]:
    return [value] if value else []
