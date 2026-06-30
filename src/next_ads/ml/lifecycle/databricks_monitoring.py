from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TimeSeriesQualityMonitorSpec:
    table_name: str
    output_schema_name: str
    assets_dir: str
    timestamp_col: str
    granularities: tuple[str, ...]
    slicing_exprs: tuple[str, ...] = ()
    skip_builtin_dashboard: bool = False


@dataclass(frozen=True)
class InferenceLogQualityMonitorSpec:
    table_name: str
    output_schema_name: str
    assets_dir: str
    problem_type: str
    timestamp_col: str
    granularities: tuple[str, ...]
    prediction_col: str
    model_id_col: str
    label_col: str | None = None
    prediction_proba_col: str | None = None
    slicing_exprs: tuple[str, ...] = ()
    skip_builtin_dashboard: bool = False


def ensure_time_series_quality_monitor(client, spec: TimeSeriesQualityMonitorSpec):
    from databricks.sdk.service.catalog import MonitorTimeSeries

    time_series = MonitorTimeSeries(
        timestamp_col=spec.timestamp_col,
        granularities=list(spec.granularities),
    )
    try:
        client.quality_monitors.get(spec.table_name)
    except Exception as error:
        if not _is_not_found(error):
            raise
        return client.quality_monitors.create(
            table_name=spec.table_name,
            output_schema_name=spec.output_schema_name,
            assets_dir=spec.assets_dir,
            time_series=time_series,
            slicing_exprs=list(spec.slicing_exprs),
            skip_builtin_dashboard=spec.skip_builtin_dashboard,
        )

    return client.quality_monitors.update(
        table_name=spec.table_name,
        output_schema_name=spec.output_schema_name,
        time_series=time_series,
        slicing_exprs=list(spec.slicing_exprs),
    )


def ensure_inference_log_quality_monitor(
    client,
    spec: InferenceLogQualityMonitorSpec,
):
    from databricks.sdk.service.catalog import (
        MonitorInferenceLog,
    )

    inference_log = MonitorInferenceLog(
        problem_type=_monitor_problem_type(spec.problem_type),
        timestamp_col=spec.timestamp_col,
        granularities=list(spec.granularities),
        prediction_col=spec.prediction_col,
        model_id_col=spec.model_id_col,
        label_col=spec.label_col,
        prediction_proba_col=spec.prediction_proba_col,
    )
    try:
        client.quality_monitors.get(spec.table_name)
    except Exception as error:
        if not _is_not_found(error):
            raise
        return client.quality_monitors.create(
            table_name=spec.table_name,
            output_schema_name=spec.output_schema_name,
            assets_dir=spec.assets_dir,
            inference_log=inference_log,
            slicing_exprs=list(spec.slicing_exprs),
            skip_builtin_dashboard=spec.skip_builtin_dashboard,
        )

    return client.quality_monitors.update(
        table_name=spec.table_name,
        output_schema_name=spec.output_schema_name,
        inference_log=inference_log,
        slicing_exprs=list(spec.slicing_exprs),
    )


def delete_quality_monitor(client, table_name: str):
    return client.quality_monitors.delete(table_name)


def refresh_quality_monitor(client, table_name: str):
    return client.quality_monitors.run_refresh(table_name)


def _monitor_problem_type(problem_type: str):
    from databricks.sdk.service.catalog import MonitorInferenceLogProblemType

    normalized = str(problem_type or "").strip().lower()
    if normalized in {"classification", "problem_type_classification"}:
        return MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION
    if normalized in {"regression", "problem_type_regression"}:
        return MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION
    raise ValueError(
        "problem_type must be classification or regression for inference-log "
        f"quality monitors, got {problem_type!r}"
    )


def _is_not_found(error: Exception) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code == 404:
        return True

    error_code = str(getattr(error, "error_code", "")).upper()
    if error_code in {"NOT_FOUND", "RESOURCE_DOES_NOT_EXIST"}:
        return True

    message = str(error).upper()
    return "NOT_FOUND" in message or "RESOURCE_DOES_NOT_EXIST" in message
