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


def refresh_quality_monitor(client, table_name: str):
    return client.quality_monitors.run_refresh(table_name)


def _is_not_found(error: Exception) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code == 404:
        return True

    error_code = str(getattr(error, "error_code", "")).upper()
    if error_code in {"NOT_FOUND", "RESOURCE_DOES_NOT_EXIST"}:
        return True

    message = str(error).upper()
    return "NOT_FOUND" in message or "RESOURCE_DOES_NOT_EXIST" in message
