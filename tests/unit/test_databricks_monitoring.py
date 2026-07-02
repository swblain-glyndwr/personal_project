from next_ads.ml.lifecycle.databricks_monitoring import (
    InferenceLogQualityMonitorSpec,
    TimeSeriesQualityMonitorSpec,
    delete_quality_monitor,
    ensure_inference_log_quality_monitor,
    ensure_time_series_quality_monitor,
    refresh_quality_monitor,
)


class _NotFoundError(Exception):
    status_code = 404


class _FakeQualityMonitors:
    def __init__(self, exists):
        self.exists = exists
        self.calls = []

    def get(self, table_name):
        self.calls.append(("get", table_name))
        if not self.exists:
            raise _NotFoundError("not found")
        return object()

    def create(self, **kwargs):
        self.calls.append(("create", kwargs))
        return {"created": kwargs["table_name"]}

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))
        return {"updated": kwargs["table_name"]}

    def run_refresh(self, table_name):
        self.calls.append(("refresh", table_name))
        return {"refreshed": table_name}

    def delete(self, table_name):
        self.calls.append(("delete", table_name))
        return {"deleted": table_name}


class _FakeClient:
    def __init__(self, exists):
        self.quality_monitors = _FakeQualityMonitors(exists)


def _spec():
    return TimeSeriesQualityMonitorSpec(
        table_name="catalog.schema.table",
        output_schema_name="catalog.schema",
        assets_dir="/Workspace/monitor",
        timestamp_col="reference_date",
        granularities=("1 day",),
        slicing_exprs=("segment",),
        custom_metrics=("metric",),
        warehouse_id="warehouse-id",
    )


def _inference_spec():
    return InferenceLogQualityMonitorSpec(
        table_name="catalog.schema.inference_log",
        output_schema_name="catalog.schema",
        assets_dir="/Workspace/monitor",
        problem_type="classification",
        timestamp_col="event_ts",
        granularities=("1 day",),
        prediction_col="prediction",
        model_id_col="model_version",
        label_col="label",
        prediction_proba_col="prediction_proba",
        slicing_exprs=("segment",),
        custom_metrics=("metric",),
        warehouse_id="warehouse-id",
    )


def test_ensure_time_series_quality_monitor_creates_missing_monitor():
    client = _FakeClient(exists=False)

    result = ensure_time_series_quality_monitor(client, _spec())

    assert result == {"created": "catalog.schema.table"}
    assert client.quality_monitors.calls[0] == ("get", "catalog.schema.table")
    created = client.quality_monitors.calls[1][1]
    assert created["table_name"] == "catalog.schema.table"
    assert created["output_schema_name"] == "catalog.schema"
    assert created["assets_dir"] == "/Workspace/monitor"
    assert created["slicing_exprs"] == ["segment"]
    assert created["custom_metrics"] == ["metric"]
    assert created["warehouse_id"] == "warehouse-id"
    assert created["skip_builtin_dashboard"] is False


def test_ensure_time_series_quality_monitor_updates_existing_monitor():
    client = _FakeClient(exists=True)

    result = ensure_time_series_quality_monitor(client, _spec())

    assert result == {"updated": "catalog.schema.table"}
    assert client.quality_monitors.calls[0] == ("get", "catalog.schema.table")
    updated = client.quality_monitors.calls[1][1]
    assert updated["table_name"] == "catalog.schema.table"
    assert updated["output_schema_name"] == "catalog.schema"
    assert updated["slicing_exprs"] == ["segment"]
    assert updated["custom_metrics"] == ["metric"]


def test_ensure_inference_log_quality_monitor_creates_missing_monitor():
    client = _FakeClient(exists=False)

    result = ensure_inference_log_quality_monitor(client, _inference_spec())

    assert result == {"created": "catalog.schema.inference_log"}
    assert client.quality_monitors.calls[0] == (
        "get",
        "catalog.schema.inference_log",
    )
    created = client.quality_monitors.calls[1][1]
    assert created["table_name"] == "catalog.schema.inference_log"
    assert created["output_schema_name"] == "catalog.schema"
    assert created["assets_dir"] == "/Workspace/monitor"
    assert created["slicing_exprs"] == ["segment"]
    assert created["custom_metrics"] == ["metric"]
    assert created["warehouse_id"] == "warehouse-id"
    inference_log = created["inference_log"]
    assert inference_log.prediction_col == "prediction"
    assert inference_log.model_id_col == "model_version"
    assert inference_log.label_col == "label"
    assert inference_log.prediction_proba_col == "prediction_proba"


def test_ensure_inference_log_quality_monitor_updates_existing_monitor():
    client = _FakeClient(exists=True)

    result = ensure_inference_log_quality_monitor(client, _inference_spec())

    assert result == {"updated": "catalog.schema.inference_log"}
    assert client.quality_monitors.calls[0] == (
        "get",
        "catalog.schema.inference_log",
    )
    updated = client.quality_monitors.calls[1][1]
    assert updated["table_name"] == "catalog.schema.inference_log"
    assert updated["output_schema_name"] == "catalog.schema"
    assert updated["slicing_exprs"] == ["segment"]
    assert updated["custom_metrics"] == ["metric"]
    assert updated["inference_log"].prediction_col == "prediction"


def test_refresh_quality_monitor_delegates_to_databricks_sdk():
    client = _FakeClient(exists=True)

    result = refresh_quality_monitor(client, "catalog.schema.table")

    assert result == {"refreshed": "catalog.schema.table"}


def test_delete_quality_monitor_delegates_to_databricks_sdk():
    client = _FakeClient(exists=True)

    result = delete_quality_monitor(client, "catalog.schema.table")

    assert result == {"deleted": "catalog.schema.table"}
