from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from next_ads.features import snapshot_reader


FEATURE_ID = "next_uk_nextads_fs_pctr_model_input"
BINDING_ROW = {
    "feature_snapshot_id": "analytics_pctr:2026-08-01",
    "feature_snapshot_attempt_id": "123",
    "feature_build_id": "123",
    "feature_build_attempt_id": "123",
    "reference_date": date(2026, 8, 1),
    "registry_checksum": "a" * 64,
    "git_commit": "abc123",
    "completed_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    "feature_id": FEATURE_ID,
    "backing_table": f"catalog.schema.{FEATURE_ID}",
    "delta_version": 42,
    "row_count": 10,
    "output_schema_checksum": "b" * 64,
    "backing_schema_checksum": "b" * 64,
    "value_checksum": "c" * 64,
    "write_receipt_id": "receipt-42",
}


def test_resolver_never_falls_back_when_no_ready_snapshot(monkeypatch):
    monkeypatch.setattr(
        snapshot_reader,
        "latest_ready_feature_binding_row",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(ValueError, match="No READY Feature Snapshot"):
        snapshot_reader.resolve_ready_feature_binding(
            object(),
            FEATURE_ID,
            catalog="catalog",
            schema="schema",
        )


def test_reader_uses_recorded_delta_version_and_reference_date(monkeypatch):
    monkeypatch.setattr(
        snapshot_reader,
        "latest_ready_feature_binding_row",
        lambda *_args, **_kwargs: BINDING_ROW,
    )
    monkeypatch.setattr(
        snapshot_reader,
        "schema_checksum",
        lambda _frame: "b" * 64,
    )

    class Frame:
        def __init__(self):
            self.filters = []

        def where(self, condition):
            self.filters.append(condition)
            return self

        def count(self):
            return 10

    class Reader:
        def __init__(self, frame):
            self.frame = frame
            self.options = []
            self.tables = []

        def option(self, name, value):
            self.options.append((name, value))
            return self

        def table(self, table):
            self.tables.append(table)
            return self.frame

    frame = Frame()
    reader = Reader(frame)
    spark = SimpleNamespace(read=reader)
    registry = SimpleNamespace(
        table_spec=lambda _feature: SimpleNamespace(
            timestamp_key="reference_date"
        )
    )

    result, binding = snapshot_reader.read_ready_feature(
        spark,
        FEATURE_ID,
        catalog="catalog",
        schema="schema",
        registry=registry,
    )

    assert result is frame
    assert binding.delta_version == 42
    assert reader.options == [("versionAsOf", 42)]
    assert reader.tables == [BINDING_ROW["backing_table"]]
    assert len(frame.filters) == 1
