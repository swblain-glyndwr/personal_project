from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

import next_ads.features.source_pinning as pinning


NOW = datetime(2026, 8, 17, tzinfo=timezone.utc)


class Reader:
    def __init__(self):
        self.version = None
        self.reads = []

    def option(self, name, value):
        assert name == "versionAsOf"
        self.version = value
        return self

    def table(self, table_path):
        self.reads.append((table_path, self.version))
        return f"frame:{table_path}:{self.version}"


def _session(table_type="MANAGED"):
    return SimpleNamespace(
        read=Reader(),
        catalog=SimpleNamespace(
            getTable=lambda _path: SimpleNamespace(tableType=table_type),
            tableExists=lambda _path: False,
        ),
    )


def test_delta_source_is_read_at_one_version_and_reused(monkeypatch):
    spark = _session()
    monkeypatch.setattr(pinning, "latest_delta_version", lambda *_args: 12)
    monkeypatch.setattr(pinning, "schema_checksum", lambda _frame: "a" * 64)
    session = pinning.PinnedSourceSession(
        spark,
        feature_build_id="build",
        feature_build_attempt_id="attempt",
        reference_date=date(2026, 8, 11),
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
        captured_at=NOW,
    )

    first = session.table("catalog.schema.source")
    second = session.table("catalog.schema.source")

    assert first == second
    assert spark.read.reads == [("catalog.schema.source", 12)]
    assert len(session.source_bindings) == 1
    assert session.source_bindings[0].delta_version == 12


def test_view_source_is_snapshotted_before_its_version_is_recorded(monkeypatch):
    spark = _session("VIEW")
    monkeypatch.setattr(pinning, "latest_delta_version", lambda *_args: 0)
    monkeypatch.setattr(pinning, "schema_checksum", lambda _frame: "a" * 64)
    monkeypatch.setattr(
        pinning,
        "snapshot_view_source",
        lambda *_args, **_kwargs: "catalog.schema.view_snapshot",
    )
    session = pinning.PinnedSourceSession(
        spark,
        feature_build_id="build",
        feature_build_attempt_id="attempt",
        reference_date=date(2026, 8, 11),
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
        captured_at=NOW,
    )

    session.table("catalog.schema.source_view")

    assert session.source_bindings[0].source_table == (
        "catalog.schema.view_snapshot"
    )


def test_registered_feature_source_uses_its_ready_snapshot(monkeypatch):
    spark = _session()
    ready = SimpleNamespace(
        reference_date=date(2026, 8, 11),
        backing_table="catalog.schema.retained_feature",
        delta_version=7,
        backing_schema_checksum="b" * 64,
        row_count=20,
        feature_id="feature_one",
        feature_build_id="upstream-build",
        feature_build_attempt_id="upstream-attempt",
        write_receipt_id="receipt-1",
    )
    registry = SimpleNamespace(
        physical_tables=(SimpleNamespace(name="feature_one"),),
        resolved_table_path=lambda *_args, **_kwargs: (
            "catalog.schema.feature_one"
        ),
    )
    monkeypatch.setattr(
        pinning,
        "read_ready_feature",
        lambda *_args, **_kwargs: ("ready-frame", ready),
    )
    session = pinning.PinnedSourceSession(
        spark,
        feature_build_id="consumer-build",
        feature_build_attempt_id="consumer-attempt",
        reference_date=date(2026, 8, 11),
        target_catalog="catalog",
        target_schema="schema",
        captured_at=NOW,
        registry=registry,
    )

    assert session.table("catalog.schema.feature_one") == "ready-frame"
    binding = session.source_bindings[0]
    assert binding.source_feature_id == "feature_one"
    assert binding.source_feature_build_id == "upstream-build"
    assert binding.source_table == "catalog.schema.retained_feature"
    assert spark.read.reads == []


def test_view_snapshot_retry_never_rewrites_the_same_attempt():
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda _path: True),
        table=lambda _path: pytest.fail("retry must not rewrite the snapshot"),
        sql=lambda _sql: pytest.fail("retry must not alter the snapshot"),
    )

    first = pinning.snapshot_view_source(
        spark,
        source_name="mapping",
        source_view="catalog.schema.mapping_view",
        feature_build_attempt_id="attempt",
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )
    second = pinning.snapshot_view_source(
        spark,
        source_name="mapping",
        source_view="catalog.schema.mapping_view",
        feature_build_attempt_id="attempt",
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )

    assert first == second
