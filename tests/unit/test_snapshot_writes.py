from datetime import date
from types import SimpleNamespace

import pytest

import next_ads.common.snapshot_writes as snapshot_writes
from next_ads.common.delta_writes import KeyValidationSummary


class FakeFrame:
    def __init__(self, columns):
        self.columns = list(columns)
        self.selected = None
        self.persisted = False
        self.unpersisted = False

    def select(self, *columns):
        self.selected = list(columns)
        return self

    def persist(self, _storage_level):
        self.persisted = True
        return self

    def unpersist(self):
        self.unpersisted = True
        return self


def test_capture_run_date_uses_one_spark_query():
    calls = []
    expected = date(2026, 7, 28)

    class Result:
        @staticmethod
        def first():
            return {"run_date": expected}

    class Spark:
        @staticmethod
        def sql(statement):
            calls.append(statement)
            return Result()

    assert snapshot_writes.capture_run_date(Spark()) == expected
    assert calls == ["SELECT current_date() AS run_date"]


def test_publish_history_then_latest_from_one_materialised_frame(monkeypatch):
    run_date = date(2026, 7, 28)
    source = FakeFrame(["AccountNumber", "Value"])
    prepared = FakeFrame(["AccountNumber", "Value", "rundate"])
    calls = []
    summary = KeyValidationSummary(2, 2, 0)

    monkeypatch.setattr(
        snapshot_writes,
        "with_run_date",
        lambda df, value, column: prepared,
    )
    monkeypatch.setattr(
        snapshot_writes,
        "validate_unique_non_null_keys",
        lambda df, keys: calls.append(("validate", df, keys)) or summary,
    )
    monkeypatch.setattr(
        snapshot_writes,
        "replace_scope_by_name",
        lambda df, table, scope, columns, spark: (
            calls.append(("history", df, table, scope, columns))
            or SimpleNamespace(statement="history", attempts=1)
        ),
    )
    monkeypatch.setattr(
        snapshot_writes,
        "replace_table_by_name",
        lambda df, table, columns, spark: (
            calls.append(("latest", df, table, columns))
            or SimpleNamespace(statement="latest", attempts=1)
        ),
    )

    result = snapshot_writes.publish_history_and_latest(
        object(),
        source,
        history_table="catalog.schema.history",
        latest_table="catalog.schema.latest",
        key_columns=["AccountNumber"],
        run_date=run_date,
    )

    assert [call[0] for call in calls] == ["validate", "history", "latest"]
    assert calls[0][2] == ["AccountNumber", "rundate"]
    assert calls[1][3] == {"rundate": run_date}
    assert calls[1][4] == ["AccountNumber", "Value", "rundate"]
    assert calls[2][3] == ["AccountNumber", "Value", "rundate"]
    assert prepared.persisted
    assert prepared.unpersisted
    assert result.run_date == run_date
    assert result.validation == summary


def test_publish_normalises_explicit_iso_run_date(monkeypatch):
    prepared = FakeFrame(["id", "rundate"])
    scopes = []

    monkeypatch.setattr(
        snapshot_writes,
        "with_run_date",
        lambda df, value, column: prepared,
    )
    monkeypatch.setattr(
        snapshot_writes,
        "validate_unique_non_null_keys",
        lambda df, keys: KeyValidationSummary(0, 0, 0),
    )
    monkeypatch.setattr(
        snapshot_writes,
        "replace_scope_by_name",
        lambda df, table, scope, columns, spark: (
            scopes.append(scope)
            or SimpleNamespace(statement="history", attempts=1)
        ),
    )
    monkeypatch.setattr(
        snapshot_writes,
        "replace_table_by_name",
        lambda *args, **kwargs: SimpleNamespace(
            statement="latest",
            attempts=1,
        ),
    )

    result = snapshot_writes.publish_history_and_latest(
        object(),
        FakeFrame(["id"]),
        history_table="history",
        latest_table="latest",
        key_columns=["id"],
        run_date="2026-07-28",
    )

    assert result.run_date == date(2026, 7, 28)
    assert scopes == [{"rundate": date(2026, 7, 28)}]


def test_latest_failure_releases_materialised_frame(monkeypatch):
    prepared = FakeFrame(["id", "rundate"])
    calls = []

    monkeypatch.setattr(
        snapshot_writes,
        "with_run_date",
        lambda df, value, column: prepared,
    )
    monkeypatch.setattr(
        snapshot_writes,
        "validate_unique_non_null_keys",
        lambda df, keys: KeyValidationSummary(1, 1, 0),
    )
    monkeypatch.setattr(
        snapshot_writes,
        "replace_scope_by_name",
        lambda *args, **kwargs: (
            calls.append("history")
            or SimpleNamespace(statement="history", attempts=1)
        ),
    )

    def fail_latest(*args, **kwargs):
        calls.append("latest")
        raise RuntimeError("latest failed")

    monkeypatch.setattr(
        snapshot_writes,
        "replace_table_by_name",
        fail_latest,
    )

    with pytest.raises(RuntimeError, match="latest failed"):
        snapshot_writes.publish_history_and_latest(
            object(),
            FakeFrame(["id"]),
            history_table="history",
            latest_table="latest",
            key_columns=["id"],
            run_date=date(2026, 7, 28),
        )

    assert calls == ["history", "latest"]
    assert prepared.unpersisted
