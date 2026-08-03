from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from jobs.orchestration import finalize_score_provider_context
from next_ads.ranking import provider_context
from next_ads.ranking.provider_context import (
    ProviderContext,
    activate_provider_context,
    transition_provider_context,
)


NOW = datetime(2026, 7, 30, 13, 0, tzinfo=timezone.utc)


class _Expression:
    def __eq__(self, other):
        return self

    def __and__(self, other):
        return self


class _CreatedFrame:
    def __init__(self, rows):
        self.rows = rows
        self.view_name = None

    def createOrReplaceTempView(self, name):  # noqa: N802
        self.view_name = name


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def where(self, condition):
        return self

    def select(self, *columns):
        return self

    def collect(self):
        return self.rows


class _Catalog:
    def __init__(self):
        self.dropped = []

    def dropTempView(self, name):  # noqa: N802
        self.dropped.append(name)


class _Spark:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements = []
        self.created = []
        self.catalog = _Catalog()

    def createDataFrame(self, rows):  # noqa: N802
        frame = _CreatedFrame(rows)
        self.created.append(frame)
        return frame

    def sql(self, statement):
        self.statements.append(statement)

    def table(self, table):
        return _Query(self.rows)


def _context(**overrides):
    values = {
        "context_slot": "theme_affinity_serving",
        "orchestration_run_id": 123,
        "provider_id": "theme_affinity",
        "provider_build_id": "build",
        "provider_build_attempt_id": "build:task:2",
        "input_snapshot_id": "input",
        "run_date": date(2026, 7, 30),
        "model_uri": "models:/catalog.schema.model/1",
        "bindings_json": "{}",
        "capability": "account_theme",
        "use_case": "theme_ranking",
        "invocation_checksum": "checksum",
        "expires_at": NOW + timedelta(hours=8),
    }
    values.update(overrides)
    return ProviderContext(**values)


def test_same_run_higher_execution_can_reclaim_its_context(monkeypatch):
    context = _context()
    spark = _Spark()
    monkeypatch.setattr(
        provider_context,
        "load_active_provider_context",
        lambda *args, **kwargs: context,
    )

    activate_provider_context(
        spark,
        context_table="catalog.schema.contexts",
        context=context,
        task_run_id=456,
        execution_count=2,
        activated_at=NOW,
    )

    claim = spark.statements[0]
    assert (
        "target.OrchestrationRunID = source.OrchestrationRunID"
        in claim
    )
    assert "source.ExecutionCount > target.ExecutionCount" in claim
    assert spark.created[0].rows[0]["ExecutionCount"] == 2


def test_foreign_run_cannot_take_an_unexpired_context(monkeypatch):
    requested = _context()
    foreign_owner = replace(
        requested,
        orchestration_run_id=999,
        provider_build_attempt_id="build:foreign:0",
    )
    spark = _Spark()
    monkeypatch.setattr(
        provider_context,
        "load_active_provider_context",
        lambda *args, **kwargs: foreign_owner,
    )

    with pytest.raises(ValueError, match="active lease"):
        activate_provider_context(
            spark,
            context_table="catalog.schema.contexts",
            context=requested,
            task_run_id=456,
            execution_count=2,
            activated_at=NOW,
        )


def test_transition_requires_exact_run_build_and_attempt_ownership(
    monkeypatch,
):
    context = _context()
    rows = [
        {
            "OrchestrationRunID": context.orchestration_run_id,
            "ProviderBuildID": context.provider_build_id,
            "ProviderBuildAttemptID": context.provider_build_attempt_id,
            "Status": "CONSUMED",
        }
    ]
    spark = _Spark(rows)
    monkeypatch.setattr(
        provider_context.F,
        "col",
        lambda name: _Expression(),
    )

    transition_provider_context(
        spark,
        context_table="catalog.schema.contexts",
        context=context,
        status="CONSUMED",
        completed_at=NOW,
    )

    statement = spark.statements[0]
    assert "AND OrchestrationRunID = 123" in statement
    assert "AND ProviderBuildID = 'build'" in statement
    assert "AND ProviderBuildAttemptID = 'build:task:2'" in statement


@pytest.mark.parametrize("rows", [[], [{"Status": "CONSUMED"}]])
def test_failed_finalizer_is_safe_before_a_lease_or_after_consumption(
    monkeypatch,
    rows,
):
    spark = _Spark(rows)
    monkeypatch.setattr(
        finalize_score_provider_context,
        "configure_spark",
        lambda: spark,
    )
    monkeypatch.setattr(
        finalize_score_provider_context.config_manager,
        "load_config",
        lambda *args, **kwargs: SimpleNamespace(
            tables_write=SimpleNamespace(
                score_provider_run_contexts="catalog.schema.contexts"
            )
        ),
    )
    monkeypatch.setattr(
        finalize_score_provider_context,
        "configure_logging",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        finalize_score_provider_context,
        "get_logger",
        lambda name: SimpleNamespace(
            info=lambda *args: None,
            warning=lambda *args: None,
        ),
    )
    monkeypatch.setattr(
        finalize_score_provider_context.F,
        "col",
        lambda name: _Expression(),
    )

    finalize_score_provider_context.main(
        "dev",
        "next_uk",
        None,
        "theme_affinity_serving",
        "123",
        "FAILED",
    )

    assert spark.statements == []
