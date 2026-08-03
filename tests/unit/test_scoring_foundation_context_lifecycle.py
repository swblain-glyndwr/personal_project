from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from jobs.orchestration import finalize_scoring_foundation_context
from next_ads.ranking import foundation_context
from next_ads.ranking.foundation_context import (
    ScoringFoundationContext,
    activate_foundation_context,
    load_active_foundation_context,
    transition_foundation_context,
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
        "context_slot": "account_theme_foundation",
        "orchestration_run_id": 123,
        "foundation_id": "account_theme_features",
        "foundation_version": "account_theme_features/v2",
        "scoring_foundation_build_id": "foundation-build",
        "scoring_foundation_build_attempt_id": "foundation-build:task:2",
        "input_snapshot_id": "input",
        "input_snapshot_attempt_id": "input:task:0",
        "run_date": date(2026, 7, 30),
        "bindings_json": "{}",
        "capability": "account_theme",
        "contract_version": "account_theme_foundation/v1",
        "invocation_checksum": "checksum",
        "expires_at": NOW + timedelta(hours=8),
    }
    values.update(overrides)
    return ScoringFoundationContext(**values)


def test_same_run_higher_execution_can_reclaim_its_context(monkeypatch):
    context = _context()
    spark = _Spark()
    monkeypatch.setattr(
        foundation_context,
        "load_active_foundation_context",
        lambda *args, **kwargs: context,
    )

    activate_foundation_context(
        spark,
        context_table="catalog.schema.contexts",
        context=context,
        task_run_id=456,
        execution_count=2,
        activated_at=NOW,
    )

    claim = spark.statements[0]
    assert "target.OrchestrationRunID = source.OrchestrationRunID" in claim
    assert "source.ExecutionCount > target.ExecutionCount" in claim
    assert spark.created[0].rows[0]["ExecutionCount"] == 2


def test_foreign_run_cannot_take_an_unexpired_context(monkeypatch):
    requested = _context()
    foreign_owner = replace(
        requested,
        orchestration_run_id=999,
        scoring_foundation_build_attempt_id="foundation:foreign:0",
    )
    spark = _Spark()
    monkeypatch.setattr(
        foundation_context,
        "load_active_foundation_context",
        lambda *args, **kwargs: foreign_owner,
    )

    with pytest.raises(ValueError, match="active lease"):
        activate_foundation_context(
            spark,
            context_table="catalog.schema.contexts",
            context=requested,
            task_run_id=456,
            execution_count=2,
            activated_at=NOW,
        )


def test_serial_job_can_take_over_a_prior_run_context(monkeypatch):
    requested = _context()
    spark = _Spark()
    monkeypatch.setattr(
        foundation_context,
        "load_active_foundation_context",
        lambda *args, **kwargs: requested,
    )

    activate_foundation_context(
        spark,
        context_table="catalog.schema.contexts",
        context=requested,
        task_run_id=456,
        execution_count=0,
        activated_at=NOW,
        allow_serial_run_takeover=True,
    )

    claim = spark.statements[0]
    assert "target.Status = 'ACTIVE'" in claim
    assert "target.OrchestrationRunID <> source.OrchestrationRunID" in claim
    assert "target.ExpiresAt > source.ActivatedAt" in claim
    assert "target.ActivatedAt < source.ActivatedAt" in claim


def test_transition_requires_exact_run_build_and_attempt_ownership(monkeypatch):
    context = _context()
    rows = [
        {
            "OrchestrationRunID": context.orchestration_run_id,
            "ScoringFoundationBuildID": context.scoring_foundation_build_id,
            "ScoringFoundationBuildAttemptID": (
                context.scoring_foundation_build_attempt_id
            ),
            "Status": "CONSUMED",
        }
    ]
    spark = _Spark(rows)
    monkeypatch.setattr(foundation_context.F, "col", lambda name: _Expression())

    transition_foundation_context(
        spark,
        context_table="catalog.schema.contexts",
        context=context,
        status="CONSUMED",
        completed_at=NOW,
    )

    statement = spark.statements[0]
    assert "AND OrchestrationRunID = 123" in statement
    assert "AND ScoringFoundationBuildID = 'foundation-build'" in statement
    assert (
        "AND ScoringFoundationBuildAttemptID = 'foundation-build:task:2'"
        in statement
    )


def test_expired_context_is_rejected(monkeypatch):
    row = {
        "ContextSlot": "account_theme_foundation",
        "Status": "ACTIVE",
        "ExpiresAt": NOW - timedelta(seconds=1),
    }
    spark = _Spark([row])
    monkeypatch.setattr(foundation_context.F, "col", lambda name: _Expression())

    with pytest.raises(ValueError, match="expired"):
        load_active_foundation_context(
            spark,
            context_table="catalog.schema.contexts",
            context_slot="account_theme_foundation",
            now=NOW,
        )


@pytest.mark.parametrize("rows", [[], [{"Status": "CONSUMED"}]])
def test_failed_finalizer_is_safe_before_a_lease_or_after_consumption(
    monkeypatch,
    rows,
):
    spark = _Spark(rows)
    monkeypatch.setattr(
        finalize_scoring_foundation_context,
        "configure_spark",
        lambda: spark,
    )
    monkeypatch.setattr(
        finalize_scoring_foundation_context.config_manager,
        "load_config",
        lambda *args, **kwargs: SimpleNamespace(
            tables_write=SimpleNamespace(
                scoring_foundation_run_contexts="catalog.schema.contexts"
            )
        ),
    )
    monkeypatch.setattr(
        finalize_scoring_foundation_context,
        "configure_logging",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        finalize_scoring_foundation_context,
        "get_logger",
        lambda name: SimpleNamespace(
            info=lambda *args: None,
            warning=lambda *args: None,
        ),
    )
    monkeypatch.setattr(
        finalize_scoring_foundation_context.F,
        "col",
        lambda name: _Expression(),
    )

    finalize_scoring_foundation_context.main(
        "dev",
        "next_uk",
        None,
        "account_theme_foundation",
        "123",
        "FAILED",
    )

    assert spark.statements == []
