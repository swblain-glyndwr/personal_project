from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from jobs.orchestration import resolve_scoring_portfolio as resolution_job
from next_ads.ranking import portfolio_resolution
from next_ads.ranking.portfolio_resolution import (
    build_scoring_portfolio,
    publish_scoring_portfolio,
    resolve_portfolio_policy,
    select_current_input_snapshot_id,
    serving_entry,
    unchanged_provider_themes,
)
from next_ads.ranking.provider_selection import ProviderBuildSelection
from next_ads.ranking.scoring_manifest import (
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2026, 8, 6)
CUTOFF = datetime(2026, 8, 6, 17, 30, tzinfo=timezone.utc)
COMPLETED_AT = datetime(2026, 8, 6, 18, 0, tzinfo=timezone.utc)


def _scoring_config():
    return yaml.safe_load(
        (PROJECT_ROOT / "configs/scoring/scoring_settings.yaml").read_text()
    )["default"]["scoring"]


def _policy(route="v1", policy_id=None, scoring=None):
    return resolve_portfolio_policy(
        scoring or _scoring_config(),
        client="next_uk",
        use_case="theme_ranking",
        route=route,
        requested_policy_id=policy_id or f"{route}_default",
    )


def _selection(
    provider_id,
    *,
    attempt=0,
    status=READY_FOR_NEXTADS,
    source_run_date=RUN_DATE,
):
    build_id = f"{provider_id}_{source_run_date:%Y%m%d}"
    return ProviderBuildSelection(
        provider_build_id=build_id,
        provider_build_attempt_id=f"{build_id}:attempt:{attempt}",
        provider_signals_table="catalog.schema.score_provider_signals",
        provider_signals_delta_version=42 + attempt,
        input_snapshot_id=f"inputs_{source_run_date:%Y%m%d}",
        scoring_foundation_build_id=(
            "foundation_20260806" if provider_id == "theme_affinity" else None
        ),
        selection_status=status,
        source_run_date=source_run_date,
    )


def _build(route="v1", selections=None, *, execution_count=0):
    return build_scoring_portfolio(
        _policy(route),
        run_date=RUN_DATE,
        selections=selections
        or {
            "theme_affinity": _selection("theme_affinity"),
            "markov": _selection("markov"),
        },
        selection_cutoff=CUTOFF,
        task_run_id=123 + execution_count,
        execution_count=execution_count,
        completed_at=COMPLETED_AT,
    )


def test_policy_resolution_uses_priority_then_stable_policy_id():
    scoring = deepcopy(_scoring_config())
    policies = scoring["client_portfolios"]["next_uk"]["theme_ranking"][
        "routes"
    ]["v1"]["policies"]
    tied = deepcopy(policies[0])
    tied["policy_id"] = "a_policy"
    tied["policy_version"] = "a_policy/v1"
    tied["entries"] = [
        {**entry, "entry_id": f"a_{entry['entry_id']}"}
        for entry in tied["entries"]
    ]
    policies.append(tied)

    with pytest.raises(ValueError, match="a_policy is"):
        _policy("v1", "v1_default", scoring)
    assert _policy("v1", "a_policy", scoring).policy_id == "a_policy"

    policies.reverse()
    assert _policy("v1", "a_policy", scoring).policy_id == "a_policy"


def test_policy_resolution_rejects_an_undeclared_override():
    with pytest.raises(ValueError, match="not declared"):
        _policy("v1", "arbitrary_provider_override")


def test_policy_checksum_is_content_stable():
    scoring = deepcopy(_scoring_config())
    original = _policy("v1", scoring=scoring)
    configured = scoring["client_portfolios"]["next_uk"]["theme_ranking"][
        "routes"
    ]["v1"]["policies"][0]
    configured["entries"].reverse()
    for values in configured["selector"].values():
        values.reverse()

    replay = _policy("v1", scoring=scoring)

    assert replay.checksum == original.checksum


def test_portfolio_binds_exact_provider_attempts_and_versions():
    portfolio = _build()
    best = serving_entry(portfolio, "best")
    challenger = serving_entry(portfolio, "best_challenger")

    assert best.provider_build_attempt_id == (
        "theme_affinity_20260806:attempt:0"
    )
    assert best.provider_output_delta_version == 42
    assert best.provider_output_table == "catalog.schema.score_provider_signals"
    assert best.input_snapshot_id == "inputs_20260806"
    assert best.experiment_id == "current_delivery"
    assert challenger.provider_build_id == best.provider_build_id
    assert challenger.variant_id == "best_challenger"
    assert portfolio.policy_version == "v1_default/v1"
    assert portfolio.policy_checksum
    assert portfolio.selection_cutoff == CUTOFF


def test_missing_shadow_is_a_warning_and_never_fills_a_serving_slot():
    portfolio = _build(
        selections={"theme_affinity": _selection("theme_affinity")}
    )

    assert portfolio.warning_count == 1
    assert {entry.portfolio_entry_id for entry in portfolio.entries} == {
        "v1_best",
        "v1_best_challenger",
    }
    assert all(entry.provider_build_id.startswith("theme_affinity") for entry in portfolio.entries)


def test_missing_required_provider_blocks_only_the_affected_route():
    with pytest.raises(ValueError, match="Required serving provider"):
        _build("v1", selections={"markov": _selection("markov")})

    healthy_v2 = _build(
        "v2",
        selections={"theme_affinity": _selection("theme_affinity")},
    )
    assert serving_entry(healthy_v2, "best").provider_build_id.startswith(
        "theme_affinity"
    )


def test_repaired_provider_attempt_is_bound_without_changing_logical_portfolio():
    original = _build()
    repaired = _build(
        selections={
            "theme_affinity": _selection("theme_affinity", attempt=1),
            "markov": _selection("markov"),
        },
        execution_count=1,
    )

    assert repaired.portfolio_id == original.portfolio_id
    assert repaired.portfolio_attempt_id != original.portfolio_attempt_id
    assert serving_entry(repaired, "best").provider_build_attempt_id.endswith(
        ":attempt:1"
    )


def test_provider_fallback_provenance_is_preserved_per_entry():
    fallback = _selection(
        "theme_affinity",
        status=FALLBACK_PREVIOUS,
        source_run_date=date(2026, 8, 5),
    )
    portfolio = _build(selections={"theme_affinity": fallback})
    best = serving_entry(portfolio, "best")

    assert best.provider_selection_status == FALLBACK_PREVIOUS
    assert best.provider_source_run_date == date(2026, 8, 5)
    assert best.input_snapshot_id == "inputs_20260805"


class _Frame:
    def __init__(self, rows):
        self.rows = rows
        self.columns = list(rows[0])


class _Spark:
    def table(self, _table):
        return SimpleNamespace(schema="schema")

    def createDataFrame(self, rows, schema=None):  # noqa: N802
        assert schema == "schema"
        return _Frame(rows)


def test_publication_writes_entries_before_ready_header(monkeypatch):
    operations = []

    def replace(frame, table, scope, columns, *, spark):
        operations.append((table, scope, len(frame.rows), tuple(columns)))

    monkeypatch.setattr(
        portfolio_resolution,
        "replace_scope_by_name",
        replace,
    )
    portfolio = _build()

    publish_scoring_portfolio(
        _Spark(),
        portfolio,
        entries_table="portfolio_entries",
        portfolios_table="portfolios",
    )

    assert [operation[0] for operation in operations] == [
        "portfolio_entries",
        "portfolios",
    ]
    assert operations[0][1] == {
        "PortfolioAttemptID": portfolio.portfolio_attempt_id
    }
    assert operations[0][2] == 3
    assert operations[1][2] == 1


def test_entry_publication_failure_never_writes_ready_header(monkeypatch):
    operations = []

    def fail_entries(_frame, table, _scope, _columns, *, spark):
        operations.append(table)
        raise RuntimeError("entry publication failed")

    monkeypatch.setattr(
        portfolio_resolution,
        "replace_scope_by_name",
        fail_entries,
    )

    with pytest.raises(RuntimeError, match="entry publication failed"):
        publish_scoring_portfolio(
            _Spark(),
            _build(),
            entries_table="portfolio_entries",
            portfolios_table="portfolios",
        )

    assert operations == ["portfolio_entries"]


def test_runtime_selector_does_not_wait_for_missing_shadow(monkeypatch):
    waits = []
    warnings = []
    policy = _policy()

    def wait(*_args, provider_id, **_kwargs):
        waits.append(provider_id)
        return _selection(provider_id)

    monkeypatch.setattr(resolution_job, "wait_for_score_provider_build", wait)
    monkeypatch.setattr(
        resolution_job,
        "load_score_provider_builds",
        lambda *_args, **_kwargs: (),
    )
    logger = SimpleNamespace(
        warning=lambda *values: warnings.append(values),
    )

    selections = resolution_job._resolve_provider_builds(
        "spark",
        policy=policy,
        builds_table="builds",
        run_date=RUN_DATE,
        wait_seconds=1800,
        poll_seconds=60,
        selection_cutoff=CUTOFF,
        logger=logger,
    )

    assert waits == ["theme_affinity"]
    assert set(selections) == {"theme_affinity"}
    assert len(warnings) == 1
    assert warnings[0][1] == "markov"


class _TaskValues:
    def __init__(self):
        self.values = {}

    def set(self, *, key, value):
        self.values[key] = value


def test_runtime_job_publishes_policy_and_emits_best_binding(monkeypatch):
    task_values = _TaskValues()
    published = []
    config = SimpleNamespace(
        scoring=_scoring_config(),
        tables_write=SimpleNamespace(
            score_provider_builds="builds",
            scoring_input_snapshots="input_snapshots",
            scoring_portfolio_entries="entries",
            scoring_portfolios="portfolios",
        ),
    )
    monkeypatch.setattr(
        resolution_job.config_manager,
        "load_config",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr(resolution_job, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(resolution_job, "configure_spark", lambda: "spark")
    monkeypatch.setattr(
        resolution_job,
        "select_current_input_snapshot_id",
        lambda *_args, **_kwargs: "inputs_20260806",
    )
    monkeypatch.setattr(
        resolution_job,
        "_resolve_provider_builds",
        lambda *_args, **_kwargs: {
            "theme_affinity": _selection("theme_affinity"),
        },
    )
    monkeypatch.setattr(
        resolution_job,
        "publish_scoring_portfolio",
        lambda spark, portfolio, **kwargs: published.append(
            (spark, portfolio, kwargs)
        ),
    )
    monkeypatch.setattr(
        resolution_job,
        "get_dbutils",
        lambda: SimpleNamespace(jobs=SimpleNamespace(taskValues=task_values)),
    )

    resolution_job.main(
        "dev",
        "next_uk",
        None,
        RUN_DATE.isoformat(),
        "v1_default",
        "account_theme",
        "theme_ranking",
        "v1",
        "1800",
        "60",
        CUTOFF.isoformat(),
        "123",
        "0",
        "456",
    )

    assert len(published) == 1
    assert published[0][0] == "spark"
    assert published[0][2] == {
        "entries_table": "entries",
        "portfolios_table": "portfolios",
    }
    assert published[0][1].warning_count == 1
    assert task_values.values["portfolio_policy_id"] == "v1_default"
    assert task_values.values["portfolio_policy_version"] == "v1_default/v1"
    assert task_values.values["provider_build_id"] == (
        "theme_affinity_20260806"
    )
    assert task_values.values["provider_build_attempt_id"] == (
        "theme_affinity_20260806:attempt:0"
    )
    assert task_values.values["provider_signals_delta_version"] == 42
    assert task_values.values["current_input_snapshot_id"] == "inputs_20260806"
    assert task_values.values["portfolio_warning_count"] == 1


def test_current_input_selection_does_not_resurrect_a_failed_repair(spark):
    manifest = spark.createDataFrame(
        [
            ("inputs_a", RUN_DATE, "READY", 0, COMPLETED_AT, 100),
            ("inputs_a", RUN_DATE, "FAILED", 1, COMPLETED_AT, 101),
            ("inputs_b", RUN_DATE, "READY_WITH_WARNINGS", 0, COMPLETED_AT, 102),
        ],
        [
            "InputSnapshotID",
            "RunDate",
            "Status",
            "ExecutionCount",
            "CompletedAt",
            "TaskRunID",
        ],
    )
    view = "portfolio_input_selection_test"
    manifest.createOrReplaceTempView(view)

    selected = select_current_input_snapshot_id(
        spark,
        snapshots_table=view,
        run_date=RUN_DATE,
    )

    assert selected == "inputs_b"


def test_fallback_theme_quarantine_keeps_only_unchanged_definitions(spark):
    item_themes = spark.createDataFrame(
        [
            ("provider", "p1", "theme_a", 1),
            ("provider", "p2", "theme_b", 1),
            ("provider", "p3", "theme_shared_changed", 1),
            ("current", "p1", "theme_a", 1),
            ("current", "p2", "theme_c", 1),
            ("current", "p3", "theme_shared_changed", 1),
            ("current", "p4", "theme_shared_changed", 2),
            ("current", "p5", "theme_new", 1),
        ],
        ["InputSnapshotID", "pid", "theme", "theme_rank"],
    )
    view = "portfolio_item_theme_quarantine_test"
    item_themes.createOrReplaceTempView(view)

    allowed = unchanged_provider_themes(
        spark,
        item_themes_table=view,
        provider_input_snapshot_id="provider",
        current_input_snapshot_id="current",
    )

    assert [row["NextTheme"] for row in allowed.collect()] == ["theme_a"]
