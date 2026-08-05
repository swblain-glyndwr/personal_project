from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from jobs.orchestration import select_score_provider_build as selection_job
from next_ads.ranking.provider_selection import (
    PROVIDER_BUILD_COLUMNS,
    ProviderBuildNotReadyError,
    ProviderBuildSelection,
    parse_score_provider_build,
    select_score_provider_build,
    wait_for_score_provider_build,
)
from next_ads.ranking.scoring_manifest import (
    FAILED_BEFORE_PUBLISH,
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
    ScoreProviderBuild,
)


RUN_DATE = date(2026, 8, 3)
CUTOFF = datetime(2026, 8, 3, 18, 30, tzinfo=timezone.utc)


def _build(
    build_id="theme_affinity_20260803",
    *,
    attempt=0,
    run_date=RUN_DATE,
    completed_at=CUTOFF - timedelta(minutes=5),
    status=READY_FOR_NEXTADS,
    provider_id="theme_affinity",
    capability="account_theme",
    use_case="theme_ranking",
):
    ready = status == READY_FOR_NEXTADS
    return ScoreProviderBuild(
        provider_build_id=build_id,
        provider_build_attempt_id=f"{build_id}:attempt:{attempt}",
        input_snapshot_id=f"inputs_{run_date:%Y%m%d}",
        run_date=run_date,
        capability=capability,
        use_case=use_case,
        provider_id=provider_id,
        provider_version=f"{provider_id}/v1",
        contract_version="account_entity_scores/v1",
        status=status,
        row_count=100 if ready else 0,
        account_count=10 if ready else 0,
        entity_count=20 if ready else 0,
        null_key_count=0,
        duplicate_key_count=0,
        invalid_score_count=0,
        warning_count=0,
        output_checksum="checksum" if ready else None,
        task_run_id=100 + attempt,
        execution_count=attempt,
        completed_at=completed_at,
        output_snapshot_id=f"output_{build_id}" if ready else None,
        output_table="catalog.schema.provider_signals" if ready else None,
        output_delta_version=42 if ready else None,
        scoring_foundation_build_id="foundation_20260803",
        scoring_foundation_build_attempt_id="foundation_20260803:attempt:0",
    )


def _manifest_row(**overrides):
    build = _build()
    row = {
        "ProviderBuildID": build.provider_build_id,
        "ProviderBuildAttemptID": build.provider_build_attempt_id,
        "InputSnapshotID": build.input_snapshot_id,
        "RunDate": build.run_date,
        "Capability": build.capability,
        "UseCase": build.use_case,
        "ProviderID": build.provider_id,
        "ProviderVersion": build.provider_version,
        "ContractVersion": build.contract_version,
        "ModelName": None,
        "ModelVersion": None,
        "ModelURI": None,
        "PipelineUpdateID": None,
        "OutputSnapshotID": build.output_snapshot_id,
        "OutputTable": build.output_table,
        "OutputDeltaVersion": build.output_delta_version,
        "RowCount": build.row_count,
        "AccountCount": build.account_count,
        "EntityCount": build.entity_count,
        "NullKeyCount": build.null_key_count,
        "DuplicateKeyCount": build.duplicate_key_count,
        "InvalidScoreCount": build.invalid_score_count,
        "OutputChecksum": build.output_checksum,
        "WarningCount": build.warning_count,
        "Status": build.status,
        "TaskRunID": build.task_run_id,
        "ExecutionCount": build.execution_count,
        "CompletedAt": build.completed_at,
        "ScoringFoundationBuildID": build.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            build.scoring_foundation_build_attempt_id
        ),
    }
    row.update(overrides)
    return row


def _select(builds, *, allow_fallback=True):
    return select_score_provider_build(
        builds,
        run_date=RUN_DATE,
        selection_cutoff=CUTOFF,
        provider_id="theme_affinity",
        capability="account_theme",
        use_case="theme_ranking",
        allow_fallback=allow_fallback,
    )


def test_same_day_ready_build_wins_over_previous_ready_build():
    previous = _build(
        "previous",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=1),
    )
    current = _build()

    selection = _select((previous, current))

    assert selection.provider_build_id == current.provider_build_id
    assert selection.selection_status == READY_FOR_NEXTADS
    assert selection.source_run_date == RUN_DATE


def test_latest_failed_attempt_does_not_resurrect_earlier_ready_attempt():
    ready = _build(attempt=0)
    failed = _build(
        attempt=1,
        completed_at=CUTOFF - timedelta(minutes=1),
        status=FAILED_BEFORE_PUBLISH,
    )

    with pytest.raises(ProviderBuildNotReadyError, match="No same-day"):
        _select((ready, failed), allow_fallback=False)


def test_selection_is_deterministic_when_ready_builds_complete_together():
    first = _build("build_a")
    second = replace(
        _build("build_z"),
        task_run_id=first.task_run_id,
        execution_count=first.execution_count,
        completed_at=first.completed_at,
    )

    assert _select((first, second)).provider_build_id == "build_z"
    assert _select((second, first)).provider_build_id == "build_z"


def test_fallback_is_bounded_by_fixed_cutoff_inclusively():
    boundary = _build(
        "boundary",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=24),
    )
    selected = _select((boundary,))
    assert selected.selection_status == FALLBACK_PREVIOUS
    assert selected.provider_build_id == "boundary"

    too_old = replace(
        boundary,
        provider_build_id="too_old",
        provider_build_attempt_id="too_old:attempt:0",
        completed_at=boundary.completed_at - timedelta(microseconds=1),
    )
    with pytest.raises(ProviderBuildNotReadyError, match="within 24 hours"):
        _select((too_old,))


def test_fallback_is_not_selected_before_the_readiness_deadline():
    fallback = _build(
        "fallback",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=1),
    )
    clock = _Clock(CUTOFF - timedelta(seconds=10))
    loads = []

    def loader(*_args, **_kwargs):
        loads.append(clock.current)
        return (fallback,)

    selected = wait_for_score_provider_build(
        object(),
        table="catalog.schema.builds",
        run_date=RUN_DATE,
        provider_id="theme_affinity",
        capability="account_theme",
        use_case="theme_ranking",
        wait_seconds=10,
        poll_seconds=4,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        load_fn=loader,
    )

    assert selected.selection_status == FALLBACK_PREVIOUS
    assert loads == [
        CUTOFF - timedelta(seconds=10),
        CUTOFF - timedelta(seconds=6),
        CUTOFF - timedelta(seconds=2),
        CUTOFF,
    ]
    assert clock.sleeps == [4, 4, 2]


def test_wait_accepts_same_day_build_as_soon_as_it_is_ready():
    fallback = _build(
        "fallback",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=1),
    )
    current = _build()
    clock = _Clock(CUTOFF - timedelta(seconds=10))
    responses = [(fallback,), (fallback,), (fallback, current)]

    def loader(*_args, **_kwargs):
        return responses.pop(0)

    selected = wait_for_score_provider_build(
        object(),
        table="catalog.schema.builds",
        run_date=RUN_DATE,
        provider_id="theme_affinity",
        capability="account_theme",
        use_case="theme_ranking",
        wait_seconds=10,
        poll_seconds=4,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        load_fn=loader,
    )

    assert selected.provider_build_id == current.provider_build_id
    assert selected.selection_status == READY_FOR_NEXTADS
    assert clock.sleeps == [4, 4]


def test_duplicate_and_contradictory_attempt_metadata_is_rejected():
    first = _build("first")
    duplicate_attempt = replace(
        _build("second"),
        provider_build_attempt_id=first.provider_build_attempt_id,
    )
    with pytest.raises(ValueError, match="attempt IDs must be unique"):
        _select((first, duplicate_attempt))

    contradictory = replace(
        first,
        provider_build_attempt_id="first:another-attempt",
    )
    with pytest.raises(ValueError, match="Contradictory"):
        _select((first, contradictory))


def test_invalid_attempts_outside_fallback_window_do_not_block_today():
    current = _build()
    ancient = _build(
        "ancient",
        run_date=RUN_DATE - timedelta(days=7),
        completed_at=CUTOFF - timedelta(days=6),
    )
    duplicate_ancient = replace(
        ancient,
        provider_build_id="other-ancient",
    )

    selected = _select((ancient, duplicate_ancient, current))

    assert selected.provider_build_id == current.provider_build_id


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("InputSnapshotID", "", "InputSnapshotID must not be empty"),
        ("OutputTable", None, "must identify its output"),
        ("OutputDeltaVersion", None, "must identify its output"),
        ("Status", "READY", "Unsupported provider build status"),
    ],
)
def test_manifest_parser_rejects_invalid_selection_metadata(
    column,
    value,
    message,
):
    with pytest.raises(ValueError, match=message):
        parse_score_provider_build(_manifest_row(**{column: value}))


def test_manifest_parser_rejects_missing_physical_columns():
    row = _manifest_row()
    del row["OutputTable"]
    with pytest.raises(ValueError, match="missing columns: OutputTable"):
        parse_score_provider_build(row)
    assert set(row) == set(PROVIDER_BUILD_COLUMNS) - {"OutputTable"}


def test_independent_provider_can_have_no_foundation_build():
    missing_foundation = replace(
        _build(),
        scoring_foundation_build_id=None,
        scoring_foundation_build_attempt_id=None,
    )

    selected = _select((missing_foundation,))

    assert selected.scoring_foundation_build_id is None


def test_default_readiness_cutoff_is_fixed_in_europe_london():
    assert selection_job._default_selection_cutoff(date(2026, 8, 3)) == (
        datetime(2026, 8, 3, 17, 30, tzinfo=timezone.utc)
    )
    assert selection_job._default_selection_cutoff(date(2026, 12, 3)) == (
        datetime(2026, 12, 3, 18, 30, tzinfo=timezone.utc)
    )


def test_other_provider_capability_and_use_case_are_not_candidates():
    candidates = (
        _build("other-provider", provider_id="markov"),
        _build("other-capability", capability="account_ad"),
        _build("other-use-case", use_case="ad_ranking"),
    )
    with pytest.raises(ProviderBuildNotReadyError, match="No same-day"):
        _select(candidates, allow_fallback=False)


def test_job_emits_the_exact_selected_provider_binding(monkeypatch):
    expected = ProviderBuildSelection(
        provider_build_id="build_123",
        provider_build_attempt_id="build_123:attempt:0",
        provider_signals_table="catalog.schema.provider_signals",
        provider_signals_delta_version=91,
        input_snapshot_id="inputs_123",
        scoring_foundation_build_id="foundation_123",
        selection_status=READY_FOR_NEXTADS,
        source_run_date=RUN_DATE,
    )
    recorded = {}
    task_values = _TaskValues()
    monkeypatch.setattr(selection_job, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(selection_job, "configure_spark", lambda: "spark")
    monkeypatch.setattr(
        selection_job.config_manager,
        "load_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            tables_write=SimpleNamespace(score_provider_builds="build_table")
        ),
    )
    monkeypatch.setattr(
        selection_job,
        "get_dbutils",
        lambda: SimpleNamespace(jobs=SimpleNamespace(taskValues=task_values)),
    )

    def selector(*args, **kwargs):
        recorded["args"] = args
        recorded["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(
        selection_job,
        "wait_for_score_provider_build",
        selector,
    )

    selection_job.main(
        "dev",
        "next_uk",
        None,
        RUN_DATE.isoformat(),
        "theme_affinity",
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

    assert recorded["args"] == ("spark",)
    assert recorded["kwargs"]["table"] == "build_table"
    assert recorded["kwargs"]["selection_cutoff"] == CUTOFF
    assert task_values.values == {
        "provider_build_id": "build_123",
        "provider_build_attempt_id": "build_123:attempt:0",
        "provider_signals_table": "catalog.schema.provider_signals",
        "provider_signals_delta_version": 91,
        "input_snapshot_id": "inputs_123",
        "scoring_foundation_build_id": "foundation_123",
        "provider_selection_status": READY_FOR_NEXTADS,
        "provider_source_run_date": RUN_DATE.isoformat(),
    }


class _Clock:
    def __init__(self, current):
        self.current = current
        self.sleeps = []

    def now(self):
        return self.current

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


class _TaskValues:
    def __init__(self):
        self.values = {}

    def set(self, *, key, value):
        self.values[key] = value
