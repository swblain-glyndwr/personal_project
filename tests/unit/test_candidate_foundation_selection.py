import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from next_ads.candidates import foundation_manifest as manifest_module
from next_ads.candidates.foundation import (
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
)
from next_ads.candidates.foundation_manifest import (
    CandidateFoundationBuild,
    CandidateFoundationNotReadyError,
    select_candidate_foundation,
    wait_for_candidate_foundation,
)


RUN_DATE = date(2026, 8, 3)
CUTOFF = datetime(2026, 8, 3, 18, 30, tzinfo=timezone.utc)


def _bindings():
    return json.dumps(
        {
            name: {
                "table": f"catalog.schema.{name}",
                "delta_version": index,
                "schema_version": "v1",
                "content_checksum": f"checksum-{name}",
                "row_count": 1,
            }
            for index, name in enumerate(
                ("customer_cells", "repeat_ad_exposure", "ad_feedback"),
                start=1,
            )
        }
    )


def _build(
    snapshot_id="foundation_today",
    *,
    run_date=RUN_DATE,
    status=READY_FOR_NEXTADS,
    completed_at=CUTOFF - timedelta(minutes=5),
    execution_count=0,
):
    return CandidateFoundationBuild(
        snapshot_id=snapshot_id,
        attempt_id=f"{snapshot_id}:{execution_count}",
        run_date=run_date,
        contract_version="nextads_candidate_foundation/v1",
        source_bindings_json="[]",
        output_bindings_json=_bindings(),
        warning_count=0,
        status=status,
        fallback_source_snapshot_id=None,
        fallback_source_run_date=None,
        task_run_id=100 + execution_count,
        execution_count=execution_count,
        completed_at=completed_at,
    )


def _select(builds):
    return select_candidate_foundation(
        builds,
        run_date=RUN_DATE,
        selection_cutoff=CUTOFF,
    )


def test_same_day_foundation_wins():
    previous = _build(
        "previous",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=1),
    )

    selection = _select((previous, _build()))

    assert selection.snapshot_id == "foundation_today"
    assert selection.selection_status == READY_FOR_NEXTADS


def test_previous_foundation_is_allowed_at_exactly_24_hours():
    previous = _build(
        "previous",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=24),
    )

    selection = _select((previous,))

    assert selection.snapshot_id == "previous"
    assert selection.selection_status == FALLBACK_PREVIOUS


def test_previous_foundation_older_than_24_hours_fails():
    previous = _build(
        "previous",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=24, microseconds=1),
    )

    with pytest.raises(CandidateFoundationNotReadyError, match="24 hours"):
        _select((previous,))


def test_latest_repaired_attempt_controls_acceptance():
    ready = _build(execution_count=0)
    failed = replace(
        _build(execution_count=1),
        status="FAILED_BEFORE_PUBLISH",
    )

    with pytest.raises(CandidateFoundationNotReadyError):
        _select((ready, failed))


def test_attempt_selection_is_input_order_independent():
    first = _build("a")
    second = replace(
        _build("z"),
        completed_at=first.completed_at,
        task_run_id=first.task_run_id,
    )

    assert _select((first, second)).snapshot_id == "z"
    assert _select((second, first)).snapshot_id == "z"


def test_post_cutoff_repair_does_not_hide_valid_pre_cutoff_attempt():
    accepted_before_cutoff = _build(execution_count=0)
    repair_after_cutoff = _build(
        execution_count=1,
        completed_at=CUTOFF + timedelta(minutes=5),
        status="FAILED_BEFORE_PUBLISH",
    )

    selection = _select((accepted_before_cutoff, repair_after_cutoff))

    assert selection.snapshot_id == accepted_before_cutoff.snapshot_id
    assert selection.selection_status == READY_FOR_NEXTADS


def test_fallback_is_immediate_when_selection_starts_after_cutoff(monkeypatch):
    previous = _build(
        "previous",
        run_date=RUN_DATE - timedelta(days=1),
        completed_at=CUTOFF - timedelta(hours=1),
    )
    monkeypatch.setattr(
        manifest_module,
        "load_candidate_foundation_builds",
        lambda *args, **kwargs: (previous,),
    )
    sleeps = []

    selection = wait_for_candidate_foundation(
        object(),
        builds_table="catalog.schema.builds",
        run_date=RUN_DATE,
        selection_cutoff=CUTOFF,
        requested_snapshot_id="same_day",
        wait_seconds=1800,
        poll_seconds=60,
        sleep=sleeps.append,
        monotonic=lambda: 100.0,
        now=lambda: CUTOFF + timedelta(minutes=1),
    )

    assert selection.snapshot_id == "previous"
    assert selection.selection_status == FALLBACK_PREVIOUS
    assert sleeps == []
