from datetime import date, datetime, timezone
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_DIRECTORY = PROJECT_ROOT / "jobs" / "features" / "nextads"
if str(JOB_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOB_DIRECTORY))

job = importlib.import_module(
    "jobs.features.nextads.verify_pctr_feature_snapshot"
)


def _binding(feature_id, attempt_id):
    return SimpleNamespace(
        feature_snapshot_id="analytics_pctr:2026-08-11",
        feature_snapshot_attempt_id=attempt_id,
        feature_build_id=attempt_id,
        feature_build_attempt_id=attempt_id,
        reference_date=date(2026, 8, 11),
        registry_checksum="a" * 64,
        git_commit="abc123",
        completed_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        feature_id=feature_id,
        backing_table=f"catalog.schema.{feature_id}",
        delta_version=42,
        row_count=10,
        output_schema_checksum="b" * 64,
        backing_schema_checksum="b" * 64,
        value_checksum="c" * 64,
        write_receipt_id=f"receipt-{feature_id}",
    )


def test_verifier_accepts_one_current_ready_attempt(monkeypatch):
    monkeypatch.setattr(
        job,
        "read_ready_feature",
        lambda _spark, feature_id, **_kwargs: (
            object(),
            _binding(feature_id, "123"),
        ),
    )

    evidence = job.verify_ready_snapshot(
        object(),
        catalog="catalog",
        schema="schema",
        reference_date="2026-08-11",
        current_attempt_id="123",
        expect_current_attempt_ready=True,
    )

    assert evidence["ready_attempt_id"] == "123"
    assert evidence["current_attempt_ready"] is True
    assert len(evidence["features"]) == 3


def test_verifier_proves_failed_attempt_did_not_replace_ready(monkeypatch):
    monkeypatch.setattr(
        job,
        "read_ready_feature",
        lambda _spark, feature_id, **_kwargs: (
            object(),
            _binding(feature_id, "previous"),
        ),
    )

    evidence = job.verify_ready_snapshot(
        object(),
        catalog="catalog",
        schema="schema",
        reference_date="2026-08-11",
        current_attempt_id="failed",
        expect_current_attempt_ready=False,
    )

    assert evidence["ready_attempt_id"] == "previous"
    assert evidence["current_attempt_ready"] is False


def test_verifier_rejects_features_from_different_attempts(monkeypatch):
    attempts = iter(("123", "other", "123"))
    monkeypatch.setattr(
        job,
        "read_ready_feature",
        lambda _spark, feature_id, **_kwargs: (
            object(),
            _binding(feature_id, next(attempts)),
        ),
    )

    with pytest.raises(ValueError, match="do not share one READY snapshot"):
        job.verify_ready_snapshot(
            object(),
            catalog="catalog",
            schema="schema",
            reference_date="2026-08-11",
            current_attempt_id="123",
            expect_current_attempt_ready=True,
        )
