from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest

from next_ads.features.sql_contracts import extract_create_table_columns
from next_ads.model_development import automl_claims, research_store


NOW = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)


def _evidence_fields() -> dict[str, object]:
    payload = {
        "schema_version": "nextads_automl_leaderboard/v1",
        "research_build_id": "research-logical",
        "discovery_id": "discovery-logical",
        "research_parent_run_id": "research-parent-run",
        "experiment_id": "experiment-1",
        "primary_metric": "roc_auc",
        "trial_count": 1,
        "best_trial_id": "trial-1",
        "trials": [
            {
                "rank": 1,
                "trial_id": "trial-1",
                "primary_metric_value": 0.81,
                "notebook_artifact_uri": "runs:/trial-1/notebook",
                "notebook_path": None,
                "notebook_url": "https://workspace/notebook/1",
                "is_best_trial": True,
            }
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "experiment_id": "experiment-1",
        "trial_count": 1,
        "best_trial_id": "trial-1",
        "primary_metric": "roc_auc",
        "trial_evidence_json": encoded,
        "leaderboard_run_id": "leaderboard-run",
        "leaderboard_artifact_sha256": hashlib.sha256(
            encoded.encode("utf-8")
        ).hexdigest(),
        "leaderboard_artifact_uri": (
            "runs:/leaderboard-run/automl_discovery/leaderboard.json"
        ),
        "recipe_artifact_uri": "https://workspace/notebook/1",
    }


def _claim(**changes) -> automl_claims.AutoMLDiscoveryClaim:
    values = {
        "discovery_id": "discovery-logical",
        "discovery_attempt_id": "discovery-attempt",
        "request_checksum": "a" * 64,
        "research_build_id": "research-logical",
        "research_attempt_id": "research-attempt",
        "research_frame_id": "frame-logical",
        "research_frame_attempt_id": "frame-attempt",
        "research_frame_delta_version": 12,
        "timeout_minutes": 30,
        "experiment_path": "/Shared/model-research/automl",
        "code_sha": "abc123",
        "owner_invocation_id": "job-run:task-run:0",
        "lease_token": "lease-token",
        "lease_expires_at": NOW + timedelta(hours=3),
        "checkpoint": automl_claims.CLAIMED,
        "checkpoint_version": 0,
        "experiment_id": None,
        "trial_count": None,
        "best_trial_id": None,
        "primary_metric": None,
        "trial_evidence_json": None,
        "leaderboard_run_id": None,
        "leaderboard_artifact_sha256": None,
        "leaderboard_artifact_uri": None,
        "recipe_artifact_uri": None,
        "failure_reason": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(changes)
    return automl_claims.AutoMLDiscoveryClaim(**values)


class _Frame:
    def __init__(self, row):
        self.row = row
        self.columns = tuple(row)

    def createOrReplaceTempView(self, _view):  # noqa: N802
        return None


class _Spark:
    def __init__(self):
        self.queries = []
        self.catalog = SimpleNamespace(dropTempView=lambda _view: None)

    def sql(self, query):
        self.queries.append(query)


def test_claim_contract_is_registered_and_matches_dataclass():
    path = research_store.RESEARCH_TABLE_CONTRACTS[
        research_store.AUTOML_DISCOVERY_CLAIM_TABLE
    ]
    sql = path.read_text()
    sql_columns = {
        name for name, _definition in extract_create_table_columns(sql)
    }

    assert sql_columns == {
        field.name for field in fields(automl_claims.AutoMLDiscoveryClaim)
    }
    assert "PRIMARY KEY (discovery_id)" in sql
    assert "lease_token STRING NOT NULL" in sql
    assert "checkpoint_version BIGINT NOT NULL" in sql


def test_claim_insert_has_no_takeover_or_matched_update():
    spark = _Spark()

    automl_claims._merge_claim_insert(
        spark,
        table="catalog.schema.automl_claims",
        frame=_Frame(asdict(_claim())),
    )

    statement = spark.queries[0]
    assert "WHEN NOT MATCHED THEN" in statement
    assert "WHEN MATCHED" not in statement
    assert "lease_expires_at" in statement


def test_expired_unknown_claim_still_blocks_duplicate_experiment(monkeypatch):
    stored = _claim(
        checkpoint=automl_claims.RUNNING,
        checkpoint_version=1,
        lease_expires_at=NOW - timedelta(minutes=1),
    )
    monkeypatch.setattr(
        automl_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        automl_claims,
        "_merge_claim_insert",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        automl_claims,
        "load_automl_claim",
        lambda *_args, **_kwargs: stored,
    )

    with pytest.raises(
        automl_claims.AutoMLClaimConflictError,
        match="duplicate experiment will not be launched",
    ):
        automl_claims.claim_automl_discovery(
            object(),
            catalog="catalog",
            schema="schema",
            discovery_id=stored.discovery_id,
            discovery_attempt_id="new-task-attempt",
            request_checksum=stored.request_checksum,
            research_build_id=stored.research_build_id,
            research_attempt_id=stored.research_attempt_id,
            research_frame_id=stored.research_frame_id,
            research_frame_attempt_id=stored.research_frame_attempt_id,
            research_frame_delta_version=(stored.research_frame_delta_version),
            timeout_minutes=stored.timeout_minutes,
            experiment_path=stored.experiment_path,
            code_sha=stored.code_sha,
            owner_invocation_id="job-run-2:task-run-2:0",
            now=NOW,
        )


def test_evidence_transition_is_token_and_version_fenced(monkeypatch):
    state = [
        _claim(
            checkpoint=automl_claims.RUNNING,
            checkpoint_version=1,
        )
    ]
    monkeypatch.setattr(
        automl_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        automl_claims,
        "load_automl_claim",
        lambda *_args, **_kwargs: state[0],
    )

    def merge(_spark, *, frame, **_kwargs):
        state[0] = automl_claims.AutoMLDiscoveryClaim(**frame.row)

    monkeypatch.setattr(automl_claims, "_merge_owned_transition", merge)

    stored = automl_claims.record_automl_evidence(
        object(),
        catalog="catalog",
        schema="schema",
        claim=state[0],
        **_evidence_fields(),
        now=NOW + timedelta(minutes=30),
    )

    assert stored.checkpoint == automl_claims.EVIDENCE_READY
    assert stored.checkpoint_version == 2
    assert stored.experiment_id == "experiment-1"
    assert stored.trial_count == 1
    assert stored.leaderboard_run_id == "leaderboard-run"


def test_transition_merge_uses_owner_token_and_checkpoint_version_cas():
    spark = _Spark()
    updated = replace(
        _claim(checkpoint=automl_claims.RUNNING, checkpoint_version=1),
        checkpoint=automl_claims.FAILED,
        checkpoint_version=2,
        failure_reason="RuntimeError: discovery failed",
    )

    automl_claims._merge_owned_transition(
        spark,
        table="catalog.schema.automl_claims",
        frame=_Frame(asdict(updated)),
        expected_checkpoint=automl_claims.RUNNING,
    )

    statement = spark.queries[0]
    assert "target.`owner_invocation_id`" in statement
    assert "target.`lease_token`" in statement
    assert "target.`checkpoint_version`" in statement
    assert "source.`checkpoint_version` - 1" in statement
    assert "= 'RUNNING'" in statement
    assignments = statement.split("THEN UPDATE SET", maxsplit=1)[1]
    assert "request_checksum" not in assignments
    assert "research_build_id" not in assignments
    assert "code_sha" not in assignments
