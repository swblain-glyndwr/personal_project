from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from next_ads.features.sql_contracts import extract_create_table_columns
from next_ads.model_development import research_claims, research_store


NOW = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)


def _binding() -> research_store.ResearchFrameBinding:
    return research_store.ResearchFrameBinding(
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        training_receipt_id="training-receipt",
        research_frame_table="catalog.schema.research_frames",
        research_frame_delta_version=12,
        research_frame_row_count=70,
        research_frame_schema_checksum="a" * 64,
        research_frame_data_checksum="b" * 64,
        research_frame_write_receipt_id="write-receipt",
        research_frame_feature_schema_json="{}",
        research_frame_slice_schema_json="{}",
    )


def _claim(**changes) -> research_claims.ResearchClaim:
    values = {
        "research_build_id": "research-logical",
        "research_attempt_id": "research-attempt",
        "model_definition_checksum": "a" * 64,
        "training_receipt_id": "training-receipt",
        "research_plan_checksum": "b" * 64,
        "evaluation_schema_version": "binary/v1",
        "code_sha": "abc123",
        "owner_invocation_id": "job-run-1",
        "lease_token": "lease-token-1",
        "lease_expires_at": NOW + timedelta(hours=1),
        "checkpoint": research_claims.CLAIMED,
        "checkpoint_version": 0,
        "research_frame_binding_json": None,
        "mlflow_experiment_id": None,
        "mlflow_parent_run_id": None,
        "selection_decision_id": None,
        "model_build_id": None,
        "failure_reason": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(changes)
    return research_claims.ResearchClaim(**values)


class _Frame:
    def __init__(self, row):
        self.row = row
        self.columns = tuple(row)
        self.view = None

    def createOrReplaceTempView(self, view):  # noqa: N802
        self.view = view


class _Spark:
    def __init__(self):
        self.queries = []
        self.catalog = SimpleNamespace(dropTempView=lambda _view: None)

    def sql(self, query):
        self.queries.append(query)


def test_claim_contract_is_registered_and_matches_dataclass():
    path = research_store.RESEARCH_TABLE_CONTRACTS[
        research_store.RESEARCH_CLAIM_TABLE
    ]
    sql = path.read_text()
    sql_columns = {
        name for name, _definition in extract_create_table_columns(sql)
    }

    assert sql_columns == {
        field.name for field in fields(research_claims.ResearchClaim)
    }
    assert "PRIMARY KEY (research_build_id)" in sql
    assert "lease_token STRING NOT NULL" in sql
    assert "checkpoint_version BIGINT NOT NULL" in sql


def test_frame_binding_serialization_is_complete_and_deterministic():
    binding = _binding()

    first = research_claims.serialize_research_frame_binding(binding)
    second = research_claims.serialize_research_frame_binding(binding)

    assert first == second
    assert research_claims.deserialize_research_frame_binding(first) == binding


def test_acquisition_merge_is_fenced_and_preserves_terminal_claims():
    spark = _Spark()
    frame = _Frame(asdict(_claim()))

    research_claims._merge_claim_acquisition(
        spark,
        table="catalog.schema.claims",
        frame=frame,
    )

    statement = spark.queries[0]
    assert "WHEN NOT MATCHED THEN" in statement
    assert "`checkpoint` NOT IN ('COMPLETE', 'FAILED')" in statement
    assert "`lease_expires_at`" in statement
    assert "`lease_token`" in statement
    assert "`checkpoint_version` =" in statement
    assert "target.`checkpoint_version` + 1" in statement
    assert "target.`checkpoint` = 'FAILED'" in statement
    assert "`research_frame_binding_json` = NULL" in statement
    assert "`failure_reason` = NULL" in statement


def test_code_change_starts_a_fresh_attempt_after_terminal_failure(
    monkeypatch,
):
    stored = _claim(
        research_attempt_id="failed-attempt",
        checkpoint=research_claims.FAILED,
        checkpoint_version=4,
        failure_reason="RuntimeError: candidate failed",
        code_sha="old-sha",
    )
    state = [stored]
    outputs = []
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )

    def merge(_spark, *, frame, **_kwargs):
        proposed = research_claims.ResearchClaim(**frame.row)
        state[0] = replace(
            proposed,
            checkpoint_version=stored.checkpoint_version + 1,
        )

    monkeypatch.setattr(research_claims, "_merge_claim_acquisition", merge)
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: state[0],
    )
    monkeypatch.setattr(
        research_claims,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )

    acquired = research_claims.claim_research_build(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=stored.research_build_id,
        research_attempt_id="fixed-attempt",
        model_definition_checksum=stored.model_definition_checksum,
        training_receipt_id=stored.training_receipt_id,
        research_plan_checksum=stored.research_plan_checksum,
        evaluation_schema_version=stored.evaluation_schema_version,
        code_sha="fixed-sha",
        owner_invocation_id="job-run-2",
        now=NOW,
    )

    assert acquired.research_attempt_id == "fixed-attempt"
    assert acquired.code_sha == "fixed-sha"
    assert acquired.checkpoint == research_claims.CLAIMED
    assert acquired.failure_reason is None
    assert outputs == [
        {
            "destination": (
                "catalog.schema.next_uk_nextads_model_research_claims"
            ),
            "kind": "delta_table",
            "details": {
                "checkpoint": research_claims.CLAIMED,
                "checkpoint_version": 5,
                "operation": "claim_research_build",
                "reused": False,
            },
        }
    ]


def test_transition_merge_uses_lease_and_checkpoint_version_cas():
    spark = _Spark()
    updated = replace(
        _claim(),
        checkpoint=research_claims.FRAME_READY,
        checkpoint_version=1,
        research_frame_binding_json=(
            research_claims.serialize_research_frame_binding(_binding())
        ),
    )

    research_claims._merge_claim_transition(
        spark,
        table="catalog.schema.claims",
        frame=_Frame(asdict(updated)),
        expected_checkpoint=research_claims.CLAIMED,
    )

    statement = spark.queries[0]
    assert "target.`owner_invocation_id`" in statement
    assert "target.`lease_token`" in statement
    assert "target.`checkpoint_version`" in statement
    assert "source.`checkpoint_version` - 1" in statement
    assert "target.`checkpoint`" in statement
    assert "= 'CLAIMED'" in statement


def test_expired_claim_reuses_stable_attempt_under_a_new_lease(monkeypatch):
    stored = _claim(
        owner_invocation_id="job-run-1",
        lease_token="old-token",
        lease_expires_at=NOW - timedelta(seconds=1),
        checkpoint=research_claims.CANDIDATES_READY,
        checkpoint_version=3,
        research_frame_binding_json=(
            research_claims.serialize_research_frame_binding(_binding())
        ),
        mlflow_experiment_id="experiment-1",
        mlflow_parent_run_id="parent-run-1",
    )
    state = [stored]
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )

    def merge(_spark, *, frame, **_kwargs):
        proposed = research_claims.ResearchClaim(**frame.row)
        state[0] = replace(
            state[0],
            owner_invocation_id=proposed.owner_invocation_id,
            lease_token=proposed.lease_token,
            lease_expires_at=proposed.lease_expires_at,
            checkpoint_version=state[0].checkpoint_version + 1,
            updated_at=proposed.updated_at,
        )

    monkeypatch.setattr(research_claims, "_merge_claim_acquisition", merge)
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: state[0],
    )

    acquired = research_claims.claim_research_build(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=stored.research_build_id,
        research_attempt_id="new-attempt-must-not-win",
        model_definition_checksum=stored.model_definition_checksum,
        training_receipt_id=stored.training_receipt_id,
        research_plan_checksum=stored.research_plan_checksum,
        evaluation_schema_version=stored.evaluation_schema_version,
        code_sha=stored.code_sha,
        owner_invocation_id="job-run-2",
        now=NOW,
    )

    assert acquired.research_attempt_id == "research-attempt"
    assert acquired.owner_invocation_id == "job-run-2"
    assert acquired.lease_token != "old-token"
    assert acquired.checkpoint == research_claims.CANDIDATES_READY
    assert acquired.checkpoint_version == 4


def test_unexpired_claim_rejects_a_different_invocation(monkeypatch):
    stored = _claim()
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        research_claims,
        "_merge_claim_acquisition",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: stored,
    )

    with pytest.raises(
        research_claims.ResearchClaimConflictError,
        match="live lease",
    ):
        research_claims.claim_research_build(
            object(),
            catalog="catalog",
            schema="schema",
            research_build_id=stored.research_build_id,
            research_attempt_id="another-attempt",
            model_definition_checksum=stored.model_definition_checksum,
            training_receipt_id=stored.training_receipt_id,
            research_plan_checksum=stored.research_plan_checksum,
            evaluation_schema_version=stored.evaluation_schema_version,
            code_sha=stored.code_sha,
            owner_invocation_id="job-run-2",
            now=NOW,
        )


def test_checkpoint_transition_locks_binding_and_fences_stale_token(
    monkeypatch,
):
    state = [_claim()]
    outputs = []
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: state[0],
    )

    def merge(_spark, *, frame, **_kwargs):
        state[0] = research_claims.ResearchClaim(**frame.row)

    monkeypatch.setattr(research_claims, "_merge_claim_transition", merge)
    monkeypatch.setattr(
        research_claims,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )

    transitioned = research_claims.advance_research_claim(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=state[0].research_build_id,
        owner_invocation_id=state[0].owner_invocation_id,
        lease_token=state[0].lease_token,
        expected_checkpoint=research_claims.CLAIMED,
        checkpoint=research_claims.FRAME_READY,
        research_frame_binding=_binding(),
        now=NOW + timedelta(minutes=1),
    )

    assert transitioned.checkpoint == research_claims.FRAME_READY
    assert transitioned.checkpoint_version == 1
    assert transitioned.research_frame_binding == _binding()
    assert outputs == [
        {
            "destination": (
                "catalog.schema.next_uk_nextads_model_research_claims"
            ),
            "kind": "delta_table",
            "details": {
                "checkpoint": research_claims.FRAME_READY,
                "checkpoint_version": 1,
                "operation": "advance_research_claim",
                "reused": False,
            },
        }
    ]

    with pytest.raises(
        research_claims.ResearchClaimConflictError,
        match="newer lease token",
    ):
        research_claims.advance_research_claim(
            object(),
            catalog="catalog",
            schema="schema",
            research_build_id=state[0].research_build_id,
            owner_invocation_id=state[0].owner_invocation_id,
            lease_token="stale-token",
            expected_checkpoint=research_claims.FRAME_READY,
            checkpoint=research_claims.PARENT_READY,
            mlflow_experiment_id="experiment-1",
            mlflow_parent_run_id="parent-run-1",
            now=NOW + timedelta(minutes=2),
        )


def test_release_keeps_checkpoint_and_expires_the_fenced_lease(monkeypatch):
    state = [
        _claim(
            checkpoint=research_claims.CANDIDATES_READY,
            checkpoint_version=3,
            research_frame_binding_json=(
                research_claims.serialize_research_frame_binding(_binding())
            ),
            mlflow_experiment_id="experiment-1",
            mlflow_parent_run_id="parent-run-1",
        )
    ]
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: state[0],
    )
    monkeypatch.setattr(
        research_claims,
        "_merge_claim_transition",
        lambda _spark, *, frame, **_kwargs: state.__setitem__(
            0, research_claims.ResearchClaim(**frame.row)
        ),
    )
    released_at = NOW + timedelta(minutes=1)

    released = research_claims.release_research_claim(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=state[0].research_build_id,
        owner_invocation_id=state[0].owner_invocation_id,
        lease_token=state[0].lease_token,
        now=released_at,
    )

    assert released.checkpoint == research_claims.CANDIDATES_READY
    assert released.checkpoint_version == 4
    assert released.lease_expires_at == released_at
    assert released.updated_at == released_at


def test_terminal_claim_noop_logs_exact_reused_table(monkeypatch):
    current = _claim(
        checkpoint=research_claims.COMPLETE,
        checkpoint_version=7,
    )
    outputs = []
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: current,
    )
    monkeypatch.setattr(
        research_claims,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )

    returned = research_claims.renew_research_claim(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=current.research_build_id,
        owner_invocation_id=current.owner_invocation_id,
        lease_token=current.lease_token,
        now=NOW,
    )

    assert returned is current
    assert outputs == [
        {
            "destination": (
                "catalog.schema.next_uk_nextads_model_research_claims"
            ),
            "kind": "delta_table",
            "details": {
                "checkpoint": research_claims.COMPLETE,
                "checkpoint_version": 7,
                "operation": "renew_research_claim",
                "reused": True,
            },
        }
    ]


def test_failed_claim_is_terminal(monkeypatch):
    state = [_claim()]
    monkeypatch.setattr(
        research_claims,
        "typed_table_frame",
        lambda _spark, _table, rows: _Frame(rows[0]),
    )
    monkeypatch.setattr(
        research_claims,
        "load_research_claim",
        lambda *_args, **_kwargs: state[0],
    )
    monkeypatch.setattr(
        research_claims,
        "_merge_claim_transition",
        lambda _spark, *, frame, **_kwargs: state.__setitem__(
            0, research_claims.ResearchClaim(**frame.row)
        ),
    )

    failed = research_claims.fail_research_claim(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=state[0].research_build_id,
        owner_invocation_id=state[0].owner_invocation_id,
        lease_token=state[0].lease_token,
        expected_checkpoint=research_claims.CLAIMED,
        failure_reason="Declared candidate contract is invalid",
        now=NOW + timedelta(minutes=1),
    )

    assert failed.checkpoint == research_claims.FAILED
    assert failed.checkpoint_version == 1
    assert failed.terminal is True
    with pytest.raises(
        research_claims.ResearchClaimConflictError,
        match="Terminal research claim",
    ):
        research_claims.advance_research_claim(
            object(),
            catalog="catalog",
            schema="schema",
            research_build_id=failed.research_build_id,
            owner_invocation_id=failed.owner_invocation_id,
            lease_token=failed.lease_token,
            expected_checkpoint=research_claims.CLAIMED,
            checkpoint=research_claims.FRAME_READY,
            research_frame_binding=_binding(),
            now=NOW + timedelta(minutes=2),
        )
