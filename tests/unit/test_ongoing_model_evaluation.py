from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

from next_ads.model_development.contracts import ModelBuild
from next_ads.model_development.ongoing_evaluation import (
    BUILDING,
    FAILED,
    READY,
    CandidateInputBinding,
    OngoingEvaluationBuild,
    _require_non_empty_candidate_scope,
    evaluation_scoring_build_id,
    persist_evaluation_build,
    scoring_build_attempt_id,
)
from next_ads.model_development.registry import load_model_definition
from next_ads.model_development.scoring_sets import ScoringFeatureBinding


NOW = datetime(2026, 8, 18, 12, tzinfo=timezone.utc)
DIGEST = "a" * 64


def _model_build():
    definition = load_model_definition("shopping_bag_pctr")
    return ModelBuild(
        model_build_id="model-build",
        model_name=definition.model_name,
        training_receipt_id="training-receipt",
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="READY",
        created_at=NOW,
        completed_at=NOW,
        mlflow_run_id="mlflow-run",
        registered_model_name="catalog.schema.shopping_bag_pctr",
        registered_model_version=7,
        model_uri="models:/catalog.schema.shopping_bag_pctr/7",
        artifact_digest="b" * 64,
    )


def _candidate_binding(route="v1", attempt="candidate-v1-attempt"):
    return CandidateInputBinding(
        route=route,
        output_grain="location" if route == "v1" else "page_type",
        candidate_build_id=f"candidate-{route}",
        candidate_build_attempt_id=attempt,
        portfolio_id=f"portfolio-{route}",
        portfolio_attempt_id=f"portfolio-{route}-attempt",
        candidate_foundation_snapshot_id=f"foundation-{route}",
        serving_slot="best",
        scopes=("SB1", "SB2") if route == "v1" else ("ShoppingBagPage",),
        builds_table="catalog.schema.candidate_builds",
        scores_table="catalog.schema.candidate_scores",
        ad_sets_table="catalog.schema.candidate_ad_sets",
    )


def _feature_binding(delta_version=42):
    return ScoringFeatureBinding(
        feature_id="next_uk_nextads_fs_account_web_activity_90d",
        reference_date=date(2026, 8, 17),
        feature_snapshot_id="snapshot",
        feature_snapshot_attempt_id="snapshot-attempt",
        feature_build_id="feature-build",
        feature_build_attempt_id="feature-attempt",
        backing_table="catalog.schema.feature_backing",
        delta_version=delta_version,
        row_count=100,
        schema_checksum="c" * 64,
        value_checksum="d" * 64,
        write_receipt_id="write-receipt",
    )


def _building():
    definition = load_model_definition("shopping_bag_pctr")
    model_build = _model_build()
    return OngoingEvaluationBuild(
        scoring_build_id="scoring-build",
        scoring_build_attempt_id="101:202:0",
        model_build_id=model_build.model_build_id,
        model_name=model_build.model_name,
        model_definition_checksum=definition.checksum,
        registered_model_name=model_build.registered_model_name,
        registered_model_version=model_build.registered_model_version,
        model_uri=model_build.model_uri,
        artifact_digest=model_build.artifact_digest,
        run_date=date(2026, 8, 18),
        serving_slot="best",
        candidate_bindings=(
            _candidate_binding(),
            _candidate_binding("v2", "candidate-v2-attempt"),
        ),
        feature_bindings=(_feature_binding(),),
        input_row_count=100,
        input_schema_checksum="e" * 64,
        input_value_checksum="f" * 64,
        git_commit="abc123",
        orchestration_run_id=101,
        task_run_id=202,
        execution_count=0,
        status=BUILDING,
        created_at=NOW,
    )


def _build_id(*, candidates=None, features=None):
    definition = load_model_definition("shopping_bag_pctr")
    return evaluation_scoring_build_id(
        definition=definition,
        model_build=_model_build(),
        run_date=date(2026, 8, 18),
        serving_slot="best",
        candidate_bindings=candidates
        or (
            _candidate_binding(),
            _candidate_binding("v2", "candidate-v2-attempt"),
        ),
        feature_bindings=features or (_feature_binding(),),
        input_row_count=100,
        input_schema_checksum="e" * 64,
        input_value_checksum="f" * 64,
        git_commit="abc123",
    )


def test_daily_build_id_pins_model_candidates_and_feature_versions():
    original = _build_id()

    assert original == _build_id()
    assert original != _build_id(
        candidates=(
            _candidate_binding(attempt="different-v1-attempt"),
            _candidate_binding("v2", "candidate-v2-attempt"),
        )
    )
    assert original != _build_id(features=(_feature_binding(43),))


def test_retry_attempt_id_is_immutable_and_execution_specific():
    assert scoring_build_attempt_id(101, 202, 0) == "101:202:0"
    assert scoring_build_attempt_id(101, 202, 1) == "101:202:1"

    with pytest.raises(ValueError, match="non-negative"):
        scoring_build_attempt_id(101, 202, -1)


def test_candidate_builder_requires_every_shopping_bag_scope():
    class EmptyFrame:
        def limit(self, value):
            assert value == 1
            return self

        def count(self):
            return 0

    with pytest.raises(
        ValueError,
        match=("route=v2, page_type=ShoppingBagPage"),
    ):
        _require_non_empty_candidate_scope(
            EmptyFrame(),
            route="v2",
            scope_type="page_type",
            scope_value="ShoppingBagPage",
        )


def test_ready_is_only_valid_after_exact_delta_output_exists():
    building = _building()

    with pytest.raises(ValueError, match="exact output proof"):
        replace(building, status=READY, completed_at=NOW)

    ready = replace(
        building,
        status=READY,
        output_table="catalog.schema.evaluation_scores",
        output_delta_version=9,
        output_row_count=100,
        output_schema_checksum=DIGEST,
        output_value_checksum="9" * 64,
        completed_at=NOW,
    )
    assert ready.status == READY

    failed = replace(
        building,
        status=FAILED,
        completed_at=NOW,
        failure_reason="model transform failed",
    )
    assert failed.output_table is None


def test_manifest_write_replaces_only_one_attempt(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "next_ads.model_development.ongoing_evaluation.typed_table_frame",
        lambda _spark, _target, rows: rows,
    )

    def replace_scope(frame, target, scope, **kwargs):
        calls.append((frame, target, scope, kwargs))

    monkeypatch.setattr(
        "next_ads.model_development.ongoing_evaluation.replace_scope_by_name",
        replace_scope,
    )

    persist_evaluation_build(
        object(),
        catalog="catalog",
        schema="schema",
        build=_building(),
    )

    assert calls[0][2] == {
        "scoring_build_id": "scoring-build",
        "scoring_build_attempt_id": "101:202:0",
    }
    assert "run_date" not in calls[0][2]
