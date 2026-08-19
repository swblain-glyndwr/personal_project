from pathlib import Path

import pytest

from jobs.model.development import run_shopping_bag_ongoing_evaluation as job


PROJECT_ROOT = Path(job.__file__).resolve().parents[3]


def test_job_accepts_auto_or_exact_feature_dates():
    assert job._feature_reference_dates("AUTO") is None
    assert job._feature_reference_dates("2026-08-17,2026-08-16") == (
        "2026-08-16",
        "2026-08-17",
    )

    with pytest.raises(ValueError, match="must not be empty"):
        job._feature_reference_dates(" , ")
    with pytest.raises(ValueError, match="must be unique"):
        job._feature_reference_dates("2026-08-17,2026-08-17")


def test_job_scores_exact_inputs_and_publishes_ready_last():
    source = Path(job.__file__).read_text()

    assert source.count("load_accepted_candidate_inputs(") == 1
    assert 'route="v1"' in source
    assert 'route="v2"' not in source
    assert "build_label_free_scoring_set(" in source
    assert source.index("build_shopping_bag_candidate_frame(") < source.index(
        "build_label_free_scoring_set("
    )
    assert "account_limit=args.account_limit" in source
    assert '"input_account_count": input_account_count' in source
    assert (
        "account_entity_scores/v1"
        in (
            PROJECT_ROOT
            / "src"
            / "next_ads"
            / "model_development"
            / "ongoing_evaluation.py"
        ).read_text()
    )
    assert (
        "ModelPluginRegistry().score_provider("
        in (
            PROJECT_ROOT
            / "src"
            / "next_ads"
            / "model_development"
            / "ongoing_evaluation.py"
        ).read_text()
    )
    assert "configure_mlflow(mlflow)" in source
    assert (
        "validate_registered_model_build(mlflow.tracking.MlflowClient(), build)"
        in source
    )
    assert "models:/{build.registered_model_name}/" in source
    assert source.index("status=BUILDING") < source.index(
        "persist_evaluation_scores("
    )
    assert source.index("persist_evaluation_scores(") < source.index(
        "status=READY"
    )
    assert "changed the candidate row count" in source
    assert "status=FAILED" in source
    assert "mktg_next_uk_nextads.yml" not in source
    assert "assignment" not in source.lower()
    assert "payload" not in source.lower()


def test_history_tables_preserve_model_candidate_and_feature_provenance():
    build_sql = (
        PROJECT_ROOT
        / "sql"
        / "model_development"
        / "create_table_next_uk_nextads_model_evaluation_scoring_builds.sql"
    ).read_text()
    scores_sql = (
        PROJECT_ROOT
        / "sql"
        / "model_development"
        / "create_table_next_uk_nextads_model_evaluation_scores.sql"
    ).read_text()

    for field in (
        "registered_model_version",
        "artifact_digest",
        "candidate_bindings_json",
        "feature_bindings_json",
        "account_limit",
        "input_account_count",
        "output_delta_version",
        "status",
    ):
        assert field in build_sql
    for field in (
        "route",
        "scope_type",
        "scope_value",
        "candidate_build_attempt_id",
        "portfolio_attempt_id",
        "candidate_foundation_snapshot_id",
        "predicted_pctr",
        "evaluation_rank",
    ):
        assert field in scores_sql
    assert "PARTITIONED BY (run_date, route)" in scores_sql
