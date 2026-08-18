from pathlib import Path

import pytest
import yaml

from jobs.model.development import run_shopping_bag_ongoing_evaluation as job


PROJECT_ROOT = Path(job.__file__).resolve().parents[3]
BUNDLE_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "databricks"
    / "jobs"
    / ("mktg_next_uk_nextads_shopping_bag_ongoing_evaluation.yml")
)


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

    assert source.count("load_accepted_candidate_inputs(") == 2
    assert 'route="v1"' in source
    assert 'route="v2"' in source
    assert "build_label_free_scoring_set(" in source
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


def test_bundle_is_manual_dev_only_and_separate_from_customer_jobs():
    config = yaml.safe_load(BUNDLE_PATH.read_text())
    resource = config["shopping_bag_ongoing_evaluation_job"]
    task = resource["tasks"][0]
    text = BUNDLE_PATH.read_text()

    assert set(config["targets"]) == {"DEV"}
    assert resource["tags"]["activation_mode"] == "EVALUATE"
    assert resource["tags"]["model"] == "shopping_bag_pctr"
    assert task["spark_python_task"]["python_file"].endswith(
        "run_shopping_bag_ongoing_evaluation.py"
    )
    assert task["job_cluster_key"] == "next_ads_job_cluster_D32ads_v5_1_4"
    assert resource["parameters"][0] == {
        "name": "model_build_id",
        "default": "REQUIRED",
    }
    assert resource["parameters"][1] == {
        "name": "run_date",
        "default": "{{job.start_time.iso_date}}",
    }
    assert "schedule:" not in text
    assert "PROD:" not in text
    assert "assign" not in text.lower()
    assert "payload" not in text.lower()
    assert (
        str(BUNDLE_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/")
        in (PROJECT_ROOT / "databricks.yml").read_text()
    )


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
