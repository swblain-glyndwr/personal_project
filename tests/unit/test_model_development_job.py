import pytest

from jobs.model.development import run_declared_model as job


def test_model_job_accepts_one_or_more_exact_feature_dates():
    assert job._reference_dates("2026-07-01, 2026-08-01") == (
        "2026-07-01",
        "2026-08-01",
    )

    with pytest.raises(ValueError):
        job._reference_dates("")
    with pytest.raises(ValueError):
        job._reference_dates("latest")


def test_model_job_uses_receipts_plugins_and_exact_promotion():
    source = job.Path(job.__file__).read_text()

    assert "build_training_set_from_feature_store" in source
    assert "persist_training_set_receipt" in source
    assert "train_or_reuse_model" in source
    assert "promote_exact_model_build" in source
    assert "provider_id=definition.provider_id" in source
    assert "ModelPluginRegistry" in source
    assert "MODEL_DEVELOPMENT_EVIDENCE=" in source
    assert "mktg_next_uk_nextads.yml" not in source


def test_model_job_is_manual_dev_evidence_only():
    project_root = job.Path(job.__file__).resolve().parents[3]
    bundle_job = (
        project_root
        / "pipelines"
        / "databricks"
        / "jobs"
        / "mktg_next_uk_nextads_model_development.yml"
    ).read_text()

    assert "activation_mode: EVALUATE" in bundle_job
    assert "provider_builds_table" in bundle_job
    assert "{{task.run_id}}" in bundle_job
    assert "feature_reference_dates\n      default: REQUIRED" in bundle_job
    assert "label_end\n      default: REQUIRED" in bundle_job
    assert "schedule:" not in bundle_job
    assert "mktg_next_uk_nextads.yml" not in bundle_job
    assert "PROD:" not in bundle_job
