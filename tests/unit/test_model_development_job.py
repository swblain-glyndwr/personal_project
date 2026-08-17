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
    assert "ModelPluginRegistry" in source
    assert "MODEL_DEVELOPMENT_EVIDENCE=" in source
    assert "mktg_next_uk_nextads.yml" not in source
