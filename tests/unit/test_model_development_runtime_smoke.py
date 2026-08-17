import pytest

from jobs.model.development import smoke_model_development_runtime as smoke


def test_model_runtime_requires_exact_dbr_and_pinned_libraries():
    smoke.validate_runtime_versions("15.4.x-scala2.12", smoke.EXPECTED_PACKAGES)

    with pytest.raises(ValueError, match="DBR 15.4"):
        smoke.validate_runtime_versions("16.4.x-scala2.12", smoke.EXPECTED_PACKAGES)
    with pytest.raises(ValueError, match="package versions"):
        smoke.validate_runtime_versions(
            "15.4.x-scala2.12",
            {**smoke.EXPECTED_PACKAGES, "mlflow": "3.10.0"},
        )


def test_model_runtime_smoke_exercises_future_lookup_rejection():
    message = smoke.prove_future_binding_rejection()

    assert "is after observation end" in message


def test_model_runtime_smoke_is_manual_dev_only():
    project_root = smoke.Path(smoke.__file__).resolve().parents[3]
    resource = (
        project_root
        / "pipelines"
        / "databricks"
        / "jobs"
        / "mktg_next_uk_nextads_model_development_runtime_smoke.yml"
    ).read_text()

    assert "15.4" in resource.splitlines()[0]
    assert "writes_performed" not in resource
    assert "schedule:" not in resource
    assert "PROD:" not in resource
