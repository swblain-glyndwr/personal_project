import pytest
import yaml

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


def test_model_job_publishes_canonical_but_adapts_scoped_scores():
    class Definition:
        evaluation_scope = (("location", ("SB1", "SB2")),)

    class Provider:
        def score(self, *_args, **_kwargs):
            pytest.fail("Scoped model must not discard location before adaptation")

        def score_with_evaluation_scope(self, *args, **kwargs):
            assert args == (Definition, "build", "training")
            assert kwargs == {"scope_columns": ("location",)}
            return "canonical", "scoped"

    canonical, scoped, columns = job._score_outputs(
        Definition,
        "build",
        "training",
        Provider(),
    )

    assert canonical == "canonical"
    assert scoped == "scoped"
    assert columns == ("location",)


def test_model_job_uses_receipts_plugins_and_exact_promotion():
    source = job.Path(job.__file__).read_text()

    assert "build_training_set_from_feature_store" in source
    assert "persist_training_set_receipt" in source
    assert "train_or_reuse_model" in source
    assert "promote_exact_model_build" in source
    assert "persist_evaluation_candidates" in source
    assert "provider_id=definition.provider_id" in source
    assert "use_case=definition.evaluation_use_case" in source
    assert "scope_filters=definition.evaluation_scope" in source
    assert "score_with_evaluation_scope" in source
    assert "adapter.apply(evaluation_scores, eligible)" in source
    assert "temporal_train_validation_split" in source
    assert "evaluation_frame.select" in source
    assert '"evaluation_mode": "HISTORICAL_TEMPORAL_HOLDOUT"' in source
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
    assert "observation_reference_dates\n      default: REQUIRED" in bundle_job
    assert "feature_reference_dates\n      default: REQUIRED" in bundle_job
    assert "label_end\n      default: REQUIRED" in bundle_job
    assert "default: SOURCE_ALIAS_REHEARSAL" in bundle_job
    assert "${workspace.root_path}/experiments/shopping_bag_pctr" in bundle_job
    assert "model_schema" in bundle_job
    assert "schedule:" not in bundle_job
    assert "mktg_next_uk_nextads.yml" not in bundle_job
    assert "PROD:" not in bundle_job


def test_generic_preprod_import_is_release_scoped_and_allowlisted():
    project_root = job.Path(job.__file__).resolve().parents[3]
    resource = project_root / "pipelines" / "databricks" / "jobs" / (
        "mktg_next_uk_nextads_model_import_preprod.yml"
    )
    config = yaml.safe_load(resource.read_text())
    promotion_job = config["model_import_preprod_config"][
        "mktg_next_uk_nextads_model_import_preprod"
    ]
    parameters = promotion_job["tasks"][0]["spark_python_task"]["parameters"]

    assert set(config["targets"]) == {"PREPROD"}
    assert "../../../jobs/model/development/promote_exact_model.py" == (
        promotion_job["tasks"][0]["spark_python_task"]["python_file"]
    )
    assert "marketingdata_dev.nextads_integration." in parameters
    assert "marketingdata_prod.ds_sandbox." in parameters
    assert "theme_affinity" not in resource.read_text()
    assert (
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_model_import_preprod.yml"
    ) in (project_root / "databricks.yml").read_text()
