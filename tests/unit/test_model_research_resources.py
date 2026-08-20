from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOBS_ROOT = PROJECT_ROOT / "pipelines" / "databricks" / "jobs"


def _yaml(path: Path):
    return yaml.safe_load(path.read_text())


def test_generic_automl_job_is_manual_dev_only():
    automl = _yaml(
        JOBS_ROOT / "mktg_next_uk_nextads_model_research_automl.yml"
    )

    assert set(automl["targets"]) == {"DEV"}
    assert "schedule" not in next(
        value for key, value in automl.items() if key.endswith("_job")
    )

    automl_job = automl["model_research_automl_job"]
    defaults = {
        item["name"]: item["default"] for item in automl_job["parameters"]
    }
    assert defaults["enabled"] == "false"
    assert defaults["timeout_minutes"] == "30"
    assert automl_job["job_clusters"] == (
        "${var.model_research_automl_job_clusters_config}"
    )
    task = automl_job["tasks"][0]
    assert "libraries" not in task
    parameters = task["spark_python_task"]["parameters"]
    experiment_index = parameters.index("--experiment_path")
    assert parameters[experiment_index + 1] == (
        "${workspace.root_path}/{{job.parameters.model_name}}_automl"
    )


def test_model_research_dependencies_and_clusters_are_isolated():
    requirements = (
        (PROJECT_ROOT / "requirements-model-research.txt")
        .read_text()
        .splitlines()
    )
    assert requirements == [
        "databricks-feature-engineering==0.12.1",
        "dynaconf[yaml]==3.2.12",
        "matplotlib==3.11.1",
        "mlflow==3.11.1",
        "numpy==1.26.4",
        "xgboost==3.0.0",
    ]
    assert not any("databricks-connect" in line for line in requirements)

    libraries = _yaml(
        PROJECT_ROOT
        / "pipelines"
        / "databricks"
        / "variables"
        / "libraries.yml"
    )["variables"]
    research_libraries = libraries["model_research_libraries"]["default"]
    assert {"requirements": "../../../requirements-model-research.txt"} in (
        research_libraries
    )

    clusters = _yaml(
        PROJECT_ROOT
        / "pipelines"
        / "databricks"
        / "variables"
        / "clusters.yml"
    )["variables"]
    standard = clusters["model_research_job_clusters_config"]["default"][0][
        "new_cluster"
    ]
    automl = clusters["model_research_automl_job_clusters_config"]["default"][
        0
    ]["new_cluster"]
    assert standard["spark_version"] == "15.4.x-scala2.12"
    assert standard["num_workers"] == 4
    assert standard["node_type_id"] == "Standard_D32ads_v5"
    assert automl["spark_version"] == "15.4.x-cpu-ml-scala2.12"
    assert "kind" not in automl
    assert "use_ml_runtime" not in automl
    assert automl["num_workers"] == 4


def test_automl_resource_is_included_once_and_scoring_can_load_xgboost():
    bundle = (PROJECT_ROOT / "databricks.yml").read_text()
    assert bundle.count("mktg_next_uk_nextads_model_research_automl.yml") == 1

    model_config = _yaml(
        PROJECT_ROOT / "configs" / "models" / "nextads_models.yaml"
    )
    shopping_bag = next(
        model
        for model in model_config["models"]
        if model["model_name"] == "shopping_bag_pctr"
    )
    xgboost = next(
        candidate
        for candidate in shopping_bag["research"]["candidates"]
        if candidate["plugin"] == "spark_xgboost"
    )
    assert "objective" not in xgboost["parameters"]
    smoke = (
        PROJECT_ROOT
        / "jobs"
        / "model"
        / "research"
        / "smoke_model_research_runtime.py"
    ).read_text()
    assert 'objective="binary:logistic"' not in smoke


def test_complete_data_scientist_walkthrough_covers_public_options():
    walkthrough_path = PROJECT_ROOT / "docs" / "model_research_walkthrough.md"
    walkthrough = walkthrough_path.read_text(encoding="utf-8")

    resources = (
        (
            "mktg_next_uk_nextads_model_development.yml",
            "model_development_job",
        ),
        (
            "mktg_next_uk_nextads_model_research_automl.yml",
            "model_research_automl_job",
        ),
        (
            "mktg_next_uk_nextads_model_scoring.yml",
            "mktg_next_uk_nextads_model_scoring_config",
        ),
        ("mktg_next_uk_nextads.yml", "mktg_next_uk_nextads_config"),
        (
            "mktg_next_uk_nextads_feature_store.yml",
            "nextads_feature_store_job",
        ),
    )
    for resource_name, job_key in resources:
        job = _yaml(JOBS_ROOT / resource_name)[job_key]
        if "parameters" not in job:
            assert len(job) == 1
            job = next(iter(job.values()))
        for parameter in job["parameters"]:
            assert f"`{parameter['name']}`" in walkthrough

    for operation in ("BUILD", "RESEARCH", "REVIEW_SELECT", "EVALUATE"):
        assert f"`{operation}`" in walkthrough

    for research_field in (
        "temporal_split",
        "candidates",
        "evaluation_rules",
        "slices",
        "selection_policy",
        "minimum_successful_candidates",
        "explanation_requirements",
        "evaluation_schema_version",
        "evidence_producers",
        "candidate_search",
    ):
        assert f"`{research_field}`" in walkthrough

    for evaluation_rule in (
        "required_metrics",
        "required_evidence",
        "top_fractions",
        "confidence_interval_metrics",
        "confidence_level",
        "confidence_interval_resamples",
        "confidence_interval_seed",
        "minimum_slice_rows",
        "prevalence_baseline",
    ):
        assert f"`{evaluation_rule}`" in walkthrough

    for output_table in (
        "next_uk_nextads_model_research_claims",
        "next_uk_nextads_model_research_frames",
        "next_uk_nextads_model_research_builds",
        "next_uk_nextads_candidate_evaluations",
        "next_uk_nextads_model_selection_decisions",
        "next_uk_nextads_automl_discovery_receipts",
        "next_uk_nextads_model_evaluation_scoring_builds",
        "next_uk_nextads_model_evaluation_scores",
    ):
        assert output_table in walkthrough

    for linked_document in (
        PROJECT_ROOT / "docs" / "model_lifecycle_runbook.md",
        PROJECT_ROOT / "docs" / "feature_store" / "README.md",
        PROJECT_ROOT / "docs" / "developer_workflow_guide.md",
        PROJECT_ROOT / "docs" / "architecture" / "README.md",
        PROJECT_ROOT / "docs" / "architecture" / "nextads_job_table_flow.md",
    ):
        assert "model_research_walkthrough.md" in linked_document.read_text(
            encoding="utf-8"
        )
