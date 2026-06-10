from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _theme_affinity_job():
    job_config = yaml.safe_load(
        (
            PROJECT_ROOT
            / "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml"
        ).read_text()
    )
    return job_config["resources"]["jobs"][
        "mktg_next_uk_nextads_theme_affinity_cicd"
    ]


def test_theme_affinity_job_is_included_in_bundle():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())

    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml"
        in bundle_config["include"]
    )


def test_theme_affinity_job_uses_target_name_with_legacy_notebook_paths():
    job = _theme_affinity_job()

    assert job["name"] == "mktg_next_uk_nextads_theme_affinity"
    assert job["tags"]["domain"] == "theme_affinity"
    assert job["tags"]["legacy_name"] == "hackathon"

    notebook_paths = [
        task["notebook_task"]["notebook_path"] for task in job["tasks"]
    ]

    assert notebook_paths == [
        "${workspace.file_path}/hackathon_model/run_pipeline_predict",
        "${workspace.file_path}/hackathon_model/run_pipeline_predict",
        "${workspace.file_path}/hackathon_model/run_pipeline_predict",
        "${workspace.file_path}/hackathon_model/simple_rules_rank",
        "${workspace.file_path}/hackathon_model/predict_model",
        "${workspace.file_path}/hackathon_model/clean_output",
    ]


def test_theme_affinity_job_preserves_live_legacy_output_parameters():
    job = _theme_affinity_job()
    prep_tasks = [
        task for task in job["tasks"] if task["task_key"].startswith("prep_")
    ]

    assert [task["task_key"] for task in prep_tasks] == [
        "prep_base_data_0_3",
        "prep_base_data_4",
        "prep_base_data_5",
    ]

    for task in prep_tasks:
        parameters = task["notebook_task"]["base_parameters"]
        assert parameters["catalog"] == "ds_sandbox"
        assert parameters["table_prefix"] == "next_uk_nextAds_predict_prod"
        assert parameters["reference_date"] == "predict"
