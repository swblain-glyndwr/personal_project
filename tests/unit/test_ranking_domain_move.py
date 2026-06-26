import importlib
from pathlib import Path

import yaml

from next_ads.ranking import scoring
from next_ads import Scoring


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    job_config = yaml.safe_load((PROJECT_ROOT / path).read_text())
    return job_config["resources"]["jobs"][key]


def test_scoring_legacy_wrapper_exports_moved_functions():
    assert Scoring.append_targeting_criteria is scoring.append_targeting_criteria
    assert Scoring.get_model_scores is scoring.get_model_scores
    assert Scoring.aggregate_model_scores is scoring.aggregate_model_scores


def test_package_code_uses_moved_scoring_import():
    source = (PROJECT_ROOT / "src/next_ads/control/load_control_sheet.py").read_text()

    assert "from next_ads.ranking.scoring import append_targeting_criteria" in source
    assert "from next_ads.Scoring import append_targeting_criteria" not in source


def test_theme_score_mapping_entrypoint_delegates_to_ranking_package():
    entrypoint = (
        PROJECT_ROOT / "jobs/nextads_main/map_theme_scores_to_ads.py"
    ).read_text()
    package_module = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_mapping.py"
    ).read_text()

    assert (
        "from next_ads.ranking.theme_score_mapping "
        "import run_theme_score_mapping"
    ) in entrypoint
    assert "run_theme_score_mapping(" in entrypoint
    assert "def run_theme_score_mapping(" in package_module
    assert "truncate_and_load(" in package_module
    assert "delete_from_and_load(" in package_module


def test_theme_affinity_job_uses_model_entrypoints():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert tasks["model_predict"]["spark_python_task"]["python_file"] == (
        "../../jobs/model/theme_affinity/model_predict.py"
    )
    assert tasks["clean_output"]["spark_python_task"]["python_file"] == (
        "../../jobs/model/theme_affinity/clean_output.py"
    )
    assert tasks["sense_check_dlt_data"]["spark_python_task"]["python_file"] == (
        "../../jobs/model/theme_affinity/sense_check.py"
    )
    assert tasks["sense_check_model_outputs"]["spark_python_task"][
        "python_file"
    ] == "../../jobs/model/theme_affinity/sense_check.py"


def test_theme_affinity_legacy_wrappers_are_importable():
    for module_name in [
        "scripts.theme_affinity.model_predict",
        "scripts.theme_affinity.clean_output",
        "scripts.theme_affinity.sense_check",
    ]:
        module = importlib.import_module(module_name)
        assert hasattr(module, "main")


def test_v2_entrypoints_stay_on_scripts():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert tasks["load_control_sheet_v2"]["spark_python_task"]["python_file"] == (
        "../../scripts/load_control_sheet_v2.py"
    )
    assert tasks["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../scripts/map_theme_scores_to_ads_v2.py"
