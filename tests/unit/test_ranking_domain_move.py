import importlib
import importlib.util
from pathlib import Path

from next_ads.ranking import scoring
from next_ads import Scoring
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    return load_job(path, key)


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
        PROJECT_ROOT / "jobs/nextads_candidates/build_theme_ad_candidates.py"
    ).read_text()
    package_module = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_mapping.py"
    ).read_text()
    retrieval_module = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_retrieval.py"
    ).read_text()
    eligibility_module = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_eligibility.py"
    ).read_text()
    ranking_module = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_ranking.py"
    ).read_text()

    assert (
        "from next_ads.ranking.theme_score_mapping "
        "import run_theme_score_mapping"
    ) in entrypoint
    assert "run_theme_score_mapping(" in entrypoint
    assert "def run_theme_score_mapping(" in package_module
    assert "truncate_and_load(" in package_module
    assert "delete_from_and_load(" in package_module
    assert "def build_ad_location_mappings(" in retrieval_module
    assert "def apply_greedy_theme_assignment(" in eligibility_module
    assert "def rank_top_ads_per_adset(" in ranking_module


def test_theme_affinity_job_uses_model_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert tasks["model_predict"]["spark_python_task"]["python_file"] == (
        "../../../jobs/model/theme_affinity/model_predict.py"
    )
    assert tasks["clean_output"]["spark_python_task"]["python_file"] == (
        "../../../jobs/model/theme_affinity/clean_output.py"
    )
    assert tasks["sense_check_dlt_data"]["spark_python_task"]["python_file"] == (
        "../../../jobs/model/theme_affinity/sense_check.py"
    )
    assert tasks["sense_check_model_outputs"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/model/theme_affinity/sense_check.py"


def test_theme_affinity_scripts_live_under_model_jobs():
    try:
        legacy_theme_affinity_spec = importlib.util.find_spec("scripts.theme_affinity")
    except ModuleNotFoundError:
        legacy_theme_affinity_spec = None

    assert legacy_theme_affinity_spec is None

    for module_name in [
        "model_predict",
        "clean_output",
        "sense_check",
    ]:
        assert (
            PROJECT_ROOT / "jobs" / "model" / "theme_affinity" / f"{module_name}.py"
        ).is_file()


def test_v2_entrypoints_stay_on_scripts():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert tasks["load_control_sheet_v2"]["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_control/load_control_sheet_v2.py"
    )
    assert tasks["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_candidates/build_page_type_candidates_v2.py"
