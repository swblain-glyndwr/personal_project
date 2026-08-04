import importlib
import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from next_ads.ranking import scoring
from next_ads.ranking.theme_coverage import build_missing_theme_affinity_coverage
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-ranking-domain-move-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


def _load_job(path, key):
    return load_job(path, key)


def test_scoring_package_exports_expected_functions():
    assert callable(scoring.append_targeting_criteria)
    assert callable(scoring.get_model_scores)
    assert callable(scoring.aggregate_model_scores)


def test_package_code_uses_moved_scoring_import():
    source = (PROJECT_ROOT / "src/next_ads/control/load_control_sheet.py").read_text()

    assert "from next_ads.ranking.scoring import append_targeting_criteria" in source


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
    assert "capture_run_date(" not in package_module
    assert "date.fromisoformat(run_date)" in package_module
    assert "publish_history_and_latest(" in package_module
    assert "replace_validated_snapshot(" in package_module
    assert "with_run_date(" in package_module
    assert "truncate_and_load(" not in package_module
    assert "delete_from_and_load(" not in package_module
    assert "control_sheet_latest_table" in package_module
    assert "output_preranked_table" in package_module
    assert "output_grain" in package_module
    assert "top_ads_per_group" in package_module
    assert "load_provider_theme_scores(" in package_module
    assert "theme_affinity_assignment_sources" not in package_module
    assert "def build_ad_location_mappings(" in retrieval_module
    assert "def build_ad_group_mappings(" in retrieval_module
    assert "def apply_greedy_theme_assignment(" in eligibility_module
    assert "def assert_eligible_groups(" in eligibility_module
    assert "def rank_top_ads_per_adset(" in ranking_module
    assert "def map_ranked_ads_to_groups(" in ranking_module


def test_v2_theme_score_mapping_uses_v2_control_sheet_directly():
    v2_entrypoint = (
        PROJECT_ROOT / "jobs/nextads_candidates/build_page_type_candidates_v2.py"
    ).read_text()

    assert "run_theme_score_mapping(" in v2_entrypoint
    assert (
        "control_sheet_latest_table=config.tables_write.control_sheet_latest_v2"
        in v2_entrypoint
    )
    assert "output_grain=\"page_type\"" in v2_entrypoint
    assert "next_theme_scores_latest_v2" not in v2_entrypoint
    assert "theme_scores_table" not in v2_entrypoint
    assert "write_score_components=False" in v2_entrypoint
    assert "preranked_ads_from_themes_latest" not in v2_entrypoint


def test_theme_affinity_coverage_finds_ad_themes_missing_from_model(local_spark):
    spark = local_spark
    control_ads = spark.createDataFrame(
        [
            ("ad1", "Summer", "0"),
            ("ad2", "denim", "0"),
            ("ad3", "ignored", "1"),
        ],
        ["UniqueAdID", "Themes", "AudienceOnly"],
    )
    theme_affinity_scores = spark.createDataFrame(
        [("acc1", "summer")],
        ["AccountNumber", "NextTheme"],
    )

    missing = build_missing_theme_affinity_coverage(
        control_ads,
        theme_affinity_scores,
        route="v2",
    )

    assert [(row.route, row.Theme, row.ad_count) for row in missing.collect()] == [
        ("v2", "denim", 1)
    ]


def test_theme_affinity_job_uses_model_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert tasks["model_predict"]["spark_python_task"]["python_file"] == (
        "../../../jobs/model/theme_affinity/model_predict.py"
    )
    assert "clean_output" not in tasks
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
        "sense_check",
    ]:
        assert (
            PROJECT_ROOT / "jobs" / "model" / "theme_affinity" / f"{module_name}.py"
        ).is_file()


def test_v2_entrypoints_use_jobs_folder():
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
