import ast
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from dsutils.etl import build_spark_schema
import next_ads.decisioning.assignment as assignment
from next_ads.decisioning.assignment import (
    assign_preranked_ads_v2,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
V2_BUILD_PAGE_PATH = PROJECT_ROOT / "jobs/nextads_v2/build_page.py"
V2_ASSIGNMENT_COLUMNS = (
    "AccountNumber",
    "PageType",
    "Rank",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "TriggerScore",
)


def _v2_builder_source_and_tree():
    source = V2_BUILD_PAGE_PATH.read_text()
    return source, ast.parse(source, filename=str(V2_BUILD_PAGE_PATH))


def _calls_named(tree, function_name):
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == function_name
    ]


def _attribute_calls(tree, attribute_name):
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attribute_name
    ]


def _call_keywords(call):
    return {keyword.arg: keyword.value for keyword in call.keywords}


@pytest.fixture
def local_spark(monkeypatch):
    try:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("next-ads-assignment-v2-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    monkeypatch.setattr(assignment, "get_spark", lambda: spark)
    yield spark


def test_assign_preranked_ads_v2_returns_trigger_score(local_spark):
    spark = local_spark

    preranked_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["PageType", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    preranked = spark.createDataFrame(
        [
            ["acc1", "ad1", "ShoppingBag", 1, 0.8],
            ["acc1", "ad2", "ShoppingBag", 2, 0.4],
            ["acc1", "ad3", "HomePage", 1, 0.9],
            ["acc2", "ad1", "ShoppingBag", 1, 0.7],
        ],
        preranked_schema,
    )
    preranked.createOrReplaceTempView("preranked_ads_v2_test")

    ads_schema = build_spark_schema([
        ["UniqueAdID", "string", "not null"],
    ])
    df_ads = spark.createDataFrame([["ad1"], ["ad2"]], ads_schema)

    cust_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
    ])
    df_cust = spark.createDataFrame([["acc1"]], cust_schema)

    result = assign_preranked_ads_v2(
        df_ads=df_ads,
        preranked_ads_table="preranked_ads_v2_test",
        page_type="ShoppingBag",
        df_cust=df_cust,
        n_ads=2,
    )

    expected_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    expected = spark.createDataFrame(
        [
            ["acc1", "ad1", 1, 0.8],
            ["acc1", "ad2", 2, 0.4],
        ],
        expected_schema,
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_assign_preranked_ads_v2_filters_by_page_type(local_spark):
    spark = local_spark

    preranked_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["PageType", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    preranked = spark.createDataFrame(
        [
            ["acc1", "ad1", "ShoppingBag", 1, 0.8],
            ["acc1", "ad2", "HomePage", 1, 0.4],
        ],
        preranked_schema,
    )
    preranked.createOrReplaceTempView("preranked_ads_v2_override_test")

    ads_schema = build_spark_schema([
        ["UniqueAdID", "string", "not null"],
    ])
    df_ads = spark.createDataFrame([["ad1"], ["ad2"]], ads_schema)

    result = assign_preranked_ads_v2(
        df_ads=df_ads,
        preranked_ads_table="preranked_ads_v2_override_test",
        page_type="ShoppingBag",
        n_ads=1,
    )

    expected_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    expected = spark.createDataFrame(
        [["acc1", "ad1", 1, 0.8]],
        expected_schema,
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_v2_builder_requires_complete_build_identity_arguments():
    _, tree = _v2_builder_source_and_tree()

    required_calls = _calls_named(tree, "_get_required_job_arg")
    required_names = {
        call.args[1].value
        for call in required_calls
        if len(call.args) > 1 and isinstance(call.args[1], ast.Constant)
    }
    assert {
        "--scope_manifest_json",
        "--run_date",
        "--build_run_id",
    }.issubset(required_names)

    integer_calls = _calls_named(tree, "_get_integer_job_arg")
    integer_arguments = {
        call.args[1].value: _call_keywords(call)["minimum"].value
        for call in integer_calls
        if len(call.args) > 1 and isinstance(call.args[1], ast.Constant)
    }
    assert integer_arguments == {
        "--task_run_id": 1,
        "--execution_count": 0,
    }

    assert len(_attribute_calls(tree, "fromisoformat")) == 1
    manifest_parse_calls = _calls_named(tree, "parse_scope_manifest_json")
    assert len(manifest_parse_calls) == 1
    required_manifest_call = manifest_parse_calls[0].args[0]
    assert isinstance(required_manifest_call, ast.Call)
    assert isinstance(required_manifest_call.func, ast.Name)
    assert required_manifest_call.func.id == "_get_required_job_arg"
    assert ast.unparse(required_manifest_call.args[0]) == "jobparser"
    assert required_manifest_call.args[1].value == "--scope_manifest_json"
    source = V2_BUILD_PAGE_PATH.read_text()
    assert 'BUILD_RUN_ID.startswith("v2_")' in source


def test_v2_builder_rejects_scope_manifest_config_drift_before_spark_work():
    source, tree = _v2_builder_source_and_tree()

    mismatch_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test)
        == "MANIFEST_SCOPES != CONFIGURED_PAGE_TYPES"
    ]
    current_scope_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) == "PAGE_TYPE not in MANIFEST_SCOPES"
    ]
    assert len(mismatch_checks) == 1
    assert len(current_scope_checks) == 1

    first_cache_line = min(
        call.lineno for call in _attribute_calls(tree, "cache")
    )
    stage_line = _calls_named(tree, "stage_assignment_scope")[0].lineno
    assert mismatch_checks[0].lineno < first_cache_line < stage_line
    assert current_scope_checks[0].lineno < first_cache_line
    assert "CONFIGURED_PAGE_TYPES = tuple(PAGE_TYPES)" in source


def test_v2_builder_stages_one_exact_page_scope_and_no_public_tables():
    source, tree = _v2_builder_source_and_tree()

    resolve_calls = _calls_named(tree, "resolve_assignment_tables")
    assert len(resolve_calls) == 1
    assert ast.unparse(resolve_calls[0].args[0]) == "config"
    assert resolve_calls[0].args[1].value == "v2"

    contract_calls = _calls_named(tree, "build_assignment_scope_contract")
    assert len(contract_calls) == 1
    assert ast.unparse(contract_calls[0].args[0]) == "'v2'"
    assert ast.unparse(contract_calls[0].args[1]) == "PAGE_SCOPE_MANIFEST"

    stage_calls = _calls_named(tree, "stage_assignment_scope")
    assert len(stage_calls) == 1
    stage_call = stage_calls[0]
    assert [ast.unparse(arg) for arg in stage_call.args] == [
        "spark",
        "df_ad_assigned",
    ]
    stage_keywords = {
        name: ast.unparse(value)
        for name, value in _call_keywords(stage_call).items()
    }
    assert stage_keywords == {
        "tables": "ASSIGNMENT_TABLES",
        "columns": "ASSIGNMENT_COLUMNS",
        "scope_contract": "ASSIGNMENT_SCOPE_CONTRACT",
        "build_run_id": "BUILD_RUN_ID",
        "build_date": "RUN_DATE",
        "scope": "PAGE_TYPE",
        "task_run_id": "TASK_RUN_ID",
        "execution_count": "EXECUTION_COUNT",
    }

    selected_column_sets = {
        tuple(arg.value for arg in call.args)
        for call in _attribute_calls(tree, "select")
        if call.args
        and all(isinstance(arg, ast.Constant) for arg in call.args)
    }
    assert V2_ASSIGNMENT_COLUMNS in selected_column_sets

    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert called_names.isdisjoint(
        {
            "delete_from_and_load",
            "truncate_and_load",
            "publish_history_and_latest",
        }
    )
    assert ".write" not in source


def test_v2_builder_retains_cached_lineage_until_staging_finishes():
    _, tree = _v2_builder_source_and_tree()

    stage_call = _calls_named(tree, "stage_assignment_scope")[0]
    counted_final_assignments = [
        call
        for call in _attribute_calls(tree, "count")
        if isinstance(call.func.value, ast.Name)
        and call.func.value.id == "df_ad_assigned"
    ]
    cache_calls = _attribute_calls(tree, "cache")
    unpersist_calls = _attribute_calls(tree, "unpersist")

    assert counted_final_assignments == []
    assert cache_calls
    assert all(call.lineno < stage_call.lineno for call in cache_calls)
    assert unpersist_calls
    assert all(call.lineno > stage_call.lineno for call in unpersist_calls)

    parent_by_node = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    current = stage_call
    while current in parent_by_node and not isinstance(current, ast.Try):
        current = parent_by_node[current]
    assert isinstance(current, ast.Try)
    assert isinstance(parent_by_node[current], ast.Module)
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "unpersist"
        for statement in current.finalbody
        for node in ast.walk(statement)
    )
