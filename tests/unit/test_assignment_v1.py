import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
V1_BUILD_PAGE_PATH = (
    PROJECT_ROOT / "jobs/nextads_assignment/build_page.py"
)
V1_ASSIGNMENT_COLUMNS = (
    "AccountNumber",
    "Location",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "MASID",
)


def _v1_builder_source_and_tree():
    source = V1_BUILD_PAGE_PATH.read_text()
    return source, ast.parse(source, filename=str(V1_BUILD_PAGE_PATH))


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


def test_v1_builder_requires_complete_build_identity_arguments():
    source, tree = _v1_builder_source_and_tree()

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
    assert ast.unparse(required_manifest_call) == "RAW_SCOPE_MANIFEST"
    raw_manifest_calls = [
        call
        for call in required_calls
        if len(call.args) > 1
        and isinstance(call.args[1], ast.Constant)
        and call.args[1].value == "--scope_manifest_json"
    ]
    assert len(raw_manifest_calls) == 1
    assert 'BUILD_RUN_ID.startswith("v1_")' in source


def test_v1_builder_validates_manifest_and_current_scope_before_spark_work():
    _, tree = _v1_builder_source_and_tree()

    current_scope_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) == "LOCATION not in MANIFEST_SCOPES"
    ]
    inheritance_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test)
        == "INHERIT_BASIC_FROM != CURRENT_SCOPE_ENTRY.inherit_basic_from"
    ]
    config_inheritance_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and "CURRENT_SCOPE_ENTRY.inherit_basic_from !="
        in ast.unparse(node.test)
        and "CONFIGURED_INHERIT_BASIC_FROM" in ast.unparse(node.test)
    ]
    split_manifest_calls = _calls_named(
        tree,
        "split_assignment_scope_manifest",
    )
    configured_manifest_calls = _calls_named(
        tree,
        "validate_configured_v1_scope_manifest",
    )

    assert len(current_scope_checks) == 1
    assert len(inheritance_checks) == 1
    assert len(config_inheritance_checks) == 1
    assert len(split_manifest_calls) == 1
    assert ast.unparse(split_manifest_calls[0].args[0]) == (
        "RAW_SCOPE_MANIFEST"
    )
    assert len(configured_manifest_calls) == 1
    assert [
        ast.unparse(arg) for arg in configured_manifest_calls[0].args
    ] == ["SCOPE_MANIFEST", "config.locations"]
    first_table_line = min(
        call.lineno for call in _attribute_calls(tree, "table")
    )
    assert split_manifest_calls[0].lineno < first_table_line
    assert current_scope_checks[0].lineno < first_table_line
    assert inheritance_checks[0].lineno < first_table_line
    assert config_inheritance_checks[0].lineno < first_table_line
    assert configured_manifest_calls[0].lineno < first_table_line


def test_v1_builder_uses_logical_run_date_for_incrementality():
    source, _ = _v1_builder_source_and_tree()

    assert (
        "CHECK_SESSIONS_FROM = RUN_DATE - timedelta("
        in source
    )
    assert "datetime.date.today" not in source
    assert "date.today" not in source


def test_v1_builder_stages_one_exact_location_and_never_writes_public_tables():
    source, tree = _v1_builder_source_and_tree()

    resolve_calls = _calls_named(tree, "resolve_assignment_tables")
    assert len(resolve_calls) == 1
    assert ast.unparse(resolve_calls[0].args[0]) == "config"
    assert resolve_calls[0].args[1].value == "v1"

    contract_calls = _calls_named(tree, "build_assignment_scope_contract")
    assert len(contract_calls) == 1
    assert ast.unparse(contract_calls[0].args[0]) == "'v1'"
    assert ast.unparse(contract_calls[0].args[1]) == (
        "LOCATION_SCOPE_MANIFEST"
    )

    stage_calls = _calls_named(tree, "stage_assignment_scope")
    assert len(stage_calls) == 1
    stage_call = stage_calls[0]
    assert [ast.unparse(arg) for arg in stage_call.args] == [
        "spark",
        "df_ad_assigned_masid_output",
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
        "scope": "LOCATION",
        "task_run_id": "TASK_RUN_ID",
        "execution_count": "EXECUTION_COUNT",
    }

    selected_column_sets = {
        tuple(arg.value for arg in call.args)
        for call in _attribute_calls(tree, "select")
        if call.args
        and all(isinstance(arg, ast.Constant) for arg in call.args)
    }
    assert V1_ASSIGNMENT_COLUMNS in selected_column_sets

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
    assert "ASSIGNMENTS_TABLE_LATEST" not in source
    assigned_names = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert "ASSIGNMENTS_TABLE" not in assigned_names
    assert ".write" not in source
    assert _attribute_calls(tree, "sql") == []


def test_v1_no_ads_builds_an_empty_scope_for_the_staging_contract():
    source, tree = _v1_builder_source_and_tree()

    no_ads_branches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) == "NO_ASSIGNABLE_ADS"
    ]
    assert len(no_ads_branches) == 1
    branch = no_ads_branches[0]
    branch_calls = [
        node
        for statement in branch.body
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
    ]
    create_calls = [
        call
        for call in branch_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "createDataFrame"
    ]
    assert len(create_calls) == 1
    assert ast.unparse(create_calls[0].args[0]) == "[]"
    assert (
        ast.unparse(_call_keywords(create_calls[0])["schema"])
        == "spark.table(ASSIGNMENT_TABLES.staging_table)"
        ".select(*ASSIGNMENT_INPUT_COLUMNS).schema"
    )
    assert not any(
        isinstance(call.func, ast.Attribute)
        and call.func.attr in {"sql", "saveAsTable", "insertInto"}
        for call in branch_calls
    )
    assert _calls_named(tree, "stage_assignment_scope")[0].lineno > branch.lineno


def test_v1_secondary_inherits_only_from_same_build_staging():
    source, tree = _v1_builder_source_and_tree()

    staging_table_reads = [
        call
        for call in _attribute_calls(tree, "table")
        if ast.unparse(call.args[0]) == "ASSIGNMENT_TABLES.staging_table"
    ]
    assert len(staging_table_reads) == 2
    assert (
        "F.col(ASSIGNMENT_COLUMNS.build_run_id) == BUILD_RUN_ID"
        in source
    )
    assert 'F.col("Location") == INHERIT_BASIC_FROM' in source
    assert 'F.col("rundate") == F.lit(RUN_DATE)' in source
    assert (
        'F.col("UniqueAdIDBasic").alias("ExcludedAdID")'
        in source
    )
    assert "df_inherited_assignments = (" in source
    assert "df_inherited_scope\n" in source
    assert 'tbls["assignments_latest"]' not in source
    assert "ASSIGNMENTS_TABLE_LATEST" not in source


def test_v1_empty_primary_scope_cascades_without_fresh_allocation():
    _, tree = _v1_builder_source_and_tree()

    cascade_checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) == "CASCADE_INHERITED_NO_ADS"
        and any(
            isinstance(child, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "NO_ASSIGNABLE_ADS"
                for target in child.targets
            )
            for child in node.body
        )
    ]
    assert len(cascade_checks) == 1
    cascade_assignments = {
        target.id: ast.unparse(statement.value)
        for statement in cascade_checks[0].body
        if isinstance(statement, ast.Assign)
        for target in statement.targets
        if isinstance(target, ast.Name)
    }
    assert cascade_assignments == {
        "HAS_TARGETED_ADS": "False",
        "NO_ASSIGNABLE_ADS": "True",
    }

    inherited_empty_calls = [
        call
        for call in _attribute_calls(tree, "isEmpty")
        if ast.unparse(call.func.value) == "df_inherited_scope"
    ]
    assert len(inherited_empty_calls) == 1

    no_ads_branches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) == "NO_ASSIGNABLE_ADS"
    ]
    assert len(no_ads_branches) == 1
    no_ads_branch = no_ads_branches[0]
    allocation_names = {
        "assign_random_ads",
        "assign_random_ads_with_exclusions",
        "assign_preranked_ads",
        "assign_nextgenads",
    }
    allocations_in_no_ads_path = {
        node.func.id
        for statement in no_ads_branch.body
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in allocation_names
    }
    allocations_in_build_path = {
        node.func.id
        for statement in no_ads_branch.orelse
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in allocation_names
    }
    all_allocations = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in allocation_names
    }
    assert allocations_in_no_ads_path == set()
    assert allocations_in_build_path == all_allocations
    assert inherited_empty_calls[0].lineno < no_ads_branch.lineno


def test_v1_builder_releases_caches_after_staging_or_task_failure():
    _, tree = _v1_builder_source_and_tree()

    stage_call = _calls_named(tree, "stage_assignment_scope")[0]
    cache_calls = _attribute_calls(tree, "cache")
    unpersist_calls = _attribute_calls(tree, "unpersist")
    counted_final_assignments = [
        call
        for call in _attribute_calls(tree, "count")
        if isinstance(call.func.value, ast.Name)
        and call.func.value.id == "df_ad_assigned_masid_output"
    ]

    assert counted_final_assignments == []
    assert len(cache_calls) == 1
    assert len(unpersist_calls) == 1

    function_defs = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }
    cache_helper = function_defs["_cache_assignment_frame"]
    release_helper = function_defs["_release_cached_assignment_frames"]
    assert cache_calls[0] in set(ast.walk(cache_helper))
    assert unpersist_calls[0] in set(ast.walk(release_helper))

    cache_helper_calls = _calls_named(tree, "_cache_assignment_frame")
    assert cache_helper_calls
    assert all(call.lineno < stage_call.lineno for call in cache_helper_calls)

    register_calls = _attribute_calls(tree, "register")
    unregister_calls = _attribute_calls(tree, "unregister")
    assert len(register_calls) == 1
    assert len(unregister_calls) == 1
    assert ast.unparse(register_calls[0]) == (
        "atexit.register(_release_cached_assignment_frames)"
    )
    assert ast.unparse(unregister_calls[0]) == (
        "atexit.unregister(_release_cached_assignment_frames)"
    )
    assert register_calls[0].lineno < cache_calls[0].lineno

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
        and isinstance(node.func, ast.Name)
        and node.func.id == "_release_cached_assignment_frames"
        for statement in current.finalbody
        for node in ast.walk(statement)
    )
