import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ASSIGN_PATH = (
    PROJECT_ROOT / "jobs/nextads_cells/assign_customer_cells.py"
)
COMBINE_PATH = (
    PROJECT_ROOT / "jobs/nextads_cells/combine_customer_cells.py"
)


def _source_and_tree(path):
    source = path.read_text()
    return source, ast.parse(source, filename=str(path))


def _calls_named(tree, function_name):
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == function_name
    ]


def _keywords(call):
    return {keyword.arg: keyword.value for keyword in call.keywords}


def _name(node):
    assert isinstance(node, ast.Name)
    return node.id


def _string_list(node):
    assert isinstance(node, ast.List)
    return [
        element.value
        for element in node.elts
        if isinstance(element, ast.Constant)
    ]


def test_assign_cells_uses_propagated_run_date_and_no_destructive_writers():
    source, tree = _source_and_tree(ASSIGN_PATH)

    assert len(_calls_named(tree, "capture_run_date")) == 0
    assert 'get_arg("--run_date")' in source
    assert "date.fromisoformat(RUN_DATE_RAW)" in source
    assert 'raise ValueError("--run_date is required")' in source
    assert "date.today" not in source
    assert "current_date()" not in source

    for legacy_write in (
        "create_table_from_df",
        "delete_from_and_load",
        "truncate_and_load",
        "truncate table",
        "drop_if_exists",
        "_backup",
    ):
        assert legacy_write not in source


def test_fixed_cells_archive_scope_and_latest_snapshot_are_atomic():
    source, tree = _source_and_tree(ASSIGN_PATH)

    scope_call = _calls_named(tree, "replace_validated_scope")
    assert len(scope_call) == 1
    scope_keywords = _keywords(scope_call[0])
    assert _name(scope_keywords["table"]) == "FIXED_CELLS_HISTORY_TABLE"
    assert _string_list(scope_keywords["key_columns"]) == ["AccountNumber"]
    assert ast.unparse(scope_keywords["scope"]) == (
        "{'RunDateEnd': archive_date}"
    )

    snapshot_call = _calls_named(tree, "replace_validated_snapshot")
    assert len(snapshot_call) == 1
    snapshot_keywords = _keywords(snapshot_call[0])
    assert _name(snapshot_keywords["table"]) == "FIXED_CELLS_TABLE"
    assert _string_list(snapshot_keywords["key_columns"]) == [
        "AccountNumber"
    ]

    assert 'column="RunDateEnd"' in source
    assert (
        "spark.createDataFrame([], schema=fixed_cells_schema)"
        in source
    )
    assert (
        'df_cells_existing = df_fixed_latest_existing.drop("rundate")'
        in source
    )
    assert (
        '.where(F.col("RunDateEnd") < F.lit(RUN_DATE))'
        in source
    )
    assert "FULL_REFRESH_REQUIRED = False" in source
    assert "if archive_date == RUN_DATE:" in source
    assert (
        "preserving the published assignment on retry"
        in source
    )
    assert "else:\n        FULL_REFRESH_REQUIRED = True" in source
    assert "if FULL_REFRESH_REQUIRED:" in source


def test_transient_cells_publish_history_then_latest_even_when_empty():
    source, tree = _source_and_tree(ASSIGN_PATH)

    publish_calls = _calls_named(tree, "publish_history_and_latest")
    assert len(publish_calls) == 1
    publish_keywords = _keywords(publish_calls[0])
    assert _name(publish_keywords["history_table"]) == (
        "TRANSIENT_CELLS_TABLE"
    )
    assert _name(publish_keywords["latest_table"]) == (
        "TRANSIENT_CELLS_TABLE_LATEST"
    )
    assert _string_list(publish_keywords["key_columns"]) == [
        "AccountNumber",
        "Cell",
    ]
    assert _name(publish_keywords["run_date"]) == "RUN_DATE"

    assert "if df_cells_transient is None:" in source
    assert (
        "spark.createDataFrame([], schema=transient_schema)"
        in source
    )


def test_combined_cells_latest_is_atomic_and_preserves_safe_fallback():
    source, tree = _source_and_tree(COMBINE_PATH)

    assert len(_calls_named(tree, "capture_run_date")) == 0
    assert 'get_arg("--run_date")' in source
    assert "date.fromisoformat(RUN_DATE_RAW)" in source
    assert 'raise ValueError("--run_date is required")' in source
    assert len(_calls_named(tree, "with_run_date")) == 1

    snapshot_calls = _calls_named(tree, "replace_validated_snapshot")
    assert len(snapshot_calls) == 1
    snapshot_keywords = _keywords(snapshot_calls[0])
    assert _name(snapshot_keywords["table"]) == "CELLS_TABLE_LATEST"
    assert _string_list(snapshot_keywords["key_columns"]) == [
        "AccountNumber"
    ]

    for legacy_write in (
        "create_table_from_df",
        "truncate table",
        "drop_if_exists",
    ):
        assert legacy_write not in source

    assert '"AlgoDivision" in df_cells_transient.columns' in source
    assert "df_cells_transient.isEmpty()" in source
    assert "spark.createDataFrame([], schema=combined_schema)" not in source
    assert "publish_new_snapshot" in source
    assert "read_delta_version(" in source
    assert "summary.require_valid(\"accepted combined customer cells\")" in source
    assert "Accepted combined customer cells are more than one day old" in source
    assert "customer_cells_delta_version" in source


def test_combined_cells_checksum_uses_target_table_column_order():
    source, tree = _source_and_tree(COMBINE_PATH)

    target_order_calls = _calls_named(tree, "validate_target_columns")
    assert len(target_order_calls) == 1
    assert [ast.unparse(arg) for arg in target_order_calls[0].args] == [
        "spark",
        "CELLS_TABLE_LATEST",
        "df_selected.columns",
    ]
    assert (
        "df_selected = df_selected.select(*target_columns).persist()"
        in source
    )

    target_order_position = source.index(
        "df_selected = df_selected.select(*target_columns).persist()"
    )
    checksum_position = source.index("summary = summarise_content(")
    write_position = source.index("replace_validated_snapshot(")
    assert target_order_position < checksum_position < write_position
