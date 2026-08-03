import ast
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from jobs.nextads_control import (
    load_control_sheet,
    load_control_sheet_v2,
    parse_attributes,
    parse_theme_mapping,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeFrame:
    def __init__(self, columns):
        self.columns = list(columns)
        self.selections = []

    def select(self, *columns):
        self.selections.append(list(columns))
        return self


def _record_publications(monkeypatch, module):
    calls = []

    def fake_publish(
        spark,
        df,
        *,
        history_table,
        latest_table,
        key_columns,
        run_date,
        columns,
    ):
        calls.append(
            {
                "spark": spark,
                "df": df,
                "history_table": history_table,
                "latest_table": latest_table,
                "key_columns": key_columns,
                "run_date": run_date,
                "columns": columns,
            }
        )

    monkeypatch.setattr(module, "publish_history_and_latest", fake_publish)
    return calls


def test_v1_input_publications_use_raw_and_placement_contracts(monkeypatch):
    calls = _record_publications(monkeypatch, load_control_sheet)
    spark = object()
    run_date = date(2026, 7, 29)
    control = FakeFrame(["UniqueAdID", "Realm", "Territory"])
    placements = FakeFrame(["Location", "Page", "Screen", "PageGroup"])
    tables = SimpleNamespace(
        control_sheet_raw="control_raw",
        control_sheet_raw_latest="control_raw_latest",
        control_sheet_plp_raw="placements_raw",
        control_sheet_plp_raw_latest="placements_raw_latest",
    )

    load_control_sheet.write_control_sheet_input_tables(
        control,
        placements,
        tables,
        spark=spark,
        run_date=run_date,
    )

    assert calls == [
        {
            "spark": spark,
            "df": control,
            "history_table": "control_raw",
            "latest_table": "control_raw_latest",
            "key_columns": ["Realm", "Territory", "UniqueAdID"],
            "run_date": run_date,
            "columns": [
                "UniqueAdID",
                "Realm",
                "Territory",
                "rundate",
            ],
        },
        {
            "spark": spark,
            "df": placements,
            "history_table": "placements_raw",
            "latest_table": "placements_raw_latest",
            "key_columns": ["Location"],
            "run_date": run_date,
            "columns": [
                "Location",
                "Page",
                "Screen",
                "PageGroup",
                "rundate",
            ],
        },
    ]


def test_v1_multipage_and_processed_publications_use_exact_keys(monkeypatch):
    calls = _record_publications(monkeypatch, load_control_sheet)
    spark = object()
    run_date = date(2026, 7, 29)
    multipage = FakeFrame(["Location", "Page", "Screen"])
    processed = FakeFrame(["UniqueAdID", "Location", "Title"])
    context = SimpleNamespace(
        target_multipage_locations_table="multipage",
        target_multipage_locations_latest_table="multipage_latest",
        target_table="control",
        target_table_latest="control_latest",
    )

    load_control_sheet.write_multipage_location_tables(
        multipage,
        context,
        spark=spark,
        run_date=run_date,
    )
    load_control_sheet.write_processed_control_sheet_tables(
        processed,
        context,
        ["UniqueAdID", "Location", "Title"],
        spark=spark,
        run_date=run_date,
    )

    assert [call["key_columns"] for call in calls] == [
        ["Location", "Page"],
        ["UniqueAdID", "Location"],
    ]
    assert [call["columns"] for call in calls] == [
        ["Location", "Page", "Screen", "rundate"],
        ["UniqueAdID", "Location", "Title", "rundate"],
    ]
    assert calls[0]["history_table"] == "multipage"
    assert calls[0]["latest_table"] == "multipage_latest"
    assert calls[1]["history_table"] == "control"
    assert calls[1]["latest_table"] == "control_latest"
    assert processed.selections == [
        ["UniqueAdID", "Location", "Title"],
    ]


def test_v2_input_and_processed_publications_use_exact_keys(monkeypatch):
    calls = _record_publications(monkeypatch, load_control_sheet_v2)
    spark = object()
    run_date = date(2026, 7, 29)
    control = FakeFrame(["UniqueAdID", "CMSPageID"])
    exclusions = FakeFrame(["url", "masidSlot", "CMSPageID"])
    processed = FakeFrame(["UniqueAdID", "PageType", "Title"])
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            control_sheet_raw_v2="control_raw_v2",
            control_sheet_raw_latest_v2="control_raw_latest_v2",
            exclusions="exclusions",
            exclusions_latest="exclusions_latest",
        )
    )

    load_control_sheet_v2.write_v2_input_tables(
        control,
        exclusions,
        config,
        spark=spark,
        run_date=run_date,
    )
    load_control_sheet_v2.write_v2_processed_control_sheet_tables(
        processed,
        "control_v2",
        "control_latest_v2",
        ["UniqueAdID", "PageType", "Title"],
        spark=spark,
        run_date=run_date,
    )

    assert [call["history_table"] for call in calls] == [
        "control_raw_v2",
        "exclusions",
        "control_v2",
    ]
    assert [call["latest_table"] for call in calls] == [
        "control_raw_latest_v2",
        "exclusions_latest",
        "control_latest_v2",
    ]
    assert [call["key_columns"] for call in calls] == [
        ["UniqueAdID"],
        ["url", "masidSlot", "CMSPageID"],
        ["UniqueAdID", "PageType"],
    ]
    assert [call["columns"] for call in calls] == [
        ["UniqueAdID", "CMSPageID", "rundate"],
        ["url", "masidSlot", "CMSPageID", "rundate"],
        ["UniqueAdID", "PageType", "Title", "rundate"],
    ]
    assert all(call["run_date"] == run_date for call in calls)


def test_attribute_outputs_use_paired_and_validated_snapshot_contracts(
    monkeypatch,
):
    publications = _record_publications(monkeypatch, parse_attributes)
    replacements = []
    spark = object()
    run_date = date(2026, 7, 29)
    attribute_set = FakeFrame(["attribute", "value"])
    item_attributes = FakeFrame(["pid", "attribute", "value"])
    dated_item_attributes = FakeFrame(
        ["pid", "attribute", "value", "rundate"]
    )

    monkeypatch.setattr(
        parse_attributes,
        "with_run_date",
        lambda df, value: (
            dated_item_attributes
            if df is item_attributes and value == run_date
            else None
        ),
    )

    def fake_replace(
        spark_arg,
        df,
        *,
        table,
        key_columns,
        columns,
    ):
        replacements.append(
            {
                "spark": spark_arg,
                "df": df,
                "table": table,
                "key_columns": key_columns,
                "columns": columns,
            }
        )

    monkeypatch.setattr(
        parse_attributes,
        "replace_validated_snapshot",
        fake_replace,
    )

    parse_attributes.write_attribute_set_tables(
        attribute_set,
        "attribute_set",
        "attribute_set_latest",
        spark=spark,
        run_date=run_date,
    )
    parse_attributes.write_item_attributes_latest(
        item_attributes,
        "item_attributes_latest",
        spark=spark,
        run_date=run_date,
    )

    assert publications == [
        {
            "spark": spark,
            "df": attribute_set,
            "history_table": "attribute_set",
            "latest_table": "attribute_set_latest",
            "key_columns": ["attribute", "value"],
            "run_date": run_date,
            "columns": ["attribute", "value", "rundate"],
        }
    ]
    assert replacements == [
        {
            "spark": spark,
            "df": dated_item_attributes,
            "table": "item_attributes_latest",
            "key_columns": [
                "pid",
                "attribute",
                "value",
                "rundate",
            ],
            "columns": [
                "pid",
                "attribute",
                "value",
                "rundate",
            ],
        }
    ]


def test_theme_and_item_theme_publications_use_exact_contracts(monkeypatch):
    calls = _record_publications(monkeypatch, parse_theme_mapping)
    spark = object()
    run_date = date(2026, 7, 29)
    themes = FakeFrame(["Theme", "attribute", "value"])
    item_themes = FakeFrame(["pid", "theme", "theme_rank", "scratch"])

    parse_theme_mapping.write_theme_mapping_tables(
        themes,
        "theme_mapping",
        "theme_mapping_latest",
        spark=spark,
        run_date=run_date,
    )
    parse_theme_mapping.write_item_theme_tables(
        item_themes,
        "item_themes",
        "item_themes_latest",
        spark=spark,
        run_date=run_date,
    )

    assert [call["key_columns"] for call in calls] == [
        ["Theme", "attribute", "value"],
        ["pid", "theme"],
    ]
    assert [call["columns"] for call in calls] == [
        ["Theme", "attribute", "value", "rundate"],
        ["pid", "theme", "theme_rank", "rundate"],
    ]
    assert item_themes.selections == [["pid", "theme", "theme_rank"]]


def _called_names(source):
    names = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.append(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.append(node.func.attr)
    return names


def test_control_entrypoints_use_one_logical_date_and_remove_legacy_writers():
    expected_date_contracts = {
        "jobs/nextads_control/load_control_sheet.py": "propagated",
        "jobs/nextads_control/load_control_sheet_v2.py": "propagated",
        "jobs/nextads_control/parse_attributes.py": "captured",
        "jobs/nextads_control/parse_theme_mapping.py": "captured",
    }

    for relative_path, date_contract in expected_date_contracts.items():
        source = (PROJECT_ROOT / relative_path).read_text()
        called_names = _called_names(source)
        if date_contract == "propagated":
            assert called_names.count("capture_run_date") == 0
            assert called_names.count("fromisoformat") == 1
            assert 'get_arg("--run_date")' in source
        else:
            assert called_names.count("capture_run_date") == 1
        assert "delete_from_and_load" not in called_names
        assert "truncate_and_load" not in called_names
        assert "current_date" not in called_names


def test_plx_fallback_and_bigquery_export_remain_in_their_entrypoints():
    v1_source = (
        PROJECT_ROOT / "jobs/nextads_control/load_control_sheet.py"
    ).read_text()
    v1_tree = ast.parse(v1_source)
    plx_try_blocks = [
        node
        for node in ast.walk(v1_tree)
        if isinstance(node, ast.Try)
        and any(
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "write_multipage_location_tables"
            for statement in node.body
            for call in ast.walk(statement)
        )
    ]
    assert len(plx_try_blocks) == 1
    assert not any(
        isinstance(node, ast.Raise)
        for handler in plx_try_blocks[0].handlers
        for node in ast.walk(handler)
    )
    assert "URLs not refreshed" in v1_source
    assert "post_to_webhook" in v1_source
    shared_control_source = (
        PROJECT_ROOT / "src/next_ads/control/load_control_sheet.py"
    ).read_text()
    assert "if df_plx_urls is None:" in shared_control_source
    assert "skipped because PLX input was unavailable" in shared_control_source
    assert "F.lit(reference_date)" in shared_control_source
    assert (
        'F.col("rundate") == underperforming_date'
        in shared_control_source
    )

    attributes_source = (
        PROJECT_ROOT / "jobs/nextads_control/parse_attributes.py"
    ).read_text()
    compact_attributes = "".join(attributes_source.split())
    bq_guard = compact_attributes.index("ifBQ_EXPORT:")
    item_snapshot = compact_attributes.rfind(
        "write_item_attributes_latest(",
        0,
        bq_guard,
    )
    bq_write = compact_attributes.index(
        'bq_item_attributes.write.format("bigquery")'
    )
    assert item_snapshot < bq_guard < bq_write
