import ast
from pathlib import Path

import pytest

import next_ads.common.delta_writes as delta_writes


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _function_source(path: str, function_name: str) -> str:
    source = _read(path)
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    )
    return ast.get_source_segment(source, function)


def _call_names(source: str) -> list[str]:
    names = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.append(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.append(node.func.attr)
    return names


def test_scoped_atomic_write_rejects_source_before_creating_write_view(
    monkeypatch,
):
    events = []

    class Frame:
        columns = ["id", "rundate"]

    monkeypatch.setattr(
        delta_writes,
        "validate_target_columns",
        lambda *_args, **_kwargs: events.append("target_schema"),
    )

    def reject_scope(*_args, **_kwargs):
        events.append("source_scope")
        raise ValueError("outside replacement scope")

    monkeypatch.setattr(
        delta_writes,
        "validate_replace_source_scope",
        reject_scope,
    )
    monkeypatch.setattr(
        delta_writes,
        "_write_from_temporary_view",
        lambda *_args, **_kwargs: events.append("write"),
    )

    with pytest.raises(ValueError, match="outside replacement scope"):
        delta_writes.atomic_replace_where_by_name(
            object(),
            Frame(),
            target_table="catalog.schema.history",
            filters={"rundate": "2026-07-29"},
        )

    assert events == ["target_schema", "source_scope"]


def test_clean_output_publishes_serving_latest_only_after_durable_outputs():
    source = _function_source(
        "src/next_ads/ranking/theme_affinity/clean_output.py",
        "clean_model_output",
    )

    validation = source.index("validate_unique_non_null_keys(")
    history = source.index("replace_scope_by_name(")
    inference_log = source.index("_write_inference_log(")
    serving_latest = source.index("replace_table_by_name(")

    assert validation < history < inference_log < serving_latest


def test_theme_scoring_writes_use_one_run_date_and_atomic_helpers():
    build_source = _function_source(
        "jobs/nextads_candidates/build_theme_scores.py",
        "main",
    )
    mapping_source = _function_source(
        "src/next_ads/ranking/theme_score_mapping.py",
        "run_theme_score_mapping",
    )

    assert build_source.count("capture_run_date(") == 1
    assert "date.today(" not in build_source
    assert "ACTIONS_END or (run_date - timedelta(days=1))" in build_source
    assert "TODAY = run_date.isoformat()" in build_source
    assert "replace_validated_snapshot(" in build_source
    assert build_source.count("publish_history_and_latest(") == 2
    assert '.mode("errorifexists").saveAsTable(' in build_source
    assert "quote_qualified_identifier(temp_table_name)" in build_source

    assert mapping_source.count("capture_run_date(") == 1
    assert "publish_history_and_latest(" in mapping_source
    assert "replace_validated_snapshot(" in mapping_source
    assert "with_run_date(df_ad_scores, run_date)" in mapping_source

    for source in (build_source, mapping_source):
        calls = _call_names(source)
        assert "truncate_and_load" not in calls
        assert "delete_from_and_load" not in calls


def test_prediction_replaces_validated_snapshot_for_both_model_paths():
    source = _function_source(
        "src/next_ads/ranking/theme_affinity/predict.py",
        "run_prediction",
    )

    assert source.count("replace_validated_snapshot(") == 2
    assert source.count('key_columns=["account_number", "theme"]') == 2
    assert ".write.mode(" not in source
    assert ".saveAsTable(" not in source
