import ast
import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CRITICAL_WRITER_FILES = (
    "jobs/nextads_cells/assign_customer_cells.py",
    "jobs/nextads_cells/combine_customer_cells.py",
    "jobs/nextads_control/load_control_sheet.py",
    "jobs/nextads_control/load_control_sheet_v2.py",
    "jobs/nextads_control/parse_attributes.py",
    "jobs/nextads_control/parse_theme_mapping.py",
    "jobs/nextads_candidates/build_theme_scores.py",
    "jobs/nextads_delivery/build_v2_payload.py",
    "src/next_ads/ranking/theme_score_mapping.py",
    "src/next_ads/ranking/theme_affinity/clean_output.py",
    "src/next_ads/ranking/theme_affinity/predict.py",
    "src/next_ads/ranking/theme_affinity/publish_outputs.py",
)
DESTRUCTIVE_CALLS = {
    "create_table_from_df",
    "delete_from_and_load",
    "truncate_and_load",
}
HOUSEKEEPING_SQL = ("OPTIMIZE ", "VACUUM ")
DESTRUCTIVE_SQL = ("DELETE FROM ", "TRUNCATE TABLE ")
UNSUPPORTED_DBR_15_4_REPLACE = re.compile(
    r"\bBY\s+NAME\b[\s\S]{0,160}\bREPLACE\s+WHERE\b",
    re.IGNORECASE,
)


def _called_names(source: str) -> set[str]:
    names = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


@pytest.mark.parametrize("relative_path", CRITICAL_WRITER_FILES)
def test_critical_route_has_no_destructive_writer_or_housekeeping(
    relative_path,
):
    source = (PROJECT_ROOT / relative_path).read_text()
    calls = _called_names(source)
    upper_source = source.upper()

    assert calls.isdisjoint(DESTRUCTIVE_CALLS)
    assert not any(sql in upper_source for sql in DESTRUCTIVE_SQL)
    assert not any(sql in upper_source for sql in HOUSEKEEPING_SQL)


def test_critical_writers_do_not_use_intermediate_save_as_table():
    save_as_table_files = []
    for relative_path in CRITICAL_WRITER_FILES:
        source = (PROJECT_ROOT / relative_path).read_text()
        if "saveAsTable(" in source:
            save_as_table_files.append(relative_path)

    assert save_as_table_files == []


def test_production_sources_do_not_use_unsupported_dbr_15_4_replace_syntax():
    offenders = []
    for source_root in ("jobs", "src"):
        for path in (PROJECT_ROOT / source_root).rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sql"}:
                continue
            source = path.read_text(encoding="utf-8", errors="ignore")
            if UNSUPPORTED_DBR_15_4_REPLACE.search(source):
                offenders.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert offenders == []
