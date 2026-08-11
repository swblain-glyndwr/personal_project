from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CRITICAL_ROUTE_FILES = (
    "jobs/nextads_data/archive_sort_order_data.py",
    "jobs/realtime/viewed_bought.py",
    "src/next_ads/delivery/google_sheets.py",
)

UNSAFE_CRITICAL_ROUTE_PATTERNS = (
    "F.rand(",
    ".sampleBy(",
    ".dropDuplicates(",
    ".drop_duplicates(",
    "delete_from_and_load(",
    "truncate_and_load(",
    "OPTIMIZE",
    "VACUUM",
    ".saveAsTable(",
    "DELETE FROM",
    "TRUNCATE TABLE",
)


def _read(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(
        encoding="utf-8",
        errors="ignore",
    )


def test_incident_route_has_no_retry_unsafe_writer_or_housekeeping():
    offenders = {
        (relative_path, pattern)
        for relative_path in CRITICAL_ROUTE_FILES
        for pattern in UNSAFE_CRITICAL_ROUTE_PATTERNS
        if pattern in _read(relative_path)
    }

    assert offenders == set()


def test_active_membership_aggregates_are_canonical():
    expected_fragments = {
        "jobs/nextads_control/load_control_sheet_v2.py": (
            'F.sort_array(F.collect_set("PageType"))'
        ),
        "src/next_ads/control/load_control_sheet.py": (
            'F.sort_array(F.collect_set("Location"))'
        ),
        "src/next_ads/control/attributes.py": (
            'F.sort_array(F.collect_set("attribute_value"))'
        ),
        "src/next_ads/control/item_attributes.py": (
            'F.sort_array(F.collect_list("value"))'
        ),
        "src/next_ads/decisioning/assignment_publication.py": (
            'F.sort_array(F.collect_set("_masid_token"))'
        ),
        "src/next_ads/realtime/decisioning/advert_affinity_data_build.py": (
            'F.sort_array(F.collect_set("itemno"))'
        ),
    }

    for relative_path, fragment in expected_fragments.items():
        assert fragment in _read(relative_path)
