import pytest

from next_ads.delivery.masid_handoff import (
    MasidHandoffSummary,
    expected_rundate,
    resolve_assignments_latest_table,
    validate_masid_handoff_summary,
)


def _summary(**overrides):
    values = {
        "table_name": "marketingdata_prod.warehouse.next_uk_nextads_assignments_latest",
        "columns": {"AccountNumber", "Location", "MASID", "rundate"},
        "row_count": 100,
        "rundates": ["2026-06-17"],
        "null_masid_count": 0,
        "location_count": 4,
    }
    values.update(overrides)
    return MasidHandoffSummary(**values)


def test_expected_rundate_accepts_explicit_iso_date():
    assert expected_rundate("2026-06-17") == "2026-06-17"


def test_expected_rundate_rejects_invalid_explicit_date():
    with pytest.raises(ValueError):
        expected_rundate("17-06-2026")


def test_resolve_assignments_latest_table_uses_runtime_config():
    class Config:
        catalog_write = "marketingdata_dev"
        schema_write = "nextads_integration"

    client_config = {
        "tables": {
            "write": {
                "assignments_latest": (
                    "{catalog}.{schema}.{client}_nextads_assignments_latest"
                )
            }
        }
    }

    assert resolve_assignments_latest_table(Config, client_config, "next_uk") == (
        "marketingdata_dev.nextads_integration."
        "next_uk_nextads_assignments_latest"
    )


def test_validate_masid_handoff_summary_passes_for_expected_contract():
    validate_masid_handoff_summary(_summary(), "2026-06-17")


def test_validate_masid_handoff_summary_fails_for_missing_columns():
    with pytest.raises(AssertionError, match="missing required columns: MASID"):
        validate_masid_handoff_summary(
            _summary(columns={"AccountNumber", "Location", "rundate"}),
            "2026-06-17",
        )


def test_validate_masid_handoff_summary_fails_for_empty_table():
    with pytest.raises(AssertionError, match="is empty"):
        validate_masid_handoff_summary(_summary(row_count=0), "2026-06-17")


def test_validate_masid_handoff_summary_fails_for_stale_rundate():
    with pytest.raises(AssertionError, match="do not match expected handoff date"):
        validate_masid_handoff_summary(
            _summary(rundates=["2026-06-16"]),
            "2026-06-17",
        )


def test_validate_masid_handoff_summary_fails_for_null_masid():
    with pytest.raises(AssertionError, match="contains 2 null MASID values"):
        validate_masid_handoff_summary(
            _summary(null_masid_count=2),
            "2026-06-17",
        )


def test_validate_masid_handoff_summary_fails_for_no_locations():
    with pytest.raises(AssertionError, match="contains no assignment locations"):
        validate_masid_handoff_summary(
            _summary(location_count=0),
            "2026-06-17",
        )
