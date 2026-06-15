import pytest

from next_ads.common import etl as new_etl
from next_ads.utils import etl as legacy_etl


def test_etl_imports_work_from_new_and_legacy_paths():
    assert legacy_etl.map_tbl is new_etl.map_tbl


def test_map_tbl_formats_table_template():
    table_name = new_etl.map_tbl(
        "{catalog}.{schema}.{client}_assignments",
        catalog="marketingdata_dev",
        schema="nextads_integration",
        client="next",
    )

    assert table_name == "marketingdata_dev.nextads_integration.next_assignments"


def test_map_tbl_rejects_empty_template():
    with pytest.raises(ValueError, match="Template cannot be empty"):
        new_etl.map_tbl("")


def test_map_tbl_reports_missing_placeholders():
    with pytest.raises(KeyError, match="Missing required placeholder"):
        new_etl.map_tbl("{catalog}.{schema}.{missing}", catalog="cat", schema="sch")
