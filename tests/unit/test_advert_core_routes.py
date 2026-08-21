from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE = (
    PROJECT_ROOT / "src" / "next_ads" / "features" / "nextads_core.py"
).read_text()


def test_advert_core_reads_both_nextads_serving_routes():
    start = SOURCE.index("def build_advert_core_df")
    end = SOURCE.index("\ndef build_item_attributes_df", start)
    function = SOURCE[start:end]

    assert "next_uk_nextads_control_sheet\"" in function
    assert "next_uk_nextads_control_sheet_v2\"" in function
    assert 'F.to_date("rundate") == source_rundate' in function
    assert 'F.col("Location")' in function
    assert 'F.col("PageType")' in function
    assert "v1.unionByName(v2)" in function


def test_v2_advert_core_maps_model_fields_without_changing_the_table_contract():
    start = SOURCE.index("def build_advert_core_df")
    end = SOURCE.index("\ndef build_item_attributes_df", start)
    function = SOURCE[start:end]

    assert '_optional_col(active_v2, "Themes")' in function
    assert '_optional_col(active_v2, "AdDriver")' in function
    assert '_optional_col(active_v2, "Brand")' in function
    assert '_optional_col(active_v2, "TemplateName")' in function
