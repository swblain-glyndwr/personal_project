from next_ads.delivery import google_sheets
from next_ads.utils import gs_helpers


def test_google_sheets_helpers_import_from_new_and_legacy_paths():
    assert (
        gs_helpers.get_service_account_dict
        is google_sheets.get_service_account_dict
    )
    assert (
        gs_helpers.read_from_google_sheets_to_dataframe
        is google_sheets.read_from_google_sheets_to_dataframe
    )
    assert (
        gs_helpers.get_masid_csmid_columns_udf
        is google_sheets.get_masid_csmid_columns_udf
    )
    assert (
        gs_helpers.format_output_col_names
        is google_sheets.format_output_col_names
    )
    assert (
        gs_helpers.resolve_plp_gs_delivery_config
        is google_sheets.resolve_plp_gs_delivery_config
    )
    assert gs_helpers.create_dl_table is google_sheets.create_dl_table
    assert gs_helpers.configure_abfs is google_sheets.configure_abfs

