from next_ads.delivery import google_sheets


def test_google_sheets_helpers_import_from_delivery_package():
    for helper in [
        "get_service_account_dict",
        "read_from_google_sheets_to_dataframe",
        "get_masid_csmid_columns_udf",
        "format_output_col_names",
        "resolve_plp_gs_delivery_config",
        "publish_plp_tables",
        "configure_abfs",
    ]:
        assert hasattr(google_sheets, helper)

