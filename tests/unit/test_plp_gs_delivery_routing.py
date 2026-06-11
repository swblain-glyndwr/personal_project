from next_ads.delivery.google_sheets import resolve_plp_gs_delivery_config
from next_ads.utils.config_manager import load_config


def test_plp_gs_delivery_route_uses_personal_dev_outputs(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "stephen_blain")

    route = resolve_plp_gs_delivery_config(
        config=load_config("dev"),
        client="next",
        territory="GB",
    )

    assert route.catalog_write == "marketingdata_dev"
    assert route.schema_write == "stephen_blain"
    assert (
        route.output_table_name
        == "marketingdata_dev.stephen_blain.next_uk_nextads_plp_gs_next_gb_latest"
    )
    assert (
        route.final_output_table_name
        == "marketingdata_dev.stephen_blain.next_uk_nextads_plp_gs_latest"
    )
    assert (
        route.az_output_abfss_path
        == "abfss://adsconfigfeeds@adsstecmdeveun.dfs.core.windows.net/input"
    )


def test_plp_gs_delivery_route_uses_dev_integration_outputs(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "nextads_integration")

    route = resolve_plp_gs_delivery_config(
        config=load_config("dev"),
        client="next",
        territory="GB",
    )

    assert route.catalog_write == "marketingdata_dev"
    assert route.schema_write == "nextads_integration"
    assert (
        route.output_table_name
        == "marketingdata_dev.nextads_integration.next_uk_nextads_plp_gs_next_gb_latest"
    )
    assert (
        route.final_output_table_name
        == "marketingdata_dev.nextads_integration.next_uk_nextads_plp_gs_latest"
    )
    assert (
        route.az_output_abfss_path
        == "abfss://adsconfigfeeds@adsstecmdeveun.dfs.core.windows.net/input"
    )

