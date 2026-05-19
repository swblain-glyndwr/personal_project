import json
from pathlib import Path

import pytest


@pytest.fixture
def project_root():
    return Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def json_config(project_root):
    with open(project_root / "config/next_uk.json") as f:
        return json.load(f)


@pytest.fixture
def dynaconf_config(config_prod):
    return config_prod


class TestLoadControlSheetConfigParityHighLevel:
    def test_all_locations_exist(self, json_config, dynaconf_config):
        json_locations = set(json_config["locations"].keys())
        dynaconf_locations = set(dynaconf_config.locations.keys())
        assert json_locations == dynaconf_locations, (
            f"Missing locations: {json_locations - dynaconf_locations}\n"
            f"Extra locations: {dynaconf_locations - json_locations}"
        )

    def test_all_locations_have_required_attributes(self, json_config, dynaconf_config):
        for location_key in json_config["locations"].keys():
            json_location = json_config["locations"][location_key]
            dynaconf_location = dynaconf_config.locations[location_key]

            for attr in json_location.keys():
                assert hasattr(dynaconf_location, attr), (
                    f"Location {location_key} missing attribute: {attr}"
                )

    def test_locations_basic_attributes_match(self, json_config, dynaconf_config):
        for location_key in json_config["locations"].keys():
            json_location = json_config["locations"][location_key]
            dynaconf_location = dynaconf_config.locations[location_key]

            assert json_location["pf_col"] == dynaconf_location.pf_col, (
                f"Location {location_key} pf_col mismatch"
            )
            assert json_location["basic_within"] == dynaconf_location.basic_within, (
                f"Location {location_key} basic_within mismatch"
            )

    def test_locations_with_constraints_match(self, json_config, dynaconf_config):
        for location_key in json_config["locations"].keys():
            json_location = json_config["locations"][location_key]

            if "constraints" in json_location:
                dynaconf_location = dynaconf_config.locations[location_key]
                assert hasattr(dynaconf_location, "constraints"), (
                    f"Location {location_key} missing constraints"
                )
                json_constraints = json_location["constraints"]
                dynaconf_constraints = dynaconf_location.constraints

                for constraint_key in json_constraints:
                    assert constraint_key in dynaconf_constraints, (
                        f"Location {location_key} missing constraint: {constraint_key}"
                    )
                    assert (
                        json_constraints[constraint_key]
                        == dynaconf_constraints[constraint_key]
                    ), f"Location {location_key} constraint {constraint_key} mismatch"

    def test_locations_with_inherit_ads_from_match(
        self, json_config, dynaconf_config
    ):
        for location_key in json_config["locations"].keys():
            json_location = json_config["locations"][location_key]

            if "inherit_ads_from" in json_location:
                dynaconf_location = dynaconf_config.locations[location_key]
                assert hasattr(dynaconf_location, "inherit_ads_from"), (
                    f"Location {location_key} missing inherit_ads_from"
                )
                assert (
                    json_location["inherit_ads_from"]
                    == dynaconf_location.inherit_ads_from
                ), f"Location {location_key} inherit_ads_from mismatch"

    def test_locations_with_best_kwargs_match(self, json_config, dynaconf_config):
        for location_key in json_config["locations"].keys():
            json_location = json_config["locations"][location_key]

            if "best_kwargs" in json_location:
                dynaconf_location = dynaconf_config.locations[location_key]
                assert hasattr(dynaconf_location, "best_kwargs"), (
                    f"Location {location_key} missing best_kwargs"
                )

                json_kwargs = json_location["best_kwargs"]
                dynaconf_kwargs = dynaconf_location.best_kwargs

                assert set(json_kwargs.keys()) == set(dynaconf_kwargs.keys()), (
                    f"Location {location_key} best_kwargs keys mismatch"
                )

    def test_gcp_config_matches(self, json_config, dynaconf_config):
        assert json_config["gcp"]["scope"] == dynaconf_config.gcp.scope
        assert json_config["gcp"]["key"] == dynaconf_config.gcp.key

    def test_control_sheet_basic_config_matches(self, json_config, dynaconf_config):
        assert (
            json_config["control_sheet"]["url"] == dynaconf_config.control_sheet.url
        )
        assert (
            json_config["control_sheet"]["sheet"]
            == dynaconf_config.control_sheet.sheet
        )
        assert (
            json_config["control_sheet"]["date_format"]
            == dynaconf_config.control_sheet.date_format
        )

    def test_control_sheet_date_regex_matches(self, json_config, dynaconf_config):
        json_regex = json_config["control_sheet"]["date_regex"]
        dynaconf_regex = dynaconf_config.control_sheet.date_regex
        assert json_regex == dynaconf_regex

    def test_control_sheet_schema_length_matches(self, json_config, dynaconf_config):
        json_schema = json_config["control_sheet"]["read_schema"]
        dynaconf_schema = list(dynaconf_config.control_sheet.read_schema)
        assert len(json_schema) == len(dynaconf_schema)

    def test_placements_sheet_config_matches(self, json_config, dynaconf_config):
        assert (
            json_config["placements_sheet"]["url"]
            == dynaconf_config.placements_sheet.url
        )
        assert (
            json_config["placements_sheet"]["sheet"]
            == dynaconf_config.placements_sheet.sheet
        )
        assert len(json_config["placements_sheet"]["read_schema"]) == len(
            dynaconf_config.placements_sheet.read_schema
        )

    def test_plx_urls_sheet_config_matches(self, json_config, dynaconf_config):
        assert (
            json_config["plx_urls_sheet"]["url"] == dynaconf_config.plx_urls_sheet.url
        )
        assert (
            json_config["plx_urls_sheet"]["sheet"]
            == dynaconf_config.plx_urls_sheet.sheet
        )
        assert len(json_config["plx_urls_sheet"]["read_schema"]) == len(
            dynaconf_config.plx_urls_sheet.read_schema
        )

    def test_tables_write_config_matches(self, json_config, dynaconf_config):
        from next_ads.utils import etl

        JOB_ENV = "prod"
        CLIENT = "next_uk"
        tables_to_check = [
            "control_sheet",
            "control_sheet_latest",
            "multipage_locations",
            "multipage_locations_latest",
        ]
        SCHEMA = json_config["schema"][JOB_ENV]

        for table in tables_to_check:
            tbl_args = {
                "catalog": dynaconf_config.catalog_write,
                "schema": SCHEMA,
                "client": CLIENT
            }
            json_table = etl.map_tbl(
                json_config["tables"]["write"][table], **tbl_args)
            assert (
                json_table
                == getattr(dynaconf_config.tables_write, table)
            ), f"Table {table} mismatch"

    def test_webhooks_input_warnings_matches(self, json_config, dynaconf_config):
        assert (
            json_config["webhooks"]["Input Warnings"]
            == dynaconf_config.webhooks.input_warnings
        )

    def test_schema_dev_matches(self, json_config, config_dev):
        assert json_config["schema"]["dev"] == config_dev.schema_write
