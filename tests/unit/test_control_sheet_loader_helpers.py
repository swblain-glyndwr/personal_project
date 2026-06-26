from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from next_ads.control.load_control_sheet import (
    align_control_sheet_to_read_schema,
    align_control_sheet_to_target_columns,
    apply_inherited_location_columns,
    assert_append_rundate_target_schema,
    build_control_sheet_run_context,
    build_control_sheet_read_schema,
    build_processed_control_sheet,
    resolve_control_sheet_locations,
    resolve_control_sheet_output_tables,
    build_requested_ad_locations,
    clean_theme_strings,
    clear_missing_premium_ad_ids,
    collect_invalid_date_ad_ids,
    constrain_premium_ads_to_sibling_locations,
    filter_non_empty_unique_ads,
    filter_valid_date_format,
    get_active_control_ads,
    normalise_active_control_ads,
    resolve_duplicate_masid_conflicts,
)
from next_ads.common.paths import load_client_config, resolve_sql_contract_path
from next_ads.utils.config_manager import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-control-sheet-loader-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


def _sorted_rows(df, *cols):
    return sorted(tuple(row[col] for col in cols) for row in df.collect())


def _create_table_sql_columns(path: Path) -> list[str]:
    columns = []
    for line in path.read_text().splitlines()[1:]:
        stripped = line.strip()
        if stripped.startswith("CONSTRAINT"):
            break
        if not stripped:
            continue
        columns.append(stripped.split()[0].strip("`,"))
    return columns


def test_resolve_control_sheet_locations_preserves_order_and_inheritance():
    locations = {
        "SB1": {"pf_col": "SB_slot_1"},
        "SB2": {"pf_col": "SB_slot_2", "inherit_ads_from": "SB1"},
        "PL1": {"pf_col": "PL_slot_1"},
        "PH3": {"pf_col": "hp_slot_3", "inherit_ads_from": "HN1"},
    }

    resolved = resolve_control_sheet_locations(locations)

    assert resolved.valid_locations == ["SB1", "SB2", "PL1", "PH3"]
    assert resolved.read_locations == ["SB1", "PL1"]
    assert resolved.inherited_locations == {"SB2": "SB1", "PH3": "HN1"}


def test_build_control_sheet_read_schema_adds_read_locations_without_mutating_base():
    base_schema = [
        ["UniqueAdID", "string", "null"],
        ["CMSPageID", "string", "null"],
        ["SB1", "string", "null"],
    ]

    schema = build_control_sheet_read_schema(
        base_schema=base_schema,
        read_locations=["SB1", "PL1"],
    )

    assert schema == [
        ["UniqueAdID", "string", "null"],
        ["CMSPageID", "string", "null"],
        ["SB1", "string", "null"],
        ["PL1", "string", "null"],
    ]
    assert base_schema == [
        ["UniqueAdID", "string", "null"],
        ["CMSPageID", "string", "null"],
        ["SB1", "string", "null"],
    ]


def test_next_uk_raw_table_sql_matches_effective_read_schema():
    client_config = load_client_config("next_uk")
    location_config = resolve_control_sheet_locations(client_config["locations"])
    read_schema = build_control_sheet_read_schema(
        client_config["control_sheet"]["read_schema"],
        location_config.read_locations,
    )
    expected_columns = [column[0] for column in read_schema] + ["rundate"]

    assert "ClusterID" in expected_columns
    assert "FY20" in expected_columns
    assert "Segment" in expected_columns
    assert "AdDriver" in expected_columns
    assert "TemplateName" in expected_columns

    for sql_file in [
        resolve_sql_contract_path("control_sheet_raw"),
        resolve_sql_contract_path("control_sheet_raw_latest"),
    ]:
        assert _create_table_sql_columns(sql_file) == expected_columns


def test_align_control_sheet_to_read_schema_drops_surplus_sheet_columns(local_spark):
    spark = local_spark
    df_control_sheet = spark.createDataFrame(
        [("ad1", "Next", "GB", "TRUE", "FALSE", "2026-06-12", "home")],
        [
            "UniqueAdID",
            "Realm",
            "Territory",
            "PL1",
            "PL1",
            "rundate",
            "Page",
        ],
    )

    aligned = align_control_sheet_to_read_schema(
        df_control_sheet,
        [
            ["UniqueAdID", "string", "null"],
            ["Realm", "string", "null"],
            ["Territory", "string", "null"],
            ["PL1", "string", "null"],
        ],
    )

    assert aligned.extra_columns == ["PL1", "rundate", "Page"]
    assert aligned.df.columns == ["UniqueAdID", "Realm", "Territory", "PL1"]
    assert _sorted_rows(aligned.df, "UniqueAdID", "Realm", "Territory", "PL1") == [
        ("ad1", "Next", "GB", "TRUE"),
    ]


def test_align_control_sheet_to_read_schema_rejects_missing_columns(local_spark):
    spark = local_spark
    df_control_sheet = spark.createDataFrame(
        [("ad1", "Next")],
        ["UniqueAdID", "Realm"],
    )

    with pytest.raises(ValueError, match="fewer columns"):
        align_control_sheet_to_read_schema(
            df_control_sheet,
            [
                ["UniqueAdID", "string", "null"],
                ["Realm", "string", "null"],
                ["Territory", "string", "null"],
            ],
        )


def test_assert_append_rundate_target_schema_rejects_drifted_target():
    with pytest.raises(ValueError) as exc_info:
        assert_append_rundate_target_schema(
            table_name="marketingdata_dev.nextads_integration.raw",
            df_columns=["UniqueAdID", "FY1", "FY20"],
            target_columns=["UniqueAdID", "FY1", "rundate"],
        )

    message = str(exc_info.value)
    assert "marketingdata_dev.nextads_integration.raw" in message
    assert "Missing target columns: FY20" in message
    assert "First order mismatch: position 2: expected FY20, found rundate" in message


def test_control_sheet_output_route_uses_personal_dev_outputs(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "stephen_blain")

    route = resolve_control_sheet_output_tables(load_config("dev"))

    assert route.catalog_write == "marketingdata_dev"
    assert route.schema_write == "stephen_blain"
    assert (
        route.control_sheet
        == "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet"
    )
    assert (
        route.control_sheet_latest
        == "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet_latest"
    )
    assert (
        route.control_sheet_raw
        == "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet_raw"
    )
    assert (
        route.control_sheet_plp_raw_latest
        == "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet_plp_raw_latest"
    )
    assert (
        route.multipage_locations_latest
        == "marketingdata_dev.stephen_blain.next_uk_nextads_multipage_locations_latest"
    )


def test_control_sheet_output_route_uses_dev_integration_outputs(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "nextads_integration")

    route = resolve_control_sheet_output_tables(load_config("dev"))

    assert route.catalog_write == "marketingdata_dev"
    assert route.schema_write == "nextads_integration"
    assert (
        route.control_sheet
        == "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet"
    )
    assert (
        route.control_sheet_latest
        == "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet_latest"
    )
    assert (
        route.control_sheet_raw_latest
        == "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet_raw_latest"
    )
    assert (
        route.control_sheet_plp_raw
        == "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet_plp_raw"
    )
    assert (
        route.multipage_locations
        == "marketingdata_dev.nextads_integration.next_uk_nextads_multipage_locations"
    )


def test_build_control_sheet_run_context_resolves_routes_and_read_schema(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "stephen_blain")
    client_config = {
        "locations": {
            "PL1": {},
            "PL2": {"inherit_ads_from": "PL1"},
        },
        "control_sheet": {
            "url": "https://example/control",
            "sheet": "control",
            "read_schema": [["UniqueAdID", "string", "null"]],
            "date_format": "dd/MM/yyyy",
            "date_regex": r"^\d{2}/\d{2}/\d{4}$",
        },
        "placements_sheet": {
            "url": "https://example/placements",
            "sheet": "placements",
            "read_schema": [["Location", "string", "null"]],
        },
        "plx_urls_sheet": {
            "url": "https://example/plx",
            "sheet": "plx",
            "read_schema": [["URL", "string", "null"]],
        },
        "webhooks": {"Input Warnings": "https://example/webhook"},
    }

    context = build_control_sheet_run_context(
        client="next_uk",
        client_config=client_config,
        config=load_config("dev"),
    )

    assert context.client == "next_uk"
    assert context.location_config.valid_locations == ["PL1", "PL2"]
    assert context.location_config.read_locations == ["PL1"]
    assert context.location_config.inherited_locations == {"PL2": "PL1"}
    assert context.control_sheet_read_schema == [
        ["UniqueAdID", "string", "null"],
        ["PL1", "string", "null"],
    ]
    assert context.schema_write == "stephen_blain"
    assert (
        context.target_table
        == "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet"
    )
    assert context.webhook_url == "https://example/webhook"


def test_active_control_ad_filters_and_normalisation(local_spark):
    spark = local_spark
    rows = [
        ("ad1", "01/06/2026", "30/06/2026", "active", "ab-12", "TRUE"),
        ("ad2", "bad-date", "30/06/2026", "active", "cd-34", "FALSE"),
        ("ad3", "12/06/2026", "30/06/2026", "active", "ef-56", ""),
        ("", "01/06/2026", "30/06/2026", "active", "gh-78", "TRUE"),
    ]
    df_control = spark.createDataFrame(
        rows,
        [
            "UniqueAdID",
            "StartDate",
            "EndDate",
            "Status",
            "Items",
            "AudienceOnly",
        ],
    )

    df_not_empty = filter_non_empty_unique_ads(df_control)
    df_valid_dates = filter_valid_date_format(
        df_not_empty,
        r"^\d{2}/\d{2}/\d{4}$",
    )
    invalid_ids = collect_invalid_date_ad_ids(df_not_empty, df_valid_dates)
    df_active = get_active_control_ads(
        df_valid_dates,
        "dd/MM/yyyy",
        reference_date=date(2026, 6, 10),
    )
    df_normalised = normalise_active_control_ads(df_active)

    assert invalid_ids == ["ad2"]
    assert _sorted_rows(df_normalised, "UniqueAdID", "Items", "AudienceOnly") == [
        ("ad1", "AB12", 1),
    ]


def test_inherited_locations_are_available_for_ad_location_requests(local_spark):
    spark = local_spark
    df_active = spark.createDataFrame(
        [
            ("ad1", "TRUE", None, "FALSE"),
            ("ad2", "FALSE", None, "TRUE"),
        ],
        "UniqueAdID string, PL1 string, PL2 string, PL3 string",
    )

    df_with_inherited = apply_inherited_location_columns(
        df_active,
        {"PL2": "PL1"},
    )
    df_locations = build_requested_ad_locations(
        df_with_inherited,
        ["PL1", "PL2", "PL3"],
    )

    assert _sorted_rows(df_locations, "UniqueAdID", "Location") == [
        ("ad1", "PL1"),
        ("ad1", "PL2"),
        ("ad2", "PL3"),
    ]


def test_build_processed_control_sheet_joins_placements_and_targeting(local_spark):
    spark = local_spark
    df_active = spark.createDataFrame(
        [
            ("ad1", "TRUE", "FALSE", "model_a", "or", "summer"),
            ("ad2", "FALSE", "TRUE", None, "and", "winter"),
        ],
        [
            "UniqueAdID",
            "PL1",
            "PL2",
            "Models",
            "ModelCombination",
            "Themes",
        ],
    )
    df_placements = spark.createDataFrame(
        [
            ("PL1", "page-one", "mobile"),
            ("PL2", "page-two", "desktop"),
        ],
        ["Location", "Page", "Screen"],
    )

    df_processed = build_processed_control_sheet(
        df_active,
        df_placements,
        ["PL1", "PL2"],
    )

    assert _sorted_rows(
        df_processed,
        "UniqueAdID",
        "Location",
        "Page",
        "Screen",
        "TargetingCriteria",
    ) == [
        ("ad1", "PL1", "page-one", "mobile", "or|model_a"),
        ("ad2", "PL2", "page-two", "desktop", None),
    ]


def test_premium_ads_are_constrained_to_sibling_locations(local_spark):
    spark = local_spark
    df_processed = spark.createDataFrame(
        [
            ("ad1", "PL1", "ad2"),
            ("ad2", "PL2", None),
            ("ad3", "PL2", "ad2"),
        ],
        ["UniqueAdID", "Location", "UniqueAdIDPremium"],
    )

    result = constrain_premium_ads_to_sibling_locations(df_processed)

    assert _sorted_rows(result, "UniqueAdID", "Location", "UniqueAdIDPremium") == [
        ("ad1", "PL1", None),
        ("ad2", "PL2", None),
        ("ad3", "PL2", "ad2"),
    ]


def test_theme_cleanup_and_missing_premium_ids(local_spark):
    spark = local_spark
    df_processed = spark.createDataFrame(
        [
            ("ad1", " SUMMER ", "ad2"),
            ("ad2", None, None),
            ("ad3", "Winter", "missing-ad"),
        ],
        ["UniqueAdID", "Themes", "UniqueAdIDPremium"],
    )

    result = clear_missing_premium_ad_ids(clean_theme_strings(df_processed))

    assert _sorted_rows(result, "UniqueAdID", "Themes", "UniqueAdIDPremium") == [
        ("ad1", "summer", "ad2"),
        ("ad2", None, None),
        ("ad3", "winter", None),
    ]


def test_align_control_sheet_to_target_columns_drops_extra_columns(local_spark):
    spark = local_spark
    df_processed = spark.createDataFrame(
        [("ad1", "PL1", "drop-me")],
        ["UniqueAdID", "Location", "ScratchColumn"],
    )

    aligned = align_control_sheet_to_target_columns(
        df_processed,
        ["UniqueAdID", "Location"],
    )

    assert aligned.extra_columns == ["ScratchColumn"]
    assert aligned.df.columns == ["UniqueAdID", "Location"]
    assert _sorted_rows(aligned.df, "UniqueAdID", "Location") == [("ad1", "PL1")]


def test_duplicate_masid_conflicts_keep_latest_ad_id(local_spark):
    spark = local_spark
    df_processed = spark.createDataFrame(
        [
            ("ad1", "mens", "PL1", "AA"),
            ("ad2", "mens", "PL1", "AA"),
            ("ad3", "womens", "PL2", "BB"),
            ("ad4", "womens", "PL2", "BB"),
            ("ad5", "womens", "PL2", "BB"),
        ],
        ["UniqueAdID", "AlgoDivision", "Location", "MASIDToken"],
    )

    result = resolve_duplicate_masid_conflicts(df_processed)

    assert result.duplicate_masids == ["AA", "BB"]
    assert "Keeping ad: ad2" in result.warning_message
    assert "Keeping ad: ad5" in result.warning_message
    assert _sorted_rows(result.df, "UniqueAdID", "MASIDToken") == [
        ("ad2", "AA"),
        ("ad5", "BB"),
    ]
