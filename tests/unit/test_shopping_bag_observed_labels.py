from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from next_ads.features.nextads_core import (
    build_observed_shopping_bag_click_labels_df,
    classify_shopping_bag_assignment_exclusion,
    classify_shopping_bag_event_route,
    normalize_shopping_bag_advert_id,
    shopping_bag_assignment_is_eligible,
    shopping_bag_label_is_mature,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DATE = date(2026, 8, 10)


@pytest.fixture(scope="module")
def local_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        pytest.skip(f"PySpark unavailable: {exc}")
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-shopping-bag-label-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


@pytest.mark.parametrize(
    ("platform", "route_tag", "expected"),
    [
        ("WEB", None, "v1"),
        ("APP", "NEXT-ADS-SB | c123", "v1"),
        ("APP", "ShoppingBagPage | c456", "v2"),
        ("APP", "unknown", None),
    ],
)
def test_event_route_keeps_coexisting_app_routes_separate(
    platform,
    route_tag,
    expected,
):
    assert classify_shopping_bag_event_route(platform, route_tag) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("P1_C2_CREATIVE.jpg", "P1_C2_CREATIVE"),
        ("P1_C2_CREATIVE_static.jpg", "P1_C2_CREATIVE"),
        ("P1_C2_CREATIVE_static", "P1_C2_CREATIVE"),
        ("P1_C2_CREATIVE?cache=1", "P1_C2_CREATIVE"),
        (None, None),
    ],
)
def test_only_known_rendering_suffixes_are_removed(raw_value, expected):
    assert normalize_shopping_bag_advert_id(raw_value) == expected


@pytest.mark.parametrize(
    ("measurement", "assigned", "treatment"),
    [
        ("P1_C1_A", "NoAd", "Best"),
        ("P1_C1_A", "P1_C1_A", "AdSuppressed"),
        ("P1_C1_A", "P1_C1_A", "Control"),
        ("NoAdFound", "P1_C1_A", "Best"),
        (None, "P1_C1_A", "Best"),
        ("P1_C1_A", "P1_C1_A", None),
        ("P1_C1_A", "P1_C1_A", "Unknown"),
    ],
)
def test_non_served_or_control_assignments_are_not_label_exposures(
    measurement,
    assigned,
    treatment,
):
    assert not shopping_bag_assignment_is_eligible(
        measurement,
        assigned,
        treatment,
    )


@pytest.mark.parametrize(
    ("measurement", "assigned", "treatment", "expected"),
    [
        ("P1_C1_A", "NoAd", "Best", "NO_AD"),
        ("P1_C1_A", "P1_C1_A", "AdSuppressed", "SUPPRESSED"),
        ("P1_C1_A", "P1_C1_A", "Control", "CONTROL"),
        ("P1_C1_A", "P1_C1_A", None, "UNKNOWN_TREATMENT"),
        (None, "P1_C1_A", "Best", "UNRESOLVED_ADVERT"),
        ("P1_C1_A", "P1_C1_A", "Best", None),
    ],
)
def test_assignment_exclusions_have_stable_evidence_reasons(
    measurement,
    assigned,
    treatment,
    expected,
):
    assert (
        classify_shopping_bag_assignment_exclusion(
            measurement,
            assigned,
            treatment,
        )
        == expected
    )


def test_served_assignment_treatments_are_eligible():
    assert shopping_bag_assignment_is_eligible(
        "P1_C1_A",
        "P1_C1_A",
        "BestChallenger",
    )


def test_only_closed_label_windows_are_mature():
    assert shopping_bag_label_is_mature(
        REFERENCE_DATE,
        0,
        date(2026, 8, 11),
    )
    assert not shopping_bag_label_is_mature(
        REFERENCE_DATE,
        1,
        date(2026, 8, 11),
    )
    assert shopping_bag_label_is_mature(
        REFERENCE_DATE,
        7,
        date(2026, 8, 18),
    )


def _label_sources(spark):
    from pyspark.sql import functions as F

    web_sessions = spark.createDataFrame(
        [
            (REFERENCE_DATE, "web-1", "rpid-1", "Desktop"),
            (REFERENCE_DATE, "web-ambiguous", "rpid-shared", "Mobile"),
            (REFERENCE_DATE, "web-no-ad", "rpid-no-ad", "Desktop"),
            (REFERENCE_DATE, "web-two-locations", "rpid-two", "Desktop"),
        ],
        "date date, UniqueVisitID string, RPID string, Device string",
    ).withColumn(
        "FirstTimestamp",
        F.lit(datetime(2026, 8, 10, 8, 0)).cast("timestamp"),
    )
    app_sessions = spark.createDataFrame(
        [
            (REFERENCE_DATE, "app-v2", "rpid-v2", "App", "iOS"),
            (REFERENCE_DATE, "app-v1", "rpid-v1", "App", "Android"),
            (REFERENCE_DATE, "app-cross-route", "rpid-cross", "App", "iOS"),
        ],
        "date date, UniqueVisitID string, RPID string, Device string, "
        "operating_system string",
    ).withColumn(
        "FirstTimestamp",
        F.lit(datetime(2026, 8, 10, 8, 0)).cast("timestamp"),
    )
    rpid_accounts = spark.createDataFrame(
        [
            ("rpid-1", "A-1"),
            ("rpid-shared", "A-2"),
            ("rpid-shared", "A-3"),
            ("rpid-no-ad", "A-4"),
            ("rpid-two", "A-5"),
            ("rpid-v2", "A-6"),
            ("rpid-v1", "A-7"),
            ("rpid-cross", "A-8"),
        ],
        "roamingprofileid string, account_number string",
    )
    web_actions = spark.createDataFrame(
        [
            (
                REFERENCE_DATE,
                "web-1",
                datetime(2026, 8, 10, 10, 0),
                "Banner Impression - Next Ads",
                None,
                "P1_C1_CREATIVE_static.jpg",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-1",
                datetime(2026, 8, 10, 11, 0),
                "Banner Impression - Next Ads",
                None,
                "P1_C1_CREATIVE_static.jpg",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-1",
                datetime(2026, 8, 10, 9, 59),
                "Banner Click - Next Ads",
                None,
                "P1_C1_CREATIVE_static.jpg",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-1",
                datetime(2026, 8, 10, 10, 0),
                "Banner Click - Next Ads",
                None,
                "P1_C1_CREATIVE_static.jpg",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-1",
                datetime(2026, 8, 10, 11, 5),
                "Banner Click - Next Ads",
                None,
                "P1_C1_CREATIVE_static.jpg",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-ambiguous",
                datetime(2026, 8, 10, 12, 0),
                "Banner Impression - Next Ads",
                None,
                "P2_C2_CREATIVE",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-no-ad",
                datetime(2026, 8, 10, 13, 0),
                "Banner Impression - Next Ads",
                None,
                "P3_C3_CREATIVE",
                "/shoppingbag",
            ),
            (
                REFERENCE_DATE,
                "web-two-locations",
                datetime(2026, 8, 10, 14, 0),
                "Banner Impression - Next Ads",
                None,
                "P4_C4_CREATIVE",
                "/shoppingbag",
            ),
        ],
        "date date, UniqueVisitID string, Timestamp timestamp, Action string, "
        "Level1 string, Level2 string, PagePath string",
    )
    app_actions = spark.createDataFrame(
        [
            (
                REFERENCE_DATE,
                "app-v2",
                datetime(2026, 8, 10, 15, 0),
                "Banner Impression - Next Ads",
                "ShoppingBagPage | c1",
                "P5_C5_CREATIVE.jpg",
                "Cart",
            ),
            (
                REFERENCE_DATE,
                "app-v2",
                datetime(2026, 8, 10, 15, 1),
                "Banner Click - Next Ads",
                "ShoppingBagPage | c1",
                "P5_C5_CREATIVE",
                "Cart",
            ),
            (
                REFERENCE_DATE,
                "app-v1",
                datetime(2026, 8, 10, 16, 0),
                "Banner Impression - Next Ads",
                "NEXT-ADS-SB | c2",
                "P6_C6_CREATIVE",
                "Cart",
            ),
            (
                REFERENCE_DATE,
                "app-v1",
                datetime(2026, 8, 10, 16, 1),
                "Banner Click - Next Ads",
                "NEXT-ADS-SB | c2",
                "P6_C6_CREATIVE",
                "Cart",
            ),
            (
                REFERENCE_DATE,
                "app-cross-route",
                datetime(2026, 8, 10, 17, 0),
                "Banner Impression - Next Ads",
                "ShoppingBagPage | c3",
                "P7_C7_CREATIVE",
                "Cart",
            ),
            (
                REFERENCE_DATE,
                "app-cross-route",
                datetime(2026, 8, 10, 17, 1),
                "Banner Click - Next Ads",
                "NEXT-ADS-SB | c3",
                "P7_C7_CREATIVE",
                "Cart",
            ),
        ],
        "date date, UniqueVisitID string, Timestamp timestamp, Action string, "
        "Level1 string, Level2 string, ScreenName string",
    )
    v1_assignments = spark.createDataFrame(
        [
            ("A-1", "SB1", "Best", "P1_C1_CREATIVE_static", "P1_C1_CREATIVE_static", date(2026, 8, 9)),
            ("A-2", "SB1", "Best", "P2_C2_CREATIVE", "P2_C2_CREATIVE", date(2026, 8, 9)),
            ("A-3", "SB1", "Best", "P2_C2_CREATIVE", "P2_C2_CREATIVE", date(2026, 8, 9)),
            ("A-4", "SB1", "Best", "P3_C3_CREATIVE", "NoAd", date(2026, 8, 9)),
            ("A-5", "SB1", "Best", "P4_C4_CREATIVE", "P4_C4_CREATIVE", date(2026, 8, 9)),
            ("A-5", "SB2", "Best", "P4_C4_CREATIVE", "P4_C4_CREATIVE", date(2026, 8, 9)),
            ("A-7", "SB2", "Best", "P6_C6_CREATIVE", "P6_C6_CREATIVE", date(2026, 8, 9)),
            ("A-8", "SB1", "Best", "P7_C7_CREATIVE", "P7_C7_CREATIVE", date(2026, 8, 9)),
        ],
        "AccountNumber string, Location string, Treatment string, "
        "UniqueAdIDMeasurement string, UniqueAdIDAssigned string, rundate date",
    )
    v2_assignments = spark.createDataFrame(
        [
            ("A-6", "ShoppingBagPage", 1, "Best", "P5_C5_CREATIVE", "P5_C5_CREATIVE", date(2026, 8, 9)),
            ("A-8", "ShoppingBagPage", 1, "Best", "P7_C7_CREATIVE", "P7_C7_CREATIVE", date(2026, 8, 9)),
        ],
        "AccountNumber string, PageType string, Rank int, Treatment string, "
        "UniqueAdIDMeasurement string, UniqueAdIDAssigned string, rundate date",
    )
    v1_control_sheet = spark.createDataFrame(
        [
            (
                "SB1",
                advert,
                "/shoppingbag",
                "Cart",
                cms_page_id,
                date(2026, 8, 9),
            )
            for advert, cms_page_id in (
                ("P1_C1_CREATIVE_static", "web-1"),
                ("P2_C2_CREATIVE", "web-2"),
                ("P3_C3_CREATIVE", "web-3"),
                ("P4_C4_CREATIVE", "web-4"),
                ("P7_C7_CREATIVE", "c3"),
            )
        ]
        + [
            (
                "SB2",
                "P4_C4_CREATIVE",
                "/shoppingbag",
                "Cart",
                "web-4b",
                date(2026, 8, 9),
            ),
            (
                "SB2",
                "P6_C6_CREATIVE",
                "/shoppingbag",
                "Cart",
                "c2",
                date(2026, 8, 9),
            ),
        ],
        "Location string, UniqueAdID string, Page string, Screen string, "
        "CMSPageID string, rundate date",
    )
    v2_control_sheet = spark.createDataFrame(
        [
            (
                "P5_C5_CREATIVE",
                "ShoppingBagPage",
                "c1",
                date(2026, 8, 9),
            ),
            (
                "P7_C7_CREATIVE",
                "ShoppingBagPage",
                "c3",
                date(2026, 8, 9),
            ),
        ],
        "UniqueAdID string, PageType string, CMSPageID string, rundate date",
    )
    v1_multipage_locations = spark.createDataFrame(
        [],
        "Location string, Page string, rundate date",
    )
    return {
        "spark": spark,
        "web_sessions": web_sessions,
        "app_sessions": app_sessions,
        "rpid_accounts": rpid_accounts,
        "web_actions": web_actions,
        "app_actions": app_actions,
        "v1_assignments": v1_assignments,
        "v2_assignments": v2_assignments,
        "v1_control_sheet": v1_control_sheet,
        "v2_control_sheet": v2_control_sheet,
        "v1_multipage_locations": v1_multipage_locations,
        "reference_date": REFERENCE_DATE.isoformat(),
    }


def test_observed_labels_use_real_impressions_and_one_click_attribution(
    local_spark,
):
    labels = build_observed_shopping_bag_click_labels_df(
        **_label_sources(local_spark),
        as_of_date="2026-08-18",
    )
    same_session = labels.where("label_horizon_days = 0").collect()

    assert len(same_session) == 5
    assert sum(row.clicked for row in same_session) == 3
    assert not any(row.account_number in {"A-2", "A-3", "A-4", "A-5"} for row in same_session)
    assert {(row.platform, row.route) for row in same_session} == {
        ("WEB", "v1"),
        ("APP", "v1"),
        ("APP", "v2"),
    }
    latest_web = max(
        (row for row in same_session if row.account_number == "A-1"),
        key=lambda row: row.exposure_timestamp,
    )
    earlier_web = min(
        (row for row in same_session if row.account_number == "A-1"),
        key=lambda row: row.exposure_timestamp,
    )
    assert latest_web.clicked == 1
    assert earlier_web.clicked == 0
    cross_route = next(row for row in same_session if row.account_number == "A-8")
    assert cross_route.clicked == 0


def test_label_advert_key_is_the_repository_measurement_entity(local_spark):
    labels = build_observed_shopping_bag_click_labels_df(
        **_label_sources(local_spark),
        as_of_date="2026-08-18",
    ).where("account_number = 'A-1' AND label_horizon_days = 0")
    advert_features = local_spark.createDataFrame(
        [("P1_C1_CREATIVE_static", "SB1")],
        "advert_id string, location string",
    )

    assert labels.select("advert_id", "location").distinct().join(
        advert_features,
        ["advert_id", "location"],
        "inner",
    ).count() == 1
    row = labels.first()
    assert row.observed_advert_id == "P1_C1_CREATIVE_static.jpg"
    assert row.normalized_observed_advert_id == "P1_C1_CREATIVE"
    assert row.advert_id == "P1_C1_CREATIVE_static"


def test_immature_horizons_are_not_published(local_spark):
    labels = build_observed_shopping_bag_click_labels_df(
        **_label_sources(local_spark),
        as_of_date="2026-08-11",
    )

    assert {
        row.label_horizon_days
        for row in labels.select("label_horizon_days").distinct().collect()
    } == {0}


def test_observed_label_contract_marks_exposure_time_as_timeseries_key():
    registry = yaml.safe_load(
        (
            PROJECT_ROOT
            / "configs"
            / "features"
            / "nextads_feature_store.yaml"
        ).read_text()
    )
    feature = next(
        table
        for table in registry["feature_store"]["physical_tables"]
        if table["name"]
        == "next_uk_nextads_fs_shopping_bag_click_labels"
    )
    ddl = (
        PROJECT_ROOT
        / "sql"
        / "features"
        / "nextads"
        / "create_table_next_uk_nextads_fs_shopping_bag_click_labels.sql"
    ).read_text()

    assert feature["primary_keys"] == [
        "exposure_id",
        "label_horizon_days",
        "exposure_timestamp",
    ]
    assert feature["timestamp_key"] == "exposure_timestamp"
    assert feature["snapshot_date_key"] == "session_date"
    assert "exposure_timestamp TIMESERIES" in ddl
