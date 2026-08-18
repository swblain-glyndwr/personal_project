from datetime import date

from next_ads.features.shopping_bag_account_activity import (
    SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS,
    build_shopping_bag_account_activity_df,
    read_shopping_bag_account_activity_sources,
    shopping_bag_activity_window,
)


def test_activity_window_contains_exactly_ninety_calendar_dates():
    start, end = shopping_bag_activity_window("2026-08-07")

    assert start == date(2026, 5, 10)
    assert end == date(2026, 8, 7)
    assert (end - start).days + 1 == SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS


def test_activity_sources_use_the_proven_shopping_bag_event_route():
    class RecordingSpark:
        def __init__(self):
            self.paths = []

        def table(self, path):
            self.paths.append(path)
            return path

    spark = RecordingSpark()
    frames = read_shopping_bag_account_activity_sources(
        spark,
        "marketingdata_prod",
        "warehouse",
    )

    assert tuple(frames) == (
        "sessions",
        "rpid_accounts",
        "pages",
        "actions",
    )
    assert spark.paths == [
        "marketingdata_prod.warehouse.bq_sessions_next_uk",
        "marketingdata_prod.warehouse.rpid_with_accounts",
        "marketingdata_prod.warehouse.bq_pages_next_uk",
        "marketingdata_prod.warehouse.bq_actions_next_uk",
    ]


def test_activity_rejects_ambiguous_visits_and_excludes_old_events(spark):
    sessions = spark.createDataFrame(
        [
            ("2026-05-09", "old", "rpid-1"),
            ("2026-05-10", "repeat", "rpid-1"),
            ("2026-08-07", "repeat", "rpid-1"),
            ("2026-08-07", "ambiguous", "rpid-shared"),
        ],
        "date string, UniqueVisitID string, RPID string",
    )
    rpid_accounts = spark.createDataFrame(
        [
            ("rpid-1", "A-1"),
            ("rpid-shared", "A-2"),
            ("rpid-shared", "A-3"),
        ],
        "roamingprofileid string, account_number string",
    )
    pages = spark.createDataFrame(
        [
            ("2026-05-09", "old", "/shoppingbag"),
            ("2026-05-10", "repeat", "/shoppingbag?from=test"),
            ("2026-08-07", "repeat", "/product/123"),
            ("2026-08-07", "repeat", "/shoppingbag"),
            ("2026-08-07", "ambiguous", "/shoppingbag"),
        ],
        "date string, UniqueVisitID string, PagePath string",
    )
    actions = spark.createDataFrame(
        [
            (
                "2026-05-10",
                "repeat",
                "Add to bag",
                None,
                None,
                None,
                "/product/1",
            ),
            (
                "2026-08-07",
                "repeat",
                "View product",
                None,
                None,
                None,
                "/product/2",
            ),
            (
                "2026-08-07",
                "ambiguous",
                "Add to bag",
                None,
                None,
                None,
                "/shoppingbag",
            ),
        ],
        "date string, UniqueVisitID string, Action string, Level1 string, "
        "Level2 string, Level3 string, PagePath string",
    )

    rows = build_shopping_bag_account_activity_df(
        sessions,
        rpid_accounts,
        pages,
        actions,
        reference_date="2026-08-07",
    ).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["account_number"] == "A-1"
    assert row["reference_date"] == date(2026, 8, 7)
    assert row["browse_sessions_90d"] == 2
    assert row["browse_active_days_90d"] == 2
    assert row["page_events_90d"] == 3
    assert row["shopping_bag_page_events_90d"] == 2
    assert row["avg_pages_per_session_90d"] == 1.5
    assert row["action_events_90d"] == 2
    assert row["action_active_days_90d"] == 2
    assert row["add_to_bag_actions_90d"] == 1
    assert row["pdp_action_rows_90d"] == 2
    assert row["browse_session_recency_days"] == 0
    assert row["action_recency_days"] == 0
