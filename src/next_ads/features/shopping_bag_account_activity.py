"""Point-in-time account activity for the Shopping Bag pCTR example."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Mapping

from next_ads.features.nextads_core import source_table


SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS = 90


def shopping_bag_activity_window(
    reference_date: str | date,
) -> tuple[date, date]:
    """Return exactly 90 inclusive calendar dates ending on the snapshot day."""
    end_date = (
        reference_date
        if isinstance(reference_date, date)
        else date.fromisoformat(str(reference_date))
    )
    return (
        end_date - timedelta(days=SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS - 1),
        end_date,
    )


def read_shopping_bag_account_activity_sources(
    spark: Any,
    source_catalog: str,
    source_schema: str,
) -> Mapping[str, Any]:
    """Read the four repository-owned sources used by the worked example."""
    return {
        "sessions": spark.table(
            source_table(
                source_catalog,
                source_schema,
                "bq_sessions_next_uk",
            )
        ),
        "rpid_accounts": spark.table(
            source_table(
                source_catalog,
                source_schema,
                "rpid_with_accounts",
            )
        ),
        "pages": spark.table(
            source_table(
                source_catalog,
                source_schema,
                "bq_pages_next_uk",
            )
        ),
        "actions": spark.table(
            source_table(
                source_catalog,
                source_schema,
                "bq_actions_next_uk",
            )
        ),
    }


def _normalise_path(column: Any) -> Any:
    from pyspark.sql import functions as F

    return F.trim(
        F.lower(
            F.regexp_replace(
                F.regexp_replace(column.cast("string"), r"[?#].*$", ""),
                r"\s+",
                "",
            )
        )
    )


def build_shopping_bag_account_activity_df(
    sessions: Any,
    rpid_accounts: Any,
    pages: Any,
    actions: Any,
    *,
    reference_date: str | date,
) -> Any:
    """Build leakage-safe activity for use on the following scoring day."""
    from pyspark.sql import functions as F

    start_date, end_date = shopping_bag_activity_window(reference_date)
    start = F.lit(start_date)
    end = F.lit(end_date)

    rpid_lookup = (
        rpid_accounts.select(
            F.col("roamingprofileid").cast("string").alias("rpid"),
            F.col("account_number").cast("string").alias("account_number"),
        )
        .where(F.col("rpid").isNotNull())
        .where(F.col("account_number").isNotNull())
        .dropDuplicates(["rpid", "account_number"])
    )
    session_accounts = (
        sessions.where(F.to_date("date").between(start, end))
        .select(
            F.to_date("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("RPID").cast("string").alias("rpid"),
        )
        .where(F.col("unique_visit_id").isNotNull())
        .where(F.col("rpid").isNotNull())
        .join(rpid_lookup, on="rpid", how="inner")
        .select("account_number", "session_date", "unique_visit_id")
        .dropDuplicates()
    )
    ambiguous_visits = (
        session_accounts.groupBy("session_date", "unique_visit_id")
        .agg(F.countDistinct("account_number").alias("account_count"))
        .where(F.col("account_count") != 1)
        .select("session_date", "unique_visit_id")
    )
    resolved_sessions = session_accounts.join(
        ambiguous_visits,
        on=["session_date", "unique_visit_id"],
        how="leftanti",
    ).dropDuplicates(["account_number", "session_date", "unique_visit_id"])

    browse = resolved_sessions.groupBy("account_number").agg(
        F.countDistinct("session_date", "unique_visit_id")
        .cast("bigint")
        .alias("browse_sessions_90d"),
        F.countDistinct("session_date").cast("bigint").alias(
            "browse_active_days_90d"
        ),
        F.datediff(end, F.max("session_date")).cast("int").alias(
            "browse_session_recency_days"
        ),
    )
    page_events = (
        pages.where(F.to_date("date").between(start, end))
        .select(
            F.to_date("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("PagePath").cast("string").alias("page_path"),
        )
        .join(
            resolved_sessions,
            on=["session_date", "unique_visit_id"],
            how="inner",
        )
    )
    pages_per_session = page_events.groupBy(
        "account_number",
        "session_date",
        "unique_visit_id",
    ).agg(
        F.count(F.lit(1)).cast("bigint").alias("pages_in_session"),
        F.sum(
            F.when(
                _normalise_path(F.col("page_path")) == "/shoppingbag",
                1,
            ).otherwise(0)
        )
        .cast("bigint")
        .alias("shopping_bag_pages_in_session"),
    )
    page_features = pages_per_session.groupBy("account_number").agg(
        F.sum("pages_in_session").cast("bigint").alias("page_events_90d"),
        F.sum("shopping_bag_pages_in_session")
        .cast("bigint")
        .alias("shopping_bag_page_events_90d"),
        F.avg("pages_in_session").cast("double").alias(
            "avg_pages_per_session_90d"
        ),
    )

    action_events = (
        actions.where(F.to_date("date").between(start, end))
        .select(
            F.to_date("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("Action").cast("string").alias("action"),
            F.col("Level1").cast("string").alias("level1"),
            F.col("Level2").cast("string").alias("level2"),
            F.col("Level3").cast("string").alias("level3"),
            F.col("PagePath").cast("string").alias("page_path"),
        )
        .join(
            resolved_sessions,
            on=["session_date", "unique_visit_id"],
            how="inner",
        )
        .withColumn(
            "action_text",
            F.lower(
                F.concat_ws(
                    " | ",
                    F.coalesce("action", F.lit("")),
                    F.coalesce("level1", F.lit("")),
                    F.coalesce("level2", F.lit("")),
                    F.coalesce("level3", F.lit("")),
                    F.coalesce("page_path", F.lit("")),
                )
            ),
        )
    )
    action_features = action_events.groupBy("account_number").agg(
        F.count(F.lit(1)).cast("bigint").alias("action_events_90d"),
        F.countDistinct("session_date").cast("bigint").alias(
            "action_active_days_90d"
        ),
        F.sum(
            F.when(F.col("action_text").rlike(r"add.?to.?bag|atb"), 1)
            .otherwise(0)
        )
        .cast("bigint")
        .alias("add_to_bag_actions_90d"),
        F.sum(
            F.when(F.col("action_text").rlike(r"pdp|product"), 1).otherwise(
                0
            )
        )
        .cast("bigint")
        .alias("pdp_action_rows_90d"),
        F.datediff(end, F.max("session_date")).cast("int").alias(
            "action_recency_days"
        ),
    )

    return (
        browse.join(page_features, on="account_number", how="left")
        .join(action_features, on="account_number", how="left")
        .fillna(
            {
                "page_events_90d": 0,
                "shopping_bag_page_events_90d": 0,
                "avg_pages_per_session_90d": 0.0,
                "action_events_90d": 0,
                "action_active_days_90d": 0,
                "add_to_bag_actions_90d": 0,
                "pdp_action_rows_90d": 0,
                "action_recency_days": SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS + 1,
            }
        )
        .withColumn("reference_date", end.cast("date"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "account_number",
            "reference_date",
            "browse_sessions_90d",
            "browse_active_days_90d",
            "page_events_90d",
            "shopping_bag_page_events_90d",
            "avg_pages_per_session_90d",
            "action_events_90d",
            "action_active_days_90d",
            "add_to_bag_actions_90d",
            "pdp_action_rows_90d",
            "browse_session_recency_days",
            "action_recency_days",
            "created_at",
            "updated_at",
        )
        .dropDuplicates(["account_number", "reference_date"])
    )


__all__ = [
    "SHOPPING_BAG_ACTIVITY_LOOKBACK_DAYS",
    "build_shopping_bag_account_activity_df",
    "read_shopping_bag_account_activity_sources",
    "shopping_bag_activity_window",
]
