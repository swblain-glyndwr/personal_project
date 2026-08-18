"""Core Next Ads feature-store materialisation logic."""

from __future__ import annotations

def source_table(catalog: str, schema: str, table_name: str) -> str:
    """Resolve a production or integration source table."""
    return f"{catalog}.{schema}.{table_name}"


def resolve_reference_date_from_theme(spark, args) -> str:
    """Resolve the shared feature-store reference date."""
    from next_ads.features.theme_affinity import resolve_theme_reference_date

    source_catalog = args.theme_source_catalog or args.source_catalog
    return resolve_theme_reference_date(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )


def _first_present(df, names: list[str]) -> str | None:
    columns = set(df.columns)
    for name in names:
        if name in columns:
            return name
    return None


def _optional_col(df, names: str | list[str], default=None):
    from pyspark.sql import functions as F

    candidates = [names] if isinstance(names, str) else names
    column_name = _first_present(df, candidates)
    if column_name:
        return F.col(column_name)
    return F.lit(default)


def _required_col(df, names: str | list[str], description: str):
    from pyspark.sql import functions as F

    candidates = [names] if isinstance(names, str) else names
    column_name = _first_present(df, candidates)
    if not column_name:
        raise ValueError(
            f"Could not resolve required {description} column from {candidates}"
        )
    return F.col(column_name)


def _latest_per_key(df, key_columns: list[str]):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order_columns = [
        column_name
        for column_name in ("rundate", "updated_at", "source_updated_at", "date")
        if column_name in df.columns
    ]
    if not order_columns:
        return df.dropDuplicates(key_columns)

    window = Window.partitionBy(*key_columns).orderBy(
        *[F.col(column_name).desc_nulls_last() for column_name in order_columns]
    )
    return (
        df.withColumn("_feature_store_row_number", F.row_number().over(window))
        .where(F.col("_feature_store_row_number") == 1)
        .drop("_feature_store_row_number")
    )


def _date_window(reference_date: str, lookback_days: int):
    from pyspark.sql import functions as F

    end_date = F.lit(reference_date).cast("date")
    start_date = F.date_sub(end_date, lookback_days)
    return start_date, end_date


def _normalise_path_expr(column_expr):
    from pyspark.sql import functions as F

    return F.trim(
        F.lower(
            F.regexp_replace(
                F.regexp_replace(column_expr.cast("string"), r"[?#].*$", ""),
                r"\s+",
                "",
            )
        )
    )


def _weighted_map(key_column: str, value_column: str):
    from pyspark.sql import functions as F

    return F.map_from_entries(
        F.collect_list(
            F.struct(
                F.col(key_column).cast("string").alias("key"),
                F.col(value_column).cast("double").alias("value"),
            )
        )
    )


def build_account_profile_df(spark, source_catalog: str, source_schema: str, reference_date: str):
    """Build account profile features from the stable customer history source."""
    from pyspark.sql import functions as F

    customer = spark.table(source_table(source_catalog, source_schema, "svoccust_hist"))
    account_expr = _required_col(
        customer,
        ["account_number", "AccountNumber", "accountnumber", "accountnumberkey"],
        "account number",
    ).cast("string")

    base = customer.withColumn("_account_number", account_expr)
    base = _latest_per_key(base.where(F.col("_account_number").isNotNull()), ["_account_number"])

    return base.select(
        F.col("_account_number").alias("account_number"),
        F.lit(reference_date).cast("date").alias("reference_date"),
        _optional_col(base, ["country_code", "country", "sites"], "next_uk").cast("string").alias("country_code"),
        _optional_col(base, ["client_name", "client"], "next_uk").cast("string").alias("client_name"),
        _optional_col(base, ["account_type", "accounttype"]).cast("string").alias("account_type"),
        _optional_col(base, ["account_age_days", "accountagedays"]).cast("int").alias("account_age_days"),
        _optional_col(base, ["postcode_area", "postcodearea"]).cast("string").alias("postcode_area"),
        _optional_col(base, ["uk_region", "region"]).cast("string").alias("region"),
        _optional_col(base, "gender").cast("string").alias("gender"),
        _optional_col(base, ["svoc_credit_type", "credit_type", "creditcustomer"]).cast("string").alias("credit_type"),
        _optional_col(base, ["latest_known_activity_recency_days", "last_activity_recency_days"]).cast("int").alias("latest_known_activity_recency_days"),
        _optional_col(base, ["online_orders_lifetime", "online_orders_n", "online_orders"]).cast("double").alias("online_orders_lifetime"),
        _optional_col(base, ["online_spend_lifetime", "online_spend_n", "online_spend"]).cast("double").alias("online_spend_lifetime"),
        _optional_col(base, ["retail_orders_lifetime", "retail_orders_n", "retail_orders"]).cast("double").alias("retail_orders_lifetime"),
        _optional_col(base, ["retail_spend_lifetime", "retail_spend_n", "retail_spend"]).cast("double").alias("retail_spend_lifetime"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    ).dropDuplicates(["account_number", "reference_date"])


def build_account_web_activity_df(spark, source_catalog: str, source_schema: str, reference_date: str):
    """Build 90-day account web activity features from BigQuery session/action feeds."""
    from pyspark.sql import functions as F

    sessions = spark.table(
        source_table(source_catalog, source_schema, "bq_views_sessions_next_uk_with_accounts")
    )
    actions = spark.table(source_table(source_catalog, source_schema, "bq_actions_next_uk"))
    start_date, end_date = _date_window(reference_date, 90)

    session_account = _required_col(
        sessions,
        ["account_number", "AccountNumber", "AccountNumber_RPID", "accountnumber_rpid"],
        "session account number",
    ).cast("string")
    session_date = _required_col(sessions, ["date", "session_date"], "session date").cast("date")
    session_visit = _required_col(
        sessions,
        ["UniqueVisitID", "unique_visit_id", "uniquevisitid"],
        "session visit id",
    ).cast("string")

    sessions_90d = (
        sessions.select(
            session_account.alias("account_number"),
            session_date.alias("event_date"),
            session_visit.alias("unique_visit_id"),
            _optional_col(sessions, ["PagePath", "page_path", "pagepath"]).cast("string").alias("page_path"),
        )
        .where(F.col("account_number").isNotNull())
        .where((F.col("event_date") >= start_date) & (F.col("event_date") <= end_date))
    )

    session_rollup = sessions_90d.groupBy("account_number").agg(
        F.countDistinct("unique_visit_id").alias("browse_sessions_90d"),
        F.countDistinct("event_date").alias("browse_active_days_90d"),
        F.count("*").alias("page_events_90d"),
        F.sum(F.when(_normalise_path_expr(F.col("page_path")) == "/shoppingbag", 1).otherwise(0)).cast("bigint").alias("shopping_bag_page_events_90d"),
        (F.count("*") / F.greatest(F.countDistinct("unique_visit_id"), F.lit(1))).cast("double").alias("avg_pages_per_session_90d"),
        F.datediff(F.lit(reference_date).cast("date"), F.max("event_date")).cast("int").alias("browse_session_recency_days"),
    )

    action_date = _required_col(actions, ["date", "action_date"], "action date").cast("date")
    action_visit = _required_col(
        actions,
        ["UniqueVisitID", "unique_visit_id", "uniquevisitid"],
        "action visit id",
    ).cast("string")
    actions_90d = (
        actions.select(
            action_date.alias("event_date"),
            action_visit.alias("unique_visit_id"),
            _optional_col(actions, ["Action", "action"]).cast("string").alias("action"),
            _optional_col(actions, ["PagePath", "page_path", "pagepath"]).cast("string").alias("page_path"),
        )
        .where((F.col("event_date") >= start_date) & (F.col("event_date") <= end_date))
        .join(
            sessions_90d.select("account_number", "event_date", "unique_visit_id").dropDuplicates(),
            on=["event_date", "unique_visit_id"],
            how="inner",
        )
    )
    action_rollup = actions_90d.groupBy("account_number").agg(
        F.count("*").alias("action_events_90d"),
        F.countDistinct("event_date").alias("action_active_days_90d"),
        F.sum(F.when(F.lower(F.col("action")).contains("add"), 1).otherwise(0)).cast("bigint").alias("add_to_bag_actions_90d"),
        F.sum(F.when(_normalise_path_expr(F.col("page_path")).contains("/shop/"), 1).otherwise(0)).cast("bigint").alias("pdp_action_rows_90d"),
        F.datediff(F.lit(reference_date).cast("date"), F.max("event_date")).cast("int").alias("action_recency_days"),
    )

    return (
        session_rollup.join(action_rollup, on="account_number", how="left")
        .withColumn("reference_date", F.lit(reference_date).cast("date"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["account_number", "reference_date"])
    )


def build_advert_core_df(spark, source_catalog: str, source_schema: str, reference_date: str):
    """Build active V1 and V2 advert metadata at the serving-route grain."""
    from pyspark.sql import functions as F

    source_rundate = F.date_sub(F.lit(reference_date).cast("date"), 1)
    control_v1 = spark.table(
        source_table(source_catalog, source_schema, "next_uk_nextads_control_sheet")
    ).where(F.to_date("rundate") == source_rundate)
    active_v1 = (
        control_v1.where(F.col("UniqueAdID").isNotNull())
        .where(F.col("Location").isNotNull())
        .where((F.col("StartDate").isNull()) | (F.col("StartDate") <= F.lit(reference_date).cast("date")))
        .where((F.col("EndDate").isNull()) | (F.col("EndDate") >= F.lit(reference_date).cast("date")))
    )
    v1 = active_v1.select(
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("Location").cast("string").alias("location"),
        F.lit(reference_date).cast("date").alias("feature_date"),
        _optional_col(active_v1, "CampaignNumber").cast("string").alias("campaign_id"),
        _optional_col(active_v1, "URL").cast("string").alias("advert_url"),
        _optional_col(active_v1, "ProductURLs").cast("string").alias("product_urls"),
        _optional_col(active_v1, "Items").cast("string").alias("control_sheet_items"),
        _optional_col(active_v1, "Title").cast("string").alias("advert_title"),
        _optional_col(active_v1, "Headline").cast("string").alias("headline"),
        _optional_col(active_v1, "Subtext").cast("string").alias("subtext"),
        _optional_col(active_v1, "CTA").cast("string").alias("cta"),
        _optional_col(active_v1, "AdTrend").cast("string").alias("advert_theme"),
        F.coalesce(
            _optional_col(active_v1, "AdCategory"),
            _optional_col(active_v1, "AdSubcategory"),
        )
        .cast("string")
        .alias("advert_category"),
        _optional_col(active_v1, "AdBrandName")
        .cast("string")
        .alias("advert_brand_name"),
        _optional_col(active_v1, "Page").cast("string").alias("page_path"),
        _optional_col(active_v1, "TemplateName")
        .cast("string")
        .alias("template_name"),
        _optional_col(active_v1, "rundate")
        .cast("date")
        .alias("source_rundate"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    control_v2 = spark.table(
        source_table(
            source_catalog,
            source_schema,
            "next_uk_nextads_control_sheet_v2",
        )
    ).where(F.to_date("rundate") == source_rundate)
    active_v2 = (
        control_v2.where(F.col("UniqueAdID").isNotNull())
        .where(F.col("PageType").isNotNull())
        .where(
            F.col("StartDate").isNull()
            | (F.col("StartDate") <= F.lit(reference_date).cast("date"))
        )
        .where(
            F.col("EndDate").isNull()
            | (F.col("EndDate") >= F.lit(reference_date).cast("date"))
        )
    )
    v2 = active_v2.select(
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("PageType").cast("string").alias("location"),
        F.lit(reference_date).cast("date").alias("feature_date"),
        _optional_col(active_v2, "CampaignNumber")
        .cast("string")
        .alias("campaign_id"),
        _optional_col(active_v2, "URL").cast("string").alias("advert_url"),
        F.lit(None).cast("string").alias("product_urls"),
        _optional_col(active_v2, "Items")
        .cast("string")
        .alias("control_sheet_items"),
        _optional_col(active_v2, "Title")
        .cast("string")
        .alias("advert_title"),
        F.lit(None).cast("string").alias("headline"),
        F.lit(None).cast("string").alias("subtext"),
        F.lit(None).cast("string").alias("cta"),
        _optional_col(active_v2, "Themes")
        .cast("string")
        .alias("advert_theme"),
        F.coalesce(
            _optional_col(active_v2, "AdDriver"),
            _optional_col(active_v2, "AlgoDivision"),
        )
        .cast("string")
        .alias("advert_category"),
        _optional_col(active_v2, "Brand")
        .cast("string")
        .alias("advert_brand_name"),
        _optional_col(active_v2, "CMSPageID")
        .cast("string")
        .alias("page_path"),
        _optional_col(active_v2, "TemplateName")
        .cast("string")
        .alias("template_name"),
        _optional_col(active_v2, "rundate")
        .cast("date")
        .alias("source_rundate"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    return v1.unionByName(v2).dropDuplicates(
        ["advert_id", "location", "feature_date"]
    )


def build_item_attributes_df(spark, source_catalog: str, source_schema: str):
    """Build one row per item from latest item attributes."""
    from pyspark.sql import functions as F

    source = spark.table(
        source_table(source_catalog, source_schema, "next_uk_nextads_item_attributes_latest")
    )
    source = source.select(
        F.col("pid").cast("string").alias("item_id"),
        F.lower(F.col("attribute").cast("string")).alias("attribute"),
        F.col("value").cast("string").alias("value"),
        _optional_col(source, "rundate").cast("timestamp").alias("source_updated_at"),
    ).where(F.col("item_id").isNotNull())

    pivoted = source.groupBy("item_id").agg(
        F.first(F.when(F.col("attribute").isin("brand", "brandname"), F.col("value")), ignorenulls=True).alias("brand"),
        F.first(F.when(F.col("attribute").isin("use", "item_use"), F.col("value")), ignorenulls=True).alias("item_use"),
        F.first(F.when(F.col("attribute").isin("colour", "color"), F.col("value")), ignorenulls=True).alias("colour"),
        F.first(F.when(F.col("attribute") == "style", F.col("value")), ignorenulls=True).alias("style"),
        F.first(F.when(F.col("attribute") == "category", F.col("value")), ignorenulls=True).alias("category"),
        F.first(F.when(F.col("attribute") == "department", F.col("value")), ignorenulls=True).alias("department"),
        F.first(F.when(F.col("attribute") == "gender", F.col("value")), ignorenulls=True).alias("gender"),
        F.first(F.when(F.col("attribute") == "pattern", F.col("value")), ignorenulls=True).alias("pattern"),
        F.first(F.when(F.col("attribute") == "fit", F.col("value")), ignorenulls=True).alias("fit"),
        F.first(F.when(F.col("attribute") == "room", F.col("value")), ignorenulls=True).alias("room"),
        F.first(F.when(F.col("attribute") == "activity", F.col("value")), ignorenulls=True).alias("activity"),
        F.first(F.when(F.col("attribute") == "material", F.col("value")), ignorenulls=True).alias("material"),
        F.first(F.when(F.col("attribute") == "collaboration", F.col("value")), ignorenulls=True).alias("collaboration"),
        F.max("source_updated_at").alias("source_updated_at"),
    )
    return (
        pivoted.withColumn(
            "attribute_value_map",
            F.create_map(
                F.lit("brand"), F.col("brand"),
                F.lit("use"), F.col("item_use"),
                F.lit("colour"), F.col("colour"),
                F.lit("style"), F.col("style"),
                F.lit("category"), F.col("category"),
                F.lit("department"), F.col("department"),
                F.lit("gender"), F.col("gender"),
            ),
        )
        .withColumn(
            "item_text_corpus",
            F.concat_ws(
                " ",
                "brand",
                "item_use",
                "colour",
                "style",
                "category",
                "department",
                "gender",
            ),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )


def build_advert_attribute_profile_df(spark, source_catalog: str, source_schema: str, reference_date: str):
    """Build advert attribute rollups from control-sheet advert items and item attributes."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    control = build_advert_core_df(spark, source_catalog, source_schema, reference_date)
    ad_items = spark.table(source_table(source_catalog, source_schema, "next_uk_nextads_ad_items"))
    item_attributes = build_item_attributes_df(spark, source_catalog, source_schema)

    exploded_items = (
        ad_items.select(
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            F.explode_outer("RepresentativeItems").alias("item_id"),
        )
        .where(F.col("item_id").isNotNull())
        .dropDuplicates(["advert_id", "item_id"])
    )
    control_ads = control.select("advert_id", "campaign_id", "feature_date").dropDuplicates(["advert_id", "feature_date"])
    item_profiles = exploded_items.join(item_attributes, on="item_id", how="left")

    def top_value(column_name: str):
        counts = (
            item_profiles.where(F.col(column_name).isNotNull())
            .groupBy("advert_id", F.col(column_name).alias("value"))
            .agg(F.count("*").alias("value_count"))
        )
        window = Window.partitionBy("advert_id").orderBy(F.col("value_count").desc(), F.col("value").asc())
        return counts.withColumn("_rank", F.row_number().over(window)).where(F.col("_rank") == 1).select("advert_id", F.col("value").alias(f"top_{column_name}"))

    brand_map = (
        item_profiles.where(F.col("brand").isNotNull())
        .groupBy("advert_id", "brand")
        .agg(F.count("*").cast("double").alias("weight"))
        .groupBy("advert_id")
        .agg(_weighted_map("brand", "weight").alias("brand_profile_map"))
    )
    category_map = (
        item_profiles.where(F.col("category").isNotNull())
        .groupBy("advert_id", "category")
        .agg(F.count("*").cast("double").alias("weight"))
        .groupBy("advert_id")
        .agg(_weighted_map("category", "weight").alias("category_profile_map"))
    )
    rollup = item_profiles.groupBy("advert_id").agg(
        F.countDistinct("item_id").alias("advert_item_count"),
        F.count("*").cast("double").alias("advert_item_weight_sum"),
        F.countDistinct("item_id").alias("attribute_profile_attribute_count"),
    )

    result = control_ads.join(rollup, "advert_id", "left")
    for column_name in ("brand", "item_use", "colour", "style", "category", "department", "gender"):
        result = result.join(top_value(column_name), "advert_id", "left")
    return (
        result.join(brand_map, "advert_id", "left")
        .join(category_map, "advert_id", "left")
        .select(
            "advert_id",
            "feature_date",
            "campaign_id",
            F.lit(None).cast("bigint").alias("advert_active_location_count"),
            (F.col("advert_item_count") > 0).alias("has_item_attribute_profile"),
            F.col("attribute_profile_attribute_count").cast("bigint"),
            F.col("advert_item_count").cast("bigint").alias("attribute_profile_value_count"),
            F.col("advert_item_count").cast("bigint"),
            F.col("advert_item_weight_sum").cast("double"),
            F.col("top_brand"),
            F.col("top_item_use").alias("top_use"),
            F.col("top_colour"),
            F.col("top_style"),
            F.col("top_category"),
            F.col("top_department"),
            F.col("top_gender"),
            "brand_profile_map",
            "category_profile_map",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["advert_id", "feature_date"])
    )


SHOPPING_BAG_LABEL_HORIZONS = (0, 1, 7)
SHOPPING_BAG_WEB_PATH = "/shoppingbag"
SHOPPING_BAG_APP_SCREEN = "Cart"
SHOPPING_BAG_V2_PAGE_TYPE = "ShoppingBagPage"
SHOPPING_BAG_APP_V1_ROUTE_PREFIX = "NEXT-ADS-SB |"
SHOPPING_BAG_APP_V2_ROUTE_PREFIX = "SHOPPINGBAGPAGE |"
SHOPPING_BAG_MASID_REFRESH_HOUR = 4
SHOPPING_BAG_SERVED_TREATMENTS = frozenset(
    {"best", "bestprem", "bestchallenger", "bestchallengerprem"}
)


def classify_shopping_bag_event_route(
    platform: str,
    route_tag: str | None,
) -> str | None:
    """Resolve the assignment route declared by one impression/click tag."""
    normalized_platform = platform.strip().upper()
    if normalized_platform == "WEB":
        return "v1"
    if normalized_platform != "APP":
        return None

    normalized_tag = (route_tag or "").strip().upper()
    if normalized_tag.startswith(SHOPPING_BAG_APP_V2_ROUTE_PREFIX):
        return "v2"
    if normalized_tag.startswith(SHOPPING_BAG_APP_V1_ROUTE_PREFIX):
        return "v1"
    return None


def normalize_shopping_bag_advert_id(value: str | None) -> str | None:
    """Normalize only the known image/static suffixes in action tags."""
    import re

    if value is None:
        return None
    normalized = re.sub(r"[?#].*$", "", value.strip())
    normalized = re.sub(
        r"(?i)\.(?:jpg|jpeg|png|webp)$",
        "",
        normalized,
    )
    return re.sub(r"(?i)_static$", "", normalized)


def shopping_bag_label_is_mature(
    session_date,
    label_horizon_days: int,
    as_of_date,
) -> bool:
    """Return whether a complete exposure-day attribution window has closed."""
    from datetime import timedelta

    if label_horizon_days not in SHOPPING_BAG_LABEL_HORIZONS:
        raise ValueError(
            "Shopping Bag label horizon must be one of "
            f"{SHOPPING_BAG_LABEL_HORIZONS}"
        )
    return as_of_date >= session_date + timedelta(
        days=label_horizon_days + 1
    )


def shopping_bag_assignment_is_eligible(
    measurement_advert_id: str | None,
    assigned_advert_id: str | None,
    treatment: str | None,
) -> bool:
    """Apply the observable-label assignment exclusions without Spark."""
    return (
        classify_shopping_bag_assignment_exclusion(
            measurement_advert_id,
            assigned_advert_id,
            treatment,
        )
        is None
    )


def classify_shopping_bag_assignment_exclusion(
    measurement_advert_id: str | None,
    assigned_advert_id: str | None,
    treatment: str | None,
) -> str | None:
    """Return one stable audit reason for an assignment rejected from labels."""
    import re

    advert_ids = {
        (measurement_advert_id or "").strip(),
        (assigned_advert_id or "").strip(),
    }
    normalized_treatment = (treatment or "").strip().lower()
    if advert_ids.intersection({"NoAd", "NoAds", "NoAdFound"}):
        return "NO_AD"
    if "AdSuppressed" in advert_ids or normalized_treatment == "adsuppressed":
        return "SUPPRESSED"
    if normalized_treatment == "control":
        return "CONTROL"
    if normalized_treatment not in SHOPPING_BAG_SERVED_TREATMENTS:
        return "UNKNOWN_TREATMENT"
    if not all(re.match(r"^P\d+_C\d+", value) for value in advert_ids):
        return "UNRESOLVED_ADVERT"
    return None


def _campaign_key(column_expr):
    from pyspark.sql import functions as F

    return F.regexp_extract(column_expr.cast("string"), r"^(P\d+_C\d+)", 1)


def _normalise_observed_advert_id(column_expr):
    """Remove known event-tag rendering suffixes without changing identity."""
    from pyspark.sql import functions as F

    value = F.regexp_replace(
        F.trim(column_expr.cast("string")), r"[?#].*$", ""
    )
    value = F.regexp_replace(
        value,
        r"(?i)\.(?:jpg|jpeg|png|webp)$",
        "",
    )
    return F.regexp_replace(value, r"(?i)_static$", "")


def _event_id(*columns):
    from pyspark.sql import functions as F

    expressions = [
        F.col(column) if isinstance(column, str) else column
        for column in columns
    ]
    return F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(column.cast("string"), F.lit("<null>"))
                for column in expressions
            ],
        ),
        256,
    )


def _normalise_label_sessions(
    sessions,
    rpid_lookup,
    platform: str,
    *,
    include_excluded: bool = False,
):
    from pyspark.sql import functions as F

    normalized = sessions.select(
        F.col("date").cast("date").alias("session_date"),
        F.col("UniqueVisitID").cast("string").alias("session_id"),
        _optional_col(
            sessions,
            ["AccountNumber_RPID", "account_number"],
        )
        .cast("string")
        .alias("direct_account_number"),
        _optional_col(sessions, ["RPID", "rpid"])
        .cast("string")
        .alias("rpid"),
        _optional_col(sessions, ["Device", "device"])
        .cast("string")
        .alias("device"),
        _optional_col(
            sessions,
            ["operating_system", "OperatingSystem", "OS"],
        )
        .cast("string")
        .alias("operating_system"),
        _optional_col(
            sessions,
            ["FirstTimestamp", "SessionStart", "first_timestamp"],
        )
        .cast("timestamp")
        .alias("session_start_timestamp"),
        _optional_col(
            sessions,
            ["visitstarthour", "VisitStartHour", "visit_start_hour"],
        )
        .cast("int")
        .alias("session_start_hour"),
        F.lit(platform).alias("platform"),
    )
    candidates = (
        normalized.withColumn(
            "session_start_timestamp",
            F.coalesce(
                F.col("session_start_timestamp"),
                F.to_timestamp(
                    F.concat_ws(
                        " ",
                        F.date_format("session_date", "yyyy-MM-dd"),
                        F.format_string(
                            "%02d:00:00",
                            F.col("session_start_hour"),
                        ),
                    )
                ),
            ),
        )
        .join(rpid_lookup, "rpid", "left")
        .withColumn(
            "account_number",
            F.coalesce("direct_account_number", "mapped_account_number"),
        )
        .withColumn(
            "session_exclusion_reason",
            F.when(F.col("session_id").isNull(), F.lit("MISSING_SESSION_ID"))
            .when(
                F.col("account_number").isNull(),
                F.lit("UNMAPPED_ACCOUNT"),
            )
            .when(
                F.col("session_start_timestamp").isNull(),
                F.lit("MISSING_SESSION_START"),
            )
            .when(
                F.hour("session_start_timestamp")
                < F.lit(SHOPPING_BAG_MASID_REFRESH_HOUR),
                F.lit("PRE_REFRESH"),
            ),
        )
        .select(
            "platform",
            "account_number",
            "session_date",
            "session_id",
            "device",
            "operating_system",
            "session_start_timestamp",
            "session_exclusion_reason",
        )
        .dropDuplicates()
    )
    if include_excluded:
        return candidates
    return candidates.where(F.col("session_exclusion_reason").isNull()).drop(
        "session_exclusion_reason"
    )


def _normalise_label_actions(
    actions,
    *,
    platform: str,
    reference_date: str,
    label_end: str,
):
    from pyspark.sql import functions as F

    reference_date_lit = F.lit(reference_date).cast("date")
    label_end_lit = F.lit(label_end).cast("date")
    if platform == "WEB":
        surface_expr = F.col("PagePath").cast("string")
        surface_filter = (
            _normalise_path_expr(surface_expr)
            == F.lit(SHOPPING_BAG_WEB_PATH)
        )
    elif platform == "APP":
        surface_expr = F.col("ScreenName").cast("string")
        surface_filter = F.lower(F.trim(surface_expr)) == F.lit(
            SHOPPING_BAG_APP_SCREEN.lower()
        )
    else:
        raise ValueError(f"Unsupported label platform: {platform}")

    return (
        actions.where(
            F.col("date").cast("date").between(
                reference_date_lit,
                label_end_lit,
            )
        )
        .where(
            F.col("Action").isin(
                "Banner Impression - Next Ads",
                "Banner Click - Next Ads",
            )
        )
        .where(surface_filter)
        .select(
            F.lit(platform).alias("platform"),
            F.col("date").cast("date").alias("event_date"),
            F.col("UniqueVisitID").cast("string").alias("session_id"),
            F.col("Timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("Action").cast("string").alias("action"),
            F.col("Level2").cast("string").alias("observed_advert_id"),
            _optional_col(actions, "Level1")
            .cast("string")
            .alias("event_route_tag"),
            surface_expr.alias("page_surface"),
        )
        .where(F.col("session_id").isNotNull())
        .where(F.col("event_timestamp").isNotNull())
        .withColumn(
            "normalized_observed_advert_id",
            _normalise_observed_advert_id(F.col("observed_advert_id")),
        )
        .where(
            F.col("normalized_observed_advert_id").rlike(r"^P\d+_C\d+")
        )
        .withColumn(
            "event_route",
            F.when(F.col("platform") == "WEB", F.lit("v1"))
            .when(
                F.upper(F.coalesce("event_route_tag", F.lit(""))).startswith(
                    SHOPPING_BAG_APP_V2_ROUTE_PREFIX
                ),
                F.lit("v2"),
            )
            .when(
                F.upper(F.coalesce("event_route_tag", F.lit(""))).startswith(
                    SHOPPING_BAG_APP_V1_ROUTE_PREFIX
                ),
                F.lit("v1"),
            ),
        )
        .where(F.col("event_route").isNotNull())
        .withColumn(
            "event_cms_page_id",
            F.when(
                F.col("platform") == "APP",
                F.lower(
                    F.trim(
                        F.element_at(
                            F.split(F.col("event_route_tag"), r"\|"),
                            -1,
                        )
                    )
                ),
            ),
        )
        .where(
            (F.col("platform") == "WEB")
            | (
                F.col("event_cms_page_id").isNotNull()
                & (F.col("event_cms_page_id") != "")
            )
        )
        .withColumn(
            "raw_event_id",
            _event_id(
                F.col("platform"),
                F.col("event_date"),
                F.col("session_id"),
                F.col("event_timestamp"),
                F.col("action"),
                F.col("observed_advert_id"),
                F.col("event_route_tag"),
            ),
        )
        .dropDuplicates(["raw_event_id"])
    )


def build_raw_shopping_bag_events_df(
    web_actions,
    app_actions,
    *,
    reference_date: str,
    label_end: str,
):
    """Return normalized Shopping Bag impression/click tags before mapping."""
    return _normalise_label_actions(
        web_actions,
        platform="WEB",
        reference_date=reference_date,
        label_end=label_end,
    ).unionByName(
        _normalise_label_actions(
            app_actions,
            platform="APP",
            reference_date=reference_date,
            label_end=label_end,
        )
    )


def _valid_assignment_filter(frame):
    from pyspark.sql import functions as F

    invalid_advert_ids = ("NoAd", "NoAds", "NoAdFound", "AdSuppressed")
    return (
        frame.where(F.col("account_number").isNotNull())
        .where(F.col("measurement_advert_id").rlike(r"^P\d+_C\d+"))
        .where(F.col("assigned_advert_id").rlike(r"^P\d+_C\d+"))
        .where(~F.col("measurement_advert_id").isin(*invalid_advert_ids))
        .where(~F.col("assigned_advert_id").isin(*invalid_advert_ids))
        .where(
            F.lower(F.trim(F.col("treatment"))).isin(
                *SHOPPING_BAG_SERVED_TREATMENTS
            )
        )
    )


def _shopping_bag_v1_assignments(
    assignments,
    control_sheet,
    multipage_locations,
    reference_date: str,
    *,
    include_ineligible: bool = False,
):
    from pyspark.sql import functions as F

    assignment_date = F.date_sub(F.lit(reference_date).cast("date"), 1)
    control = (
        control_sheet.where(F.to_date("rundate") == assignment_date)
        .withColumn("session_date", F.date_add(F.to_date("rundate"), 1))
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("UniqueAdID").cast("string").alias(
                "control_measurement_advert_id"
            ),
            F.col("Page").cast("string").alias("configured_page_path"),
            _optional_col(control_sheet, "Screen")
            .cast("string")
            .alias("configured_screen"),
            _optional_col(control_sheet, "CMSPageID")
            .cast("string")
            .alias("cms_page_id"),
        )
    )
    multipage = (
        multipage_locations.where(F.to_date("rundate") == assignment_date)
        .withColumn("session_date", F.date_add(F.to_date("rundate"), 1))
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("Page").cast("string").alias("multipage_page_path"),
        )
        .where(
            _normalise_path_expr(F.col("multipage_page_path"))
            == F.lit(SHOPPING_BAG_WEB_PATH)
        )
        .dropDuplicates(["session_date", "location"])
    )
    web_shopping_bag_ads = (
        control.join(multipage, ["session_date", "location"], "left")
        .withColumn(
            "resolved_page_path",
            F.coalesce("multipage_page_path", "configured_page_path"),
        )
        .where(
            _normalise_path_expr(F.col("resolved_page_path"))
            == F.lit(SHOPPING_BAG_WEB_PATH)
        )
        .select(
            F.lit("WEB").alias("platform"),
            "session_date",
            "location",
            "control_measurement_advert_id",
            F.lit(None).cast("string").alias("cms_page_id"),
        )
        .dropDuplicates()
    )
    app_shopping_bag_ads = (
        control.where(
            F.lower(F.trim("configured_screen"))
            == F.lit(SHOPPING_BAG_APP_SCREEN.lower())
        )
        .where(F.col("cms_page_id").isNotNull())
        .where(F.trim("cms_page_id") != "")
        .select(
            F.lit("APP").alias("platform"),
            "session_date",
            "location",
            "control_measurement_advert_id",
            F.lower(F.trim("cms_page_id")).alias("cms_page_id"),
        )
        .dropDuplicates()
    )
    shopping_bag_ads = web_shopping_bag_ads.unionByName(
        app_shopping_bag_ads
    )
    normalized = (
        assignments.where(F.to_date("rundate") == assignment_date)
        .withColumn("session_date", F.date_add(F.to_date("rundate"), 1))
        .select(
            F.col("AccountNumber").cast("string").alias("account_number"),
            "session_date",
            F.to_date("rundate").alias("assignment_rundate"),
            F.col("Location").cast("string").alias("location"),
            F.lit(None).cast("int").alias("placement_rank"),
            F.col("Treatment").cast("string").alias("treatment"),
            F.col("UniqueAdIDMeasurement")
            .cast("string")
            .alias("measurement_advert_id"),
            F.col("UniqueAdIDAssigned")
            .cast("string")
            .alias("assigned_advert_id"),
        )
        .join(
            shopping_bag_ads,
            ["session_date", "location"],
            "inner",
        )
        .where(
            F.col("measurement_advert_id")
            == F.col("control_measurement_advert_id")
        )
        .drop("control_measurement_advert_id")
        .withColumn("route", F.lit("v1"))
    )
    return normalized if include_ineligible else _valid_assignment_filter(normalized)


def _shopping_bag_v2_assignments(
    assignments,
    control_sheet,
    reference_date: str,
    *,
    include_ineligible: bool = False,
):
    from pyspark.sql import functions as F

    assignment_date = F.date_sub(F.lit(reference_date).cast("date"), 1)
    control = (
        control_sheet.where(F.to_date("rundate") == assignment_date)
        .where(F.col("PageType") == SHOPPING_BAG_V2_PAGE_TYPE)
        .select(
            F.col("UniqueAdID")
            .cast("string")
            .alias("control_measurement_advert_id"),
            F.col("PageType").cast("string").alias("location"),
            F.lower(F.trim(F.col("CMSPageID").cast("string"))).alias(
                "cms_page_id"
            ),
        )
        .where(F.col("cms_page_id").isNotNull())
        .where(F.col("cms_page_id") != "")
        .dropDuplicates()
    )
    normalized = (
        assignments.where(F.to_date("rundate") == assignment_date)
        .where(F.col("PageType") == SHOPPING_BAG_V2_PAGE_TYPE)
        .withColumn("session_date", F.date_add(F.to_date("rundate"), 1))
        .select(
            F.lit("APP").alias("platform"),
            F.lit("v2").alias("route"),
            F.col("AccountNumber").cast("string").alias("account_number"),
            "session_date",
            F.to_date("rundate").alias("assignment_rundate"),
            F.col("PageType").cast("string").alias("location"),
            F.col("Rank").cast("int").alias("placement_rank"),
            F.col("Treatment").cast("string").alias("treatment"),
            F.col("UniqueAdIDMeasurement")
            .cast("string")
            .alias("measurement_advert_id"),
            F.col("UniqueAdIDAssigned")
            .cast("string")
            .alias("assigned_advert_id"),
        )
        .join(control, ["location"], "inner")
        .where(
            F.col("measurement_advert_id")
            == F.col("control_measurement_advert_id")
        )
        .drop("control_measurement_advert_id")
    )
    return normalized if include_ineligible else _valid_assignment_filter(normalized)


def build_observed_shopping_bag_click_labels_df(
    *,
    spark,
    web_sessions,
    app_sessions,
    rpid_accounts,
    web_actions,
    app_actions,
    v1_assignments,
    v2_assignments,
    v1_control_sheet,
    v2_control_sheet,
    v1_multipage_locations,
    reference_date: str,
    as_of_date: str,
):
    """Build auditable labels from real Shopping Bag impression events.

    Web `/shoppingbag` events resolve against the V1 daily assignment history.
    App `Cart` route tags resolve `NEXT-ADS-SB |` through V1 and
    `ShoppingBagPage |` through V2. Exact normalized measurement/assigned advert
    matches take precedence over campaign matches; ambiguous best matches are
    excluded. Each click is assigned to one exposure per horizon: exact advert
    first, then the most recent preceding exposure.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rpid_lookup = (
        rpid_accounts.select(
            F.col("roamingprofileid").cast("string").alias("rpid"),
            F.col("account_number")
            .cast("string")
            .alias("mapped_account_number"),
        )
        .where(F.col("rpid").isNotNull())
        .dropDuplicates(["rpid", "mapped_account_number"])
    )
    sessions = _normalise_label_sessions(
        web_sessions, rpid_lookup, "WEB"
    ).unionByName(
        _normalise_label_sessions(app_sessions, rpid_lookup, "APP")
    )
    ambiguous_sessions = (
        sessions.groupBy("platform", "session_date", "session_id")
        .agg(F.countDistinct("account_number").alias("account_count"))
        .where(F.col("account_count") != 1)
        .select("platform", "session_date", "session_id")
    )
    sessions = sessions.join(
        ambiguous_sessions,
        ["platform", "session_date", "session_id"],
        "leftanti",
    ).dropDuplicates(["platform", "session_date", "session_id"])

    actions = build_raw_shopping_bag_events_df(
        web_actions,
        app_actions,
        reference_date=reference_date,
        label_end=as_of_date,
    )
    mapped_actions = (
        actions.alias("act")
        .join(
            sessions.alias("sess"),
            (F.col("act.platform") == F.col("sess.platform"))
            & (F.col("act.event_date") == F.col("sess.session_date"))
            & (F.col("act.session_id") == F.col("sess.session_id")),
            "inner",
        )
        .select(
            "act.*",
            F.col("sess.account_number"),
            F.col("sess.device"),
            F.col("sess.operating_system"),
            F.col("sess.session_start_timestamp"),
        )
    )
    raw_impressions = mapped_actions.where(
        F.col("action") == "Banner Impression - Next Ads"
    ).where(F.col("event_date") == F.lit(reference_date).cast("date"))
    raw_clicks = mapped_actions.where(
        F.col("action") == "Banner Click - Next Ads"
    )

    assignments = _shopping_bag_v1_assignments(
        v1_assignments,
        v1_control_sheet,
        v1_multipage_locations,
        reference_date,
    ).unionByName(
        _shopping_bag_v2_assignments(
            v2_assignments,
            v2_control_sheet,
            reference_date,
        )
    )
    exposure_candidates = (
        raw_impressions.alias("imp")
        .join(
            assignments.alias("asg"),
            (F.col("imp.platform") == F.col("asg.platform"))
            & (F.col("imp.event_route") == F.col("asg.route"))
            & (F.col("imp.account_number") == F.col("asg.account_number"))
            & (F.col("imp.event_date") == F.col("asg.session_date"))
            & (
                (F.col("imp.platform") == F.lit("WEB"))
                | (
                    F.col("imp.event_cms_page_id")
                    == F.col("asg.cms_page_id")
                )
            ),
            "inner",
        )
        .withColumn(
            "exposure_match_priority",
            F.when(
                F.col("imp.normalized_observed_advert_id")
                == _normalise_observed_advert_id(
                    F.col("asg.measurement_advert_id")
                ),
                F.lit(0),
            )
            .when(
                F.col("imp.normalized_observed_advert_id")
                == _normalise_observed_advert_id(
                    F.col("asg.assigned_advert_id")
                ),
                F.lit(1),
            )
            .when(
                (
                    _campaign_key(F.col("imp.normalized_observed_advert_id"))
                    != ""
                )
                & (
                    (
                        _campaign_key(
                            F.col("imp.normalized_observed_advert_id")
                        )
                        == _campaign_key(F.col("asg.measurement_advert_id"))
                    )
                    | (
                        _campaign_key(
                            F.col("imp.normalized_observed_advert_id")
                        )
                        == _campaign_key(F.col("asg.assigned_advert_id"))
                    )
                ),
                F.lit(2),
            ),
        )
        .where(F.col("exposure_match_priority").isNotNull())
        .withColumn(
            "best_priority",
            F.min("exposure_match_priority").over(
                Window.partitionBy(F.col("imp.raw_event_id"))
            ),
        )
        .where(F.col("exposure_match_priority") == F.col("best_priority"))
        .withColumn(
            "best_match_count",
            F.count("*").over(
                Window.partitionBy(F.col("imp.raw_event_id"))
            ),
        )
        .where(F.col("best_match_count") == 1)
    )
    exposures = (
        exposure_candidates.select(
            F.col("imp.raw_event_id").alias("impression_event_id"),
            F.col("imp.account_number"),
            F.col("imp.observed_advert_id"),
            F.col("imp.normalized_observed_advert_id"),
            F.col("asg.measurement_advert_id").alias("advert_id"),
            F.col("asg.measurement_advert_id"),
            F.col("asg.assigned_advert_id"),
            F.col("asg.route"),
            F.col("imp.platform"),
            F.col("asg.location"),
            F.col("asg.placement_rank"),
            F.col("imp.event_date").alias("session_date"),
            F.col("imp.session_id"),
            F.col("imp.event_timestamp").alias("exposure_timestamp"),
            F.col("imp.event_route_tag"),
            F.col("imp.event_cms_page_id"),
            F.col("asg.cms_page_id").alias("assignment_cms_page_id"),
            F.col("imp.device"),
            F.col("imp.operating_system"),
            F.col("imp.session_start_timestamp"),
            F.col("imp.page_surface"),
            F.col("asg.treatment"),
            F.col("asg.assignment_rundate"),
            F.when(
                F.col("exposure_match_priority") == 0,
                F.lit("MEASUREMENT_EXACT"),
            )
            .when(
                F.col("exposure_match_priority") == 1,
                F.lit("ASSIGNED_EXACT"),
            )
            .otherwise(F.lit("CAMPAIGN"))
            .alias("exposure_match_type"),
        )
        .withColumn(
            "exposure_id",
            _event_id(
                "impression_event_id",
                "route",
                "location",
                "placement_rank",
                "measurement_advert_id",
                "assigned_advert_id",
            ),
        )
    )

    horizons = spark.createDataFrame(
        [(horizon,) for horizon in SHOPPING_BAG_LABEL_HORIZONS],
        "label_horizon_days int",
    )
    as_of_date_expr = F.lit(as_of_date).cast("date")
    exposure_horizons = (
        exposures.crossJoin(horizons)
        .withColumn(
            "label_window_end_timestamp",
            F.when(
                F.col("label_horizon_days") == 0,
                F.date_add("session_date", 1).cast("timestamp"),
            )
            .when(
                F.col("label_horizon_days") == 1,
                F.col("exposure_timestamp") + F.expr("INTERVAL 24 HOURS"),
            )
            .otherwise(
                F.col("exposure_timestamp") + F.expr("INTERVAL 7 DAYS")
            ),
        )
        .withColumn(
            "label_maturity_date",
            F.expr("date_add(session_date, label_horizon_days + 1)"),
        )
        .withColumn(
            "label_is_mature",
            as_of_date_expr >= F.col("label_maturity_date"),
        )
        .withColumn("label_cutoff_date", as_of_date_expr)
        .where(F.col("label_is_mature"))
    )
    clicks = (
        raw_clicks.select(
            F.col("raw_event_id").alias("click_id"),
            "account_number",
            "platform",
            "event_route",
            "event_route_tag",
            "event_cms_page_id",
            F.col("observed_advert_id").alias("click_advert_id"),
            F.col("normalized_observed_advert_id").alias(
                "normalized_click_advert_id"
            ),
            F.col("session_id").alias("click_session_id"),
            F.col("event_timestamp").alias("click_timestamp"),
        )
        .dropDuplicates(["click_id"])
    )
    eligible_clicks = (
        exposure_horizons.alias("exp")
        .join(
            clicks.alias("clk"),
            (F.col("exp.account_number") == F.col("clk.account_number"))
            & (F.col("exp.platform") == F.col("clk.platform"))
            & (F.col("exp.route") == F.col("clk.event_route"))
            & (
                (F.col("exp.platform") == F.lit("WEB"))
                | (
                    F.col("exp.event_cms_page_id")
                    == F.col("clk.event_cms_page_id")
                )
            )
            & (
                F.col("clk.click_timestamp")
                > F.col("exp.exposure_timestamp")
            )
            & (
                F.col("clk.click_timestamp")
                <= F.col("exp.label_window_end_timestamp")
            )
            & (
                (F.col("exp.label_horizon_days") != 0)
                | (
                    F.col("clk.click_session_id")
                    == F.col("exp.session_id")
                )
            ),
            "inner",
        )
        .withColumn(
            "click_match_priority",
            F.when(
                (
                    F.col("clk.normalized_click_advert_id")
                    == F.col("exp.normalized_observed_advert_id")
                )
                | (
                    F.col("clk.normalized_click_advert_id")
                    == _normalise_observed_advert_id(
                        F.col("exp.measurement_advert_id")
                    )
                )
                | (
                    F.col("clk.normalized_click_advert_id")
                    == _normalise_observed_advert_id(
                        F.col("exp.assigned_advert_id")
                    )
                ),
                F.lit(0),
            ).when(
                (_campaign_key(F.col("clk.normalized_click_advert_id")) != "")
                & (
                    (
                        _campaign_key(F.col("clk.normalized_click_advert_id"))
                        == _campaign_key(
                            F.col("exp.normalized_observed_advert_id")
                        )
                    )
                    | (
                        _campaign_key(F.col("clk.normalized_click_advert_id"))
                        == _campaign_key(F.col("exp.measurement_advert_id"))
                    )
                    | (
                        _campaign_key(F.col("clk.normalized_click_advert_id"))
                        == _campaign_key(F.col("exp.assigned_advert_id"))
                    )
                ),
                F.lit(1),
            ),
        )
        .where(F.col("click_match_priority").isNotNull())
        .withColumn(
            "click_exposure_rank",
            F.row_number().over(
                Window.partitionBy(
                    F.col("clk.click_id"),
                    F.col("exp.label_horizon_days"),
                ).orderBy(
                    F.col("click_match_priority").asc(),
                    F.col("exp.exposure_timestamp").desc(),
                    F.col("exp.exposure_id").asc(),
                )
            ),
        )
        .where(F.col("click_exposure_rank") == 1)
        .select(
            F.col("exp.exposure_id"),
            F.col("exp.label_horizon_days"),
            F.col("clk.click_id"),
            F.col("clk.click_advert_id"),
            F.col("clk.event_route_tag").alias("click_route_tag"),
            F.col("clk.click_timestamp"),
            F.when(
                F.col("click_match_priority") == 0,
                F.lit("EXACT_AD"),
            )
            .otherwise(F.lit("CAMPAIGN"))
            .alias("click_match_type"),
        )
    )
    click_summary = eligible_clicks.groupBy(
        "exposure_id", "label_horizon_days"
    ).agg(
        F.countDistinct("click_id").cast("bigint").alias("click_count"),
        F.min(
            F.struct(
                "click_timestamp",
                "click_id",
                "click_advert_id",
                "click_route_tag",
                "click_match_type",
            )
        ).alias("first_click"),
    )
    return (
        exposure_horizons.join(
            click_summary,
            ["exposure_id", "label_horizon_days"],
            "left",
        )
        .withColumn("impression_count", F.lit(1).cast("bigint"))
        .withColumn(
            "click_count",
            F.coalesce("click_count", F.lit(0)).cast("bigint"),
        )
        .withColumn(
            "clicked", F.when(F.col("click_count") > 0, 1).otherwise(0)
        )
        .withColumn(
            "first_click_timestamp", F.col("first_click.click_timestamp")
        )
        .withColumn("first_click_id", F.col("first_click.click_id"))
        .withColumn(
            "first_click_advert_id",
            F.col("first_click.click_advert_id"),
        )
        .withColumn("first_click_route_tag", F.col("first_click.click_route_tag"))
        .withColumn("click_match_type", F.col("first_click.click_match_type"))
        .drop("first_click")
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )


def read_shopping_bag_click_label_sources(
    spark,
    source_catalog: str,
    source_schema: str,
):
    """Read the repository-owned web, app and assignment label sources."""
    table = lambda name: spark.table(  # noqa: E731
        source_table(source_catalog, source_schema, name)
    )
    return {
        "web_sessions": table("bq_sessions_next_uk"),
        "app_sessions": table("bq_sessions_next_uk_app"),
        "rpid_accounts": table("rpid_with_accounts"),
        "web_actions": table("bq_actions_next_uk"),
        "app_actions": table("bq_actions_next_uk_app"),
        "v1_assignments": table("next_uk_nextads_assignments"),
        "v2_assignments": table("next_uk_nextads_assignments_v2"),
        "v1_control_sheet": table("next_uk_nextads_control_sheet"),
        "v2_control_sheet": table("next_uk_nextads_control_sheet_v2"),
        "v1_multipage_locations": table(
            "next_uk_nextads_multipage_locations"
        ),
    }


def build_click_labels_df(
    spark,
    source_catalog: str,
    source_schema: str,
    reference_date: str,
):
    """Build the existing inferred Shopping Bag compatibility labels."""
    from pyspark.sql import functions as F

    session_date_lit = F.lit(reference_date).cast("date")
    assignment_rundate = F.date_sub(session_date_lit, 1)
    click_end_date = F.date_add(session_date_lit, 7)
    shopping_bag_path = F.lit("/shoppingbag")

    sessions = spark.table(
        source_table(source_catalog, source_schema, "bq_sessions_next_uk")
    )
    rpid_lookup = (
        spark.table(
            source_table(source_catalog, source_schema, "rpid_with_accounts")
        )
        .select(
            F.col("account_number").cast("string").alias("account_number"),
            F.col("roamingprofileid").cast("string").alias("rpid"),
        )
        .where(F.col("rpid").isNotNull())
        .dropDuplicates(["account_number", "rpid"])
    )
    session_accounts = (
        sessions.where(
            (F.col("date") >= session_date_lit)
            & (F.col("date") <= click_end_date)
        )
        .select(
            F.col("date").cast("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("RPID").cast("string").alias("rpid"),
        )
        .join(rpid_lookup, "rpid", "inner")
        .select("account_number", "session_date", "unique_visit_id")
        .dropDuplicates()
    )
    sb_page_visits = (
        spark.table(
            source_table(
                source_catalog,
                source_schema,
                "bq_pages_next_uk",
            )
        )
        .where(F.col("date") == session_date_lit)
        .select(
            F.col("date").cast("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("PagePath").cast("string").alias("page_path"),
            F.coalesce(F.col("FirstTimestamp"), F.col("LastTimestamp"))
            .cast("timestamp")
            .alias("event_timestamp"),
        )
        .where(
            _normalise_path_expr(F.col("page_path")) == shopping_bag_path
        )
        .join(
            session_accounts,
            ["session_date", "unique_visit_id"],
            "inner",
        )
        .where(F.col("event_timestamp").isNotNull())
        .dropDuplicates(
            [
                "account_number",
                "session_date",
                "unique_visit_id",
                "event_timestamp",
            ]
        )
    )
    sb_account_days = sb_page_visits.select(
        "account_number",
        "session_date",
    ).dropDuplicates()

    control_sheet = (
        spark.table(
            source_table(
                source_catalog,
                source_schema,
                "next_uk_nextads_control_sheet",
            )
        )
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn(
            "session_date", F.date_add(F.to_date(F.col("rundate")), 1)
        )
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            F.col("Page").cast("string").alias("page_path"),
        )
        .where(F.col("advert_id").rlike("^P"))
    )
    multipage_shopping_bag = (
        spark.table(
            source_table(
                source_catalog,
                source_schema,
                "next_uk_nextads_multipage_locations",
            )
        )
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn(
            "session_date", F.date_add(F.to_date(F.col("rundate")), 1)
        )
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("Page").cast("string").alias("multipage_page_path"),
        )
        .where(
            _normalise_path_expr(F.col("multipage_page_path"))
            == shopping_bag_path
        )
        .dropDuplicates(["session_date", "location"])
    )
    sb_ad_metadata = (
        control_sheet.join(
            multipage_shopping_bag,
            on=["session_date", "location"],
            how="left",
        )
        .withColumn(
            "configured_page_path",
            F.coalesce(F.col("multipage_page_path"), F.col("page_path")),
        )
        .where(
            _normalise_path_expr(F.col("configured_page_path"))
            == shopping_bag_path
        )
        .select("session_date", "location", "advert_id")
        .dropDuplicates(["session_date", "location", "advert_id"])
    )

    assignments = (
        spark.table(
            source_table(
                source_catalog,
                source_schema,
                "next_uk_nextads_assignments",
            )
        )
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn(
            "session_date", F.date_add(F.to_date(F.col("rundate")), 1)
        )
        .select(
            F.col("AccountNumber").cast("string").alias("account_number"),
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("Treatment").cast("string").alias("treatment"),
            F.col("UniqueAdIDMeasurement")
            .cast("string")
            .alias("advert_id"),
            F.col("UniqueAdIDAssigned")
            .cast("string")
            .alias("assigned_advert_id"),
        )
        .where(F.col("treatment") != "AdSuppressed")
        .where(F.col("account_number").isNotNull())
        .where(F.col("advert_id").isNotNull())
        .where(F.col("advert_id") != "NoAdFound")
        .where(F.col("advert_id").rlike("^P"))
        .join(sb_account_days, ["account_number", "session_date"], "inner")
        .join(
            sb_ad_metadata,
            ["session_date", "location", "advert_id"],
            "inner",
        )
    )
    exposures = (
        sb_page_visits.join(
            assignments,
            ["account_number", "session_date"],
            "inner",
        )
        .groupBy(
            "account_number",
            "session_date",
            "unique_visit_id",
            "location",
            "advert_id",
            "assigned_advert_id",
        )
        .agg(F.min("event_timestamp").alias("exposure_timestamp"))
        .withColumn(
            "campaign_key",
            F.regexp_extract("advert_id", r"^(P\d+_C\d+)", 1),
        )
        .withColumn(
            "assigned_campaign_key",
            F.regexp_extract("assigned_advert_id", r"^(P\d+_C\d+)", 1),
        )
        .where(
            (F.col("campaign_key") != "")
            | (F.col("assigned_campaign_key") != "")
        )
    )

    actions = spark.table(
        source_table(source_catalog, source_schema, "bq_actions_next_uk")
    )
    raw_clicks = (
        actions.where(
            (F.col("date") >= session_date_lit)
            & (F.col("date") <= F.date_add(session_date_lit, 7))
        )
        .where(F.col("Action") == "Banner Click - Next Ads")
        .where(F.col("Level2").cast("string").rlike("^P"))
        .where(
            _normalise_path_expr(F.col("PagePath")) == shopping_bag_path
        )
        .select(
            F.col("date").cast("date").alias("click_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("Timestamp").cast("timestamp").alias("click_timestamp"),
            F.col("Level2").cast("string").alias("click_advert_id"),
        )
    )
    clicks = (
        raw_clicks.alias("act")
        .join(
            session_accounts.alias("sess"),
            (F.col("act.click_date") == F.col("sess.session_date"))
            & (
                F.col("act.unique_visit_id")
                == F.col("sess.unique_visit_id")
            ),
            "inner",
        )
        .select(
            F.col("sess.account_number"),
            F.col("act.unique_visit_id").alias("click_unique_visit_id"),
            F.col("act.click_timestamp"),
            F.col("act.click_advert_id"),
            F.regexp_extract(
                "act.click_advert_id", r"^(P\d+_C\d+)", 1
            ).alias("click_campaign_key"),
        )
        .where(F.col("click_timestamp").isNotNull())
        .where(F.col("click_campaign_key") != "")
        .dropDuplicates(
            ["account_number", "click_advert_id", "click_timestamp"]
        )
    )
    horizons = spark.createDataFrame(
        [(0,), (1,), (7,)], ["label_horizon_days"]
    )
    impressions = exposures.crossJoin(horizons)
    labelled = (
        impressions.alias("imp")
        .join(
            clicks.alias("clk"),
            (F.col("imp.account_number") == F.col("clk.account_number"))
            & (
                (F.col("imp.campaign_key") == F.col("clk.click_campaign_key"))
                | (
                    F.col("imp.assigned_campaign_key")
                    == F.col("clk.click_campaign_key")
                )
            )
            & (F.col("clk.click_timestamp") >= F.col("imp.exposure_timestamp"))
            & (
                (
                    (F.col("imp.label_horizon_days") == F.lit(0))
                    & (
                        F.col("clk.click_unique_visit_id")
                        == F.col("imp.unique_visit_id")
                    )
                )
                | (
                    (F.col("imp.label_horizon_days") == F.lit(1))
                    & (
                        F.col("clk.click_timestamp")
                        <= F.col("imp.exposure_timestamp")
                        + F.expr("INTERVAL 24 HOURS")
                    )
                )
                | (
                    (F.col("imp.label_horizon_days") == F.lit(7))
                    & (
                        F.col("clk.click_timestamp")
                        <= F.col("imp.exposure_timestamp")
                        + F.expr("INTERVAL 7 DAYS")
                    )
                )
            ),
            "left",
        )
        .groupBy(
            F.col("imp.account_number").alias("account_number"),
            F.col("imp.advert_id").alias("advert_id"),
            F.col("imp.location").alias("location"),
            F.col("imp.session_date").alias("session_date"),
            F.col("imp.label_horizon_days").alias("label_horizon_days"),
        )
        .agg(
            F.countDistinct(F.col("imp.unique_visit_id"))
            .cast("bigint")
            .alias("impression_count"),
            F.countDistinct(F.col("clk.click_timestamp"))
            .cast("bigint")
            .alias("click_count"),
            F.min("clk.click_timestamp").alias("first_click_timestamp"),
        )
    )
    return (
        labelled.withColumn(
            "clicked", F.when(F.col("click_count") > 0, 1).otherwise(0)
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
