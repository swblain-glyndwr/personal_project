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
    """Build active advert metadata from the production control sheet."""
    from pyspark.sql import functions as F

    control = spark.table(
        source_table(source_catalog, source_schema, "next_uk_nextads_control_sheet_latest")
    )
    active = (
        control.where(F.col("UniqueAdID").isNotNull())
        .where(F.col("Location").isNotNull())
        .where((F.col("StartDate").isNull()) | (F.col("StartDate") <= F.lit(reference_date).cast("date")))
        .where((F.col("EndDate").isNull()) | (F.col("EndDate") >= F.lit(reference_date).cast("date")))
    )
    return active.select(
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("Location").cast("string").alias("location"),
        F.lit(reference_date).cast("date").alias("feature_date"),
        _optional_col(active, "CampaignNumber").cast("string").alias("campaign_id"),
        _optional_col(active, "URL").cast("string").alias("advert_url"),
        _optional_col(active, "ProductURLs").cast("string").alias("product_urls"),
        _optional_col(active, "Items").cast("string").alias("control_sheet_items"),
        _optional_col(active, "Title").cast("string").alias("advert_title"),
        _optional_col(active, "Headline").cast("string").alias("headline"),
        _optional_col(active, "Subtext").cast("string").alias("subtext"),
        _optional_col(active, "CTA").cast("string").alias("cta"),
        _optional_col(active, "AdTrend").cast("string").alias("advert_theme"),
        F.coalesce(_optional_col(active, "AdCategory"), _optional_col(active, "AdSubcategory")).cast("string").alias("advert_category"),
        _optional_col(active, "AdBrandName").cast("string").alias("advert_brand_name"),
        _optional_col(active, "Page").cast("string").alias("page_path"),
        _optional_col(active, "TemplateName").cast("string").alias("template_name"),
        _optional_col(active, "rundate").cast("date").alias("source_rundate"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    ).dropDuplicates(["advert_id", "location", "feature_date"])


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


def build_click_labels_df(spark, source_catalog: str, source_schema: str, reference_date: str):
    """Build Shopping Bag tagged-click labels for assigned advert impressions."""
    from pyspark.sql import functions as F

    session_date_lit = F.lit(reference_date).cast("date")
    assignment_rundate = F.date_sub(session_date_lit, 1)
    click_end_date = F.date_add(session_date_lit, 7)
    shopping_bag_path = F.lit("/shoppingbag")

    sessions = spark.table(source_table(source_catalog, source_schema, "bq_sessions_next_uk"))
    rpid_lookup = (
        spark.table(source_table(source_catalog, source_schema, "rpid_with_accounts"))
        .select(
            F.col("account_number").cast("string").alias("account_number"),
            F.col("roamingprofileid").cast("string").alias("rpid"),
        )
        .where(F.col("rpid").isNotNull())
        .dropDuplicates(["account_number", "rpid"])
    )
    session_accounts = (
        sessions.where((F.col("date") >= session_date_lit) & (F.col("date") <= click_end_date))
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
        spark.table(source_table(source_catalog, source_schema, "bq_pages_next_uk"))
        .where(F.col("date") == session_date_lit)
        .select(
            F.col("date").cast("date").alias("session_date"),
            F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
            F.col("PagePath").cast("string").alias("page_path"),
            F.coalesce(F.col("FirstTimestamp"), F.col("LastTimestamp"))
            .cast("timestamp")
            .alias("event_timestamp"),
        )
        .where(_normalise_path_expr(F.col("page_path")) == shopping_bag_path)
        .join(session_accounts, ["session_date", "unique_visit_id"], "inner")
        .where(F.col("event_timestamp").isNotNull())
        .dropDuplicates(
            ["account_number", "session_date", "unique_visit_id", "event_timestamp"]
        )
    )
    sb_account_days = sb_page_visits.select(
        "account_number",
        "session_date",
    ).dropDuplicates()

    control_sheet = (
        spark.table(source_table(source_catalog, source_schema, "next_uk_nextads_control_sheet"))
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            F.col("Page").cast("string").alias("page_path"),
        )
        .where(F.col("advert_id").rlike("^P"))
    )
    multipage_shopping_bag = (
        spark.table(source_table(source_catalog, source_schema, "next_uk_nextads_multipage_locations"))
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
        .select(
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("Page").cast("string").alias("multipage_page_path"),
        )
        .where(_normalise_path_expr(F.col("multipage_page_path")) == shopping_bag_path)
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
        .where(_normalise_path_expr(F.col("configured_page_path")) == shopping_bag_path)
        .select("session_date", "location", "advert_id")
        .dropDuplicates(["session_date", "location", "advert_id"])
    )

    assignments = (
        spark.table(source_table(source_catalog, source_schema, "next_uk_nextads_assignments"))
        .where(F.to_date(F.col("rundate")) == assignment_rundate)
        .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
        .select(
            F.col("AccountNumber").cast("string").alias("account_number"),
            "session_date",
            F.col("Location").cast("string").alias("location"),
            F.col("Treatment").cast("string").alias("treatment"),
            F.col("UniqueAdIDMeasurement").cast("string").alias("advert_id"),
            F.col("UniqueAdIDAssigned").cast("string").alias("assigned_advert_id"),
        )
        .where(F.col("treatment") != "AdSuppressed")
        .where(F.col("account_number").isNotNull())
        .where(F.col("advert_id").isNotNull())
        .where(F.col("advert_id") != "NoAdFound")
        .where(F.col("advert_id").rlike("^P"))
        .join(sb_account_days, ["account_number", "session_date"], "inner")
        .join(sb_ad_metadata, ["session_date", "location", "advert_id"], "inner")
    )
    exposures = (
        sb_page_visits.join(assignments, ["account_number", "session_date"], "inner")
        .groupBy(
            "account_number",
            "session_date",
            "unique_visit_id",
            "location",
            "advert_id",
            "assigned_advert_id",
        )
        .agg(F.min("event_timestamp").alias("exposure_timestamp"))
        .withColumn("campaign_key", F.regexp_extract("advert_id", r"^(P\d+_C\d+)", 1))
        .withColumn(
            "assigned_campaign_key",
            F.regexp_extract("assigned_advert_id", r"^(P\d+_C\d+)", 1),
        )
        .where(
            (F.col("campaign_key") != "")
            | (F.col("assigned_campaign_key") != "")
        )
    )

    actions = spark.table(source_table(source_catalog, source_schema, "bq_actions_next_uk"))
    raw_clicks = (
        actions.where(
            (F.col("date") >= session_date_lit)
            & (F.col("date") <= F.date_add(session_date_lit, 7))
        )
        .where(F.col("Action") == "Banner Click - Next Ads")
        .where(F.col("Level2").cast("string").rlike("^P"))
        .where(_normalise_path_expr(F.col("PagePath")) == shopping_bag_path)
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
            & (F.col("act.unique_visit_id") == F.col("sess.unique_visit_id")),
            "inner",
        )
        .select(
            F.col("sess.account_number"),
            F.col("act.unique_visit_id").alias("click_unique_visit_id"),
            F.col("act.click_timestamp"),
            F.col("act.click_advert_id"),
            F.regexp_extract("act.click_advert_id", r"^(P\d+_C\d+)", 1).alias(
                "click_campaign_key"
            ),
        )
        .where(F.col("click_timestamp").isNotNull())
        .where(F.col("click_campaign_key") != "")
        .dropDuplicates(["account_number", "click_advert_id", "click_timestamp"])
    )
    horizons = spark.createDataFrame([(0,), (1,), (7,)], ["label_horizon_days"])
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
            F.countDistinct(F.col("imp.unique_visit_id")).cast("bigint").alias("impression_count"),
            F.countDistinct(F.col("clk.click_timestamp")).cast("bigint").alias("click_count"),
            F.min("clk.click_timestamp").alias("first_click_timestamp"),
        )
    )
    return (
        labelled.withColumn("clicked", F.when(F.col("click_count") > 0, 1).otherwise(0))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
