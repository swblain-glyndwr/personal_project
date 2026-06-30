"""Theme Affinity feature-store materialisation logic."""

from __future__ import annotations

from datetime import date


THEME_AFFINITY_MODEL_FEATURE_COLUMNS = [
    "month",
    "algo_baskets1__cs_top10",
    "algo_baskets5__freq12_top10",
    "algo_baskets5__cs_top10",
    "algo_baskets5__freq12_norm_top10",
    "algo_views5__cs_top10",
    "views_behavior__recency",
    "views_behavior__frequency",
    "views_behavior__recency_rank",
    "baskets_behavior__frequency",
    "num_retrieval_methods",
    "repurchase_ratio",
    "repurchase_stage",
    "user_total_views",
    "user_view_to_atb_rate",
    "GmaName",
    "views_ly_7",
    "views_ly_30",
    "baskets_ly_7",
    "baskets_ly_30",
    "trending_7x30",
    "simple_rules_rank",
]


def theme_source_table(
    catalog: str,
    schema: str,
    table_prefix: str,
    suffix: str,
) -> str:
    """Resolve a legacy Theme Affinity source table."""
    return f"{catalog}.{schema}.{table_prefix}_{suffix}"


def parse_reference_date(reference_date: str | None) -> date | None:
    """Parse an explicit reference date, leaving predict mode unresolved."""
    if reference_date is None or reference_date.lower() == "predict":
        return None
    return date.fromisoformat(reference_date)


def resolve_theme_reference_date(
    spark,
    source_catalog: str,
    source_schema: str,
    table_prefix: str,
    reference_date: str | None,
) -> str:
    """Resolve predict mode to the latest available ranked source date."""
    explicit_date = parse_reference_date(reference_date)
    if explicit_date:
        return explicit_date.isoformat()

    ranked_table = theme_source_table(
        source_catalog,
        source_schema,
        table_prefix,
        "ranked",
    )
    result = (
        spark.table(ranked_table)
        .selectExpr("max(reference_date) as reference_date")
        .first()
    )
    if result is None or result["reference_date"] is None:
        raise ValueError(
            f"Could not resolve reference_date=predict from {ranked_table}"
        )
    return result["reference_date"].isoformat()


def _optional_col(df, column_name: str, default=None):
    from pyspark.sql import functions as F

    if column_name in df.columns:
        return F.col(column_name)
    return F.lit(default)


def _filter_reference_date(df, reference_date: str):
    from pyspark.sql import functions as F

    return df.where(F.col("reference_date") == F.lit(reference_date).cast("date"))


def _top_ranked_model_rows(ranked_df):
    from pyspark.sql import functions as F

    return ranked_df.where(F.col("simple_rules_rank") <= F.lit(100))


def _filter_required_keys(df, *columns: str):
    from pyspark.sql import functions as F

    result = df
    for column in columns:
        result = result.where(F.col(column).isNotNull())
    return result


def read_theme_source_tables(
    spark,
    source_catalog: str,
    source_schema: str,
    table_prefix: str,
    reference_date: str,
) -> dict[str, object]:
    """Read the legacy Theme Affinity source tables for one reference date."""
    from pyspark.sql import functions as F

    ranked = _filter_reference_date(
        spark.table(
            theme_source_table(source_catalog, source_schema, table_prefix, "ranked")
        ),
        reference_date,
    )
    popularity = _filter_reference_date(
        spark.table(
            theme_source_table(
                source_catalog,
                source_schema,
                table_prefix,
                "popularity_metrics",
            )
        ),
        reference_date,
    )
    latest_reference_date = resolve_theme_reference_date(
        spark,
        source_catalog,
        source_schema,
        table_prefix,
        "predict",
    )
    if reference_date == latest_reference_date:
        prediction = spark.table(
            theme_source_table(source_catalog, source_schema, table_prefix, "half")
        ).withColumnRenamed("theme", "theme_clean")
    else:
        prediction = (
            ranked.select("account_number", "theme_clean")
            .limit(0)
            .withColumn("prediction", F.lit(None).cast("double"))
        )
    return {
        "ranked": ranked,
        "popularity": popularity,
        "prediction": prediction,
    }


def read_theme_account_source_tables(
    spark,
    source_catalog: str,
    source_schema: str,
    table_prefix: str,
    reference_date: str,
) -> dict[str, object]:
    """Read account-level outputs already produced by Theme Affinity prep."""
    ranked = _filter_reference_date(
        spark.table(
            theme_source_table(source_catalog, source_schema, table_prefix, "ranked")
        ),
        reference_date,
    )
    advanced = _filter_reference_date(
        spark.table(
            theme_source_table(
                source_catalog,
                source_schema,
                table_prefix,
                "advanced_features",
            )
        ),
        reference_date,
    )
    customer_features = _filter_reference_date(
        spark.table(
            theme_source_table(
                source_catalog,
                source_schema,
                table_prefix,
                "customer_features",
            )
        ),
        reference_date,
    )
    customer_segments = _filter_reference_date(
        spark.table(
            theme_source_table(
                source_catalog,
                source_schema,
                table_prefix,
                "customer_segments",
            )
        ),
        reference_date,
    )
    return {
        "ranked": ranked,
        "advanced": advanced,
        "customer_features": customer_features,
        "customer_segments": customer_segments,
    }


def build_theme_affinity_account_profile_df(
    customer_features_df,
    customer_segments_df,
    reference_date: str,
):
    """Build account profile features from Theme Affinity customer outputs."""
    from pyspark.sql import functions as F

    customer_features_df = _filter_required_keys(
        customer_features_df,
        "account_number",
    )
    customer_segments_df = _filter_required_keys(
        customer_segments_df,
        "account_number",
    )
    customer = customer_features_df.alias("customer")
    segments = customer_segments_df.alias("segments")
    return (
        customer.select(
            F.col("account_number").cast("string").alias("account_number"),
            F.lit(reference_date).cast("date").alias("reference_date"),
            _optional_col(customer_features_df, "sites", "next_uk")
            .cast("string")
            .alias("country_code"),
            F.lit("next_uk").cast("string").alias("client_name"),
            _optional_col(customer_features_df, "app_web").cast("string").alias("account_type"),
            _optional_col(customer_features_df, "age").cast("int").alias("account_age_days"),
            _optional_col(customer_features_df, "PostcodeArea_GB")
            .cast("string")
            .alias("postcode_area"),
            _optional_col(customer_features_df, "GmaName").cast("string").alias("region"),
            _optional_col(customer_features_df, "gender_customer").cast("string").alias("gender"),
        )
        .join(
            segments.select(
                F.col("account_number").cast("string").alias("account_number"),
                _optional_col(customer_segments_df, "total_spend")
                .cast("double")
                .alias("online_spend_lifetime"),
            ),
            on="account_number",
            how="left",
        )
        .withColumn("credit_type", F.lit(None).cast("string"))
        .withColumn("latest_known_activity_recency_days", F.lit(None).cast("int"))
        .withColumn("online_orders_lifetime", F.lit(None).cast("double"))
        .withColumn("retail_orders_lifetime", F.lit(None).cast("double"))
        .withColumn("retail_spend_lifetime", F.lit(None).cast("double"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["account_number", "reference_date"])
    )


def build_theme_affinity_account_web_activity_df(
    ranked_df,
    advanced_df,
    reference_date: str,
):
    """Build account web-activity features from Theme Affinity prep outputs."""
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number")
    advanced_df = _filter_required_keys(advanced_df, "account_number")
    ranked_rollup = ranked_df.groupBy("account_number").agg(
        F.min(_optional_col(ranked_df, "views_behavior__recency")).cast("int").alias(
            "browse_session_recency_days"
        ),
        F.sum(_optional_col(ranked_df, "views_behavior__frequency", 0))
        .cast("bigint")
        .alias("theme_view_events_90d"),
        F.sum(_optional_col(ranked_df, "baskets_behavior__frequency", 0))
        .cast("bigint")
        .alias("basket_events_90d"),
    )
    advanced = advanced_df.select(
        F.col("account_number").cast("string").alias("account_number"),
        _optional_col(advanced_df, "user_total_views", 0)
        .cast("bigint")
        .alias("page_events_90d"),
        _optional_col(advanced_df, "user_view_to_atb_rate", 0.0)
        .cast("double")
        .alias("user_view_to_atb_rate"),
    )
    return (
        advanced.join(ranked_rollup, on="account_number", how="left")
        .withColumn("reference_date", F.lit(reference_date).cast("date"))
        .withColumn("browse_sessions_90d", F.lit(None).cast("bigint"))
        .withColumn("browse_active_days_90d", F.lit(None).cast("bigint"))
        .withColumn("shopping_bag_page_events_90d", F.lit(None).cast("bigint"))
        .withColumn("avg_pages_per_session_90d", F.lit(None).cast("double"))
        .withColumn("action_events_90d", F.col("basket_events_90d").cast("bigint"))
        .withColumn("action_active_days_90d", F.lit(None).cast("bigint"))
        .withColumn(
            "add_to_bag_actions_90d",
            F.round(F.col("page_events_90d") * F.col("user_view_to_atb_rate"))
            .cast("bigint"),
        )
        .withColumn("pdp_action_rows_90d", F.lit(None).cast("bigint"))
        .withColumn("action_recency_days", F.lit(None).cast("int"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["account_number", "reference_date"])
    )


def build_account_theme_interactions_df(ranked_df, reference_date: str):
    """Build account-theme interaction features from ranked source rows."""
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number", "theme_clean")
    return ranked_df.select(
        F.col("account_number"),
        F.col("theme_clean").alias("theme"),
        F.lit(reference_date).cast("date").alias("reference_date"),
        _optional_col(ranked_df, "views_behavior__recency").alias(
            "views_behavior__recency"
        ),
        _optional_col(ranked_df, "views_behavior__frequency").alias(
            "views_behavior__frequency"
        ),
        _optional_col(ranked_df, "views_behavior__recency_rank").alias(
            "views_behavior__recency_rank"
        ),
        _optional_col(ranked_df, "baskets_behavior__frequency").alias(
            "baskets_behavior__frequency"
        ),
        _optional_col(ranked_df, "baskets_behavior__recency_rank").alias(
            "baskets_behavior__recency_rank"
        ),
        _optional_col(ranked_df, "repurchase_ratio").alias("repurchase_ratio"),
        _optional_col(ranked_df, "repurchase_stage").alias("repurchase_stage"),
        _optional_col(ranked_df, "user_total_views").alias("user_total_views"),
        _optional_col(ranked_df, "user_view_to_atb_rate").alias(
            "user_view_to_atb_rate"
        ),
        _optional_col(ranked_df, "num_retrieval_methods").alias(
            "num_retrieval_methods"
        ),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    ).dropDuplicates(["account_number", "theme", "reference_date"])


def build_account_theme_affinity_df(
    ranked_df,
    prediction_df,
    reference_date: str,
):
    """Build account-theme model score features from legacy predictions."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number", "theme_clean")
    prediction_df = _filter_required_keys(
        prediction_df,
        "account_number",
        "theme_clean",
    )
    score_df = (
        _top_ranked_model_rows(ranked_df).select(
            "account_number",
            "theme_clean",
            "month",
            "simple_rules_rank",
        )
        .join(
            prediction_df.select(
                "account_number",
                "theme_clean",
                F.col("prediction").alias("model_score"),
            ),
            on=["account_number", "theme_clean"],
            how="left",
        )
        .withColumn("reference_date", F.lit(reference_date).cast("date"))
        .withColumn("theme_affinity_score", F.col("model_score"))
        .withColumn("adjusted_score", F.col("model_score"))
    )
    rank_window = Window.partitionBy("account_number", "reference_date").orderBy(
        F.col("adjusted_score").desc_nulls_last(),
        F.col("simple_rules_rank").asc_nulls_last(),
        F.col("theme_clean").asc(),
    )
    return (
        score_df.withColumn("rank", F.row_number().over(rank_window))
        .select(
            F.col("account_number"),
            F.col("theme_clean").alias("theme"),
            F.col("reference_date"),
            F.col("month"),
            F.col("theme_affinity_score"),
            F.col("simple_rules_rank"),
            F.col("model_score"),
            F.col("adjusted_score"),
            F.col("rank"),
            F.lit("hackathon_theme_affinity").alias("model_name"),
            F.lit(None).cast("string").alias("model_version"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["account_number", "theme", "reference_date"])
    )


def build_theme_popularity_df(popularity_df, reference_date: str):
    """Build theme popularity features from legacy popularity metrics."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    popularity_df = _filter_required_keys(popularity_df, "theme_clean")
    rank_window = Window.orderBy(
        _optional_col(popularity_df, "views_30").desc_nulls_last(),
        F.col("theme_clean").asc(),
    )
    return (
        popularity_df.withColumn("popularity_rank", F.row_number().over(rank_window))
        .select(
            F.col("theme_clean").alias("theme"),
            F.lit(reference_date).cast("date").alias("reference_date"),
            _optional_col(popularity_df, "views_7").alias("views_7d"),
            _optional_col(popularity_df, "views_30").alias("views_30d"),
            _optional_col(popularity_df, "baskets_7").alias("baskets_7d"),
            _optional_col(popularity_df, "baskets_30").alias("baskets_30d"),
            _optional_col(popularity_df, "views_ly_7").alias("views_ly_7"),
            _optional_col(popularity_df, "views_ly_30").alias("views_ly_30"),
            _optional_col(popularity_df, "baskets_ly_7").alias("baskets_ly_7"),
            _optional_col(popularity_df, "baskets_ly_30").alias("baskets_ly_30"),
            _optional_col(popularity_df, "trending_7x30").alias("trending_7x30"),
            F.col("popularity_rank"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["theme", "reference_date"])
    )


def build_theme_response_labels_df(ranked_df, reference_date: str):
    """Build Theme Affinity label features from legacy target rows."""
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number", "theme_clean")
    return (
        ranked_df.select(
            F.col("account_number"),
            F.col("theme_clean").alias("theme"),
            F.lit(reference_date).cast("date").alias("reference_date"),
            F.lit("baskets_target").alias("label_name"),
            _optional_col(ranked_df, "label", 0.0).cast("double").alias("label_value"),
            _optional_col(ranked_df, "label", 0)
            .cast("bigint")
            .alias("target_event_count"),
            F.lit(31).alias("target_window_days"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["account_number", "theme", "reference_date", "label_name"])
    )


def build_theme_affinity_model_input_df(
    ranked_df,
    prediction_df,
    reference_date: str,
):
    """Build current Theme Affinity model-ready feature rows."""
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number", "theme_clean")
    prediction_df = _filter_required_keys(
        prediction_df,
        "account_number",
        "theme_clean",
    )
    select_columns = [
        F.col("account_number"),
        F.col("theme_clean").alias("theme"),
        F.lit(reference_date).cast("date").alias("reference_date"),
    ]
    select_columns.extend(
        F.col(column) for column in THEME_AFFINITY_MODEL_FEATURE_COLUMNS
    )
    select_columns.append(_optional_col(ranked_df, "label", 0.0).alias("label"))

    base_df = _top_ranked_model_rows(ranked_df).select(*select_columns).alias("base")
    predictions_df = prediction_df.select(
        "account_number",
        "theme_clean",
        F.col("prediction").alias("model_score"),
    ).alias("pred")

    model_input_df = base_df.join(
        predictions_df,
        (F.col("base.account_number") == F.col("pred.account_number"))
        & (F.col("base.theme") == F.col("pred.theme_clean")),
        how="left",
    )
    return model_input_df.select(
        "base.*",
        F.col("pred.model_score"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    ).dropDuplicates(["account_number", "theme", "reference_date"])


def build_theme_affinity_training_input_df(ranked_df, reference_date: str):
    """Build labelled historical training rows from ranked prep output."""
    from pyspark.sql import functions as F

    ranked_df = _filter_required_keys(ranked_df, "account_number", "theme_clean")
    empty_prediction_df = (
        ranked_df.select("account_number", "theme_clean")
        .limit(0)
        .withColumn("prediction", F.lit(None).cast("double"))
    )
    return build_theme_affinity_model_input_df(
        ranked_df,
        empty_prediction_df,
        reference_date,
    )
