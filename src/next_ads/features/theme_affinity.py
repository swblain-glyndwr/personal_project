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


def build_account_theme_interactions_df(ranked_df, reference_date: str):
    """Build account-theme interaction features from ranked source rows."""
    from pyspark.sql import functions as F

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
    ).dropDuplicates(["account_number", "theme", "reference_date"])
