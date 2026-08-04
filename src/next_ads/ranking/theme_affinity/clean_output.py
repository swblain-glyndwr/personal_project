def _ranked_theme_mapping(
    spark,
    item_themes_table: str,
    *,
    input_snapshot_id=None,
    run_date=None,
    item_themes_frame=None,
):
    from pyspark.sql import functions as F

    if input_snapshot_id is None:
        theme_mapping = spark.sql(
            "SELECT DISTINCT theme, "
            "regexp_replace(theme, '[^a-zA-Z0-9]', '') AS theme_clean "
            f"FROM {item_themes_table} WHERE theme_rank = 1"
        )
    else:
        source = (
            item_themes_frame
            if item_themes_frame is not None
            else spark.table(item_themes_table)
        )
        source = source.where(
            (F.col("InputSnapshotID") == input_snapshot_id)
            & (F.col("RunDate") == F.lit(run_date))
        )
        theme_mapping = (
            source.where(F.col("theme_rank") == 1)
            .select("theme")
            .distinct()
            .withColumn(
                "theme_clean",
                F.regexp_replace("theme", "[^a-zA-Z0-9]", ""),
            )
        )
    if theme_mapping.limit(1).count() == 0:
        raise ValueError(
            f"Theme mapping table {item_themes_table} has no theme_rank = 1 rows. "
            "Run the DEV table population job before provider scoring."
        )
    return theme_mapping


def _write_inference_log(
    spark,
    signals,
    model_tables,
    model_id: str,
    *,
    run_date,
    inference_timestamp,
):
    from pyspark.sql import functions as F
    from next_ads.common.snapshot_writes import replace_validated_scope

    inference_log_table = model_tables.inference_log
    inference_log = signals.select(
        F.col("RunDate").alias("inference_date"),
        F.lit(inference_timestamp).cast("timestamp").alias(
            "inference_timestamp"
        ),
        F.lit(model_id).alias("model_id"),
        F.col("AccountNumber").alias("account_number"),
        F.regexp_replace("EntityID", "[^a-zA-Z0-9]", "").alias("theme"),
        F.col("Score").cast("double").alias("prediction"),
        F.col("ProviderRank").cast("int").alias("rank"),
        F.lit(None).cast("int").alias("label"),
        F.lit(None).cast("date").alias("label_observed_until"),
        F.lit(None).cast("timestamp").alias("label_updated_timestamp"),
    )
    replace_validated_scope(
        spark,
        inference_log,
        table=inference_log_table,
        scope={"inference_date": run_date, "model_id": model_id},
        key_columns=[
            "inference_date",
            "model_id",
            "account_number",
            "theme",
        ],
    )


def _rerank_model_output(full_results, penalty_themes, penalty: float):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    penalty_window = Window.partitionBy("account_number").orderBy(
        F.col("prediction").desc_nulls_last(),
        F.col("theme").asc(),
    )
    reranked = (
        full_results.withColumn(
            "rank",
            F.row_number().over(penalty_window),
        )
        .join(
            penalty_themes.withColumn("is_penalty_theme", F.lit(True)),
            full_results.theme == penalty_themes.theme_clean,
            "left",
        )
        .withColumn(
            "adjusted_score",
            F.when(
                (F.col("rank") == 1)
                & (F.col("baskets_behavior__recency_rank") == 1)
                & F.col("is_penalty_theme"),
                F.col("prediction") * (1 - penalty),
            ).otherwise(F.col("prediction")),
        )
    )
    final_window = Window.partitionBy("account_number").orderBy(
        F.col("adjusted_score").desc_nulls_last(),
        F.col("theme").asc(),
    )
    return reranked.withColumn(
        "final_rank",
        F.row_number().over(final_window),
    )


def stage_model_output(spark, runtime, predictions) -> int:
    from pyspark.sql import functions as F
    from next_ads.ranking.provider_publication import stage_provider_signals
    from next_ads.ranking.provider_signals import adapt_account_entity_scores
    from next_ads.ranking.theme_affinity.config import (
        read_runtime_foundation_output,
    )
    from next_ads.ranking.provider_context import pinned_item_themes

    model_config = runtime.config.ranking_model
    run_date = runtime.run_date
    if run_date is None:
        raise ValueError("Theme Affinity staging requires an exact run date")
    if runtime.provider_context is None:
        raise ValueError("Theme Affinity staging requires a provider context")

    stats_df = (
        read_runtime_foundation_output(spark, runtime, "complete")
        .groupBy("theme_clean")
        .agg(
            F.avg("repurchase_ratio").alias("rep_ratio"),
            F.sum("baskets_behavior__frequency").alias("baskets_freq"),
        )
    )
    thresholds = stats_df.select(
        F.percentile_approx("rep_ratio", 0.10).alias("rep_limit"),
        F.percentile_approx("baskets_freq", 0.40).alias("freq_limit"),
    ).collect()[0]
    dynamic_themes_df = stats_df.filter(
        (F.col("rep_ratio") <= thresholds["rep_limit"])
        & (F.col("baskets_freq") >= thresholds["freq_limit"])
    ).select("theme_clean")
    manual_themes_df = spark.createDataFrame(
        [(theme,) for theme in model_config.high_repurchase_manual_themes],
        ["theme_clean"],
    )
    penalty_themes = dynamic_themes_df.union(manual_themes_df).distinct()

    penalty = float(model_config.high_repurchase_penalty)
    final_results = _rerank_model_output(
        predictions,
        penalty_themes,
        penalty,
    )
    item_themes_table = (
        runtime.item_themes_table
        or runtime.config.tables_write.item_themes_latest
    )
    theme_mapping = _ranked_theme_mapping(
        spark,
        item_themes_table,
        input_snapshot_id=runtime.input_snapshot_id,
        run_date=run_date,
        item_themes_frame=pinned_item_themes(
            spark,
            runtime.provider_context,
            input_table=item_themes_table,
        ),
    )
    fixed = (
        final_results.alias("scores")
        .join(
            theme_mapping.alias("mapping"),
            F.col("scores.theme") == F.col("mapping.theme_clean"),
            how="left",
        )
        .select(
            F.col("scores.account_number").alias("AccountNumber"),
            F.col("mapping.theme").alias("NextTheme"),
            F.col("scores.prediction").alias("RawScore"),
            F.col("scores.adjusted_score").alias("Score"),
        )
    )
    provider = runtime.config.scoring.providers[
        runtime.provider_context.provider_id
    ]
    signals = adapt_account_entity_scores(
        fixed,
        provider_build_id=runtime.provider_context.provider_build_id,
        provider_id=runtime.provider_context.provider_id,
        entity_type=provider.entity_type,
        run_date=run_date,
        account_column="AccountNumber",
        entity_column="NextTheme",
        raw_score_column="RawScore",
        score_column="Score",
        score_direction=provider.score_direction,
        max_entities_per_account=int(provider.max_entities_per_account),
    )
    return stage_provider_signals(
        spark,
        signals,
        context=runtime.provider_context,
        table=runtime.config.tables_write.score_provider_signals,
    )


def _require_single_transaction(
    spark,
    table: str,
    previous_version: int,
) -> int:
    from next_ads.ranking.scoring_inputs import latest_delta_version

    output_version = latest_delta_version(spark, table)
    if output_version != previous_version + 1:
        raise ValueError(f"Table {table} changed during provider publication")
    return output_version


def publish_theme_affinity_compatibility_outputs(
    spark,
    runtime,
    signals,
    completed_at,
):
    """Publish legacy Theme Affinity tables behind the provider gate."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from next_ads.common.delta_writes import (
        replace_scope_by_name,
        replace_table_by_name,
    )
    from next_ads.ranking.scoring_inputs import latest_delta_version

    model_tables = runtime.config.ranking_model_tables
    run_date = runtime.run_date
    legacy = signals.select(
        "AccountNumber",
        F.col("EntityID").alias("NextTheme"),
        F.col("Score").cast("float").alias("ProbAggRebased"),
        F.col("RunDate").alias("rundate"),
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        history_before = latest_delta_version(spark, model_tables.model_full)
        replace_scope_by_name(
            legacy,
            model_tables.model_full,
            {"rundate": run_date},
            legacy.columns,
            spark=spark,
        )
        history_version = _require_single_transaction(
            spark,
            model_tables.model_full,
            history_before,
        )

        inference_before = latest_delta_version(
            spark,
            model_tables.inference_log,
        )
        _write_inference_log(
            spark,
            signals,
            model_tables,
            runtime.model_uri,
            run_date=run_date,
            inference_timestamp=completed_at,
        )
        inference_version = _require_single_transaction(
            spark,
            model_tables.inference_log,
            inference_before,
        )

        latest_before = latest_delta_version(spark, model_tables.model_latest)
        replace_table_by_name(
            legacy,
            model_tables.model_latest,
            legacy.columns,
            spark=spark,
        )
        latest_version = _require_single_transaction(
            spark,
            model_tables.model_latest,
            latest_before,
        )
        return {
            "history": history_version,
            "inference_log": inference_version,
            "latest": latest_version,
        }
    finally:
        legacy.unpersist()


def publish_theme_affinity_provider_build(
    spark,
    runtime,
    *,
    provider_signals_delta_version: int,
    task_run_id: int,
    execution_count: int,
):
    """Publish compatible outputs and accept the canonical build last."""
    from next_ads.ranking.provider_publication import publish_provider_build

    context = runtime.provider_context
    if context is None:
        raise ValueError("Theme Affinity publication requires a provider context")
    provider = runtime.config.scoring.providers[context.provider_id]
    return publish_provider_build(
        spark,
        context=context,
        signals_table=runtime.config.tables_write.score_provider_signals,
        signals_delta_version=int(provider_signals_delta_version),
        builds_table=runtime.config.tables_write.score_provider_builds,
        provider_config=provider,
        contract_version=runtime.config.scoring.contract_version,
        compatibility_publisher=(
            lambda signals, completed_at: (
                publish_theme_affinity_compatibility_outputs(
                    spark,
                    runtime,
                    signals,
                    completed_at,
                )
            )
        ),
        task_run_id=int(task_run_id),
        execution_count=int(execution_count),
    )
