def _ranked_theme_mapping(spark, item_themes_table: str):
    theme_mapping = spark.sql(
        "SELECT DISTINCT theme, regexp_replace(theme, '[^a-zA-Z0-9]', '') AS theme_clean "
        f"FROM {item_themes_table} WHERE theme_rank = 1"
    )
    if theme_mapping.limit(1).count() == 0:
        raise ValueError(
            f"Theme mapping table {item_themes_table} has no theme_rank = 1 rows. "
            "Run the DEV table population job before clean_output."
        )
    return theme_mapping


def _delete_current_inference_log_partition(spark, table_name: str, model_id: str):
    safe_model_id = model_id.replace("'", "''")
    spark.sql(
        f"DELETE FROM {table_name} "
        "WHERE inference_date = current_date() "
        f"AND model_id = '{safe_model_id}'"
    )


def _write_inference_log(spark, full_results, model_tables, model_id: str):
    from pyspark.sql import functions as F

    inference_log_table = model_tables.inference_log
    inference_log = full_results.select(
        F.col("rundate").alias("inference_date"),
        F.current_timestamp().alias("inference_timestamp"),
        F.lit(model_id).alias("model_id"),
        F.col("AccountNumber").alias("account_number"),
        F.col("NextTheme").alias("theme"),
        F.col("ProbAggRebased").cast("double").alias("prediction"),
        F.col("final_rank").cast("int").alias("rank"),
        F.lit(None).cast("int").alias("label"),
        F.lit(None).cast("date").alias("label_observed_until"),
        F.lit(None).cast("timestamp").alias("label_updated_timestamp"),
    )
    if spark.catalog.tableExists(inference_log_table):
        _delete_current_inference_log_partition(spark, inference_log_table, model_id)
    inference_log.write.mode("append").saveAsTable(inference_log_table)


def clean_model_output(spark, runtime):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    model_config = runtime.config.ranking_model
    model_tables = runtime.config.ranking_model_tables

    full_results = spark.table(model_tables.predict_output_table)
    stats_df = (
        spark.table(model_tables.predict_complete)
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

    window_spec = Window.partitionBy("account_number").orderBy(
        F.col("prediction").desc()
    )
    reranking_df = full_results.withColumn(
        "rank", F.row_number().over(window_spec)
    )
    reranking_df = reranking_df.join(
        penalty_themes.withColumn("is_penalty_theme", F.lit(True)),
        reranking_df.theme == penalty_themes.theme_clean,
        "left",
    )
    penalty = float(model_config.high_repurchase_penalty)
    reranking_df = reranking_df.withColumn(
        "adjusted_score",
        F.when(
            (F.col("rank") == 1)
            & (F.col("baskets_behavior__recency_rank") == 1)
            & F.col("is_penalty_theme"),
            F.col("prediction") * (1 - penalty),
        ).otherwise(F.col("prediction")),
    )
    final_window = Window.partitionBy("account_number").orderBy(
        F.col("adjusted_score").desc()
    )
    final_results = reranking_df.withColumn(
        "final_rank", F.row_number().over(final_window)
    )
    full_results = (
        final_results.withColumnRenamed("adjusted_score", "ProbAggRebased")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumnRenamed("theme", "NextTheme")
        .withColumn("rundate", F.current_date())
    )
    theme_mapping = _ranked_theme_mapping(
        spark,
        runtime.config.tables_write.item_themes_latest,
    )
    fixed = (
        full_results.join(
            theme_mapping,
            full_results["NextTheme"] == theme_mapping["theme_clean"],
            how="left",
        )
        .select("AccountNumber", "theme", "ProbAggRebased", "rundate")
        .withColumnRenamed("theme", "NextTheme")
    )

    (
        fixed.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(model_tables.model_latest)
    )
    if spark.catalog.tableExists(model_tables.model_full):
        spark.sql(
            f"DELETE FROM {model_tables.model_full} WHERE rundate = current_date()"
        )
    fixed.write.mode("append").saveAsTable(model_tables.model_full)
    _write_inference_log(spark, full_results, model_tables, runtime.model_uri)
