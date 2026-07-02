"""Build labelled Theme Affinity training input from historical prep outputs."""

from __future__ import annotations

from dataclasses import replace
import logging

from _registry_job import configure_job_logging, log_owned_tables, parse_common_args
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.materialization import (
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.theme_affinity import build_theme_affinity_training_input_df
from pyspark.sql import functions as F


LOGGER = logging.getLogger(__name__)
TRAINING_TABLE_NAME = "next_uk_nextads_fs_theme_affinity_training_input"


def _should_skip(reference_date: str | None) -> bool:
    return not reference_date or reference_date.strip().lower() in {"skip", "none"}


def _validate_labelled_training_frame(df) -> dict[str, int]:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("positive_rows"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("negative_rows"),
    ).first()
    row_count = int(stats["rows"] or 0)
    positive_rows = int(stats["positive_rows"] or 0)
    negative_rows = int(stats["negative_rows"] or 0)
    if row_count == 0:
        raise ValueError("Theme Affinity training input has zero rows")
    if positive_rows == 0:
        raise ValueError(
            "Theme Affinity training input has zero positive labels. "
            "Use a historical reference date where the next 31 days of basket "
            "targets exist."
        )
    if negative_rows == 0:
        raise ValueError("Theme Affinity training input has zero negative labels")
    return {
        "rows": row_count,
        "positive_rows": positive_rows,
        "negative_rows": negative_rows,
    }


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables("build_theme_affinity_training_input", args)

    if _should_skip(args.theme_training_reference_date):
        LOGGER.info(
            "Skipping Theme Affinity labelled training input; "
            "--theme_training_reference_date is %r",
            args.theme_training_reference_date,
        )
        return

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    replace_reference_date = args.replace_reference_date.lower() == "true"
    from next_ads.ranking.theme_affinity.config import resolve_runtime
    from next_ads.ranking.theme_affinity.data_prep import rank_complete_table, run_layers

    runtime = resolve_runtime(args.job_env, args.client)
    runtime = replace(runtime, table_prefix=args.theme_training_table_prefix)

    LOGGER.info(
        "Building Theme Affinity labelled training input: "
        "reference_date=%s namespace=%s prefix=%s",
        args.theme_training_reference_date,
        runtime.namespace,
        runtime.table_prefix,
    )
    run_layers(
        spark,
        runtime,
        "all",
        args.theme_training_reference_date,
        dry_run=False,
    )
    rank_complete_table(spark, runtime)

    ranked_table = f"{runtime.namespace}.{runtime.table_prefix}_ranked"
    ranked_df = spark.table(ranked_table).where(
        F.col("reference_date")
        == F.lit(args.theme_training_reference_date).cast("date")
    )
    training_input_df = build_theme_affinity_training_input_df(
        ranked_df,
        args.theme_training_reference_date,
    )
    stats = _validate_labelled_training_frame(training_input_df)

    table_path = write_feature_table(
        spark,
        TRAINING_TABLE_NAME,
        training_input_df,
        catalog=target_catalog,
        schema=target_schema,
        reference_date=args.theme_training_reference_date,
        replace_reference_date=replace_reference_date,
        feature_engineering_client=create_feature_engineering_client(),
    )
    LOGGER.info(
        "Wrote Theme Affinity labelled training input feature table: %s stats=%s",
        table_path,
        stats,
    )


if __name__ == "__main__":
    main()
