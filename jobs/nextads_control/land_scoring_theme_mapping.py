import sys
from datetime import date
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark.sql import functions as F

from dsutils import gcp
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.common.delta_writes import replace_scope_by_name
from next_ads.common.paths import load_client_config
from next_ads.control.theme_mapping import (
    normalise_theme_mapping,
    valid_theme_rank_condition,
)
from next_ads.control.theme_mapping_sync import build_theme_mapping_differences
from next_ads.ranking.scoring_inputs import (
    build_input_snapshot_id,
    latest_delta_version,
    summarise_content,
)


MAPPING_COLUMNS = (
    "Theme",
    "TargetingAttributes",
    "ThemeType",
    "ThemeTypeRank",
    "AdType",
    "AdTypeRank",
)


def _with_source_row_key(df):
    canonical = F.to_json(
        F.struct(*[F.col(column) for column in MAPPING_COLUMNS]),
        options={"ignoreNullFields": "false"},
    )
    return df.withColumn("SourceRowKey", F.sha2(canonical, 256))


def main(JOB_ENV, CLIENT, LOG_LEVEL, RUN_DATE):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    run_date = date.fromisoformat(RUN_DATE)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    cfg = load_client_config(CLIENT)
    mapping = cfg["theme_mapping_v2"]
    if not mapping.get("source_of_truth"):
        raise ValueError("theme_mapping_v2 must be marked as source_of_truth")

    landed = normalise_theme_mapping(
        gcp.spark_df_from_sheets(
            url=mapping["url"],
            worksheet_name=mapping["sheet"],
            gcp_scope=cfg["gcp"]["scope"],
            gcp_key=cfg["gcp"]["key"],
            schema=mapping["read_schema"],
        )
    ).select(*MAPPING_COLUMNS)
    legacy = gcp.spark_df_from_sheets(
        url=cfg["theme_mapping"]["url"],
        worksheet_name=cfg["theme_mapping"]["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=cfg["theme_mapping"]["read_schema"],
    )
    warning_count = build_theme_mapping_differences(legacy, landed).count()
    if warning_count:
        logger.warning(
            "The copied v1 Theme Mapping differs from the captured v2 source "
            "in %s row(s); v2 remains authoritative",
            warning_count,
        )
    invalid_required = landed.where(
        F.col("Theme").isNull()
        | (F.trim(F.col("Theme")) == "")
        | F.col("TargetingAttributes").isNull()
        | (F.trim(F.col("TargetingAttributes")) == "")
        | ~valid_theme_rank_condition()
    ).limit(1)
    if invalid_required.count():
        raise ValueError(
            "Authoritative v2 Theme Mapping contains empty required values "
            "or non-positive ranks"
        )
    summary = summarise_content(
        landed,
        key_columns=("Theme",),
        content_columns=MAPPING_COLUMNS,
    )
    summary.require_valid("authoritative_v2_theme_mapping")
    legacy_normalised = normalise_theme_mapping(legacy).select(*MAPPING_COLUMNS)
    legacy_summary = summarise_content(
        legacy_normalised,
        key_columns=("Theme",),
        content_columns=MAPPING_COLUMNS,
    )
    legacy_invalid_count = legacy_normalised.where(
        F.col("Theme").isNull()
        | (F.trim(F.col("Theme")) == "")
        | F.col("TargetingAttributes").isNull()
        | (F.trim(F.col("TargetingAttributes")) == "")
        | ~valid_theme_rank_condition()
    ).count()
    warning_count += (
        legacy_summary.null_key_count
        + legacy_summary.duplicate_key_count
        + legacy_invalid_count
        + int(legacy_summary.row_count == 0)
    )
    landing_id = build_input_snapshot_id(
        run_date,
        {
            "authoritative_v2_theme_mapping": summary,
            "copied_v1_theme_mapping": legacy_summary,
        },
        contract_version="nextads_theme_mapping_landing/v1",
    ).replace("scoring_inputs_", "theme_mapping_")
    authoritative_output = _with_source_row_key(landed).select(
        F.lit(landing_id).alias("LandingID"),
        F.lit(run_date).cast("date").alias("RunDate"),
        F.lit("authoritative_v2").alias("SourceRole"),
        "SourceRowKey",
        *MAPPING_COLUMNS,
    )
    legacy_output = _with_source_row_key(legacy_normalised).select(
        F.lit(landing_id).alias("LandingID"),
        F.lit(run_date).cast("date").alias("RunDate"),
        F.lit("copied_v1").alias("SourceRole"),
        "SourceRowKey",
        *MAPPING_COLUMNS,
    )
    output = authoritative_output.unionByName(legacy_output)
    replace_scope_by_name(
        output,
        config.tables_write.scoring_input_theme_mapping_raw,
        {"LandingID": landing_id},
        output.columns,
        spark=spark,
    )
    landing_version = latest_delta_version(
        spark,
        config.tables_write.scoring_input_theme_mapping_raw,
    )
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="landing_id", value=landing_id)
    task_values.set(key="landing_version", value=landing_version)
    task_values.set(key="warning_count", value=warning_count)
    logger.info(
        "Landed authoritative Theme Mapping as %s at Delta version %s",
        landing_id,
        landing_version,
    )


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--run_date"),
    )
