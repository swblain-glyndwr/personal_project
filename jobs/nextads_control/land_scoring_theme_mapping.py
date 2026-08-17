import sys
import hashlib
import json
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


def _build_landing_id(run_date, git_commit, mapping, legacy_mapping):
    payload = {
        "contract_version": "nextads_theme_mapping_landing/v2",
        "run_date": run_date.isoformat(),
        "git_commit": git_commit,
        "sources": {
            "authoritative_v2": {
                "url": mapping["url"],
                "sheet": mapping["sheet"],
            },
            "copied_v1": {
                "url": legacy_mapping["url"],
                "sheet": legacy_mapping["sheet"],
            },
        },
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return f"theme_mapping_{run_date:%Y%m%d}_{digest[:20]}"


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    GIT_COMMIT,
    TASK_RUN_ID,
    EXECUTION_COUNT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    run_date = date.fromisoformat(RUN_DATE)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    cfg = load_client_config(CLIENT)
    mapping = cfg["theme_mapping_v2"]
    legacy_mapping = cfg["theme_mapping"]
    if not mapping.get("source_of_truth"):
        raise ValueError("theme_mapping_v2 must be marked as source_of_truth")
    if not GIT_COMMIT or not GIT_COMMIT.strip():
        raise ValueError("GIT_COMMIT must not be empty")
    task_run_id = int(TASK_RUN_ID)
    execution_count = int(EXECUTION_COUNT)
    if task_run_id < 1 or execution_count < 0:
        raise ValueError("Invalid task run identity")

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
        url=legacy_mapping["url"],
        worksheet_name=legacy_mapping["sheet"],
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
    legacy_normalised = normalise_theme_mapping(legacy).select(
        *MAPPING_COLUMNS
    )
    legacy_invalid = (
        F.col("Theme").isNull()
        | (F.trim(F.col("Theme")) == "")
        | F.col("TargetingAttributes").isNull()
        | (F.trim(F.col("TargetingAttributes")) == "")
        | ~valid_theme_rank_condition()
    )
    legacy_status = legacy_normalised.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(legacy_invalid, F.lit(1)).otherwise(F.lit(0))).alias(
            "invalid_count"
        ),
    ).first()
    warning_count += int(legacy_status["invalid_count"] or 0) + int(
        legacy_status["row_count"] == 0
    )
    landing_id = _build_landing_id(
        run_date,
        GIT_COMMIT.strip(),
        mapping,
        legacy_mapping,
    )
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
    receipt = replace_scope_by_name(
        output,
        config.tables_write.scoring_input_theme_mapping_raw,
        {"LandingID": landing_id},
        output.columns,
        spark=spark,
        build_id=landing_id,
        attempt_id=f"{landing_id}:{task_run_id}:{execution_count}",
        git_commit=GIT_COMMIT.strip(),
    )
    if receipt.delta_version is None:
        raise RuntimeError("Theme Mapping write has no Delta receipt")
    landing_version = receipt.delta_version
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
        parser.get_arg("--git_commit"),
        parser.get_arg("--task_run_id"),
        parser.get_arg("--execution_count"),
    )
