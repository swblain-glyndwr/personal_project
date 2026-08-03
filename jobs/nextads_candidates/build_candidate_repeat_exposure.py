import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    notebook_path = (
        get_dbutils()
        .notebook.entry_point.getDbutils()
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

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import functions as F

from next_ads.candidates.foundation import (
    build_repeat_ad_exposure,
    parse_run_date,
    source_binding,
)
from next_ads.candidates.foundation_manifest import canonical_json
from next_ads.common import config_manager
from next_ads.common.delta_writes import replace_scope_by_name
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
    summarise_content,
)


OUTPUT_COLUMNS = (
    "CandidateFoundationSnapshotID",
    "RunDate",
    "AccountNumber",
    "AdSeen",
    "sessions_seen_ad_in_last_7_days",
    "MultiSessionDownweightScore",
)


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    FOUNDATION_SNAPSHOT_ID,
):
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    logical_date = parse_run_date(RUN_DATE)
    snapshot_id = (FOUNDATION_SNAPSHOT_ID or "").strip()
    if not snapshot_id:
        raise ValueError("--foundation_snapshot_id is required")
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    source_tables = {
        "sessions_web": config.tables_read.bq_sessions,
        "sessions_app": config.tables_read.bq_sessions_app,
        "actions_web": config.tables_read.bq_actions,
        "actions_app": config.tables_read.bq_actions_app,
    }
    frames = {}
    source_bindings = []
    for name, table in source_tables.items():
        version = latest_delta_version(spark, table)
        frame = read_delta_version(spark, table, version)
        frames[name] = frame
        source_bindings.append(
            source_binding(
                name=name,
                role="repeat_ad_exposure",
                table=table,
                delta_version=version,
                frame=frame,
            )
        )

    output = build_repeat_ad_exposure(
        frames["sessions_web"],
        frames["sessions_app"],
        frames["actions_web"],
        frames["actions_app"],
        run_date=logical_date,
    ).select(
        F.lit(snapshot_id).alias("CandidateFoundationSnapshotID"),
        F.lit(logical_date).cast("date").alias("RunDate"),
        "AccountNumber",
        "AdSeen",
        "sessions_seen_ad_in_last_7_days",
        "MultiSessionDownweightScore",
    )
    output = output.persist()
    try:
        summary = summarise_content(
            output,
            key_columns=(
                "CandidateFoundationSnapshotID",
                "AccountNumber",
                "AdSeen",
            ),
        )
        if summary.null_key_count or summary.duplicate_key_count:
            summary.require_valid("candidate repeat-ad exposure")
        table = config.tables_write.candidate_repeat_ad_exposure
        replace_scope_by_name(
            output,
            table,
            {
                "CandidateFoundationSnapshotID": snapshot_id,
                "RunDate": logical_date,
            },
            OUTPUT_COLUMNS,
            spark=spark,
        )
        output_version = latest_delta_version(spark, table)
    finally:
        output.unpersist()

    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="repeat_ad_exposure_table", value=table)
    task_values.set(key="repeat_ad_exposure_delta_version", value=output_version)
    task_values.set(key="repeat_ad_exposure_row_count", value=summary.row_count)
    task_values.set(
        key="repeat_ad_exposure_content_checksum",
        value=summary.content_checksum,
    )
    task_values.set(
        key="repeat_ad_exposure_source_bindings_json",
        value=canonical_json(source_bindings),
    )
    logger.info(
        "Published %s repeat-ad exposure rows for %s at Delta version %s",
        summary.row_count,
        snapshot_id,
        output_version,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    return {
        "JOB_ENV": parser.get_arg("--job_env"),
        "CLIENT": parser.get_arg("--client") or "next_uk",
        "LOG_LEVEL": parser.get_arg("--log_level"),
        "RUN_DATE": parser.get_arg("--run_date"),
        "FOUNDATION_SNAPSHOT_ID": parser.get_arg("--foundation_snapshot_id"),
    }


if __name__ == "__main__":
    main(**parse_args())
