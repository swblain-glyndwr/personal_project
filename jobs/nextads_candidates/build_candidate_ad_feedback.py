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
    build_ad_feedback_metrics,
    parse_run_date,
    source_binding,
)
from next_ads.candidates.foundation_manifest import canonical_json
from next_ads.common import config_manager, etl
from next_ads.common.delta_writes import replace_scope_by_name
from next_ads.common.paths import load_client_config
from next_ads.common.spark_runtime import configure_lean_spark
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
)


OUTPUT_COLUMNS = (
    "CandidateFoundationSnapshotID",
    "RunDate",
    "UniqueAdID",
    "IncARPSAdjPct",
)


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    FOUNDATION_SNAPSHOT_ID,
    GIT_COMMIT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    configure_lean_spark(spark)
    logical_date = parse_run_date(RUN_DATE)
    snapshot_id = (FOUNDATION_SNAPSHOT_ID or "").strip()
    if not snapshot_id:
        raise ValueError("--foundation_snapshot_id is required")
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    cfg = load_client_config(CLIENT)
    results_table = etl.map_tbl(
        cfg["tables"]["write"]["results_ads"],
        catalog="marketingdata_prod",
        schema="warehouse",
        client=CLIENT,
    )
    results_version = latest_delta_version(spark, results_table)
    results = read_delta_version(spark, results_table, results_version)
    source_bindings = [
        source_binding(
            name="ad_results",
            role="ad_feedback",
            table=results_table,
            delta_version=results_version,
            frame=results,
        )
    ]
    metrics = build_ad_feedback_metrics(
        results,
        run_date=logical_date,
        sessions_threshold=int(cfg["results_prm"]["min_c_sessions"]),
        lookback_period_days=int(
            cfg["incrementality"]["incremental_lookback"]
        ),
    ).select(
        F.lit(snapshot_id).alias("CandidateFoundationSnapshotID"),
        F.lit(logical_date).cast("date").alias("RunDate"),
        "UniqueAdID",
        "IncARPSAdjPct",
    )
    table = config.tables_write.candidate_ad_feedback
    receipt = replace_scope_by_name(
        metrics,
        table,
        {
            "CandidateFoundationSnapshotID": snapshot_id,
            "RunDate": logical_date,
        },
        OUTPUT_COLUMNS,
        spark=spark,
        build_id=snapshot_id,
        attempt_id=snapshot_id,
        git_commit=GIT_COMMIT,
    )

    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="ad_feedback_table", value=table)
    task_values.set(
        key="ad_feedback_delta_version", value=receipt.delta_version
    )
    task_values.set(key="ad_feedback_row_count", value=receipt.row_count)
    task_values.set(
        key="ad_feedback_schema_checksum",
        value=receipt.schema_checksum,
    )
    task_values.set(
        key="ad_feedback_write_receipt_id",
        value=receipt.receipt_id,
    )
    task_values.set(
        key="ad_feedback_source_bindings_json",
        value=canonical_json(source_bindings),
    )
    logger.info(
        "Published %s advert feedback rows for %s at Delta version %s",
        receipt.row_count,
        snapshot_id,
        receipt.delta_version,
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
        "GIT_COMMIT": parser.get_arg("--git_commit"),
    }


if __name__ == "__main__":
    main(**parse_args())
