import sys
from datetime import datetime, timezone
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
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark.sql import functions as F

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ranking.provider_context import (
    ProviderContext,
    transition_provider_context,
)


def _optional_row_value(row, field):
    try:
        return row[field]
    except (KeyError, ValueError):
        return None


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    CONTEXT_SLOT,
    ORCHESTRATION_RUN_ID,
    STATUS="FAILED",
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    table = config.tables_write.score_provider_run_contexts
    rows = (
        spark.table(table)
        .where(
            (F.col("ContextSlot") == CONTEXT_SLOT)
            & (
                F.col("OrchestrationRunID")
                == int(ORCHESTRATION_RUN_ID)
            )
        )
        .collect()
    )
    if not rows and STATUS == "FAILED":
        logger.info("No provider context lease requires cleanup")
        return
    if len(rows) != 1:
        raise ValueError("Provider context finaliser cannot verify ownership")
    row = rows[0]
    if STATUS not in {"CONSUMED", "FAILED"}:
        raise ValueError("Final provider context status is invalid")
    if row["Status"] == STATUS:
        logger.info(
            "Provider context already has final status %s",
            STATUS,
        )
        return
    if row["Status"] == "CONSUMED" and STATUS == "FAILED":
        logger.info("Provider context was consumed successfully")
        return
    if row["Status"] != "ACTIVE":
        raise ValueError(f"Unexpected provider context status {row['Status']}")
    context = ProviderContext(
        context_slot=row["ContextSlot"],
        orchestration_run_id=int(row["OrchestrationRunID"]),
        provider_id=row["ProviderID"],
        provider_build_id=row["ProviderBuildID"],
        provider_build_attempt_id=row["ProviderBuildAttemptID"],
        input_snapshot_id=row["InputSnapshotID"],
        run_date=row["RunDate"],
        model_uri=row["ModelURI"],
        bindings_json=row["BindingsJSON"],
        capability=row["Capability"],
        use_case=row["UseCase"],
        invocation_checksum=row["InvocationChecksum"],
        expires_at=row["ExpiresAt"].replace(tzinfo=timezone.utc),
        scoring_foundation_build_id=_optional_row_value(
            row,
            "ScoringFoundationBuildID",
        ),
        scoring_foundation_build_attempt_id=_optional_row_value(
            row,
            "ScoringFoundationBuildAttemptID",
        ),
    )
    transition_provider_context(
        spark,
        context_table=table,
        context=context,
        status=STATUS,
        completed_at=datetime.now(timezone.utc),
    )
    if STATUS == "CONSUMED":
        logger.info("Provider context was consumed successfully")
    else:
        logger.warning("Released failed provider context %s", CONTEXT_SLOT)


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--context_slot"),
        parser.get_arg("--orchestration_run_id"),
        parser.get_arg("--status") or "FAILED",
    )
