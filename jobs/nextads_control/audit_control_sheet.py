import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

import pyspark.sql.functions as F
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import DataFrame, SparkSession

from next_ads.common import config_manager
from next_ads.control.control_sheet_audit import (
    ControlSheetAuditSpec,
    audit_control_sheet,
)
from next_ads.control.load_control_sheet import (
    resolve_control_sheet_locations,
)


@dataclass(frozen=True)
class AuditTables:
    raw_latest: str
    raw_history: str
    processed_latest: str
    processed_history: str
    cms_latest: str


def parse_run_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if parsed.isoformat() != value:
        raise ValueError("--run_date must use ISO format YYYY-MM-DD")
    return parsed


def resolve_audit_tables(config, route: str) -> AuditTables:
    if route == "v1":
        return AuditTables(
            raw_latest=config.tables_write.control_sheet_raw_latest,
            raw_history=config.tables_write.control_sheet_raw,
            processed_latest=config.tables_write.control_sheet_latest,
            processed_history=config.tables_write.control_sheet,
            cms_latest=config.tables_write.cms_content_latest,
        )
    if route == "v2":
        return AuditTables(
            raw_latest=config.tables_write.control_sheet_raw_latest_v2,
            raw_history=config.tables_write.control_sheet_raw_v2,
            processed_latest=config.tables_write.control_sheet_latest_v2,
            processed_history=config.tables_write.control_sheet_v2,
            cms_latest=config.tables_write.cms_content_latest,
        )
    raise ValueError("--route must be either v1 or v2")


def build_audit_spec(config, route: str, run_date: date):
    if route == "v1":
        location_config = resolve_control_sheet_locations(config.locations)
        return ControlSheetAuditSpec(
            route=route,
            run_date=run_date,
            placement_columns=tuple(location_config.read_locations),
            expected_scopes=tuple(location_config.valid_locations),
            scope_column="Location",
            date_format=config.control_sheet.date_format,
        )
    if route == "v2":
        page_types = tuple(config.page_types.keys())
        return ControlSheetAuditSpec(
            route=route,
            run_date=run_date,
            placement_columns=page_types,
            expected_scopes=page_types,
            scope_column="PageType",
            date_format=config.control_sheet_v2.date_format,
        )
    raise ValueError("--route must be either v1 or v2")


def load_previous_partition(
    history: DataFrame,
    run_date: date,
) -> tuple[DataFrame | None, date | None]:
    history_date = F.to_date(F.col("rundate"))
    previous_row = (
        history.where(history_date < F.lit(run_date))
        .agg(F.max(history_date).alias("previous_rundate"))
        .first()
    )
    previous_date = previous_row["previous_rundate"]
    if previous_date is None:
        return None, None
    return history.where(history_date == F.lit(previous_date)), previous_date


def post_warning_safely(logger, url: str, message: str) -> None:
    """Attempt a diagnostic notification without failing the candidate job."""
    try:
        post_to_webhook(url, message)
    except Exception:
        logger.exception(
            "Unable to send the control-sheet warning notification"
        )


def run_audit(
    *,
    spark: SparkSession,
    config,
    route: str,
    run_date: date,
    warn_only: bool,
    job_env: str,
    logger,
):
    tables = resolve_audit_tables(config, route)
    spec = build_audit_spec(config, route, run_date)

    logger.info(
        "Loading %s control snapshots: raw=%s, processed=%s",
        route,
        tables.raw_latest,
        tables.processed_latest,
    )
    logger.info(
        "Reading the latest available CMS snapshot: %s",
        tables.cms_latest,
    )

    raw_current = spark.table(tables.raw_latest)
    raw_history = spark.table(tables.raw_history)
    processed_current = spark.table(tables.processed_latest)
    processed_history = spark.table(tables.processed_history)
    cms_latest = spark.table(tables.cms_latest)

    previous_raw, previous_raw_date = load_previous_partition(
        raw_history,
        run_date,
    )
    previous_processed, previous_processed_date = load_previous_partition(
        processed_history,
        run_date,
    )
    logger.info(
        "Previous %s snapshots before %s: raw=%s, processed=%s",
        route,
        run_date.isoformat(),
        previous_raw_date,
        previous_processed_date,
    )

    report = audit_control_sheet(
        raw_current=raw_current,
        processed_current=processed_current,
        cms_latest=cms_latest,
        spec=spec,
        previous_raw=previous_raw,
        previous_processed=previous_processed,
    )
    rendered_report = report.render()
    if report.has_warnings:
        logger.warning("\n%s", rendered_report)
        if job_env.lower() == "prod":
            post_warning_safely(
                logger,
                config.webhooks.input_warnings,
                report.compact_message(max_chars=3500),
            )
        if not warn_only:
            raise ValueError(
                f"{route} control-sheet audit found "
                f"{report.warning_count:,} warning(s)"
            )
    else:
        logger.info("\n%s", rendered_report)

    return report


def run_warning_only_audit(**kwargs):
    """Keep business findings warning-only; propagate technical failures."""
    return run_audit(**kwargs)


def main(
    JOB_ENV: str,
    CLIENT: str,
    ROUTE: str,
    RUN_DATE: str,
    LOG_LEVEL: str,
    WARN_ONLY: bool = False,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)

    if not CLIENT:
        if JOB_ENV.lower() != "dev":
            raise ValueError(
                f"Client must be specified when running in {JOB_ENV}"
            )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    route = ROUTE.lower()
    run_date = parse_run_date(RUN_DATE)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    spark = configure_spark()

    logger.info(
        "Auditing %s control data for client=%s, "
        "environment=%s, run_date=%s",
        route,
        CLIENT,
        JOB_ENV,
        run_date.isoformat(),
    )
    run_warning_only_audit(
        spark=spark,
        config=config,
        route=route,
        run_date=run_date,
        warn_only=WARN_ONLY,
        job_env=JOB_ENV,
        logger=logger,
    )
    logger.info("Control-sheet audit complete")


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "ROUTE": jobparser.get_arg("--route"),
        "RUN_DATE": jobparser.get_arg("--run_date"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "WARN_ONLY": jobparser.has_arg("--warn-only"),
    }


if __name__ == "__main__":
    main(**parse_args())
