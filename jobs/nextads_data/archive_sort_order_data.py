from pathlib import Path
import sys
from datetime import date

# get dbutils and resolve project root for both local and databricks environments
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

from dsutils.dbc import get_dbutils, configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from next_ads.common import config_manager
from next_ads.common.snapshot_writes import (
    replace_validated_scope,
    with_run_date,
)


def setup_run_context(JOB_ENV: str, CLIENT: str, LOG_LEVEL: str):
    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()

    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    logger.info(f"Configuring run for client: {CLIENT}")

    return logger, spark, CLIENT, config


def write_history_table(
    df_output,
    table: str,
    logger,
    pk_cols: list[str],
    *,
    spark,
    run_date: date,
):
    logger.info(f"Loading payload output to {table}")
    prepared = with_run_date(
        df_output.drop("run_date").drop("rundate"),
        run_date,
    )
    replace_validated_scope(
        spark,
        prepared,
        table=table,
        scope={"rundate": run_date},
        key_columns=pk_cols,
        columns=prepared.columns,
    )


def resolve_run_date(run_date: str | date) -> date:
    if isinstance(run_date, date):
        return run_date
    if not isinstance(run_date, str):
        raise ValueError("--run_date must use ISO format YYYY-MM-DD")
    run_date_text = run_date.strip()
    try:
        parsed_run_date = date.fromisoformat(run_date_text)
    except ValueError as exc:
        raise ValueError(
            "--run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if parsed_run_date.isoformat() != run_date_text:
        raise ValueError("--run_date must use ISO format YYYY-MM-DD")
    return parsed_run_date


def main(JOB_ENV, CLIENT, LOG_LEVEL, RUN_DATE):
    run_date = resolve_run_date(RUN_DATE)
    logger, spark, CLIENT, config = setup_run_context(
        JOB_ENV, CLIENT, LOG_LEVEL
    )

    sort_order_latest = spark.table(config.tables_write.sort_order_v2_latest)
    logger.info("Writing sort order history table..")
    # write the output to the payload tables
    write_history_table(
        sort_order_latest,
        config.tables_write.sort_order_v2,
        logger,
        pk_cols=["UniqueAdID", "item_pos"],
        spark=spark,
        run_date=run_date,
    )

    cms_content_latest = spark.table(config.tables_write.cms_content_latest)
    logger.info("Writing CMS content history table..")
    # write the output to the payload tables
    write_history_table(
        cms_content_latest,
        config.tables_write.cms_content,
        logger,
        pk_cols=["CMSPageID"],
        spark=spark,
        run_date=run_date,
    )

    logger.info("Ads Data Pull history updated successfully!")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    RUN_DATE = jobparser.get_arg("--run_date")
    main(JOB_ENV, CLIENT, LOG_LEVEL, RUN_DATE)
