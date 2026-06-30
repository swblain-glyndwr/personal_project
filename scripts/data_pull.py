from pathlib import Path
import sys

# get dbutils and resolve project root for both local and databricks environments
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.dbc import get_dbutils, configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from next_ads.utils import config_manager
from dsutils.etl import delete_from_and_load


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

    config = config_manager.load_config(JOB_ENV)
    logger.info(f"Configuring run for client: {CLIENT}")

    return logger, spark, CLIENT, config


def write_history_table(
    df_output,
    table: str,
    logger,
):
    logger.info(f"Loading payload output to {table}")

    delete_from_and_load(
        df_output.drop("run_date").drop("rundate"),
        table,
        pk_cols=["UniqueAdID", "item_pos"],
        del_where={"rundate": "current_date()"},
    )


def main(JOB_ENV, CLIENT, LOG_LEVEL):
    logger, spark, CLIENT, config = setup_run_context(
        JOB_ENV, CLIENT, LOG_LEVEL
    )

    sort_order_latest = spark.table(config.tables_write.sort_order_v2_latest)
    logger.info("Writing to history table..")
    # write the output to the payload tables
    write_history_table(
        sort_order_latest,
        config.tables_write.sort_order_v2,
        logger,
    )

    logger.info("Ads Data Pull history updated successfully!")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(JOB_ENV, CLIENT, LOG_LEVEL)
