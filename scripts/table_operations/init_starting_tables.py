"""Script to setup minimal starting tables in dev environment.

Usage guide: python scripts/table_operations/init_starting_tables.py
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
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

from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from dsutils.dbc import configure_spark


def main(CLIENT, LOG_LEVEL):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()

    if not CLIENT:
        CLIENT = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    # load configuration
    config_dev = config_manager.load_config("dev")
    config_prod = config_manager.load_config("prod")
    logger.info(f"Configuring run for client: {CLIENT}")

    sql_scripts = [
        f"TRUNCATE TABLE {config_dev.tables_write.attribute_set_latest}",
        f"""
        INSERT INTO {config_dev.tables_write.attribute_set_latest}
        SELECT *
        FROM {config_prod.tables_write.attribute_set_latest}
        """,
        f"TRUNCATE TABLE {config_dev.tables_write.item_attributes_latest}",
        f"""
        INSERT INTO {config_dev.tables_write.item_attributes_latest}
        SELECT *
        FROM {config_prod.tables_write.item_attributes_latest}
        """,
        f"TRUNCATE TABLE {config_dev.tables_write.theme_mapping_latest}",
        f"""
        INSERT INTO {config_dev.tables_write.theme_mapping_latest}
        SELECT *
        FROM {config_prod.tables_write.theme_mapping_latest}
        """,
        f"TRUNCATE TABLE {config_dev.tables_write.theme_transitions_latest}",
        f"""
        INSERT INTO {config_dev.tables_write.theme_transitions_latest}
        SELECT *
        FROM {config_prod.tables_write.theme_transitions_latest}
        """,
    ]

    for sql in sql_scripts:
        try:
            logger.info(f"Executing SQL script: {sql}")
            spark.sql(sql)
        except Exception as e:
            logger.error(f"Failed to execute SQL script: {str(e)}")

    logger.info("Run Complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(CLIENT, LOG_LEVEL)
