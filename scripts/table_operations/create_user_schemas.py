"""Script to setup minimal starting tables in dev environment.

Usage guide: python scripts/table_operations/init_starting_tables.py
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
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

import json
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import etl
from next_ads.utils import config_manager
from dsutils.dbc import configure_spark, get_dbutils


jobparser = get_job_parser()
jobparser._parse_args()
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
dbutils = get_dbutils()

# load configuration
config_dev = config_manager.load_config("dev")
spn_client_id = dbutils.secrets.get(
    scope=config_dev.secret_key_spn_secret,
    key=config_dev.secret_key_spn_clientid,
)

users = config_dev.databricks_user_names
for user_name in users:
    try:
        logger.info(
            f"Creating schema {config_dev.catalog_write}.{user_name} for user: {user_name}"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {config_dev.catalog_write}.{user_name}"
        )
        spark.sql(
            f"GRANT MANAGE ON SCHEMA {config_dev.catalog_write}.{user_name} TO `{spn_client_id}`"
        )
        spark.sql(
            f"GRANT ALL PRIVILEGES ON SCHEMA {config_dev.catalog_write}.{user_name} TO `{user_name}@next.co.uk`"
        )
    except Exception as e:
        logger.error(f"Failed to create schema for user {user_name}: {str(e)}")

logger.info("Run Complete")
