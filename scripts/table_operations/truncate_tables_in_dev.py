"""Script to truncate tables in dev environment. This is useful for resetting state during development or testing.

Usage guide: python scripts/table_operations/truncate_tables_in_dev.py
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
from next_ads.utils import etl
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from dsutils.dbc import configure_spark


jobparser = get_job_parser()
jobparser._parse_args()
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()

if not CLIENT:
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

# load configuration
config_dev = config_manager.load_config("dev")
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

tbls = cfg["tables"]["write"]

for k, v in tbls.items():
    tbl_dev = etl.map_tbl(
        v,
        catalog=config_dev.catalog_write,
        schema=config_dev.schema_write,
        client=CLIENT,
    )
    try:
        logger.info(f"Truncating {tbl_dev} table")
        spark.sql(f"truncate table {tbl_dev}")
    except Exception as e:
        logger.error(f"Failed to truncate {k} table: {str(e)}")

logger.info("Run Complete")
