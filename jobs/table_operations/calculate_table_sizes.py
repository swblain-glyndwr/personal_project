"""Job script designed to run only in DEV environment to calculate tables sizes."""
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
    PROJECT_ROOT = Path(notebook_path).parent.parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import os
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (
    delete_from_and_load,
    create_table_from_df
)
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.data_validation import schemas
import pandas as pd


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
if LOG_LEVEL:
    configure_logging(log_level=LOG_LEVEL)
else:
    configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")


def extract_table_paths(tables_dict: dict, prefix: str = "") -> dict:
    """Recursively extract all table paths from nested dictionary structure."""
    result = {}
    for key, value in tables_dict.items():
        current_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, str):
            # This is a table path (Dynaconf @format string)
            result[current_key] = value
        elif isinstance(value, dict):
            # Recursively process nested dicts
            result.update(extract_table_paths(value, current_key))
    
    return result


def get_table_size_gb(table_path: str) -> float:
    """Get size of a table in GB using DESCRIBE DETAIL."""
    try:
        result = spark.sql(f"DESCRIBE DETAIL {table_path}").collect()
        if result:
            size_bytes = result[0]["sizeInBytes"]
            size_gb = size_bytes / (1024 ** 3)
            return size_gb
        return 0.0
    except Exception as e:
        logger.warning(f"Failed to get size for {table_path}: {e}")
        return None


config = config_manager.load_config(JOB_ENV)
schemas = config.databricks_user_names + ["ds_sandbox"]

df_summary = pd.DataFrame()

for schema in schemas:
    # reload config file with USER_SCHEMA set to current schema to get correct table paths
    os.environ["USER_SCHEMA"] = schema
    config = config_manager.load_config(JOB_ENV)
    tbls_write = config.get("tables_write", {})

    """Calculate and report table sizes."""
    # Extract all table paths
    tbls = extract_table_paths(tbls_write)
    logger.info(f"Found {len(tbls)} table definitions in tables_write")

    if not tbls:
        logger.warning("No tables found to analyze")

    # Calculate sizes
    tables = []
    
    total_size_gb = 0.0
    for table_key, table_path in sorted(tbls.items()):
        table_sizes = {}
        size_gb = get_table_size_gb(table_path)
        
        if size_gb is not None:
            table_sizes["table_key"] = table_key
            table_sizes["table_size_GB"] = size_gb
            table_sizes["table_path"] = tbls[table_key]
            tables.append(table_sizes)

    df = pd.DataFrame(tables)
    df["schema"] = schema
    # df["rundate"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    df_summary = pd.concat([df_summary, df], ignore_index=True)

sdf_summary = spark.createDataFrame(df_summary)

# Write results to table to ds_sandbox schema
os.environ["USER_SCHEMA"] = "ds_sandbox"
config = config_manager.load_config(JOB_ENV)
delete_from_and_load(
    sdf_summary,
    config.tables_write.nextads_table_sizes,
    pk_cols=["schema", "table_key"],
    del_where={"rundate": "current_date()"},
)