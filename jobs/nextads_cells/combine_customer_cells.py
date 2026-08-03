import sys
from datetime import date, timedelta
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

import pyspark.sql.functions as F
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.common import config_manager, etl
from next_ads.common.paths import load_client_config
from next_ads.common.snapshot_writes import (
    replace_validated_snapshot,
    with_run_date,
)
from next_ads.candidates.foundation import (
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
    schema_checksum,
)
from next_ads.decisioning.customer_cells import ensure_audience_column
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
    summarise_content,
)


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
RUN_DATE_RAW = jobparser.get_arg("--run_date")
if not RUN_DATE_RAW:
    raise ValueError("--run_date is required")
try:
    RUN_DATE = date.fromisoformat(RUN_DATE_RAW)
except ValueError as exc:
    raise ValueError("--run_date must use ISO format YYYY-MM-DD") from exc
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")
logger.info(f"Run date set to: {RUN_DATE}")

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

# load configuration
config = config_manager.load_config(JOB_ENV, client=CLIENT)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f"Write schema set to {SCHEMA}")

# Map write schema to parameterised write table names
tbl_args = {
    "catalog": config.catalog_write,
    "schema": SCHEMA,
    "client": CLIENT,
}
FIXED_CELLS_TABLE_LATEST = etl.map_tbl(
    tbls["customer_cells_fixed_latest"], **tbl_args
)
TRANSIENT_CELLS_TABLE_LATEST = etl.map_tbl(
    tbls["customer_cells_transient_latest"], **tbl_args
)
CELLS_TABLE_LATEST = etl.map_tbl(tbls["customer_cells_latest"], **tbl_args)
PREMIUM_CUST_TABLE = cfg["tables"]["read"]["premium_customers"]


logger.info("Combining latest fixed and transient cell assignments")

df_cells_fixed = spark.table(FIXED_CELLS_TABLE_LATEST).drop("rundate")

df_cells_transient = (
    spark.table(TRANSIENT_CELLS_TABLE_LATEST)
    .drop("rundate")
    .groupBy("AccountNumber")
    .pivot("Cell")
    .agg(F.max("CellValue"))
)

# Inner join will remove customers that don't have AlgoDivision
# TODO: Will this bias the results? Address when reviewing AlgoDivision.

transient_is_usable = "AlgoDivision" in df_cells_transient.columns
if transient_is_usable:
    df_cells_transient = df_cells_transient.where(
        F.col("AlgoDivision").isNotNull()
    ).cache()
    transient_is_usable = not df_cells_transient.isEmpty()

if transient_is_usable:
    df_cells = df_cells_fixed.join(
        df_cells_transient, on="AccountNumber", how="inner"
    )
    df_dropped = df_cells_fixed.join(
        df_cells_transient, on="AccountNumber", how="leftanti"
    )
    n_dropped = df_dropped.count()
    logger.warning(
        f"{n_dropped:,} customers dropped " + "when joining transient cells"
    )

    # Collect premium flag values for customers
    df_premium_cust = (
        spark.table(PREMIUM_CUST_TABLE)
        .withColumn(
            "is_premium_flag",
            F.when(F.col("PS1") == "premium", 1).otherwise(0),
        )
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumnRenamed("is_premium_flag", "IsPremium")
        .select("AccountNumber", "IsPremium")
    )
    # Left join and fill any blanks with 0 (not premium)
    df_cells = df_cells.join(
        df_premium_cust, on="AccountNumber", how="left_outer"
    )
    df_cells = df_cells.fillna(0, subset=["IsPremium"])
else:
    logger.warning(
        "No usable AlgoDivision assignments found; preserving the last "
        "accepted combined-cell snapshot"
    )
    df_cells = None

publish_new_snapshot = df_cells is not None and not df_cells.isEmpty()
try:
    if publish_new_snapshot:
        df_cells = ensure_audience_column(df_cells)
        df_selected = with_run_date(df_cells, RUN_DATE).persist()
        summary = summarise_content(
            df_selected,
            key_columns=("AccountNumber",),
        )
        summary.require_valid("combined customer cells")
        logger.info(f"Atomically replacing combined cells: {CELLS_TABLE_LATEST}")
        replace_validated_snapshot(
            spark,
            df_selected,
            table=CELLS_TABLE_LATEST,
            key_columns=["AccountNumber"],
        )
        selected_version = latest_delta_version(spark, CELLS_TABLE_LATEST)
        selected_run_date = RUN_DATE
        selection_status = READY_FOR_NEXTADS
        warning_count = 0
    else:
        selected_version = latest_delta_version(spark, CELLS_TABLE_LATEST)
        df_selected = read_delta_version(
            spark,
            CELLS_TABLE_LATEST,
            selected_version,
        ).persist()
        summary = summarise_content(
            df_selected,
            key_columns=("AccountNumber",),
        )
        summary.require_valid("accepted combined customer cells")
        date_bounds = df_selected.agg(
            F.min("rundate").alias("min_rundate"),
            F.max("rundate").alias("max_rundate"),
        ).first()
        if (
            date_bounds["min_rundate"] is None
            or date_bounds["min_rundate"] != date_bounds["max_rundate"]
        ):
            raise ValueError(
                "Accepted combined customer cells must contain one rundate"
            )
        selected_run_date = date_bounds["min_rundate"]
        if not (RUN_DATE - timedelta(days=1) <= selected_run_date <= RUN_DATE):
            raise ValueError(
                "Accepted combined customer cells are more than one day old"
            )
        selection_status = FALLBACK_PREVIOUS
        warning_count = 1
        logger.warning(
            "Using combined customer cells from %s at Delta version %s",
            selected_run_date,
            selected_version,
        )

    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="customer_cells_table", value=CELLS_TABLE_LATEST)
    task_values.set(key="customer_cells_delta_version", value=selected_version)
    task_values.set(
        key="customer_cells_source_run_date",
        value=selected_run_date.isoformat(),
    )
    task_values.set(key="customer_cells_selection_status", value=selection_status)
    task_values.set(key="customer_cells_row_count", value=summary.row_count)
    task_values.set(
        key="customer_cells_content_checksum",
        value=summary.content_checksum,
    )
    task_values.set(
        key="customer_cells_schema_checksum",
        value=schema_checksum(df_selected),
    )
    task_values.set(key="customer_cells_warning_count", value=warning_count)
finally:
    if "df_selected" in locals():
        df_selected.unpersist()
    if "AlgoDivision" in df_cells_transient.columns:
        df_cells_transient.unpersist()

logger.info("Run complete")
