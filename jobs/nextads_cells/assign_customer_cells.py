import sys
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

from pyspark.sql import functions as F
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (
    assert_pk,
    chain_when_thens,
)
from dsutils.argparser import get_job_parser
from next_ads.decisioning.assignment import (
    assign_predetermined_audience,
    get_algo_divisions,
    melt_transient_cells,
)
from next_ads.common import config_manager, etl
from next_ads.common.determinism import stable_fraction
from next_ads.common.paths import load_client_config, resolve_sql_path
from next_ads.common.snapshot_writes import (
    capture_run_date,
    publish_history_and_latest,
    replace_validated_scope,
    replace_validated_snapshot,
    with_run_date,
)


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
SAMPLE_MODE = jobparser.get_arg("--sample_mode")  # True/False
REFRESH_CONTROL_DATE = jobparser.get_arg("--refresh_control_date")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
RUN_DATE = capture_run_date(spark)
logger.info(f"Running in job environment: {JOB_ENV}")
logger.info(f"Run date set to: {RUN_DATE}")
if SAMPLE_MODE:
    SAMPLE_FRACTION = 0.0001
    logger.warning(
        f"SAMPLE MODE ENABLED - Using {SAMPLE_FRACTION * 100:.5f}% of data"
    )
else:
    SAMPLE_FRACTION = 1.0

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
FIXED_CELLS_TABLE = etl.map_tbl(
    tbls["customer_cells_fixed_latest"], **tbl_args
)
FIXED_CELLS_HISTORY_TABLE = etl.map_tbl(
    tbls["customer_cells_fixed_history"], **tbl_args
)
TRANSIENT_CELLS_TABLE = etl.map_tbl(
    tbls["customer_cells_transient"], **tbl_args
)
TRANSIENT_CELLS_TABLE_LATEST = etl.map_tbl(
    tbls["customer_cells_transient_latest"], **tbl_args
)

# Get read tables
TABLES_READ = cfg["tables"]["read"]
SVOC = TABLES_READ["svoc_cust"]
RPID_WITH_ACCOUNTS = TABLES_READ["rpid_with_accounts"]
MODEL_SCORES_LATEST = TABLES_READ["model_scores_latest"]

SQL_FILES = cfg["sql_files"]
ACCOUNT_DEPARTMENT_SCORE_SQL = str(
    resolve_sql_path(SQL_FILES["account_department_scores"])
)
WEBHOOK_URL_DS = cfg["webhooks"]["DS Warnings"]

FALLOW_PC = cfg["fallow_control"]["proportion"]
FALLOW_SEED = cfg["fallow_control"]["seed"]
FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]
FALLOW_FALSE_LABEL = cfg["fallow_control"]["false_label"]
FIXED_CELLS = cfg["fixed_cells"]

TODAY = RUN_DATE.isoformat()
REFRESH_REQUESTED = REFRESH_CONTROL_DATE == TODAY
FULL_REFRESH_REQUIRED = False
df_fixed_latest_existing = spark.table(FIXED_CELLS_TABLE)

if REFRESH_REQUESTED:
    logger.info(f"Control refresh requested on today's date: {TODAY}")
    logger.info(
        "Archiving existing fixed cells table from: "
        + f"{FIXED_CELLS_TABLE} to {FIXED_CELLS_HISTORY_TABLE}"
    )
    archive_dates = [
        row["rundate"]
        for row in (
            df_fixed_latest_existing.select("rundate").distinct().collect()
        )
    ]
    archive_date_count_error = (
        "Fixed cells latest must contain exactly one rundate before refresh"
    )
    assert len(archive_dates) == 1, archive_date_count_error
    archive_date = archive_dates[0]
    if archive_date == RUN_DATE:
        logger.info(
            "Fixed cells were already refreshed for this run date; "
            "preserving the published assignment on retry"
        )
    else:
        FULL_REFRESH_REQUIRED = True
        df_to_archive = with_run_date(
            df_fixed_latest_existing.drop("rundate"),
            archive_date,
            column="RunDateEnd",
        )
        replace_validated_scope(
            spark,
            df_to_archive,
            table=FIXED_CELLS_HISTORY_TABLE,
            scope={"RunDateEnd": archive_date},
            key_columns=["AccountNumber"],
        )
        logger.info(
            "Fixed cells archived successfully for RunDateEnd: "
            + f"{archive_date}"
        )

# Checking how many times the control group has been refreshed to increment
# the fallow seed for different deterministic random assignments
refresh_count = (
    spark.table(FIXED_CELLS_HISTORY_TABLE)
    .where(F.col("RunDateEnd") < F.lit(RUN_DATE))
    .select("RunDateEnd")
    .distinct()
).count()
logger.info(f"Times control has been refreshed: {refresh_count}")
FALLOW_SEED += refresh_count
logger.info(f"Fallow seed set to: {FALLOW_SEED}")

transient_cells = False
if "transient_cells" in cfg:
    transient_cells = True
    TRANSIENT_CELLS = cfg["transient_cells"]

# Query inherited from legacy script
# TODO: Should we take lastest updated record to de-dup instead?
df_rpid_w_acc = spark.table(RPID_WITH_ACCOUNTS).select(
    "account_number", "roamingprofileid"
)

# SVOC table used because it contains older accounts too, apparently
# Where clause inherited from legacy script
df_cust = (
    spark.table(SVOC)
    .where(
        (F.col("countrycode").isin("GB"))
        & (F.col("client") == "NEXT")
        & (F.col("AccountIsCurrent") == "Y")
        & (F.col("LatestAccountKeyIndicator") == 1)
    )
    .withColumn(
        "_sample_fraction",
        stable_fraction(
            "account_number",
            seed=42,
            namespace="customer-cells-dev-sample",
        ),
    )
    .filter(F.col("_sample_fraction") < F.lit(SAMPLE_FRACTION))
    .drop("_sample_fraction")
)
df_cust = (
    df_cust.join(df_rpid_w_acc, on="account_number")
    .select("account_number", "specialaccountindicator")
    .withColumnRenamed("account_number", "AccountNumber")
)
df_cust = df_cust.distinct()
df_staff = df_cust.where(F.col("specialaccountindicator") == "S")
df_cust = df_cust.select("AccountNumber")

assert_pk(df_cust, ["AccountNumber"])
df_cust.cache()
logger.info(f"Customer base: {df_cust.count():,}")

df_fallow = (
    df_cust.withColumn(
        "RandomFallow",
        stable_fraction(
            "AccountNumber",
            seed=FALLOW_SEED,
            namespace="customer-cells-fallow",
        ),
    )
    .withColumn("FallowControl", F.col("RandomFallow") <= FALLOW_PC)
)
df_fallow.cache()
# TODO: Calibrate spend per customer of fallow and test group?

df_fc = df_fallow.select("AccountNumber")

for fixed_cell in FIXED_CELLS:
    df_fc = (
        df_fc.withColumn(
            f"Random{fixed_cell}",
            stable_fraction(
                "AccountNumber",
                seed=FIXED_CELLS[fixed_cell]["seed"],
                namespace=f"customer-cells-{fixed_cell}",
            ),
        )
        .withColumn(
            fixed_cell, chain_when_thens(FIXED_CELLS[fixed_cell]["cells"])
        )
    )
df_fc.cache()

df_cells = df_fallow.join(df_fc, on="AccountNumber", how="left").select(
    "AccountNumber", "FallowControl", *list(FIXED_CELLS.keys())
)
df_cells.cache()


df_cells = df_cells.withColumn(
    "FallowControl",
    F.when(F.col("FallowControl"), F.lit(FALLOW_TRUE_LABEL)).otherwise(
        F.lit(FALLOW_FALSE_LABEL)
    ),
)

df_cells = (
    df_cells.join(df_staff, on="AccountNumber", how="left")
    .withColumn(
        "FallowControl",
        F.when(
            F.col("specialaccountindicator") == "S", FALLOW_FALSE_LABEL
        ).otherwise(F.col("FallowControl")),
    )
    .withColumn(
        "HomePageTest1",
        F.when(F.col("specialaccountindicator") == "S", "Best").otherwise(
            F.col("HomePageTest1")
        ),
    )
    .withColumn(
        "ShoppingBagTest1",
        F.when(F.col("specialaccountindicator") == "S", "Best").otherwise(
            F.col("ShoppingBagTest1")
        ),
    )
    .withColumn(
        "OrderCompleteTest1",
        F.when(F.col("specialaccountindicator") == "S", "Best").otherwise(
            F.col("OrderCompleteTest1")
        ),
    )
    .withColumn(
        "LandingPageTest1",
        F.when(F.col("specialaccountindicator") == "S", "Best").otherwise(
            F.col("LandingPageTest1")
        ),
    )
    .withColumn(
        "ChampionChallenger",
        F.when(
            F.col("specialaccountindicator") == "S", "Challenger"
        ).otherwise(F.col("ChampionChallenger")),
    )
    .withColumn(
        "PageTypeIsolation",
        F.when(F.col("specialaccountindicator") == "S", "AllPages").otherwise(
            F.col("PageTypeIsolation")
        ),
    )
)

if FULL_REFRESH_REQUIRED:
    logger.info("Using an empty existing-cell snapshot for full refresh")
    fixed_cells_schema = df_fixed_latest_existing.drop("rundate").schema
    df_cells_existing = spark.createDataFrame([], schema=fixed_cells_schema)
else:
    df_cells_existing = df_fixed_latest_existing.drop("rundate")
df_cells_existing.cache()

n_cust_existing = df_cells_existing.count()
logger.info(f"Existing customers: {n_cust_existing:,}")

df_cust_new = df_cells.select("AccountNumber").join(
    df_cells_existing.select("AccountNumber"),
    on="AccountNumber",
    how="leftanti",
)

n_cust_new = df_cust_new.count()
logger.info(f"New customers: {n_cust_new:,}")

pk_columns = ["AccountNumber", "rundate"]
existing_cols = [c for c in df_cells_existing.columns if c not in pk_columns]
proposed_cols = [c for c in df_cells.columns if c not in pk_columns]
overlapping_cols = [c for c in proposed_cols if c in existing_cols]
new_cols = [c for c in proposed_cols if c not in existing_cols]
deprecated_cols = [c for c in existing_cols if c not in proposed_cols]

logger.info(f"Existing columns:    {existing_cols}")
logger.info(f"Proposed columns:    {proposed_cols}")
logger.info(f"Overlapping columns: {overlapping_cols}")
logger.info(f"New columns:         {new_cols}")
logger.info(f"Deprecated columns:  {deprecated_cols}")

df_cells_new = df_cust_new.join(
    df_cells.select("AccountNumber", *overlapping_cols),
    on="AccountNumber",
    how="left",
)

for dcol in deprecated_cols:
    df_cells_new = df_cells_new.withColumn(dcol, F.lit(None))

if n_cust_new > 0:
    logger.info("Unioning new customers for existing columns")
    cols_for_union = ["AccountNumber", *existing_cols]
    schema_mismatch_msg = "New cell schema mismatch with existing"
    assert cols_for_union == df_cells_existing.columns, schema_mismatch_msg
    df_cells_existing_updated = df_cells_existing.unionByName(
        df_cells_new.select(cols_for_union)
    )
else:
    df_cells_existing_updated = df_cells_existing

if len(new_cols) > 0:
    df_cells_new_cols = df_cells.select("AccountNumber", *new_cols)
    logger.info("Joining new columns for all customers")
    df_cells_full = df_cells_existing_updated.join(
        df_cells_new_cols, on="AccountNumber", how="left"
    )
else:
    df_cells_full = df_cells_existing_updated

df_cells_full.cache()

for ncol in new_cols:
    n_null = df_cells_full.where(F.col(ncol).isNull()).count()
    logger.warning(f"{n_null:,} existing customers not assigned {ncol}")

logger.info(f"Atomically replacing fixed cells snapshot: {FIXED_CELLS_TABLE}")
df_cells_full_with_date = with_run_date(df_cells_full, RUN_DATE)
replace_validated_snapshot(
    spark,
    df_cells_full_with_date,
    table=FIXED_CELLS_TABLE,
    key_columns=["AccountNumber"],
)

df_cells_transient = None
if transient_cells:
    logger.info("Transient Cells requested")
    transient_cell_dfs = []
    if "AlgoDivision" in TRANSIENT_CELLS:
        logger.info("Assigning AlgoDivision")
        df_divs = get_algo_divisions(
            ACCOUNT_DEPARTMENT_SCORE_SQL,
            TRANSIENT_CELLS_TABLE_LATEST,
            WEBHOOK_URL_DS,
            JOB_ENV,
        )
        transient_cell_dfs.append(df_divs)

    if "Audiences" in TRANSIENT_CELLS:
        logger.info("Assigning Audiences")
        df_audiences = assign_predetermined_audience(
            audiences=TRANSIENT_CELLS["Audiences"], tables=TABLES_READ
        )
        transient_cell_dfs.append(df_audiences)

    if transient_cell_dfs:
        df_cells_transient = melt_transient_cells(transient_cell_dfs.pop())
        for df_tc in transient_cell_dfs:
            df_tc_long = melt_transient_cells(df_tc)
            df_cells_transient = df_cells_transient.unionByName(df_tc_long)
else:
    logger.info("No Transient Cells requested")

if df_cells_transient is None:
    logger.info("Publishing a valid empty transient-cell snapshot")
    transient_schema = (
        spark.table(TRANSIENT_CELLS_TABLE_LATEST).drop("rundate").schema
    )
    df_cells_transient = spark.createDataFrame([], schema=transient_schema)

logger.info(
    "Publishing transient-cell history before atomically replacing latest"
)
publish_history_and_latest(
    spark,
    df_cells_transient,
    history_table=TRANSIENT_CELLS_TABLE,
    latest_table=TRANSIENT_CELLS_TABLE_LATEST,
    key_columns=["AccountNumber", "Cell"],
    run_date=RUN_DATE,
)

df_cust.unpersist()
df_fallow.unpersist()
df_fc.unpersist()
df_cells.unpersist()
df_cells_existing.unpersist()
df_cells_full.unpersist()

logger.info("Run complete")
