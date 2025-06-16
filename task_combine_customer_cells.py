import json
import pyspark.sql.functions as F
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import create_table_from_df, map_tbl
from dsutils.argparser import get_job_parser


jobparser = get_job_parser()
jobparser._parse_args()
JOBNAME = jobparser.get_arg('--jobname')
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert not JOBNAME, 'Client must be specified when running as a job'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
FIXED_CELLS_TABLE_LATEST = map_tbl(
    tbls["customer_cells_fixed_latest"], **tbl_args)
TRANSIENT_CELLS_TABLE_LATEST = map_tbl(
    tbls["customer_cells_transient_latest"], **tbl_args)
CELLS_TABLE_LATEST = map_tbl(tbls["customer_cells_latest"], **tbl_args)
PREMIUM_CUST_TABLE = cfg["tables"]["read"]["premium_customers"]


logger.info("Combining latest fixed and transient cell assignments")

df_cells_fixed = (
    spark
    .table(FIXED_CELLS_TABLE_LATEST)
    .drop("rundate")
)

df_cells_transient = (
    spark
    .table(TRANSIENT_CELLS_TABLE_LATEST)
    .drop("rundate")
    .groupBy("AccountNumber")
    .pivot("Cell")
    .agg(F.max("CellValue"))
    .where(F.col('AlgoDivision').isNotNull())
)

# Inner join will remove customers that don't have AlgoDivision
# TODO: Will this bias the results? Address when reviewing AlgoDivision.

if df_cells_transient.count() > 0:
    df_cells = (
        df_cells_fixed
        .join(df_cells_transient,
              on="AccountNumber",
              how="inner")
    )
    df_dropped = (
        df_cells_fixed
        .join(df_cells_transient,
              on="AccountNumber",
              how="leftanti")
    )
    n_dropped = df_dropped.count()
    logger.warning(f"{n_dropped:,} customers dropped " +
                   "when joining transient cells")

    # Collect premium flag values for customers
    df_premium_cust = (
        spark
        .table(PREMIUM_CUST_TABLE)
        .withColumn(
            "is_premium_flag",
            F.when(F.col("PS1") == "premium", 1).otherwise(0))
        .withColumnRenamed('account_number', 'AccountNumber')
        .withColumnRenamed('is_premium_flag', 'IsPremium')
        .select('AccountNumber', 'IsPremium')
    )
    # Left join and fill any blanks with 0 (not premium)
    df_cells = df_cells.join(
                            df_premium_cust, on="AccountNumber",
                            how="left_outer")
    df_cells = df_cells.fillna(
                            0, subset=["IsPremium"])
else:
    df_cells = df_cells_fixed

logger.info(f"Writing combined cells to {CELLS_TABLE_LATEST}")
create_table_from_df(
    df=df_cells,
    table=CELLS_TABLE_LATEST,
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True,
    append_rundate=True
    )

logger.info("Run complete")
