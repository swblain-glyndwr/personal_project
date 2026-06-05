import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from pyspark.sql import functions as F
from datetime import date
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (assert_pk,
                         create_table_from_df, delete_from_and_load,
                         chain_when_thens, truncate_and_load)
from dsutils.argparser import get_job_parser
from next_ads.Assignment import (assign_predetermined_audience,
                                 get_algo_divisions,
                                 melt_transient_cells)
from next_ads.utils import etl
from next_ads.utils import config_manager


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
SAMPLE_MODE = jobparser.get_arg('--sample_mode')  # True/False
REFRESH_CONTROL_DATE = jobparser.get_arg('--refresh_control_date')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")
if SAMPLE_MODE:
    SAMPLE_FRACTION= 0.0001
    logger.warning(f"SAMPLE MODE ENABLED - Using {SAMPLE_FRACTION*100:.5f}% of data")
else:
    SAMPLE_FRACTION=1.0

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
FIXED_CELLS_TABLE = etl.map_tbl(tbls["customer_cells_fixed_latest"], **tbl_args)
FIXED_CELLS_HISTORY_TABLE = etl.map_tbl(tbls["customer_cells_fixed_history"],
                                    **tbl_args)
TRANSIENT_CELLS_TABLE = etl.map_tbl(tbls["customer_cells_transient"], **tbl_args)
TRANSIENT_CELLS_TABLE_LATEST = etl.map_tbl(
    tbls["customer_cells_transient_latest"], **tbl_args)

# Get read tables
TABLES_READ = cfg["tables"]["read"]
SVOC = TABLES_READ["svoc_cust"]
RPID_WITH_ACCOUNTS = TABLES_READ["rpid_with_accounts"]
MODEL_SCORES_LATEST = TABLES_READ["model_scores_latest"]

SQL_FILES = cfg['sql_files']
ACCOUNT_DEPARTMENT_SCORE_SQL = str(PROJECT_ROOT / "sql" / SQL_FILES['account_department_scores'])
WEBHOOK_URL_DS = cfg['webhooks']['DS Warnings']

FALLOW_PC = cfg["fallow_control"]["proportion"]
FALLOW_SEED = cfg["fallow_control"]["seed"]
FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]
FALLOW_FALSE_LABEL = cfg["fallow_control"]["false_label"]
FIXED_CELLS = cfg["fixed_cells"]

TODAY = date.today().strftime(format='%Y-%m-%d')
if REFRESH_CONTROL_DATE == TODAY:
    logger.info(f"Control refresh requested on today's date: {TODAY}")
    logger.info('Archiving existing fixed cells table from: ' +
                f'{FIXED_CELLS_TABLE} to {FIXED_CELLS_HISTORY_TABLE}')
    df_to_archive = (
        spark
        .table(FIXED_CELLS_TABLE)
        .withColumnRenamed('rundate', 'RunDateEnd')
    )
    df_to_archive.createOrReplaceTempView('tbl_to_archive')
    spark.sql(f'''
              insert into {FIXED_CELLS_HISTORY_TABLE}
              select * from tbl_to_archive
              ''')
    logger.info('Checking that fixed cells have been archived')
    df_date_check_from = (
        spark
        .table(FIXED_CELLS_TABLE)
        .select('rundate')
        .distinct()
    )
    assert df_date_check_from.count() == 1
    date_check_from = df_date_check_from.collect()[0][0]
    df_archived = (
        spark
        .table(FIXED_CELLS_HISTORY_TABLE)
        .where(F.col('RunDateEnd') == date_check_from)
        .drop('RunDateEnd')
    )
    archive_count_error = ('Dataframe to archive and dataframe archived' +
                           ' have different row counts')
    assert df_to_archive.count() == df_archived.count(), archive_count_error
    logger.info('Fixed cells table archived successfully')
    logger.info('Truncating fixed cells table for full refresh')
    spark.sql(f'truncate table {FIXED_CELLS_TABLE}')
    logger.info('Archiving of fixed cells complete')

# Checking how many times the control group has been refreshed to increment
# the fallow seed for different deterministic random assignments
refresh_count = (
    spark
    .table(FIXED_CELLS_HISTORY_TABLE)
    .select('RunDateEnd')
    .distinct()
).count()
logger.info(f'Times control has been refreshed: {refresh_count}')
FALLOW_SEED += refresh_count
logger.info(f'Fallow seed set to: {FALLOW_SEED}')

transient_cells = False
if "transient_cells" in cfg:
    transient_cells = True
    TRANSIENT_CELLS = cfg["transient_cells"]

# Query inherited from legacy script
# TODO: Should we take lastest updated record to de-dup instead?
df_rpid_w_acc = (
        spark
        .table(RPID_WITH_ACCOUNTS)
        .select("account_number", "roamingprofileid")
)

# SVOC table used because it contains older accounts too, apparently
# Where clause inherited from legacy script
df_cust = (
    spark
    .table(SVOC)
    .where(
        (F.col("countrycode").isin("GB"))
        & (F.col("client") == "NEXT")
        & (F.col("AccountIsCurrent") == "Y")
        & (F.col("LatestAccountKeyIndicator") == 1)
    )
    .sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)
)
df_cust = (
    df_cust
    .join(df_rpid_w_acc, on="account_number")
    .select("account_number", "specialaccountindicator")
    .withColumnRenamed("account_number", "AccountNumber")
)
df_cust = df_cust.distinct()
df_staff = df_cust.where(F.col("specialaccountindicator") == 'S')
df_cust = df_cust.select("AccountNumber")

assert_pk(df_cust, ["AccountNumber"])
df_cust.cache()
logger.info(f"Customer base: {df_cust.count():,}")

df_fallow = (
    df_cust
    .orderBy(F.col("AccountNumber"))
    .withColumn("RandomFallow", F.rand(seed=FALLOW_SEED))
    .withColumn("FallowControl", F.col("RandomFallow") <= FALLOW_PC)
    )
df_fallow.cache()
# TODO: Calibrate spend per customer of fallow and test group?

df_fc = df_fallow.select("AccountNumber")

for fixed_cell in FIXED_CELLS:
    df_fc = (
        df_fc
        .orderBy(F.col("AccountNumber"))
        .withColumn(f"Random{fixed_cell}",
                    F.rand(seed=FIXED_CELLS[fixed_cell]["seed"]))
        .withColumn(fixed_cell,
                    chain_when_thens(FIXED_CELLS[fixed_cell]["cells"]))
    )
df_fc.cache()

df_cells = (
    df_fallow
    .join(df_fc, on="AccountNumber", how="left")
    .select("AccountNumber", "FallowControl", *list(FIXED_CELLS.keys()))
)
df_cells.cache()


df_cells = (
    df_cells.withColumn("FallowControl",
                        F.when(F.col("FallowControl"),
                               F.lit(FALLOW_TRUE_LABEL)
                               ).otherwise(F.lit(FALLOW_FALSE_LABEL)))
)

df_cells = (
    df_cells
    .join(
        df_staff, on='AccountNumber', how='left'
    ).withColumn(
        'FallowControl',
        F.when(F.col('specialaccountindicator') == 'S',
               FALLOW_FALSE_LABEL).otherwise(F.col('FallowControl'))
    ).withColumn(
        'HomePageTest1',
        F.when(F.col('specialaccountindicator') == 'S',
               'Best').otherwise(F.col('HomePageTest1'))
    ).withColumn(
        'ShoppingBagTest1',
        F.when(F.col('specialaccountindicator') == 'S',
               'Best').otherwise(F.col('ShoppingBagTest1'))
    ).withColumn(
        'OrderCompleteTest1',
        F.when(F.col('specialaccountindicator') == 'S',
               'Best').otherwise(F.col('OrderCompleteTest1'))
    ).withColumn(
        'LandingPageTest1',
        F.when(F.col('specialaccountindicator') == 'S',
               'Best').otherwise(F.col('LandingPageTest1'))
    ).withColumn(
        'ChampionChallenger',
        F.when(F.col('specialaccountindicator') == 'S',
               'Challenger').otherwise(F.col('ChampionChallenger'))
    )
)

df_cells_existing = (
    spark
    .table(FIXED_CELLS_TABLE)
    .drop("rundate")
)
df_cells_existing.cache()

n_cust_existing = df_cells_existing.count()
logger.info(f"Existing customers: {n_cust_existing:,}")

df_cust_new = (
    df_cells
    .select("AccountNumber")
    .join(df_cells_existing.select("AccountNumber"),
          on="AccountNumber", how="leftanti")
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

df_cells_new = (
    df_cust_new
    .join(
        df_cells.select("AccountNumber", *overlapping_cols),
        on="AccountNumber", how="left")
)

for dcol in deprecated_cols:
    df_cells_new = df_cells_new.withColumn(dcol, F.lit(None))

if n_cust_new > 0:
    logger.info("Unioning new customers for existing columns")
    cols_for_union = ["AccountNumber", *existing_cols]
    schema_mismatch_msg = "New cell schema mismatch with existing"
    assert cols_for_union == df_cells_existing.columns, schema_mismatch_msg
    df_cells_existing_updated = (
        df_cells_existing
        .unionByName(df_cells_new.select(cols_for_union))
        )
else:
    df_cells_existing_updated = df_cells_existing

if len(new_cols) > 0:
    df_cells_new_cols = df_cells.select("AccountNumber", *new_cols)
    logger.info("Joining new columns for all customers")
    df_cells_full = (
        df_cells_existing_updated
        .join(df_cells_new_cols, on="AccountNumber", how="left")
    )
else:
    df_cells_full = df_cells_existing_updated

df_cells_full.cache()

for ncol in new_cols:
    n_null = df_cells_full.where(F.col(ncol).isNull()).count()
    logger.warning(f"{n_null:,} existing customers not assigned {ncol}")

logger.info("Backing up fixed cells table")
create_table_from_df(
    df=df_cells_existing,
    table=FIXED_CELLS_TABLE + "_backup",
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True,
    append_rundate=True
    )

logger.info("Dropping and recreating fixed cells table")
create_table_from_df(
    df=df_cells_full,
    table=FIXED_CELLS_TABLE,
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True,
    append_rundate=True
    )

if transient_cells:
    logger.info("Transient Cells requested")
    transient_cell_dfs = []
    if "AlgoDivision" in TRANSIENT_CELLS:
        logger.info("Assigning AlgoDivision")
        df_divs = get_algo_divisions(ACCOUNT_DEPARTMENT_SCORE_SQL, TRANSIENT_CELLS_TABLE_LATEST, WEBHOOK_URL_DS, JOB_ENV)
        transient_cell_dfs.append(df_divs)

    if "Audiences" in TRANSIENT_CELLS:
        logger.info("Assigning Audiences")
        df_audiences = assign_predetermined_audience(
            audiences=TRANSIENT_CELLS["Audiences"],
            tables=TABLES_READ
        )
        transient_cell_dfs.append(df_audiences)

    df_cells_transient = transient_cell_dfs.pop()
    df_cells_transient = melt_transient_cells(df_cells_transient)

    if transient_cell_dfs:
        for df_tc in transient_cell_dfs:
            df_tc_long = melt_transient_cells(df_tc)
            df_cells_transient = df_cells_transient.unionByName(df_tc_long)

    df_cells_transient.cache()
    delete_from_and_load(df_cells_transient,
                         TRANSIENT_CELLS_TABLE,
                         pk_cols=["AccountNumber", "Cell"],
                         del_where={"rundate": "current_date()"})

    truncate_and_load(df_cells_transient,
                      TRANSIENT_CELLS_TABLE_LATEST,
                      pk_cols=["AccountNumber", "Cell"])
else:
    logger.info("No Transient Cells requested - truncating latest table")
    spark.sql(f"truncate table {TRANSIENT_CELLS_TABLE_LATEST}")

df_cust.unpersist()
df_fallow.unpersist()
df_fc.unpersist()
df_cells.unpersist()
df_cells_existing.unpersist()
df_cells_full.unpersist()
df_cells_transient.unpersist()

logger.info("Run complete")
