import logging
import logging.config
import json
from next_ads.Assignment import (assign_predetermined_audience,
                                 get_algo_divisions_legacy)
from next_ads.utils.dbc import get_spark
from pyspark.sql import functions as F
from next_ads.utils.etl import (assert_pk,
                                JobParser,
                                create_table_from_df, delete_from_and_load,
                                map_schema,
                                chain_when_thens, truncate_and_load)


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

SCHEMA = rsc["schema"][job_env]
tbls = rsc["tables"]["write"]
FIXED_CELLS_TABLE = map_schema(tbls["customer_cells_fixed_latest"], SCHEMA)
TRANSIENT_CELLS_TABLE = map_schema(tbls["customer_cells_transient"], SCHEMA)
TRANSIENT_CELLS_LATEST_TABLE = map_schema(
    tbls["customer_cells_transient_latest"], SCHEMA)

TABLES_READ = rsc["tables"]["read"]
# SVOC table used for customer base because it contains older accounts
SVOC = TABLES_READ["svoc_pii"]
RPID_WITH_ACCOUNTS = TABLES_READ["rpid_with_accounts"]
MODEL_SCORES_LATEST = TABLES_READ["model_scores_latest"]
LEGACY_EXCL = rsc["legacy"]["account_exclusions"]

FALLOW_PC = prm["fallow_control"]["proportion"]
FALLOW_SEED = prm["fallow_control"]["seed"]
FIXED_CELLS = prm["fixed_cells"]

transient_cells = False
if "transient_cells" in prm:
    transient_cells = True
    TRANSIENT_CELLS = prm["transient_cells"]

# Query inherited from legacy script
# TODO: Should we take lastest updated record to de-dup instead?
df_rpid_w_acc = (
        get_spark()
        .table(RPID_WITH_ACCOUNTS)
        .select("account_number", "roamingprofileid")
        .where(~F.col("account_number").isin(LEGACY_EXCL))
        .distinct()
)

# SVOC table used because it contains older accounts too
# Where clause inherited from legacy script
df_cust = (
    get_spark()
    .table(SVOC)
    .where(
        (F.col("countrycode").isin("GB"))
        & (F.col("client") == "NEXT")
        & (F.col("AccountIsCurrent") == "Y")
        & (F.col("LatestAccountKeyIndicator") == 1)
        )
    .join(df_rpid_w_acc, on="account_number")
    .select("account_number")
    .withColumnRenamed("account_number", "AccountNumber")
)

assert_pk(df_cust, ["AccountNumber"])
df_cust.cache()
log.info(f"Customer base size: {df_cust.count():,}")

df_fallow = (
    df_cust
    .orderBy(F.col("AccountNumber"))
    .withColumn("RandomFallow", F.rand(seed=FALLOW_SEED))
    .withColumn("FallowControl", F.col("RandomFallow") <= FALLOW_PC)
    )
df_fallow.cache()
# TODO: Calibrate spend per customer of fallow and test group?

df_test_ads = (
    df_fallow
    .where(~F.col("FallowControl"))
    .select("AccountNumber")
)

for fixed_cell in FIXED_CELLS:
    df_test_ads = (
        df_test_ads
        .orderBy(F.col("AccountNumber"))
        .withColumn(f"Random{fixed_cell}",
                    F.rand(seed=FIXED_CELLS[fixed_cell]["seed"]))
        .withColumn(fixed_cell,
                    chain_when_thens(FIXED_CELLS[fixed_cell]["cells"]))
    )
df_test_ads.cache()

df_cells = (
    df_fallow
    .join(df_test_ads, on="AccountNumber", how="left")
    .select("AccountNumber", "FallowControl", *list(FIXED_CELLS.keys()))
)
df_cells.cache()

log.info(f"Base customers: {df_cust.count():,}")
log.info(f"Customers not in fallow cell: {df_test_ads.count():,}")

for fixed_cell in FIXED_CELLS:
    df_cells = (
        df_cells
        .withColumn(fixed_cell,
                    F.when(F.col("FallowControl"),
                           F.lit("4: Overall")).otherwise(F.col(fixed_cell)))
    )

df_cells = (
    df_cells.withColumn("FallowControl",
                        F.when(F.col("FallowControl"),
                               F.lit("NoAds")).otherwise(F.lit("Ads")))
)

df_cells_existing = (
    get_spark()
    .table(FIXED_CELLS_TABLE)
    .drop("rundate")
)

n_cust_existing = df_cells_existing.count()
log.info(f"Existing customers: {n_cust_existing:,}")

df_cust_new = (
    df_cells
    .select("AccountNumber")
    .join(df_cells_existing.select("AccountNumber"),
          on="AccountNumber", how="leftanti")
    )

n_cust_new = df_cust_new.count()
log.info(f"New customers: {n_cust_new:,}")

existing_cols = [c for c in df_cells_existing.columns if c != "AccountNumber"]
proposed_cols = [c for c in df_cells.columns if c != "AccountNumber"]
overlapping_cols = [c for c in proposed_cols if c in existing_cols]
new_cols = [c for c in proposed_cols if c not in existing_cols]
deprecated_cols = [c for c in existing_cols if c not in proposed_cols]

log.info(f"Existing columns:    {existing_cols}")
log.info(f"Proposed columns:    {proposed_cols}")
log.info(f"Overlapping columns: {overlapping_cols}")
log.info(f"New columns:         {new_cols}")
log.info(f"Deprecated columns:  {deprecated_cols}")

df_cells_new = df_cust_new.join(
    df_cells.select("AccountNumber", *overlapping_cols),
    on="AccountNumber", how="left")

for dcol in deprecated_cols:
    df_cells_new = df_cells_new.withColumn(dcol, F.lit(None))

if n_cust_new > 0:
    log.info("Unioning new customers for existing columns")
    cols_for_union = ["AccountNumber", *existing_cols]
    schema_mismatch_msg = "New cell schema mismatch with existing"
    assert cols_for_union == df_cells_existing.columns, schema_mismatch_msg
    df_cells_existing_updated = (
        df_cells_existing
        .unionByName(df_cells_new.select("AccountNumber", *existing_cols))
        )
else:
    df_cells_existing_updated = df_cells_existing

if len(new_cols) > 0:
    df_cells_new_cols = df_cells.select("AccountNumber", *new_cols)
    log.info("Joining new columns for all customers")
    df_cells_full = (
        df_cells_existing_updated
        .join(df_cells_new_cols, on="AccountNumber", how="left")
    )
else:
    df_cells_full = df_cells_existing_updated

for ncol in new_cols:
    n_null = df_cells_full.where(F.col(ncol).isNull()).count()
    log.warning(f"{n_null:,} existing customers not assigned {ncol}")

# Back up existing table before overwriting cells table
create_table_from_df(
    df=df_cells_existing,
    table=FIXED_CELLS_TABLE + "_backup",
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True
    )

create_table_from_df(
    df=df_cells,
    table=FIXED_CELLS_TABLE,
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True
    )

# TODO: Figure out what's going on with the Exponea load

# Transient cells - append to history - store in long format


def melt_transient_cells(df):
    df_melted = df.unpivot(
        ids="AccountNumber",
        values=None,
        variableColumnName="Cell",
        valueColumnName="CellValue")
    return df_melted


if transient_cells:
    transient_cell_dfs = []
    if "AlgoDivision" in TRANSIENT_CELLS:
        # TODO: Review methodology of AlgoDivision assignment
        # Due to time constraints, old methodology was ported across without
        # full review
        log.info("[Legacy] Assigning AlgoDivision via legacy method")
        df_divs = get_algo_divisions_legacy()
        transient_cell_dfs.append(df_divs)

    if "Audiences" in TRANSIENT_CELLS:
        log.info("Assigning Audiences")
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

delete_from_and_load(df_cells_transient,
                     TRANSIENT_CELLS_TABLE,
                     pk_cols=["AccountNumber", "Cell"],
                     del_where={"rundate": "current_date()"})

truncate_and_load(df_cells_transient,
                  TRANSIENT_CELLS_LATEST_TABLE,
                  pk_cols=["AccountNumber", "Cell"])

log.info("Run complete")
