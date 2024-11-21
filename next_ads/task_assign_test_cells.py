import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Window
from next_ads.utils.etl import (assert_pk,
                                JobParser,
                                create_table_from_df,
                                map_schema,
                                chain_when_thens)


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
CUST_CELLS_TABLE = map_schema(rsc["tables"]["write"]["customer_cells"],
                              SCHEMA)

TABLES_READ = rsc["tables"]["read"]
# SVOC table used for customer base because it contains older accounts
SVOC = TABLES_READ["svoc_pii"]
RPID_WITH_ACCOUNTS = TABLES_READ["rpid_with_accounts"]
MODEL_SCORES_LATEST = TABLES_READ["model_scores_latest"]
LEGACY_EXCL = rsc["legacy"]["account_exclusions"]

FALLOW_PC = prm["fallow_control"]["proportion"]
FALLOW_SEED = prm["fallow_control"]["seed"]
TEST_CELLS = prm["test_cells"]
ALGO_DIVISIONS = prm["algo_divisions"]

attach_audiences = False
if "audiences" in prm:
    attach_audiences = True
    AUDIENCES = prm["audiences"]

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

for test_cell in TEST_CELLS:
    df_test_ads = (
        df_test_ads
        .orderBy(F.col("AccountNumber"))
        .withColumn(f"Random{test_cell}",
                    F.rand(seed=TEST_CELLS[test_cell]["seed"]))
        .withColumn(test_cell,
                    chain_when_thens(TEST_CELLS[test_cell]["cells"]))
    )
df_test_ads.cache()

df_cells = (
    df_fallow
    .join(df_test_ads, on="AccountNumber", how="left")
    .select("AccountNumber", "FallowControl", *list(TEST_CELLS.keys()))
)
df_cells.cache()

print(f"Customers: {df_cust.count():,}")
print(f"Customers Fallow table: {df_fallow.count():,}")
print(f"""Customers Fallow True: {
    df_fallow.where(~F.col('FallowControl')).count():,}""")
print(f"Customers Ads: {df_test_ads.count():,}")
n_diff = (df_fallow.where(~F.col('FallowControl')).count()
          - df_test_ads.count())
print(f"Not in Fallow test_cells diff: {n_diff:,}")

for test_cell in TEST_CELLS:
    df_cells = (
        df_cells
        .withColumn(test_cell,
                    F.when(F.col("FallowControl"),
                           F.lit("4: Overall")).otherwise(F.col(test_cell)))
    )

df_cells = (
    df_cells.withColumn("FallowControl",
                        F.when(F.col("FallowControl"),
                               F.lit("NoAds")).otherwise(F.lit("Ads")))
)

# TODO: Ad algo division assignment
# BOOKMARK

# TODO: Audience assignment
if attach_audiences:
    df_audience_list = []
    for (i, a) in enumerate(AUDIENCES):
        a_name = AUDIENCES[i][0]
        a_cols = AUDIENCES[i][1]
        df_a = (
            get_spark()
            .table(TABLES_READ[a_name])
            .withColumnsRenamed(
                {
                    a_cols["account_col"]: "AccountNumber",
                    a_cols["label_col"]: "Audience"
                }
            )
            .withColumn("AudiencePriority", F.lit(i))
            )
        df_audience_list.append(df_a)

    df_audiences = df_audience_list.pop()
    if len(df_audience_list) >= 1:
        for df_a_i in df_audience_list:
            df_audiences = df_audiences.union(df_a_i)

    accW = Window.partitionBy("AccountNumber")
    df_audiences = (
        df_audiences
        .withColumn("MaxPriority",
                    F.min(F.col("AudiencePriority")).over(accW))
        .where(F.col("AudiencePriority") == F.col("MaxPriority"))
    )

    assert_pk(df_audiences, ["AccountNumber"])

    df_cells = df_cells.join(
        df_audiences.select("AccountNumber", "Audience"),
        on="AccountNumber", how="left")
    n_with_audience = df_cells.where(F.col("Audience").isNotNull()).count()
    log.info(f"{n_with_audience:,} customers assigned a predefined audience")
else:
    df_cells = df_cells.withColumn("Audience", F.lit(None))

assert_pk(df_cells, ["AccountNumber"])


# Get existing table
df_cells_existing = get_spark().table(CUST_CELLS_TABLE)
log.info(f"Customers with existing cells: {df_cells_existing.count():,}")

# Backup existing table
# TODO: Partition on AlgoDivision instead?
df_cells_existing = create_table_from_df(
    df=df_cells,
    table=CUST_CELLS_TABLE + "_backup",
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"]
    )

# Get new customers
df_cust_new = (
    df_cells_existing
    .select("AccountNumber")
    .join(df_cells.select("AccountNumber"),
          on="AccountNumber", how="leftanti")
    )
log.info(f"New customers to assign existing cells: {df_cust_new.count():,}")

# Filter new cell assignments to new customers only
df_cells_new = df_cust_new.join(df_cells,
                                on="AccountNumber", how="left")

# Where columns in existing table, union new customers
cols_exist = df_cells_existing.columns
df_cells_updated = (
    df_cells_existing
    .union(df_cells_new.select(*cols_exist))
    )


# Where columns not in existing table, join additional columns
col_not_exist = [c for c in df_cells.columns if c not in cols_exist]
df_cells_new_cols = df_cells.select("AccountNumber", *col_not_exist)

df_cells_full = (
    df_cells_updated
    .join(df_cells_new_cols, on="AccountNumber", how="left")
)

for col in col_not_exist:
    n_null = df_cells_updated.where(F.col(col).isNull()).count()
    log.warning(f"{n_null:,} existing customers not present in new col {col}")


# TODO: Partition on AlgoDivision instead?
df_cells_full = create_table_from_df(
    df=df_cells,
    table=CUST_CELLS_TABLE,
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True
    )


# Figure out what's going on with the Exponea load

log.info("Run complete")
