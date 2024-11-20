import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from pyspark.sql import functions as F
from next_ads.utils.etl import (assert_pk,
                                JobParser,
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
TEST_CELLS_TABLE = map_schema(rsc["tables"]["write"]["test_cells"], SCHEMA)

# SVOC table used for customer base because it contains older accounts
SVOC = rsc["tables"]["read"]["svoc_pii"]
RPID_WITH_ACCOUNTS = rsc["tables"]["read"]["rpid_with_accounts"]
MODEL_SCORES_LATEST = rsc["tables"]["read"]["model_scores_latest"]

LEGACY_EXCL = rsc["legacy"]["account_exclusions"]

FALLOW_PC = prm["fallow_control"]["proportion"]
FALLOW_SEED = prm["fallow_control"]["seed"]
TEST_CELLS = prm["test_cells"]


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

df_test_cells = (
    df_fallow
    .join(df_test_ads, on="AccountNumber", how="left")
    .select("AccountNumber", "FallowControl", *list(TEST_CELLS.keys()))
)
df_test_cells.cache()

print(f"Customers: {df_cust.count():,}")
print(f"Customers Fallow table: {df_fallow.count():,}")
print(f"""Customers Fallow True: {
    df_fallow.where(~F.col('FallowControl')).count():,}""")
print(f"Customers Ads: {df_test_ads.count():,}")
print(f"""
      Not in Fallow test_cells diff:
      {df_fallow.where(
          ~F.col('FallowControl')).count() - df_test_ads.count()}""")


for test_cell in TEST_CELLS:
    df_test_cells = (
        df_test_cells
        .withColumn(test_cell,
                    F.when(F.col("FallowControl"),
                           F.lit("4: Overall")).otherwise(F.col(test_cell)))
    )

df_test_cells = (
    df_test_cells.withColumn("FallowControl",
                             F.when(F.col("FallowControl"),
                                    F.lit("NoAds")).otherwise(F.lit("Ads")))
)

assert_pk(df_test_cells, ["AccountNumber"])


# Backup existing table

# Get existing table

# Where columns in existing table, union new customers

# Where columns not in existing table, join additional columns

# log.info("Run complete")
