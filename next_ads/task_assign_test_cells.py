import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from pyspark.sql import functions as F
from next_ads.utils.etl import (assert_pk,
                                JobParser,
                                build_spark_schema,
                                map_schema)


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

df_test_cell_existing = get_spark().table(TEST_CELLS_TABLE)

df_fallow = (
    df_cust
    .orderBy(F.col("AccountNumber"))
    .withColumn("RandomFallow", F.rand(seed=FALLOW_SEED))
    .withColumn("FallowControl", F.col("RandomFallow") <= FALLOW_PC)
    )
# TODO: Calibrate spend per customer of fallow and test group?

test_cells = list(TEST_CELLS.keys())
test_cell_seeds = {k: TEST_CELLS[k]["seed"] for k in test_cells}

sch = build_spark_schema([["TestCell", "string", "not null"]])
df_test_cell = get_spark().createDataFrame([(c,) for c in test_cells], sch)

df_test_rdm = (
    df_fallow
    .where(~F.col("FallowControl"))
    .drop("RandomFallow", "FallowControl")
)

for test_cell in test_cells:
    df_test_rdm = (
        df_test_rdm
        .withColumn(f"{test_cell}Random",
                    F.rand(seed=test_cell_seeds[test_cell]))
    )

# Build case-when to map cell references
test_cell_assignments = []
for test_cell in test_cells:
    cells = TEST_CELLS[test_cell]["cells"]
    when_strs = []
    for cell in cells:
        when_str = f"when {test_cell}Random <= {cell[0]} then '{cell[1]}'"
        when_strs.append(when_str)
    test_cell_assignment = (
        "case "
        + " ".join(when_strs)
        + f" else null end as {test_cell}"
    )
    test_cell_assignments.append(test_cell_assignment)

test_cell_case_when = ",\n".join(test_cell_assignments)

df_test_rdm.createOrReplaceTempView("df_test_rdm_tmp")
df_test_cells_ads = (
    get_spark()
    .sql(f"""
         select a.*,
         {test_cell_case_when}
         from df_test_rdm_tmp a
         """)
)

df_test_cells = (
    df_fallow
    .select("AccountNumber", "FallowControl")
    .join(df_test_cells_ads.select("AccountNumber", *test_cells),
          on="AccountNumber", how="left")
)

for test_cell in test_cells:
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
df_test_cells.cache()
assert_pk(df_test_cells, ["AccountNumber"])

log.info("Run complete")
