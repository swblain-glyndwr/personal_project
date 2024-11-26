import logging
import logging.config
import json
from next_ads.Assignment import (
    assign_best_ads_with_constraints,
    assign_random_ads,
    assign_best_ads
    )
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                build_spark_schema,
                                chain_when_thens,
                                map_schema,
                                delete_from_and_load)
from pyspark.sql import functions as F
from next_ads.utils.columnscalers import subtract_mean


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname", "--location"])
req_location = pargs["location"] if pargs["location"] else "HN1"
log.info(f"Running in job environment: {job_env}")

LOCATIONS = prm["locations"]
SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
CONTROL_SHEET_LATEST = map_schema(tbls["control_sheet_latest"], SCHEMA)
TARGETING_SCORES_TABLE = map_schema(tbls["targeting_scores_latest"], SCHEMA)
ASSIGNMENTS_TABLE = map_schema(tbls["assignments"], SCHEMA)
ASSIGNMENTS_TABLE_LATEST = map_schema(tbls["assignments_latest"], SCHEMA)
FIXED_CELLS_TABLE_LATEST = map_schema(
    tbls["customer_cells_fixed_latest"], SCHEMA)
TRANSIENT_CELLS_TABLE = map_schema(tbls["customer_cells_transient"], SCHEMA)
TRANSIENT_CELLS_TABLE_LATEST = map_schema(
    tbls["customer_cells_transient_latest"], SCHEMA)

VALID_LOCATIONS = set(LOCATIONS.keys())
if req_location in VALID_LOCATIONS:
    LOCATION = req_location
else:
    raise Exception(f"Invalid Location requested: {req_location}")
log.info(f"Assigning Ads for Location: {LOCATION}")

CELL_MAP = LOCATIONS[LOCATION]


log.info("Getting Ads")
df_ads = (
    get_spark()
    .table(CONTROL_SHEET_LATEST)
    .where(F.col("Location") == LOCATION)
    .select(
        "UniqueAdID",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria")
)
# TODO: Remove underperforming Ads


# Inner join will remove customers that don't have transient cells
# (e.g. AlgoDivision)
# TODO: Will this bias the results?
log.info("Getting customer cell assignments")
df_cells = (
    get_spark()
    .table(FIXED_CELLS_TABLE_LATEST)
    .drop("rundate")
    .join(
        get_spark()
        .table(TRANSIENT_CELLS_TABLE_LATEST)
        .groupBy("AccountNumber")
        .pivot("Cell")
        .agg(F.max("CellValue")),
        on="AccountNumber",
        how="inner"
    )
)

df_cust = (
    df_cells
    .select("AccountNumber", "AlgoDivision")
    .where(F.col("AlgoDivision").isNotNull())
)


log.info("Assigning Random Ads")
df_assigned_rdm = assign_random_ads(
    df_ads.select("UniqueAdID", "AlgoDivision"),
    df_cust,
    grp_col="AlgoDivision"
    )
df_assigned_rdm.cache()


log.info("Assigning Best Ads")

best_kwargs = {
    "targeting_scores_table": TARGETING_SCORES_TABLE,
    "score_scale_fn": subtract_mean
}

if "best_kwargs" in LOCATIONS[LOCATION]:
    best_kwargs = best_kwargs | LOCATIONS[LOCATION]["best_kwargs"]

if "constraints" in LOCATIONS[LOCATION]:
    df_assigned_best = assign_best_ads_with_constraints(
        df_ads=df_ads,
        df_cust=df_cust,
        constraints=LOCATIONS[LOCATION]["constraints"],
        best_kwargs=best_kwargs
    )
else:
    df_assigned_best = assign_best_ads(
        df_ads=df_ads,
        df_cust=df_cust,
        **best_kwargs
    )

df_assigned_best.cache()


log.info("Assigning Best Ads (Challenger)")
# Assigning best to best_challenger effectively switches challenger off
df_assigned_best_challenger = df_assigned_best


# Assign Random, Best etc. based on assigned cells
# TODO: Create dedicated Challenger split in overall_control_and_div
log.info("Determining Ad to be shown based on assignments and fixed cells")
df_assignments = (
    df_cells
    .withColumn("NoAd", F.lit("Z"))
    .join(
        (
            df_assigned_rdm
            .select("AccountNumber", "UniqueAdID")
            .withColumnRenamed("UniqueAdID", "UniqueAdIDRandom")
        ),
        on="AccountNumber", how="left")
    .join(
        (
            df_assigned_best
            .select("AccountNumber", "UniqueAdID")
            .withColumnRenamed("UniqueAdID", "UniqueAdIDBest")
        ),
        on="AccountNumber", how="left")
    .join(
        (
            df_assigned_best_challenger
            .select("AccountNumber", "UniqueAdID")
            .withColumnRenamed("UniqueAdID", "UniqueAdIDBestChallenger")
        ),
        on="AccountNumber", how="left")
)
df_assignments.cache()

df_ad_assigned = (
        df_assignments
        .withColumn("UniqueAdIDAssigned",
                    chain_when_thens(CELL_MAP["map"]))
    )

df_ad_masid = (
    df_ads
    .select("UniqueAdID", "MASIDToken")
    .withColumn("Location", F.lit(LOCATION))
    .withColumn("MASID",
                F.concat(F.col("Location"),
                         F.lit("_"),
                         F.col("MASIDToken")))
    .drop("Location", "MASIDToken")
    .distinct()
)
# TODO: Move to load_control_file? Concat XX_XXXX as MASIDSlot?

ctrl_masid_cols = ["UniqueAdID", "MASID"]
ctrl_masid_vals = [("Z", f"{LOCATION}_Z")]

df_control_masid = (
    get_spark().createDataFrame(
        data=ctrl_masid_vals,
        schema=build_spark_schema(
            [["UniqueAdID", "string", "not null"],
             ["MASID", "string", "not null"]]
            )
        )
)
df_ad_masid = df_ad_masid.unionByName(df_control_masid)

df_ad_assigned_masid = (
    df_ad_assigned
    .join(df_ad_masid,
          on=df_ad_assigned.UniqueAdIDAssigned == df_ad_masid.UniqueAdID,
          how="left")
    .drop("UniqueAdID")
)
df_ad_assigned_masid.cache()

# Check and warn if null MASID assignments exist
n_null_masid = df_ad_assigned_masid.where(F.col("MASID").isNull()).count()
if n_null_masid > 0:
    log.warning(f"Removing {n_null_masid:,} accounts with null MASID")
    df_ad_assigned_masid = (
        df_ad_assigned_masid
        .where(F.col("MASID").isNotNull())
    )

df_ad_assigned_masid_output = (
    df_ad_assigned_masid
    .withColumn("Location", F.lit(LOCATION))
    .select(
        "AccountNumber",
        "Location",
        "UniqueAdIDRandom",
        "UniqueAdIDBest",
        "UniqueAdIDBestChallenger",
        "UniqueAdIDAssigned",
        "MASID")
)

log.info(f"Loading output to {ASSIGNMENTS_TABLE}")
delete_from_and_load(df_ad_assigned_masid_output,
                     ASSIGNMENTS_TABLE,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"rundate": "current_date()",
                                "Location": f"'{LOCATION}'"})

log.info(f"Loading output to {ASSIGNMENTS_TABLE_LATEST}")
delete_from_and_load(df_ad_assigned_masid_output,
                     ASSIGNMENTS_TABLE_LATEST,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"Location": f"'{LOCATION}'"})

df_cust.unpersist()
df_assigned_best.unpersist()
df_assignments.unpersist()
df_ad_assigned_masid.unpersist()

log.info("Run complete")
