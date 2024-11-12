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
FIXED_CELLS = map_schema(tbls["fixed_cells"], SCHEMA)

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


log.info("Getting fixed cell assignments")
df_cells = (
    get_spark()
    .table(FIXED_CELLS)
    .drop("rundate")
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


# Dynamically construct assignment map from config
case_whens = []
for cell_map in CELL_MAP["map"]:
    case_when_and = []
    for wh in cell_map["when"]:
        cwa = f"{wh['col']} = '{wh['match']}'"
        case_when_and.append(cwa)
        case_when_token = " and ".join(case_when_and)
    case_whens.append(f"when {case_when_token} then {cell_map['then']}")

case_when_str = "\n".join(case_whens)
case_when_str = case_when_str + "\nelse null end as UniqueAdIDShown"

df_assignments.createOrReplaceTempView("df_assignments")

df_ad_shown = (
    get_spark().sql(
        f"select a.*,\ncase {case_when_str}\nfrom df_assignments as a"
        )
)

# Ad-MASID lookup
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

# Append codes for control cells
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
df_ad_masid = df_ad_masid.union(df_control_masid)

df_ad_shown_masid = (
    df_ad_shown
    .join(df_ad_masid,
          on=df_ad_shown.UniqueAdIDShown == df_ad_masid.UniqueAdID, how="left")
    .drop("UniqueAdID")
)
df_ad_shown_masid.cache()

# Check and warn if null MASID assignments exist
n_null_masid = df_ad_shown_masid.where(F.col("MASID").isNull()).count()
if n_null_masid > 0:
    log.warning(f"{n_null_masid:,} accounts with null MASID - removing")
    df_ad_shown_masid = df_ad_shown_masid.where(F.col("MASID").isNotNull())

df_ad_shown_masid_output = (
    df_ad_shown_masid
    .withColumn("Location", F.lit(LOCATION))
    .withColumn("MacroLocation", F.lit(CELL_MAP["macro"]))
    .withColumn("MacroLocationCell", F.col(CELL_MAP["macro"]))
    .select(
        "AccountNumber",
        "Location",
        "MacroLocation",
        "MacroLocationCell",
        "AdHocAB1",
        "AdHocAB2",
        "AdHocAB3",
        "AdHocAB4",
        "ChampionChallenger",
        "AlgoDivision",
        "UniqueAdIDShown",
        "MASID")
)

log.info(f"Loading output to {ASSIGNMENTS_TABLE}")
delete_from_and_load(df_ad_shown_masid_output,
                     ASSIGNMENTS_TABLE,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"rundate": "current_date()",
                                "Location": f"'{LOCATION}'"})

log.info(f"Loading output to {ASSIGNMENTS_TABLE_LATEST}")
delete_from_and_load(df_ad_shown_masid_output,
                     ASSIGNMENTS_TABLE_LATEST,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"Location": f"'{LOCATION}'"})

df_cust.unpersist()
df_assigned_best.unpersist()
df_assignments.unpersist()
df_ad_shown_masid.unpersist()

log.info("Run complete")
