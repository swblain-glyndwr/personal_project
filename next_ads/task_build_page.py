import logging
import logging.config
import json
from next_ads.Assignment import (
    assign_random_ads,
    assign_best_ads
    )
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
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

SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
CONTROL_SHEET_LATEST = map_schema(tbls["control_sheet_latest"], SCHEMA)
TARGETING_SCORES_TABLE = map_schema(tbls["targeting_scores_latest"], SCHEMA)
ASSIGNMENTS_TABLE = map_schema(tbls["assignments"], SCHEMA)
ASSIGNMENTS_TABLE_LATEST = map_schema(tbls["assignments_latest"], SCHEMA)
FIXED_CELLS = map_schema(tbls["fixed_cells"], SCHEMA)

VALID_LOCATIONS = set(prm["locations"].keys())
if req_location in VALID_LOCATIONS:
    LOCATION = req_location
else:
    raise Exception(f"Invalid Location requested: {req_location}")
log.info(f"Assigning Ads for Location: {LOCATION}")


log.info("Getting Ads")
df_ads = (
    get_spark()
    .table(CONTROL_SHEET_LATEST)
    .select(
        "UniqueAdID",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria")
)
# TODO: Remove underperforming Ads

# Ad ID - MASID lookup
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

log.info("Getting fixed cell assignments")
df_cells = (
    get_spark()
    .table(FIXED_CELLS)
)
df_cust_div = (
    df_cells
    .select("AccountNumber", "AlgoDivision")
    .where(F.col("AlgoDivision").isNotNull())
)


log.info("Assigning Random Ads by AlgoDivision")
df_assigned_rdm = assign_random_ads(
    df_ads.select("UniqueAdID", "AlgoDivision"),
    df_cust_div,
    grp_col="AlgoDivision"
    )

# BOOKMARK
log.info("Assigning Best Ads")
# Iterate by Division - customer should receive best ad within Division
divs = [row[0] for row in df_ads.select("AlgoDivision").distinct().collect()]
df_ads_best_div_list = []

for div in divs:

    df_ads_d = (
        df_ads
        .where(F.col("AlgoDivision") == div)
        .where(F.col("TargetingCriteria").isNotNull())
        .select("UniqueAdID", "TargetingCriteria")
    )

    df_cust_d = (
        df_cust_div
        .where(F.col("AlgoDivision") == div)
        .select("AccountNumber")
    )
    # TODO: Some division customers may not have relevant scores - capture?

    df_ads_best_d = (
        assign_best_ads(
            df_ads=df_ads_d,
            df_cust=df_cust_d,
            targeting_scores_table=TARGETING_SCORES_TABLE,
            score_scale_fn=subtract_mean
            )
        .join(df_cust_div.where(F.col("AlgoDivision") == div),
              on="AccountNumber", how="inner")
        .drop("Division")
    )
    df_ads_best_div_list.append(df_ads_best_d)

df_assigned_best = df_ads_best_div_list.pop()
for df_ads_best_div in df_ads_best_div_list:
    df_assigned_best = df_assigned_best.union(df_ads_best_div)

df_assigned_best.cache()


log.info("Assigning Best Ads (Challenger)")
df_assigned_best_challenger = df_assigned_best


log.info("Getting Cell assignments")
# TODO: Make this generalisable - HPTest hardcoded as column
# TODO: Dedicated Champion-Challenger column, instead of random_var1
MACRO_LOCATION = LOCATION[:2]
if LOCATION in ["HN1"]:
    test_col = "HPTest"
else:
    test_col = f"{MACRO_LOCATION}Test"


# Assign Random, Best etc. based on assigned cells
# TODO: Create dedicated Challenger split in overall_control_and_div
log.info("Assigning MASID tokens based Targeting and Cells")
df_assignments = (
    df_cells
    .join((df_assigned_rdm
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "RandomMASID")
           .withColumnRenamed("UniqueAdID", "RandomUniqueAdID")),
          on="AccountNumber",
          how="left")
    .join((df_assigned_best
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASID")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdID")),
          on="AccountNumber",
          how="left")
    .join((df_assigned_best_challenger
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASIDChallenger")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdIDChallenger")),
          on="AccountNumber",
          how="left")
    .fillna({"RandomMASID": f"{LOCATION}_N"})
    .withColumn("ChampionChallenger",
                F.when((F.col("random_var1") <= 0.5)
                       & (F.col(test_col) == "1: Personalised"),
                       "Champion")
                .when((F.col("random_var1") > 0.5)
                      & (F.col(test_col) == "1: Personalised"),
                      "Challenger")
                .otherwise(None)
                )
    .withColumn(
        "UniqueAdID",
        F.when(
            (F.col("ChampionChallenger") == "Champion")
            & (F.col("BestUniqueAdID").isNotNull()),
            F.col("BestUniqueAdID")
            )
        .when(
            (F.col("ChampionChallenger") == "Challenger")
            & (F.col("BestUniqueAdIDChallenger").isNotNull()),
            F.col("BestUniqueAdIDChallenger")
            )
        .when(F.col(test_col) == "2: Random", F.col("RandomUniqueAdID"))
        .when(F.col(test_col) == "3: No Banner", F.lit("_location_control"))
        .when(F.col(test_col) == "4: Overall", F.lit("_overall_control"))
        .otherwise(F.lit(None))
        )
    .withColumn(
        "MASID",
        F.when(
            (F.col("ChampionChallenger") == "Champion")
            & (F.col("BestMASID").isNotNull()),
            F.col("BestMASID")
            )
        .when(
            (F.col("ChampionChallenger") == "Challenger")
            & (F.col("BestMASIDChallenger").isNotNull()),
            F.col("BestMASIDChallenger")
            )
        .when(F.col(test_col) == "2: Random", F.col("RandomMASID"))
        .when(F.col(test_col) == "3: No Banner", F.lit(f"{LOCATION}_C"))
        .when(F.col(test_col) == "4: Overall", F.lit(f"{LOCATION}_Z"))
        .otherwise(F.lit(f"{LOCATION}_Z"))
        )
    .withColumn("Location", F.lit(LOCATION))
    .withColumn("MacroLocation", F.lit(MACRO_LOCATION))
    .withColumnRenamed(test_col, "MacroLocationCell")
    .select("AccountNumber",
            "Location",
            "MacroLocation",
            "MacroLocationCell",
            "ChampionChallenger",
            "RandomUniqueAdID",
            "RandomMASID",
            "BestUniqueAdID",
            "BestMASID",
            "BestUniqueAdIDChallenger",
            "BestMASIDChallenger",
            "UniqueAdID",
            "MASID"
            )
)
df_assignments.cache()


log.info(f"Loading output to {ASSIGNMENTS_TABLE}")
delete_from_and_load(df_assignments,
                     ASSIGNMENTS_TABLE,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"rundate": "current_date()",
                                "Location": f"'{LOCATION}'"})

log.info(f"Loading output to {ASSIGNMENTS_TABLE_LATEST}")
delete_from_and_load(df_assignments,
                     ASSIGNMENTS_TABLE_LATEST,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"Location": f"'{LOCATION}'"})

df_cust_div.unpersist()
df_assigned_best.unpersist()
df_assignments.unpersist()

log.info("Run complete")
