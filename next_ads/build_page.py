import logging
import logging.config
import json
from AdRetrieval import get_latest_ads
from next_ads.Assignment import (
    assign_random_ads,
    assign_best_ads
    )
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import delete_from_and_load
from pyspark.sql import functions as F
import sys
from next_ads.utils.columnscalers import subtract_mean


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")


log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)


DIVISION_ASSIGNMENTS = rsc["files"]["div_assignment"]
TARGETING_SCORES_TABLE = rsc["tables"]["targeting_scores"]
CELL_ASSIGNMENT_FILE = rsc["files"]["cell_assignment"]
ASSIGNMENTS_TABLE = rsc["tables"]["assignments"]
ASSIGNMENTS_TABLE_LATEST = rsc["tables"]["assignments_latest"]
VALID_LOCATIONS = set(prm["locations"].keys())
ASSIGNMENTS_TABLE = rsc["tables"]["assignments"]
ASSIGNMENTS_TABLE_LATEST = rsc["tables"]["assignments_latest"]


requested_locations = list(VALID_LOCATIONS.intersection(set(sys.argv)))

if len(requested_locations) > 1:
    raise Exception(f"More than one Location requested: {requested_locations}")
elif len(requested_locations) == 1:
    LOCATION = requested_locations[0]
else:
    LOCATION = "HN1"  # For interactive debugging

log.info(f"Assigning Ads for Location: {LOCATION}")
if LOCATION == "HN1":
    filter_underperf = True
else:
    filter_underperf = False
# TODO: Remove once config can point to results for other locations


# Get Ad data
log.info("Getting Ads")
df_ads = (
    get_latest_ads(LOCATION, filter_underperforming=filter_underperf)
    .select("UniqueAdID",
            "AlgoDivision",
            "MASIDToken",
            "Models",
            "ModelCombination")
    .withColumnRenamed("AlgoDivision", "Division")
)
# TODO: Remove renaming of AlgoDiv once fully migrated to new control sheet

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

# TODO: Replace separate files with single table
log.info("Gathering Division assignments")
div_asgn_list = []
for div_k in DIVISION_ASSIGNMENTS.keys():

    df_div = (
        get_spark()
        .read.format("delta")
        .load(DIVISION_ASSIGNMENTS[div_k])
        .select("account_number")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumn("Division", F.lit(div_k))
    )

    div_asgn_list.append(df_div)

df_cust_div = div_asgn_list.pop()
for df_asgn in div_asgn_list:
    df_cust_div = df_cust_div.union(df_asgn)

df_cust_div.cache()


log.info("Assigning Random Ads by Division")
df_assigned_rdm = assign_random_ads(
    df_ads.select("UniqueAdID", "Division"),
    df_cust_div,
    grp_col="Division"
    )
df_assigned_rdm = df_assigned_rdm.join(df_ad_masid, on="UniqueAdID")


log.info("Assigning Best Ads")
# Iterate by Division - customer should receive best ad within Division
divs = [row[0] for row in df_ads.select("Division").distinct().collect()]
df_ads_best_div_list = []

for div in divs:

    df_ads_d = (
        df_ads
        .where(F.col("Division") == div)
        .select("UniqueAdID", "Models", "ModelCombination")
        .where(F.col("Models").isNotNull())
        .fillna({"ModelCombination": "and"})
    )
    # TODO: Remove force "and" for combination

    df_cust_d = (
        df_cust_div
        .where(F.col("Division") == div)
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
        .join(df_cust_div.where(F.col("Division") == div),
              on="AccountNumber", how="inner")
        .drop("Division")
    )
    df_ads_best_div_list.append(df_ads_best_d)

df_assigned_best = df_ads_best_div_list.pop()
for df_ads_best_div in df_ads_best_div_list:
    df_assigned_best = df_assigned_best.union(df_ads_best_div)

df_assigned_best = df_assigned_best.join(df_ad_masid, on="UniqueAdID")

df_assigned_best.cache()


log.info("Assigning Best Ads (Challenger)")
df_assigned_best_challenger = df_assigned_best


log.info("Getting Cell assignments")
# TODO: Make this generalisable - HPTest hardcoded as column
# TODO: Dedicated Champion-Challenger column, instead of random_var1
if LOCATION in ["HN1"]:
    test_col = "HPTest"
else:
    test_col = f"{LOCATION[:2]}Test"

df_cell = (
        get_spark()
        .read.format("delta")
        .load(CELL_ASSIGNMENT_FILE)
        .select("account_number",
                test_col,
                "random_var1")
        .withColumnRenamed("account_number", "AccountNumber")
    )

# Assign Random, Best etc. based on assigned cells
# TODO: Create dedicated Challenger split in overall_control_and_div
log.info("Assigning MASID tokens based Targeting and Cells")
df_assignments = (
    df_cell
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
    .withColumn(
        "MASID",
        F.when(
            (F.col(test_col) == "1: Personalised")
            & (F.col("random_var1") <= 0.5)
            & (F.col("BestMASID").isNotNull()),
            F.col("BestMASID")
            )
        .when(
            (F.col(test_col) == "1: Personalised")
            & (F.col("random_var1") > 0.5)
            & (F.col("BestMASID").isNotNull()),
            F.col("BestMASIDChallenger")
            )
        .when(F.col(test_col) == "2: Random", F.col("RandomMASID"))
        .when(F.col(test_col) == "3: No Banner", F.lit(f"{LOCATION}_C"))
        .when(F.col(test_col) == "4: Overall", F.lit(f"{LOCATION}_Z"))
        .otherwise(F.lit(f"{LOCATION}_Z"))
        )
    .withColumn("Location", F.lit(LOCATION))
    .select("AccountNumber",
            "Location",
            "RandomUniqueAdID",
            "RandomMASID",
            "BestUniqueAdID",
            "BestMASID",
            "BestUniqueAdIDChallenger",
            "BestMASIDChallenger",
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
