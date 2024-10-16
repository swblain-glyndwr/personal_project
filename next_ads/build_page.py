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


# Configure logging
logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

# Configure run
log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)


# Constants
DIVISION_ASSIGNMENTS_DICT = rsc["files"]["div_assignment"]
TARGETING_SCORES_TABLE = rsc["tables"]["targeting_scores"]
CELL_ASSIGNMENT_FILE = rsc["files"]["cell_assignment"]
ASSIGNMENTS_TABLE = rsc["tables"]["assignments"]
ASSIGNMENTS_TABLE_LATEST = rsc["tables"]["assignments_latest"]
VALID_LOCATIONS = prm["locations"].keys()


# Set Location for run
# If valid location not specified via sys.argv (run as job),
# will take hardcoded Location (useful for interactive debugging)
loc_args = list(set(VALID_LOCATIONS).intersection(set(sys.argv)))

if len(loc_args) > 1:
    raise Exception(f"More than one Location specified: {loc_args}")
elif len(loc_args) == 1:
    LOCATION = loc_args[0]
else:
    LOCATION = "HN1"  # For interactive debugging

log.info(f"Assigning Ads for Location: {LOCATION}")


# Temp fix while underperforming only works for HN1
if LOCATION == "HN1":
    filter_underperf = True
else:
    filter_underperf = False
# TODO: Sort out results files and underperforming for other locations


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


# Get division assignments as one dataframe
# TODO: Replace separate files with single table
log.info("Gathering Division assignments")
div_asgn_list = []
for div_k in DIVISION_ASSIGNMENTS_DICT.keys():

    df_div = (
        get_spark()
        .read.format("delta")
        .load(DIVISION_ASSIGNMENTS_DICT[div_k])
        .select("account_number")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumn("Division", F.lit(div_k))
    )

    div_asgn_list.append(df_div)

df_cust_div = div_asgn_list.pop()
for df_asgn in div_asgn_list:
    df_cust_div = df_cust_div.union(df_asgn)

df_cust_div.cache()


# Determine Random (within Division) Ad for each customer
log.info("Assigning Random Ads by Division")
df_ads_rdm = assign_random_ads(
    df_ads.select("UniqueAdID", "Division"),
    df_cust_div,
    grp_col="Division"
    )
# Append MASID to each Ad
df_ads_rdm = df_ads_rdm.join(df_ad_masid, on="UniqueAdID")


log.info("Assigning Best Ads")
# Determine Best Ad for each customer

# Iterate by Division - customer should receive best ad within Division
divs = [row[0] for row in df_ads.select("Division").distinct().collect()]

df_ads_best_div_list = []
for div in divs:
    # Get i division's ads
    df_ads_i = (
        df_ads
        .select("UniqueAdID", "Models", "ModelCombination")
        .where(F.col("Models").isNotNull())
        .fillna({"ModelCombination": "and"})
        .where(F.col("Division") == div)
    )
    # TODO: Remove force "and" for combination

    # Get i division's customers
    df_cust_i = (
        df_cust_div
        .where(F.col("Division") == div)
        .select("AccountNumber")
    )

    # Assign best Ad within Division
    df_ads_best_i = (
        assign_best_ads(
            df_ads=df_ads_i,
            df_cust=df_cust_i,
            targeting_scores_table=TARGETING_SCORES_TABLE,
            scale_fn=subtract_mean,
            scale_partition=["TargetingCriteria"]
            )
        .join(df_cust_div.where("Division" == div), on="AccountNumber")
        .drop("Division")
    )
    df_ads_best_div_list.append(df_ads_best_i)

# Combine division runs into single df
df_ads_best = df_ads_best_div_list.pop()
for df_ads_best_div in df_ads_best_div_list:
    df_ads_best = df_ads_best.union(df_ads_best_div)

# Append MASID to each Ad
df_ads_best = df_ads_best.join(df_ad_masid, on="UniqueAdID")

df_ads_best.cache()


# Assign Best Ad for each customer (via "challenger" method)
# When no challenger, challenger assignment == champion_assignment
df_ads_best_chall = df_ads_best


# Append to overall cell assignments
# TODO: Make this generalisable - HPTest hardcoded as column
# TODO: Dedicated Champion-Challenger column, instead of random_var1
log.info("Getting Cell assignments")

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
# TODO: Create Champion-Challenger split in new overall control and div
log.info("Assigning MASID tokens based Targeting and Cells")
df_assigned_ads = (
    df_cell
    .join((df_ads_rdm
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "RandomMASID")
           .withColumnRenamed("UniqueAdID", "RandomUniqueAdID")),
          on="AccountNumber",
          how="inner")
    .join((df_ads_best
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASID")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdID")),
          on="AccountNumber",
          how="left")
    .join((df_ads_best_chall
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASIDChall")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdIDChall")),
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
            F.col("BestMASIDChall")
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
            "BestUniqueAdIDChall",
            "BestMASIDChall",
            "MASID"
            )
)
df_assigned_ads.cache()

# Load output into assignments table
ASSIGNMENTS_TABLE = rsc["tables"]["assignments"]
ASSIGNMENTS_TABLE_LATEST = rsc["tables"]["assignments_latest"]

log.info(f"Loading output to {ASSIGNMENTS_TABLE}")
delete_from_and_load(df_assigned_ads,
                     ASSIGNMENTS_TABLE,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"rundate": "current_date()",
                                "Location": f"'{LOCATION}'"})

log.info(f"Loading output to {ASSIGNMENTS_TABLE_LATEST}")
delete_from_and_load(df_assigned_ads,
                     ASSIGNMENTS_TABLE_LATEST,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"Location": f"'{LOCATION}'"})

df_cust_div.unpersist()
df_ads_best.unpersist()
df_assigned_ads.unpersist()
