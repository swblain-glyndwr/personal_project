import logging
import logging.config
import json
from AdRetrieval import get_live_ads
from next_ads.Assignment import (
    assign_random_ads,
    assign_best_ads,
    assign_scores_to_entity
    )
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import delete_from_and_load
from pyspark.sql import functions as F
import sys


# Configure logging
logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

# Configure run
log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)


# Set Location for run
# If valid location not specified via sys.argv (run as job),
# will take hardcoded Location (useful for interactive debugging)
loc_args = list(set(prm["locations"].keys()).intersection(set(sys.argv)))

if len(loc_args) > 1:
    raise Exception(f"More than one Location specified: {loc_args}")
elif len(loc_args) == 1:
    LOCATION = loc_args[0]
else:
    LOCATION = "HN1"  # For interactive debugging

log.info(f"Assigning Ads for Location: {LOCATION}")


# Get Ad data
log.info("Getting Ads")
ad_cols = [
    "UniqueAdID",
    "AlgoDivision",
    "MASIDToken",
    "Models",
    "ModelCombination"
    ]
df_ads = get_live_ads(LOCATION,
                      cols=ad_cols,
                      filter_underperforming=False)
df_ads = df_ads.withColumnRenamed("AlgoDivision", "Division")
# TODO: Remove renaming once fully migrated to new control sheet

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
for div_k in rsc["files"]["div_assignment"].keys():

    df_div = (
        get_spark()
        .read.format("delta")
        .load(rsc["files"]["div_assignment"][div_k])
        .select("account_number")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumn("Division", F.lit(div_k))
    )

    div_asgn_list.append(df_div)

df_cust_div = div_asgn_list.pop()
for df_asgn in div_asgn_list:
    df_cust_div = df_cust_div.union(df_asgn)


# Determine Random (within Division) Ad for each customer
log.info("Assigning Random Ads by Division")
df_ads_rdm = assign_random_ads(
    df_ads.select("UniqueAdID", "Division"),
    df_cust_div,
    grp_col="Division"
    )
# Append MASID to each Ad
df_ads_rdm = df_ads_rdm.join(df_ad_masid, on="UniqueAdID")


# Assign propensity scores to Ads (irrespective of Division)
# df_adscores = get_spark().table(rsc["tables"]["scored_ads_latest"])
df_adscores = assign_scores_to_entity(
    df_ads.select("UniqueAdID", "Models", "ModelCombination"),
    entity_col="UniqueAdID",
    model_score_table=rsc["tables"]["model_scores_latest"],
    patch_model_refs=True
    )

# Limit ad scores to within Division
# TODO: Remove this restriction for cross-division targeting?
# e.g. LP Sport, LP Brands, Homepage Teasers
df_adscores_div = (
    df_cust_div.join(
        (df_adscores
         .join(df_ads.select("UniqueAdID", "Division"),
               on="UniqueAdID")),
        on=["AccountNumber", "Division"]
    )
)
# TODO: Tidy this bit up - Add to function above?
df_adscores_div = (
    df_adscores_div
    .withColumnRenamed("ScoreSubMean", "Score")
    .drop("ScoreRaw", "ScoreZ")
)


log.info("Assigning Best Ads")
# Determine Best Ad for each customer
df_ads_best = assign_best_ads(df_adscores_div)
# Append MASID to each Ad
df_ads_best = df_ads_best.join(df_ad_masid, on="UniqueAdID")


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
        .load(rsc["files"]["cell_assignment"])
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
           .withColumnRenamed("MASID", "RandMASID")
           .withColumnRenamed("UniqueAdID", "RandUniqueAdID")),
          on="AccountNumber")
    .join((df_ads_best
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASID")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdID")),
          on="AccountNumber")
    .join((df_ads_best_chall
           .select("AccountNumber", "MASID", "UniqueAdID")
           .withColumnRenamed("MASID", "BestMASIDChall")
           .withColumnRenamed("UniqueAdID", "BestUniqueAdIDChall")),
          on="AccountNumber")
    .withColumn(
        "MASID",
        F.when(
            (F.col("HPTest") == "1: Personalised")
            & (F.col("random_var1") <= 0.5),
            F.col("BestMASID")
            )
        .when(
            (F.col("HPTest") == "1: Personalised")
            & (F.col("random_var1") > 0.5),
            F.col("BestMASIDChall")
            )
        .when(F.col("HPTest") == "2: Random", F.col("RandMASID"))
        .when(F.col("HPTest") == "3: No Banner", F.lit("HN1_C"))
        .when(F.col("HPTest") == "4: Overall", F.lit("HN1_Z"))
        .otherwise(F.lit("HN1_Z"))
        )
    .withColumn("Location", F.lit(LOCATION))
    .select("AccountNumber",
            "Location",
            "RandUniqueAdID",
            "RandMASID",
            "BestUniqueAdID",
            "BestMASID",
            "BestUniqueAdIDChall",
            "BestMASIDChall",
            "MASID"
            )
)


# Load output into assignments table
target_table = rsc["tables"]["assignments"]
target_table_latest = rsc["tables"]["assignments_latest"]

log.info(f"Loading output to {target_table}")
delete_from_and_load(df_assigned_ads,
                     target_table,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"rundate": "current_date()",
                                "Location": f"'{LOCATION}'"})

log.info(f"Loading output to {target_table_latest}")
delete_from_and_load(df_assigned_ads,
                     target_table_latest,
                     pk_cols=["AccountNumber", "Location"],
                     del_where={"Location": f"'{LOCATION}'"})
