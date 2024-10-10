import logging
import logging.config
import json
from AdRetrieval import get_underperforming_ads, get_live_ads
from PageBuilder import (
    assign_random_ads,
    assign_pscores_to_ads,
    assign_best_ads
    )
from utils.dbcutils import get_spark
from utils.sparkutils import delete_from_and_load
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
df_live_ads = get_live_ads(LOCATION)

# Get underperforming Ads
df_under_perf = get_underperforming_ads(LOCATION)


# Remove underperforming from Ads to process
log.info("Removing underperforming Ads")
df_ads = (
    df_live_ads
    .join(
        df_under_perf,
        on=["UniqueAdID", "Division"],
        how="leftanti"
    )
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
df_ads_rdm = assign_random_ads(df_ads, df_cust_div, grp_col="Division")


# Assign propensity scores to Ads
log.info("Assigning Best Ads")
df_adscores = assign_pscores_to_ads(df_ads)
# Determine Best Ad for each customer
df_ads_best = assign_best_ads(df_adscores)
# TODO: Untidy having to sort these columns out post-hoc - tidy
df_ads_best = (df_ads_best.join(df_ads.select("UniqueAdID", "MASID"),
                                on=["UniqueAdID"]))


# Assign Best Ad for each customer (via "challenger" method)
# When no challenger, challenger assignment == champion_assignment
df_ads_best.cache()  # Cache when no challenger for speed
df_ads_best_chall = df_ads_best


# Append to overall cell assignments
# TODO: Make this generalisable - HPTest hardcoded as column
log.info("Getting Cell assignments")
df_cell = (
        get_spark()
        .read.format("delta")
        .load(rsc["files"]["cell_assignment"])
        .select("account_number", "HPTest")
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
            (F.col("HPTest") == "1: Personalised") & (F.col("ABtest1") <= 0.5),
            F.col("BestMASID"))
        .when(
            (F.col("HPTest") == "1: Personalised") & (F.col("ABtest1") > 0.5),
            F.col("BestMASIDChall"))
        .when(F.col("HPTest") == "2: Random", F.col("RandMASID"))
        .when(F.col("HPTest") == "3: No Banner", F.lit("HN1_Z"))
        .when(F.col("HPTest") == "4: Overall", F.lit("HN1_Z"))
        .otherwise(F.lit("HN1_Z"))
        )
    .withColumn("Location", F.lit(LOCATION))
    .select("AccountNumber",
            "Location",
            "RandUniqueAdID",
            "RandMASID",
            "BestUniqueAdID",
            "RandMASIDChall",
            "BestUniqueAdIDChall",
            "BestMASID",
            "MASID"
            )
)

# Cache df_assigned_ads to improve performance when loading twice
df_assigned_ads.cache()

# Load output into assignments table
target_table = rsc["tables"]["assignments"]
target_table_latest = rsc["tables"]["assignments_latest"]

log.info(f"Loading output to {target_table}")
delete_from_and_load(df_assigned_ads,
                     target_table,
                     del_where={"rundate": "current_date()",
                                "Location": LOCATION})

log.info(f"Loading output to {target_table_latest}")
delete_from_and_load(df_assigned_ads,
                     target_table_latest,
                     del_where={"Location": LOCATION})
