import argparse
import logging
import logging.config
import json
import datetime as dt
from next_ads.utils.etl import get_job_env
from next_ads.utils.dbc import get_spark
import pyspark.sql.functions as F

logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--f", help="dummy arg enabling interactive debugging")
parser.add_argument("--jobname", nargs="?", const="dev_", type=str)
parser.add_argument("--testlocation", nargs="?", const="HN1", type=str)
pargs = vars(parser.parse_args())
req_testlocation = pargs["testlocation"] if pargs["testlocation"] else "SB"
job_env = get_job_env(pargs)
log.info(f"Running in: {job_env}")

RPID_WITH_ACCOUNTS = rsc["tables"]["read"]["rpid_with_accounts"]
UK_PREFERENCE_FRAMEWORK = rsc["tables"]["read"]["uk_preference_framework"]

BQ_UK_SESSIONS = rsc["tables"]["read"]["bq_uk_sessions"]
BQ_UK_PAGES = rsc["tables"]["read"]["bq_uk_pages"]
BQ_UK_ADD_TO_BASKET = rsc["tables"]["read"]["bq_uk_add_to_basket"]
BQ_UK_TRANSACTIONS = rsc["tables"]["read"]["bq_uk_transactions"]

BQ_UK_SESSIONS_APP = rsc["tables"]["read"]["bq_uk_sessions_app"]
BQ_UK_SCREENS = rsc["tables"]["read"]["bq_uk_screens"]
BQ_UK_ADD_TO_BASKET_APP = rsc["tables"]["read"]["bq_uk_add_to_basket_app"]
BQ_UK_TRANSACTIONS_APP = rsc["tables"]["read"]["bq_uk_transactions_app"]

AD_ASSIGNMENTS = rsc["tables"]["write"]["assignments"]

ANCHOR_DATE = dt.date.today() - dt.timedelta(days=2)

TEST_LOCATIONS = prm["test_locations"]
if req_testlocation in TEST_LOCATIONS:
    TEST_LOCATION = req_testlocation
else:
    raise Exception(f"Invalid TestLocation requested: {req_testlocation}")
log.info(f"Building results tables for TestLocation: {TEST_LOCATION}")


# There were a lot of instances of "select distinct..." in legacy code
# therefore drop_duplicates() has been used to ensure the same uniqueness
# TODO: Better understand entity relations here to detect spurious dups
# instead of forcing them out
df_customers = (
    get_spark()
    .table(RPID_WITH_ACCOUNTS)
    .select("roamingprofileid", "account_number")
    .withColumnsRenamed({
        "roamingprofileid": "RPID",
        "account_number": "AccountNumber"
    })
)

df_sessions_web = (
    get_spark()
    .table(BQ_UK_SESSIONS)
    .where(F.col("date") == ANCHOR_DATE)
    .select(
        "UniqueVisitID",
        "Device",
        "RPID",
        "date",
        "TransactionRevenue")
    .withColumnRenamed("date", "SessionDate")
    .drop_duplicates()
)

df_sessions_app = (
    get_spark()
    .table(BQ_UK_SESSIONS_APP)
    .where(F.col("date") == ANCHOR_DATE)
    .select(
        "UniqueVisitID",
        "Device",
        "RPID",
        "date",
        "TransactionRevenue")
    .withColumnRenamed("date", "SessionDate")
    .drop_duplicates()
)

df_sessions = df_sessions_web.union(df_sessions_app)
df_sessions.cache()


df_first_hits_web = (
    get_spark()
    .table(BQ_UK_PAGES)
    .where(F.col("PagePath") == TEST_LOCATIONS[TEST_LOCATION]["page"])
    .where(F.col("date") == ANCHOR_DATE)
    .select("UniqueVisitID", "FirstTimestamp")
    .groupBy("UniqueVisitID")
    .agg(F.min("FirstTimestamp").alias("FirstHit"))
)

df_first_hits_app = (
    get_spark()
    .table(BQ_UK_SCREENS)
    .where(F.col("ScreenName") == TEST_LOCATIONS[TEST_LOCATION]["screen"])
    .where(F.col("date") == ANCHOR_DATE)
    .select("UniqueVisitID", "FirstTimestamp")
    .groupBy("UniqueVisitID")
    .agg(F.min("FirstTimestamp").alias("FirstHit"))
)

df_first_hits = df_first_hits_web.union(df_first_hits_app)


# Looks like there is insufficient data in the BQ tables to do this
# precisely? Example: same item is added to basket multiple times
# (perhaps multiple variants? size? colour?), some of these are before
# viewing the ad, some are after...
# - The add_to_basket view doesn't tell you the item value
# - The transaction view doesn't tell you which item was added when
# Assumption made in order to progress - All items with same ProductSKU
# in a given basket are of equal value (ascertained via the median)
# if three of five of that Product were added after the ad view, two units
# multiplied by the median value for that Product will be returned

# What to do with cases where VisitID doesn't appear in add to basket table
# but does appear in transactions table?
# See visit ID "1449884134.1727259093-1730140221-23	" as an example
# This case would produce a None in the table below...
# 	PostFirstHit	count(DISTINCT UniqueVisitID)
# 0	None	        50,375
# 1	True	        40,964
# 2	False	        81,476
df_post_ad_view_value = (
    df_first_hits_web
    .join(
        get_spark()
        .table(BQ_UK_ADD_TO_BASKET)
        .where(F.col("date") == ANCHOR_DATE)
        .select("UniqueVisitID", "Timestamp", "ProductSKU"),
        on="UniqueVisitID",
        how="left")
    # .withColumn("PostFirstHit", F.col("Timestamp") > F.col("FirstHit"))
    # .groupBy("PostFirstHit").agg(F.countDistinct("UniqueVisitID")))
    .where(F.col("Timestamp") > F.col("FirstHit"))
    .groupBy("UniqueVisitID", "ProductSKU")
    .count().withColumnRenamed("count", "Units")
    .join(
        get_spark()
        .table(BQ_UK_TRANSACTIONS)
        .where(F.col("date") == ANCHOR_DATE)
        .select("UniqueVisitID", "ProductSKU", "productRevenue")
        .groupBy("UniqueVisitID", "ProductSKU")
        .agg(F.median("productRevenue").alias("MedianProductRevenue")),
        on="UniqueVisitID",
        how="left")
    .withColumn("ProductRevenue",
                F.col("Units") * F.col("MedianProductRevenue"))
)
df_post_ad_view_value.cache()


# SB_TransValue=spark.sql("""
# select r.UniqueVisitID, sum(ProductRevenue) as RPS_post_SB
# from ds_sandbox.NextAds_SB_Hit r
# left join (
#     select UniqueVisitID, date, Timestamp, ProductSKU
#     from warehouse.bq_atbs_next_uk where date=current_date()-1) a
# on r.UniqueVisitID=a.UniqueVisitID
# and a.Timestamp>SBHit
# left join (
#     select UniqueVisitID, date, Timestamp, productRevenue, ProductSKU
#     from warehouse.bq_transactions_next_uk where date=current_date()-1) t
# on a.UniqueVisitID=t.UniqueVisitID
# and a.ProductSKU=t.ProductSKU
# group by all
# """)


df_revenue_per_session_web = (
    df_customers
    .join(df_sessions, how="inner", on="RPID")
    .join(df_first_hits, how="inner", on="UniqueVisitID")
    .groupBy([
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "SessionDate",
        ])
    .agg(F.min("TransactionRevenue").alias("RPS"))
)

df_revenue_per_session_app = (
    df_customers
    .join(df_sessions_app, how="inner", on="RPID")
    .join(df_first_hits_app, how="inner", on="UniqueVisitID")
    .groupBy([
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "SessionDate",
        ])
    .agg(F.min("TransactionRevenue").alias("RPS"))
)

df_revenue_per_session = (
    df_revenue_per_session_web
    .union(df_revenue_per_session_app)
)

df_revenue_per_session.cache()

# df_next_page_path = (
#     get_spark()
#     .table(BQ_UK_PAGES)
#     .where(F.col("PagePath") == TEST_LOCATIONS[TEST_LOCATION]["page"])
#     .where(F.col("date") == ANCHOR_DATE)
#     .select("UniqueVisitID", "NextPagePath")
#     .drop_duplicates()
# )

# df_next_page_path_app = (
#     get_spark()
#     .table(BQ_UK_SCREENS)
#     .where(F.col("ScreenName") == TEST_LOCATIONS[TEST_LOCATION]["screen"])
#     .where(F.col("date") == ANCHOR_DATE)
#     .select("UniqueVisitID", "NextScreenName")
#     .withColumnRenamed("NextScreenName", "NextPagePath")
#     .drop_duplicates()
# )

# df_next_page = (
#     df_sessions
#     .select("UniqueVisitID", "RPID")
#     .drop_duplicates()
#     .join(df_next_page_path, how="inner", on="UniqueVisitID")
#     .drop_duplicates()
# )

# df_next_page_app = (
#     df_sessions_app
#     .select("UniqueVisitID", "RPID")
#     .drop_duplicates()
#     .join(df_next_page_path_app, how="inner", on="UniqueVisitID")
#     .drop_duplicates()
# )

print(f"Hit: {df_revenue_per_session_web.count():,}")  # Matches
# print(f"Next_Page: {df_next_page.count():,}")  # Matches
print(f"Hit_APP: {df_revenue_per_session_app.count():,}")  # Matches
# print(f"Next_Page_APP: {df_next_page_app.count():,}")  # Matches

df_revenue_per_session.printSchema()  # Matches HP_Hit schema
# df_next_page.printSchema()  # Matches HP_Next_Page schema
df_revenue_per_session_app.printSchema()  # Matches HP_Hit schema
# df_next_page_app.printSchema()  # Matches HP_Next_Page schema


# Three ds_sandbox tables created:
# {TEST_LOCATION}_Hit as select * from HP_Hit/df_revenue_per_session
# {TEST_LOCATION}_Next_Page as select * from HP_Next_Page/df_next_page
# {TEST_LOCATION}_Account as select distinct acc from HP_Hit/df_rps


# 1 Combine App and Web AccountNumber lists to create master unique accs list
# 2 Get distinct accs from MASID (UK_PF) table
#   (rundate -2)
# 3 Seg?? Customer cell assignments are in
#   (rundate -3, i.e. extra day b/c assigned pre-midnight)
#   Currently retrieved from Homepage Ads table, but cells are duplicated
#   columns from OVERALL_TEST_AND_CONTROL...
# Inner join 1, 2, 3 above to create base audience

df_accounts_sessions = (
    df_revenue_per_session
    .select("AccountNumber")
    .drop_duplicates()
)

df_accounts_pf = (
    get_spark()
    .table(UK_PREFERENCE_FRAMEWORK)
    .where(F.col("rundate") == ANCHOR_DATE)
    .select("account_number")
    .withColumnRenamed("account_number", "AccountNumber")
    .drop_duplicates()
)

# TODO: Check why this has a max(rundate) clause in the legacy script
# Additional day in arrears, as cells are assigned the evening before
df_accounts_cell = (
    get_spark()
    .table(AD_ASSIGNMENTS)
    .where(F.col("rundate") == ANCHOR_DATE - dt.timedelta(days=1))
    .withColumnRenamed("account_number", "AccountNumber")
)

# App and Web are then combined to create "Hit_all" df
# I've done this earlier to reduce fragmentation
# and joined to base audience

df_results_total = (
    df_accounts_sessions
    .join(df_accounts_pf, how="inner", on="AccountNumber")
    .join(df_accounts_cell, how="inner", on="AccountNumber")
    .join(df_revenue_per_session, how="inner", on="AccountNumber")
)

# df_results_total.count()
df_results_total
df_results_total.groupBy("TestLocationCell").count()


# History then loaded and overwritten


# Repeat everything with date offset one day forward
# hit and next_page queries with date offset of -1 (already done -2)
# UK_PF date offset rundate -1
# Seg date offset rundate -2

# History then loaded
#   New data (with fillna(0)?) appended
#   History overwritten with new data appended
# TODO: Why is the process repeated for two separate dates? Data lag?


# GA_Out - Homepage overall results(??) written out as BQ dataset
#   Schema:
#   AccountNumber, HPTest, RPS, Device, UniqueVisitID, SessionDate, Division
#   where OverallTestControl = 'Ads'

bq_nextads_homepage_report = (
    df_results_total
    .select(
        "AccountNumber",
        "TestLocationCell",  # Replacing HPTest
        "RPS",
        "Device",
        "UniqueVisitID",
        "SessionDate",
        "Division"
    )
    .where(F.col("TestLocationCell") != "4: Overall")
)
# TODO: Export to BQ


# Calculate descriptive stats on history

# Calculate first & last shown date for each ad
#   (use random ad id to ensure all are captured)

# FullSummary is ad level reporting
#   Output to separate BQ table

# Decouple exports to BQ?
