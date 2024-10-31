import argparse
import logging
import logging.config
import json
import datetime as dt
from next_ads.utils.etl import assert_pk, get_job_env
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
log.info(f"Running in job environment: {job_env}")

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

df_sessions_web = (
    get_spark()
    .table(BQ_UK_SESSIONS)
    .where(F.col("date") == ANCHOR_DATE)
    .join(
        get_spark()
        .table(RPID_WITH_ACCOUNTS)
        .select("roamingprofileid", "account_number")
        .withColumnsRenamed({
            "roamingprofileid": "RPID",
            "account_number": "AccountNumber"
        })
        .drop_duplicates(),
        on="RPID", how="inner")
    .select(
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "date",
        "TransactionRevenue")
    .withColumnsRenamed({
        "date": "SessionDate",
        "TransactionRevenue": "SessionRevenue"
    })
    .drop_duplicates()
)

df_sessions_app = (
    get_spark()
    .table(BQ_UK_SESSIONS_APP)
    .where(F.col("date") == ANCHOR_DATE)
    .join(
        get_spark()
        .table(RPID_WITH_ACCOUNTS)
        .select("roamingprofileid", "account_number")
        .withColumnsRenamed({
            "roamingprofileid": "RPID",
            "account_number": "AccountNumber"
        })
        .drop_duplicates(),
        on="RPID", how="inner")
    .select(
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "date",
        "TransactionRevenue")
    .withColumnsRenamed({
        "date": "SessionDate",
        "TransactionRevenue": "SessionRevenue"
    })
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


# Counting value of items only after page hit
# Assumption: All items with same ProductSKU in a given basket are
# of equal value (approximated via median)
# If three of five of that Product were added before the first hit,
# two units multiplied by the median value for that Product will be returned
df_value_of_transacted_items_web = (
    get_spark()
    .table(BQ_UK_TRANSACTIONS)
    .where(F.col("date") == ANCHOR_DATE)
    .select("UniqueVisitID", "ProductSKU", "productRevenue")
    .groupBy("UniqueVisitID", "ProductSKU")
    .agg(F.median("productRevenue").alias("MedianProductRevenue"))
)
assert_pk(df_value_of_transacted_items_web,
          ["UniqueVisitID", "ProductSKU"])

df_rev_post_first_hit_web = (
    df_first_hits_web
    .join(
        get_spark()
        .table(BQ_UK_ADD_TO_BASKET)
        .where(F.col("date") == ANCHOR_DATE)
        .groupBy("UniqueVisitID", "Timestamp", "ProductSKU")
        .agg(F.count(F.lit(1)).alias("Units")),
        on="UniqueVisitID",
        how="left")
    .join(df_value_of_transacted_items_web,
          on=["UniqueVisitID", "ProductSKU"],
          how="left")
    .withColumn("ProductRevenue",
                F.col("Units") * F.col("MedianProductRevenue"))
    .withColumn("ProductRevenuePostFirstHit",
                F.when(
                    F.col("Timestamp") > F.col("FirstHit"),
                    F.col("ProductRevenue")).otherwise(None))
    .groupBy("UniqueVisitID")
    .agg(
        F.sum("ProductRevenue").alias("ProductRevenue"),
        F.sum("ProductRevenuePostFirstHit").alias("ProductRevenuePostFirstHit")
        )
)

df_value_of_transacted_items_app = (
    get_spark()
    .table(BQ_UK_TRANSACTIONS_APP)
    .where(F.col("UniqueVisitID").isNotNull())  # Masking issue with the table?
    .where(F.col("ProductSKU").isNotNull())  # Masking issue with the table?
    .where(F.col("date") == ANCHOR_DATE)
    .select("UniqueVisitID", "ProductSKU", "productRevenue")
    .groupBy("UniqueVisitID", "ProductSKU")
    .agg(F.median("productRevenue").alias("MedianProductRevenue"))
)
assert_pk(df_value_of_transacted_items_app,
          ["UniqueVisitID", "ProductSKU"])

df_rev_post_first_hit_app = (
    df_first_hits_app
    .join(
        get_spark()
        .table(BQ_UK_ADD_TO_BASKET_APP)
        .where(F.col("date") == ANCHOR_DATE)
        .groupBy("UniqueVisitID", "Timestamp", "ProductSKU")
        .agg(F.count(F.lit(1)).alias("Units")),
        on="UniqueVisitID",
        how="left")
    .join(df_value_of_transacted_items_app,
          on=["UniqueVisitID", "ProductSKU"],
          how="left")
    .withColumn("ProductRevenue",
                F.col("Units") * F.col("MedianProductRevenue"))
    .withColumn("ProductRevenuePostFirstHit",
                F.when(
                    F.col("Timestamp") > F.col("FirstHit"),
                    F.col("ProductRevenue")).otherwise(None))
    .groupBy("UniqueVisitID")
    .agg(
        F.sum("ProductRevenue").alias("ProductRevenue"),
        F.sum("ProductRevenuePostFirstHit").alias("ProductRevenuePostFirstHit")
        )
)


df_session_revenue_web = (
    df_sessions_web
    .join(df_rev_post_first_hit_web, how="inner", on="UniqueVisitID")
    .withColumn("Revenue",
                F.col(TEST_LOCATIONS[TEST_LOCATION]["reported_value"]))
    .select(
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "SessionDate",
        "Revenue"
    )
)
assert_pk(df_session_revenue_web, ["UniqueVisitID"])

df_session_revenue_app = (
    df_sessions_app
    .join(df_first_hits_app, how="inner", on="UniqueVisitID")
    .withColumn("Revenue",
                F.col(TEST_LOCATIONS[TEST_LOCATION]["reported_value"]))
    .select(
        "AccountNumber",
        "UniqueVisitID",
        "Device",
        "SessionDate",
        "Revenue"
    )
)
assert_pk(df_session_revenue_app, ["UniqueVisitID"])

df_session_revenue = (
    df_session_revenue_web
    .union(df_session_revenue_app)
)

df_session_revenue.cache()

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

print(f"Hit: {df_session_revenue_web.count():,}")  # Matches
# print(f"Next_Page: {df_next_page.count():,}")  # Matches
print(f"Hit_APP: {df_session_revenue_app.count():,}")  # Matches
# print(f"Next_Page_APP: {df_next_page_app.count():,}")  # Matches

df_session_revenue.printSchema()  # Matches HP_Hit schema
# df_next_page.printSchema()  # Matches HP_Next_Page schema
df_session_revenue_app.printSchema()  # Matches HP_Hit schema
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
    df_session_revenue
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
# I've done this earlier to reduce fragmentation and joined to base audience

df_results_total = (
    df_accounts_sessions
    .join(df_accounts_pf, how="inner", on="AccountNumber")
    .join(df_accounts_cell, how="inner", on="AccountNumber")
    .join(df_session_revenue, how="inner", on="AccountNumber")
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
