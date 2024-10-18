import logging
import logging.config
import pyspark.sql.functions as F
import next_ads.utils.gcp as gcp
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import delete_from_and_load


# Configure logging
logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")


# Parameters
log.info("Configuring run")
with open("config/parameters.json") as f:
    prm = json.load(f)
# Resources
with open("config/resources.json") as f:
    rsc = json.load(f)

VALID_LOCATIONS = list(prm["locations"].keys())
CONTROL_SHEET = rsc["control_sheet"]
TARGET_TABLE = rsc["tables"]["write"]["control_sheet"]
TARGET_TABLE_LATEST = rsc["tables"]["write"]["control_sheet_latest"]


# Get valid locations from page keys in resources file
log.info(f"Valid locations: {' '.join(VALID_LOCATIONS)}")

# Read schema and append valid locations

for v in VALID_LOCATIONS:
    CONTROL_SHEET["read_schema"].append([v, "string", "null"])

# Import control sheet
log.info("Reading Control Sheet from Google Sheets")
df_ctrl_raw = gcp.spark_df_from_sheets(
    url=CONTROL_SHEET["url"],
    worksheet_name=CONTROL_SHEET["sheet"],
    schema=CONTROL_SHEET["read_schema"]
    )

log.info("Processing Control Sheet")
# Remove Ads without a UniqueAdID and those not Active
df_ctrl_filtered = (
    df_ctrl_raw
    .where(F.col("UniqueAdID") != "")
    .where(F.col("Status") == "Active")
    .drop("Status")
)
log.info(f"Active Ads: {df_ctrl_filtered.count():,}")

# Apply legacy corercion of Item codes to upper case and replace("-","")
df_ctrl_filtered = df_ctrl_filtered.withColumn(
    "Items",
    F.regexp_replace(F.upper(F.col("Items")), "-", "")
)


# Melt Locations and filter out 'FALSE' permutations to get unique ID-Location
df_id_loc = (
    df_ctrl_filtered
    .unpivot("UniqueAdID", VALID_LOCATIONS, "Location", "Requested")
    .where(F.col("Requested") == "TRUE")
    .drop_duplicates()
    .drop("Requested")
)
# TODO: Warn Ads team if duplciates found in Input
distinct_locations = df_id_loc.select('Location').distinct().count()
log.info(f"Active Locations: {distinct_locations:,}")
log.info(f"Active Ad-Locations: {df_id_loc.count():,}")

# Exclude Location toggle columns and clean up df
df_ad_attributes = (
    df_ctrl_filtered
    .drop(*VALID_LOCATIONS)
    .drop_duplicates()
    .replace("", None)
)

# Cast date columns to date type
for date_col in ["StartDate", "EndDate"]:
    df_ad_attributes = df_ad_attributes.withColumn(
        date_col, F.to_date(F.col(date_col), "dd/MM/yyyy")
    )

# Combine primary key columns with Ad attributes
df_processed = (
    df_id_loc
    .join(df_ad_attributes, on="UniqueAdID", how="left")
)


# Checks
log.info("Running checks")
rows = df_processed.count()
rows_pk = (
    df_processed
    .select("UniqueAdID", "Location")
    .drop_duplicates()
    .count()
    )
assert rows == rows_pk, "Duplicate (UniqueAdID, Locations) found"


target_cols = (
    get_spark()
    .table(TARGET_TABLE)
    .drop("rundate")
    ).columns

# Safeguard in case additional columns have been added to gsheet
if set(target_cols) == set(df_processed.columns):
    log.info("Control Sheet columns match Target table columns")
elif set(target_cols).issubset(set(df_processed.columns)):
    log.warning("Target table cols are subset of Control Sheet cols")
    extra_cols = set(df_processed.columns).difference(set(target_cols))
    log.warning("Dropping superfluous columns: %s", ", ".join(extra_cols))
    df_processed = df_processed.drop(*list(extra_cols))
else:
    raise Exception("Target table cols not a subset of Control Sheet cols")


# Create Temp View, Delete current rundate and Insert
df_processed.createOrReplaceTempView("df_output")

log.info(f"Loading output to {TARGET_TABLE}")
delete_from_and_load(df_processed,
                     TARGET_TABLE,
                     pk_cols=["UniqueAdID", "Location"],
                     del_where={"rundate": "current_date()"})

log.info(f"Loading output to {TARGET_TABLE_LATEST}")
delete_from_and_load(df_processed,
                     TARGET_TABLE_LATEST,
                     pk_cols=["UniqueAdID", "Location"])
