import logging
import logging.config
import pyspark.sql.functions as F
import utils.gcputils as gcp
import json
from utils.dbcutils import get_spark
from datetime import date


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

# Parameters
log.info("Reading parameters")
with open("config/params.json") as f:
    prm = json.load(f)
# Resources
with open("config/resources.json") as f:
    rsc = json.load(f)

# Get valid locations from page keys in resources file
valid_locations = list(prm["pages"].keys())
log.info(f"Valid locations: {' '.join(valid_locations)}")

# Read schema and append valid locations
import_schema = rsc["control_sheet"]["read_schema"]
for v in valid_locations:
    import_schema.append([v, "string", "nullable"])

# Import control sheet
log.info("Reading Control Sheet from Google Sheets")
df_ctrl_raw = gcp.spark_df_from_sheets(
    url=rsc["control_sheet"]["url"],
    worksheet_name=rsc["control_sheet"]["sheet"],
    schema=import_schema
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
    .unpivot("UniqueAdID", valid_locations, "Location", "Requested")
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
    .drop(*valid_locations)
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

target_table = rsc["tables"]["control_sheet"]
target_cols = (
    get_spark()
    .table(target_table)
    .drop("rundate")
    ).columns

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

log.info(f"Deleting from {target_table} where rundate == {date.today()}")
get_spark().sql(
    f'''
    delete from {target_table}
    where rundate = current_date()
    ''')
log.info(f"Writing processed Control Sheet to {target_table}")
get_spark().sql(
    f'''
    insert into {target_table}
    select *, current_date() as rundate
    from df_output
    '''
    )

target_table_latest = rsc["tables"]["control_sheet_latest"]
log.info(f"Truncating {target_table_latest}")
get_spark().sql(
    f'''
    truncate table {target_table_latest}
    ''')
log.info(f"Writing processed Control Sheet to {target_table_latest}")
get_spark().sql(
    f'''
    insert into {target_table_latest}
    select *, current_date() as rundate
    from df_output
    '''
    )
