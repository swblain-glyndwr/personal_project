import logging
import logging.config
import pyspark.sql.functions as F
from next_ads.Scoring import append_targeting_criteria
import next_ads.utils.gcp as gcp
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (assert_pk,
                                truncate_and_load,
                                delete_from_and_load,
                                JobParser,
                                map_schema)


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/parameters.json") as f:
    prm = json.load(f)
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

VALID_LOCATIONS = list(prm["locations"].keys())
CONTROL_SHEET = rsc["control_sheet"]

SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
TARGET_TABLE = map_schema(tbls["control_sheet"], SCHEMA)
TARGET_TABLE_LATEST = map_schema(tbls["control_sheet_latest"], SCHEMA)

log.info(f"Valid locations: {' '.join(VALID_LOCATIONS)}")

for v in VALID_LOCATIONS:
    CONTROL_SHEET["read_schema"].append([v, "string", "null"])


log.info("Reading Control Sheet from Google Sheets")

df_ctrl_raw = gcp.spark_df_from_sheets(
    url=CONTROL_SHEET["url"],
    worksheet_name=CONTROL_SHEET["sheet"],
    schema=CONTROL_SHEET["read_schema"]
    )


log.info("Processing Control Sheet")

df_ctrl_active = (
    df_ctrl_raw
    .where(F.col("UniqueAdID") != "")
    .where(F.col("Status") == "Active")
    .drop("Status")
)
log.info(f"Active Ads: {df_ctrl_active.count():,}")

# Legacy corercion of item codes to upper case and replace("-","")
df_ctrl_active = df_ctrl_active.withColumn(
    "Items",
    F.regexp_replace(F.upper(F.col("Items")), "-", "")
)

# Primary Key (UniqueAdID, Location)
df_id_loc = (
    df_ctrl_active
    .unpivot(ids="UniqueAdID",
             values=VALID_LOCATIONS,
             variableColumnName="Location",
             valueColumnName="Requested")
    .where(F.col("Requested") == "TRUE")
    .drop_duplicates()
    .drop("Requested")
)
# TODO: Warn Ads team if duplciates found in Input?
active_locs = set([row[0] for row in df_id_loc.select('Location').collect()])
log.info(f"Active Locations: {len(active_locs):,} {active_locs}")
log.info(f"Active Ad-Locations: {df_id_loc.count():,}")


df_ad_attributes = (
    df_ctrl_active
    .drop(*VALID_LOCATIONS)
    .drop_duplicates()
    .replace("", None)
)


for date_col in ["StartDate", "EndDate"]:
    df_ad_attributes = df_ad_attributes.withColumn(
        date_col, F.to_date(F.col(date_col), "dd/MM/yyyy")
    )


df_processed = (
    df_id_loc
    .join(df_ad_attributes, on="UniqueAdID", how="left")
)
df_processed = (
    df_processed
    .fillna({"ModelCombination": "and"})
    .withColumn("ModelCombination",
                F.when(F.col("Models").isNull(),
                       F.lit(None)).otherwise(F.col("ModelCombination")))
)
df_processed = append_targeting_criteria(df_processed)

log.info("Checking input Primary Key")
assert_pk(df_processed, ["UniqueAdID", "Location"])


target_cols = (
    get_spark()
    .table(TARGET_TABLE)
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


log.info("Loading output to table")
delete_from_and_load(df_processed.select(*target_cols),
                     TARGET_TABLE,
                     job_env=job_env,
                     pk_cols=["UniqueAdID", "Location"],
                     del_where={"rundate": "current_date()"})

log.info("Loading output to table (latest)")
truncate_and_load(df_processed.select(*target_cols),
                  TARGET_TABLE_LATEST,
                  job_env=job_env,
                  pk_cols=["UniqueAdID", "Location"])

log.info("Run complete")
