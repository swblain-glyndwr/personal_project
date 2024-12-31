import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import JobParser, create_table_from_df, map_schema
import pyspark.sql.functions as F


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

tbls = rsc["tables"]["write"]

SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
FIXED_CELLS_TABLE_LATEST = map_schema(
    tbls["customer_cells_fixed_latest"], SCHEMA)
TRANSIENT_CELLS_TABLE_LATEST = map_schema(
    tbls["customer_cells_transient_latest"], SCHEMA)
CELLS_TABLE_LATEST = map_schema(tbls["customer_cells_latest"], SCHEMA)


log.info("Combining latest fixed and transient cell assignments")

df_cells_fixed = (
    get_spark()
    .table(FIXED_CELLS_TABLE_LATEST)
    .drop("rundate")
)

df_cells_transient = (
    get_spark()
    .table(TRANSIENT_CELLS_TABLE_LATEST)
    .drop("rundate")
    .groupBy("AccountNumber")
    .pivot("Cell")
    .agg(F.max("CellValue"))
    .where(F.col('AlgoDivision').isNotNull())
)

# Inner join will remove customers that don't have AlgoDivision
# TODO: Will this bias the results? Address when reviewing AlgoDivision.

if df_cells_transient.count() > 0:
    df_cells = (
        df_cells_fixed
        .join(df_cells_transient,
              on="AccountNumber",
              how="inner")
    )
    df_dropped = (
        df_cells_fixed
        .join(df_cells_transient,
              on="AccountNumber",
              how="leftanti")
    )
    n_dropped = df_dropped.count()
    log.warning(f"{n_dropped:,} customers dropped " +
                "when joining transient cells")
else:
    df_cells = df_cells_fixed

log.info(f"Writing combined cells to {CELLS_TABLE_LATEST}")
create_table_from_df(
    df=df_cells,
    table=CELLS_TABLE_LATEST,
    partitioned_by=["FallowControl"],
    pk_cols=["AccountNumber"],
    drop_if_exists=True
    )

log.info("Run complete")
