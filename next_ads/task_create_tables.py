import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import JobParser, map_schema


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname", "--droptables"])
log.info(f"Running in job environment: {job_env}")

SCHEMA = rsc["schema"][job_env]
TABLES = rsc["tables"]["write"]

for table_ref in TABLES:
    table = map_schema(TABLES[table_ref], SCHEMA)

    if pargs["droptables"] == "True" and job_env == "dev":
        log.info(f"Dropping table {table} as --droptables set to 'True'")
        get_spark().sql(f"drop table if exists {table}")

    if get_spark().catalog.tableExists(table):
        log.warning(f"Table {table} already exists - skipping")
        continue

    with open(f"sql/create_table_{table_ref}.sql") as f:
        query = map_schema("".join(f.readlines()), SCHEMA)

    log.info(f"Creating {table_ref} table as: {table}")
    get_spark().sql(query)

log.info("Run complete")
