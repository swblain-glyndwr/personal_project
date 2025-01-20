import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import JobParser, map_tbl


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname", "--droptables"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

TABLES = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}

for table_ref in TABLES:
    table = map_tbl(TABLES[table_ref], **tbl_args)

    if pargs["droptables"] == "True" and job_env == "dev":
        log.info(f"Dropping table {table} as --droptables set to 'True'")
        get_spark().sql(f"drop table if exists {table}")

    if get_spark().catalog.tableExists(table):
        log.warning(f"Table {table} already exists - skipping")
        continue

    with open(f"sql/create_table_{table_ref}.sql") as f:
        query = map_tbl("".join(f.readlines()), **tbl_args)

    log.info(f"Creating {table_ref} table as: {table}")
    get_spark().sql(query)

log.info("Run complete")
