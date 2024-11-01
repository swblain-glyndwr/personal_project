import logging
import logging.config
import json
import argparse
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import get_job_env, map_schema


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--f", help="dummy arg enabling interactive debugging")
parser.add_argument("--jobname", nargs="?", const="dev_", type=str)
known_args, unknown_args = parser.parse_known_args()
pargs = vars(known_args)
job_env = get_job_env(pargs)
log.info(f"Running in job environment: {job_env}")

SCHEMA = rsc["schema"][job_env]
TABLES = rsc["tables"]["write"]

for table_ref in TABLES:

    table = map_schema(TABLES[table_ref], SCHEMA)

    if get_spark().catalog.tableExists(table):
        log.warning(f"Table {table} already exists - skipping")
        continue

    with open(f"sql/create_table_{table_ref}.sql") as f:
        query = "".join(f.readlines())

    log.info(f"Creating {table_ref} table as: {table}")
    get_spark().sql(query)

log.info("Run complete")
