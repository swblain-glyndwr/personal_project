import logging
import logging.config
import json
import argparse
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (apply_job_env_prefix,
                                extract_table_name,
                                get_job_env)


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--f", help="dummy arg enabling interactive debugging")
parser.add_argument("--jobname", nargs="?", const="dev_", type=str)
pargs = vars(parser.parse_args())
job_env = get_job_env(pargs)
log.info(f"Running in job environment: {job_env}")

TABLES = rsc["tables"]["write"]

for table_ref in TABLES:

    table = TABLES[table_ref]
    table_name_raw = extract_table_name(table)
    table_name = apply_job_env_prefix(table_name_raw, job_env)
    table_path = table.replace(table_name_raw, table_name)

    if get_spark().catalog.tableExists(table_path):
        log.warning(f"Table {table_path} already exists - skipping")
        continue

    with open(f"sql/create_table_{table_ref}.sql") as f:
        raw_query = "".join(f.readlines())

    query = raw_query.replace(table_name_raw, table_name)

    get_spark().sql(query)
