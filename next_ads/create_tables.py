import logging
import logging.config
import json
import argparse
# from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import get_job_env, get_env_prefix


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

CATALOG = rsc["write"]["catalog"]
SCHEMA = rsc["write"]["schema"]
TABLES = rsc["write"]["tables"]
ENV_PREFIX = get_env_prefix(job_env)

log.info(f"Creating tables in {CATALOG}.{SCHEMA}")

for table in TABLES:
    if not TABLES[table]:
        continue
    with open(f"sql/create_table_{table}.sql") as f:
        raw_query = "".join(f.readlines())
    query = raw_query.format_map({
        "catalog": CATALOG,
        "schema": SCHEMA,
        "env_prefix": ENV_PREFIX,
        "table": table})
    print(query)
    print("")
