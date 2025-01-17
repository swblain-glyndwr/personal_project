import logging
import logging.config
import json
from next_ads.utils.etl import (JobParser,
                                map_schema,
                                copy_table_from_to)


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

SCHEMA_DICT = cfg["schema"]

TABLE_DICT = cfg["tables"]["write"]

for (k, v) in TABLE_DICT.items():

    log.info(f"Mirroring {k} table")

    tbl_prod = map_schema(v, SCHEMA_DICT["prod"])
    tbl_dev = map_schema(v, SCHEMA_DICT["dev"])

    log.info(f"From {tbl_prod}")
    log.info(f"To {tbl_dev}")

    copy_table_from_to(
        table_from=tbl_prod,
        table_to=tbl_dev,
        history_days=7,
        copy_partitioning=True,
        copy_primary_key=True,
        overwrite_table_to=True
    )

log.info("Run Complete")
