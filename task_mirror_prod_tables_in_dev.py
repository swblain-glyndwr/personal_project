import logging
import logging.config
import json
from next_ads.utils.etl import (JobParser,
                                map_tbl,
                                insert_table_from_to)


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

TABLE_DICT = cfg["tables"]["write"]
SCHEMA_DICT = cfg["schema"]

for (k, v) in TABLE_DICT.items():

    log.info(f"Mirroring {k} table")

    tbl_prod = map_tbl(v, schema=SCHEMA_DICT["prod"], domain=DOMAIN)
    tbl_dev = map_tbl(v, schema=SCHEMA_DICT["dev"], domain=DOMAIN)

    log.info(f"From {tbl_prod}")
    log.info(f"To {tbl_dev}")

    insert_table_from_to(
        table_from=tbl_prod,
        table_to=tbl_dev,
        history_days=1,
        truncate_table_to=True
    )

log.info("Run Complete")
