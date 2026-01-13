import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from dsutils.etl import map_tbl, insert_table_from_to
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

HISTORY_DAYS = jobparser.get_typed_arg('--history_days', int)
if not HISTORY_DAYS:
    assert JOB_ENV.lower() == 'dev', \
        f'History Days must be specified when running in {JOB_ENV}'
    HISTORY_DAYS = 1  # History Days can be specified for interactive debugging
    logger.warning(
        f'History Days not specified (defaulting to {HISTORY_DAYS})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
SCHEMA_DICT = cfg["schema"]

for (k, v) in tbls.items():

    logger.info(f"Mirroring {k} table (history days: {HISTORY_DAYS})")

    tbl_prod = map_tbl(v, schema=SCHEMA_DICT["prod"], client=CLIENT)
    tbl_dev = map_tbl(v, schema=SCHEMA_DICT["dev"], client=CLIENT)

    logger.info(f"From {tbl_prod}")
    logger.info(f"To {tbl_dev}")

    insert_table_from_to(
        table_from=tbl_prod,
        table_to=tbl_dev,
        history_days=1,
        truncate_table_to=True
    )

logger.info("Run Complete")
