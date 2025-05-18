import json
from dsutils.etl import map_tbl, insert_table_from_to
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser


jobparser = get_job_parser()
jobparser._parse_args()
JOBNAME = jobparser.get_arg('--jobname')
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert not JOBNAME, 'Client must be specified when running as a job'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

HISTORY_DAYS = jobparser.get_typed_arg('--history_days', int)
if not HISTORY_DAYS:
    assert not JOBNAME, 'History Days must be specified when running as a job'
    HISTORY_DAYS = 1  # History Days can be specified for interactive debugging
    logger.warning(
        f'History Days not specified (defaulting to {HISTORY_DAYS})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(f"config/{CLIENT}.json") as f:
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
