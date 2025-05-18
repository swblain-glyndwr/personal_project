import json
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import map_tbl
from dsutils.argparser import get_job_parser


jobparser = get_job_parser()
jobparser._parse_args()
JOBNAME = jobparser.get_arg('--jobname')
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert not JOBNAME, 'Client must be specified when running as a job'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

DROP_TABLES = jobparser.get_typed_arg('--droptables', bool)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}

for table_ref in tbls:
    table = map_tbl(tbls[table_ref], **tbl_args)

    if DROP_TABLES and JOB_ENV == "dev":
        logger.info(f"Dropping table {table} as --droptables is 'True'")
        spark.sql(f"drop table if exists {table}")

    if spark.catalog.tableExists(table):
        logger.warning(f"Table {table} already exists - skipping")
        continue

    with open(f"sql/create_table_{table_ref}.sql") as f:
        query = map_tbl("".join(f.readlines()), **tbl_args)

    logger.info(f"Creating {table_ref} table as: {table}")
    spark.sql(query)

logger.info("Run complete")
