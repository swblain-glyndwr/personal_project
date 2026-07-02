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

from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from next_ads.utils import etl


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
ASSIGNMENTS_TABLE_LATEST = etl.map_tbl(tbls["assignments_latest"], **tbl_args)

logger.info(f'Truncating {ASSIGNMENTS_TABLE_LATEST} ' +
            'to remove any discontinued assignments')
spark.sql(f'truncate table {ASSIGNMENTS_TABLE_LATEST}')

logger.info('Run Complete')
