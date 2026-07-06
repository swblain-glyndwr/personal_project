import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from next_ads.ranking.theme_score_mapping import run_theme_score_mapping
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
ALGO = jobparser.get_arg('--algo') or 'champion'
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

run_theme_score_mapping(
    spark=spark,
    config=config,
    cfg=cfg,
    client=CLIENT,
    job_env=JOB_ENV,
    algo=ALGO,
    apply_ad_feedback=jobparser.has_arg('--apply-ad-feedback'),
    ad_feedback_weight=jobparser.get_arg('--ad-feedback-weight') or 0.05,
    top_ads_per_location=jobparser.get_arg('--top-ads-per-location') or 20,
    logger=logger,
)
