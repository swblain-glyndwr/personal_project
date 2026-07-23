import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from next_ads.common.paths import load_client_config
from next_ads.ranking.theme_score_mapping import run_theme_score_mapping
from next_ads.utils import config_manager, etl


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

top_ads_arg = jobparser.get_arg("--top-ads-per-page-type")
TOP_ADS_PER_PAGE_TYPE = int(top_ads_arg or 100)
assert TOP_ADS_PER_PAGE_TYPE > 0, (
    "top-ads-per-page-type must be greater than zero"
)

config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

tbl_args = {
    "catalog": config.catalog_write,
    "schema": config.schema_write,
    "client": CLIENT,
}
tbls = cfg["tables"]["write"]
preranked_ads_from_themes_v2_latest = etl.map_tbl(
    tbls["preranked_ads_from_themes_v2_latest"],
    **tbl_args,
)

run_theme_score_mapping(
    spark=spark,
    config=config,
    cfg=cfg,
    client=CLIENT,
    job_env=JOB_ENV,
    control_sheet_latest_table=config.tables_write.control_sheet_latest_v2,
    output_preranked_table=preranked_ads_from_themes_v2_latest,
    output_grain="page_type",
    top_ads_per_group=TOP_ADS_PER_PAGE_TYPE,
    write_score_components=False,
    logger=logger,
)
