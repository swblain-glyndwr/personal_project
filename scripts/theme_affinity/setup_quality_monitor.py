import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from databricks.sdk import WorkspaceClient
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ml.lifecycle.databricks_monitoring import (
    TimeSeriesQualityMonitorSpec,
    ensure_time_series_quality_monitor,
    refresh_quality_monitor,
)
from next_ads.ranking.theme_affinity.lifecycle_spec import resolve_lifecycle_config


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _bool_arg(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
TABLE_NAME = jobparser.get_arg("--table_name")
OUTPUT_SCHEMA_NAME = jobparser.get_arg("--output_schema_name")
ASSETS_DIR = jobparser.get_arg("--assets_dir")
TIMESTAMP_COL = jobparser.get_arg("--timestamp_col") or "reference_date"
GRANULARITIES = _split_csv(jobparser.get_arg("--granularities") or "1 day")
SLICING_EXPRS = _split_csv(jobparser.get_arg("--slicing_exprs"))
RUN_REFRESH = _bool_arg(jobparser.get_arg("--run_refresh"))

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
config = config_manager.load_config(JOB_ENV)
lifecycle_config = resolve_lifecycle_config(config)

table_name = TABLE_NAME or lifecycle_config.train_table
output_schema_name = OUTPUT_SCHEMA_NAME or ".".join(table_name.split(".")[:2])
assets_dir = ASSETS_DIR or f"/Shared/nextads/quality_monitors/{CLIENT}"

logger.info("Validating source table exists: %s", table_name)
spark.table(table_name).limit(1).count()

spec = TimeSeriesQualityMonitorSpec(
    table_name=table_name,
    output_schema_name=output_schema_name,
    assets_dir=assets_dir,
    timestamp_col=TIMESTAMP_COL,
    granularities=GRANULARITIES,
    slicing_exprs=SLICING_EXPRS,
)

logger.info("Creating or updating Databricks quality monitor for %s", table_name)
monitor = ensure_time_series_quality_monitor(WorkspaceClient(), spec)
logger.info("Quality monitor ready for %s", table_name)

if RUN_REFRESH:
    logger.info("Starting quality monitor refresh for %s", table_name)
    refresh = refresh_quality_monitor(WorkspaceClient(), table_name)
    logger.info("Quality monitor refresh started: %s", refresh)
