import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
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
    PROJECT_ROOT = Path(notebook_path).parents[3]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.ranking.theme_affinity.clean_output import clean_model_output
from next_ads.ranking.theme_affinity.config import resolve_context_runtime
from next_ads.ranking.provider_context import transition_provider_context


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
RUN_DATE = jobparser.get_arg("--run_date")
INPUT_SNAPSHOT_ID = jobparser.get_arg("--input_snapshot_id")
PROVIDER_BUILD_ID = jobparser.get_arg("--provider_build_id")
PROVIDER_BUILD_ATTEMPT_ID = jobparser.get_arg("--provider_build_attempt_id")
CONTEXT_SLOT = jobparser.get_arg("--context_slot")

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
runtime, context = resolve_context_runtime(
    spark,
    job_env=JOB_ENV,
    client=CLIENT,
    context_slot=CONTEXT_SLOT,
    expected_run_date=RUN_DATE,
    expected_input_snapshot_id=INPUT_SNAPSHOT_ID,
    expected_provider_build_id=PROVIDER_BUILD_ID,
    expected_provider_build_attempt_id=PROVIDER_BUILD_ATTEMPT_ID,
)

logger.info("Cleaning Theme Affinity output into %s", runtime.namespace)
clean_model_output(spark, runtime)
from datetime import datetime, timezone

transition_provider_context(
    spark,
    context_table=runtime.config.tables_write.score_provider_run_contexts,
    context=context,
    status="CONSUMED",
    completed_at=datetime.now(timezone.utc),
)
