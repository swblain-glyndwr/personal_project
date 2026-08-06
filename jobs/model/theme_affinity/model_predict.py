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
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from next_ads.ranking.theme_affinity.clean_output import stage_model_output
from next_ads.ranking.theme_affinity.config import resolve_context_runtime
from next_ads.ranking.theme_affinity.predict import build_predictions


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
MODEL_URI = jobparser.get_arg("--model_uri")
RUN_DATE = jobparser.get_arg("--run_date")
INPUT_SNAPSHOT_ID = jobparser.get_arg("--input_snapshot_id")
PROVIDER_BUILD_ID = jobparser.get_arg("--provider_build_id")
PROVIDER_BUILD_ATTEMPT_ID = jobparser.get_arg("--provider_build_attempt_id")
CONTEXT_SLOT = jobparser.get_arg("--context_slot")
GIT_COMMIT = jobparser.get_arg("--git_commit")

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
runtime, _context = resolve_context_runtime(
    spark,
    job_env=JOB_ENV,
    client=CLIENT,
    context_slot=CONTEXT_SLOT,
    expected_run_date=RUN_DATE,
    expected_input_snapshot_id=INPUT_SNAPSHOT_ID,
    expected_provider_build_id=PROVIDER_BUILD_ID,
    expected_provider_build_attempt_id=PROVIDER_BUILD_ATTEMPT_ID,
    git_commit=GIT_COMMIT,
)
if runtime.model_uri != MODEL_URI:
    raise ValueError("Model URI does not match the active provider context")

logger.info("Running Theme Affinity prediction into %s", runtime.namespace)
predictions = build_predictions(spark, runtime)
receipt = stage_model_output(spark, runtime, predictions)
get_dbutils().jobs.taskValues.set(
    key="provider_signals_delta_version",
    value=receipt.delta_version,
)
logger.info(
    "Staged canonical provider signals at Delta version %s",
    receipt.delta_version,
)
