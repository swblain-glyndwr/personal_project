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

from next_ads.ranking.theme_affinity.config import resolve_runtime
from next_ads.ranking.theme_affinity.sense_check import (
    SenseCheckConfig,
    default_summary_table,
    run_sense_checks,
)


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
BASELINE_NAMESPACE = (
    jobparser.get_arg("--baseline_intermediate_namespace")
    or "marketingdata_prod.ds_sandbox"
)
BASELINE_PREFIX = (
    jobparser.get_arg("--baseline_intermediate_prefix")
    or "next_uk_nextAds_predict_prod"
)
BASELINE_FINAL_TABLE = (
    jobparser.get_arg("--baseline_final_table")
    or "marketingdata_prod.ds_sandbox.next_uk_next_ads_hackathon_model_full"
)
CANDIDATE_NAMESPACE = jobparser.get_arg("--candidate_intermediate_namespace")
SUMMARY_TABLE = jobparser.get_arg("--summary_table")
CHECK_SCOPE = jobparser.get_arg("--check_scope") or "all"

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
runtime = resolve_runtime(JOB_ENV, CLIENT)
summary_table = SUMMARY_TABLE or default_summary_table(runtime)

logger.info("Running Theme Affinity sense checks into %s", summary_table)
summary_df = run_sense_checks(
    spark,
    runtime,
    SenseCheckConfig(
        baseline_intermediate_namespace=BASELINE_NAMESPACE,
        baseline_intermediate_prefix=BASELINE_PREFIX,
        baseline_final_table=BASELINE_FINAL_TABLE,
        summary_table=summary_table,
        check_scope=CHECK_SCOPE,
        candidate_intermediate_namespace=CANDIDATE_NAMESPACE,
    ),
)

summary_df.orderBy("check_name").show(200, truncate=False)
