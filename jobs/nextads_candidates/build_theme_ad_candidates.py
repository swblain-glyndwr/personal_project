import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from next_ads.ranking.theme_score_mapping import run_theme_score_mapping
from next_ads.candidates.foundation import load_candidate_foundation_inputs
from next_ads.common import config_manager
from next_ads.common.paths import load_client_config


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
RUN_DATE = jobparser.get_arg("--run_date")
PROVIDER_BUILD_ID = jobparser.get_arg("--provider_build_id")
PROVIDER_SIGNALS_TABLE = jobparser.get_arg("--provider_signals_table")
PROVIDER_SIGNALS_DELTA_VERSION = jobparser.get_arg(
    "--provider_signals_delta_version"
)
PROVIDER_SOURCE_RUN_DATE = jobparser.get_arg("--provider_source_run_date")
FOUNDATION_SNAPSHOT_ID = jobparser.get_arg("--foundation_snapshot_id")
FOUNDATION_SOURCE_RUN_DATE = jobparser.get_arg("--foundation_source_run_date")
CUSTOMER_CELLS_TABLE = jobparser.get_arg("--customer_cells_table")
CUSTOMER_CELLS_DELTA_VERSION = jobparser.get_arg(
    "--customer_cells_delta_version"
)
REPEAT_AD_EXPOSURE_TABLE = jobparser.get_arg("--repeat_ad_exposure_table")
REPEAT_AD_EXPOSURE_DELTA_VERSION = jobparser.get_arg(
    "--repeat_ad_exposure_delta_version"
)
AD_FEEDBACK_TABLE = jobparser.get_arg("--ad_feedback_table")
AD_FEEDBACK_DELTA_VERSION = jobparser.get_arg("--ad_feedback_delta_version")
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

config = config_manager.load_config(JOB_ENV, client=CLIENT)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

if not RUN_DATE:
    raise ValueError("--run_date is required")
if not PROVIDER_BUILD_ID:
    raise ValueError("--provider_build_id is required")
if not PROVIDER_SIGNALS_TABLE:
    raise ValueError("--provider_signals_table is required")
if not PROVIDER_SOURCE_RUN_DATE:
    raise ValueError("--provider_source_run_date is required")
try:
    PROVIDER_SIGNALS_DELTA_VERSION = int(PROVIDER_SIGNALS_DELTA_VERSION)
except (TypeError, ValueError) as exc:
    raise ValueError(
        "--provider_signals_delta_version must be an integer"
    ) from exc

foundation_values = {
    "snapshot_id": FOUNDATION_SNAPSHOT_ID,
    "source_run_date": FOUNDATION_SOURCE_RUN_DATE,
    "customer_cells_table": CUSTOMER_CELLS_TABLE,
    "repeat_ad_exposure_table": REPEAT_AD_EXPOSURE_TABLE,
    "ad_feedback_table": AD_FEEDBACK_TABLE,
}
missing_foundation = [
    name for name, value in foundation_values.items() if not value
]
if missing_foundation:
    raise ValueError(
        "Missing candidate foundation values: "
        + ", ".join(missing_foundation)
    )
try:
    foundation_inputs = load_candidate_foundation_inputs(
        spark,
        **foundation_values,
        customer_cells_delta_version=int(CUSTOMER_CELLS_DELTA_VERSION),
        repeat_ad_exposure_delta_version=int(
            REPEAT_AD_EXPOSURE_DELTA_VERSION
        ),
        ad_feedback_delta_version=int(AD_FEEDBACK_DELTA_VERSION),
    )
except (TypeError, ValueError) as exc:
    raise ValueError("Candidate foundation bindings are invalid") from exc

run_theme_score_mapping(
    spark=spark,
    config=config,
    cfg=cfg,
    client=CLIENT,
    job_env=JOB_ENV,
    run_date=RUN_DATE,
    provider_build_id=PROVIDER_BUILD_ID,
    provider_signals_table=PROVIDER_SIGNALS_TABLE,
    provider_signals_delta_version=PROVIDER_SIGNALS_DELTA_VERSION,
    provider_source_run_date=PROVIDER_SOURCE_RUN_DATE,
    apply_ad_feedback=jobparser.has_arg("--apply-ad-feedback"),
    ad_feedback_weight=jobparser.get_arg("--ad-feedback-weight") or 0.05,
    top_ads_per_location=jobparser.get_arg("--top-ads-per-location") or 20,
    foundation_inputs=foundation_inputs,
    logger=logger,
)
