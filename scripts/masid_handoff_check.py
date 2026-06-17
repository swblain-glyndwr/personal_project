import json
import sys
from pathlib import Path

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from next_ads.delivery.masid_handoff import (  # noqa: E402
    check_masid_handoff_table,
    expected_rundate,
    resolve_assignments_latest_table,
)
from next_ads.utils import config_manager  # noqa: E402


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
EXPECTED_RUNDATE = expected_rundate(jobparser.get_arg("--expected_rundate"))

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

config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring MASID handoff check for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

assignments_latest = resolve_assignments_latest_table(config, cfg, CLIENT)

logger.info(f"Checking MASID handoff table: {assignments_latest}")
check_masid_handoff_table(
    spark=spark,
    assignments_latest=assignments_latest,
    expected_run_date=EXPECTED_RUNDATE,
    logger=logger,
)
