import json
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

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ranking.theme_affinity.gpu_xgboost_lifecycle import (
    train_and_register_gpu_xgboost_model,
)


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
INPUT_TABLE = jobparser.get_arg("--input_table")
FEATURE_COLS = jobparser.get_arg("--feature_cols")
PARAMS = jobparser.get_arg("--params")
NUM_BOOST_ROUND = jobparser.get_arg("--num_boost_round")
EARLY_STOPPING = jobparser.get_arg("--early_stopping")
ALIAS_SUFFIX = jobparser.get_arg("--alias_suffix") or "gpu_xgboost"

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
config = config_manager.load_config(JOB_ENV)

logger.info(
    "Training Theme Affinity GPU XGBoost MLflow model for %s/%s",
    JOB_ENV,
    CLIENT,
)
result = train_and_register_gpu_xgboost_model(
    spark,
    config,
    input_table=INPUT_TABLE,
    feature_cols=json.loads(FEATURE_COLS) if FEATURE_COLS else None,
    params=json.loads(PARAMS) if PARAMS else None,
    num_boost_round=int(NUM_BOOST_ROUND) if NUM_BOOST_ROUND else None,
    early_stopping_rounds=int(EARLY_STOPPING) if EARLY_STOPPING else None,
    alias_suffix=ALIAS_SUFFIX,
)
logger.info(
    "Registered %s version %s as alias %s from run %s",
    result["registered_model_name"],
    result["version"],
    result["alias"],
    result["run_id"],
)
