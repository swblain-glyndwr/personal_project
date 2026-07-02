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
from next_ads.ml.lifecycle.monitoring import log_table_drift_to_mlflow
from next_ads.ranking.theme_affinity.lifecycle_spec import resolve_lifecycle_config


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client") or "next_uk"
LOG_LEVEL = jobparser.get_arg("--log_level")
BASELINE_TABLE = jobparser.get_arg("--baseline_table")
CANDIDATE_TABLE = jobparser.get_arg("--candidate_table")
SAMPLE_LIMIT = jobparser.get_arg("--sample_limit")

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
config = config_manager.load_config(JOB_ENV)
lifecycle_config = resolve_lifecycle_config(config)

baseline_table = BASELINE_TABLE or lifecycle_config.train_table
candidate_table = CANDIDATE_TABLE or config.ranking_model_tables.predict_input_table
sample_limit = (
    int(SAMPLE_LIMIT) if SAMPLE_LIMIT else lifecycle_config.monitoring_sample_limit
)

import mlflow

logger.info(
    "Monitoring Theme Affinity model drift for %s/%s from %s to %s",
    JOB_ENV,
    CLIENT,
    baseline_table,
    candidate_table,
)
result = log_table_drift_to_mlflow(
    spark=spark,
    mlflow_module=mlflow,
    experiment_path=lifecycle_config.experiment_path,
    baseline_table=baseline_table,
    candidate_table=candidate_table,
    feature_cols=list(lifecycle_config.feature_cols),
    categorical_cols=list(lifecycle_config.categorical_cols),
    prediction_col=lifecycle_config.prediction_col,
    sample_limit=sample_limit,
    thresholds=lifecycle_config.drift_thresholds,
    tags={
        "client": CLIENT,
        "job_env": lifecycle_config.job_env,
        "model_name": lifecycle_config.registered_model_name,
    },
)
logger.info(
    "Logged drift monitor run %s with status %s",
    result["run_id"],
    result["assessment"].status,
)
