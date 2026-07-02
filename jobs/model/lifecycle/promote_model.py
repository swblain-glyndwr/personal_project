import logging
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
from dsutils.logtools import configure_logging, get_logger

from next_ads.ml.lifecycle import (
    configure_mlflow,
    copy_model_version_to_registered_model,
    set_model_alias,
)


jobparser = get_job_parser()
jobparser._parse_args()
LOG_LEVEL = jobparser.get_arg("--log_level")
SOURCE_MODEL_NAME = jobparser.get_arg("--source_model_name")
SOURCE_MODEL_VERSION = jobparser.get_arg("--source_model_version")
SOURCE_ALIAS = jobparser.get_arg("--source_alias")
TARGET_MODEL_NAME = jobparser.get_arg("--target_model_name")
TARGET_ALIAS = jobparser.get_arg("--target_alias")
SOURCE_ENVIRONMENT = jobparser.get_arg("--source_environment")
TARGET_ENVIRONMENT = jobparser.get_arg("--target_environment")
CLIENT = jobparser.get_arg("--client")
MODEL_FAMILY = jobparser.get_arg("--model_family")
ALLOWED_SOURCE_MODEL_PREFIX = jobparser.get_arg("--allowed_source_model_prefix")
ALLOWED_TARGET_MODEL_PREFIX = jobparser.get_arg("--allowed_target_model_prefix")

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)

if not SOURCE_MODEL_NAME:
    raise ValueError("source_model_name must be provided")
if not SOURCE_MODEL_VERSION:
    raise ValueError("source_model_version must be provided")
if not TARGET_MODEL_NAME:
    raise ValueError("target_model_name must be provided")
if not TARGET_ALIAS:
    raise ValueError("target_alias must be provided")
if ALLOWED_SOURCE_MODEL_PREFIX and not SOURCE_MODEL_NAME.startswith(
    ALLOWED_SOURCE_MODEL_PREFIX
):
    raise ValueError(
        f"source_model_name must start with {ALLOWED_SOURCE_MODEL_PREFIX}"
    )
if ALLOWED_TARGET_MODEL_PREFIX and not TARGET_MODEL_NAME.startswith(
    ALLOWED_TARGET_MODEL_PREFIX
):
    raise ValueError(
        f"target_model_name must start with {ALLOWED_TARGET_MODEL_PREFIX}"
    )

import mlflow

configure_mlflow(mlflow)
client = mlflow.tracking.MlflowClient()

if SOURCE_ALIAS:
    logger.info(
        "Setting source alias %s on %s version %s",
        SOURCE_ALIAS,
        SOURCE_MODEL_NAME,
        SOURCE_MODEL_VERSION,
    )
    set_model_alias(
        client,
        SOURCE_MODEL_NAME,
        SOURCE_MODEL_VERSION,
        SOURCE_ALIAS,
    )

logger.info(
    "Promoting model version from %s/%s to %s as alias %s",
    SOURCE_MODEL_NAME,
    SOURCE_MODEL_VERSION,
    TARGET_MODEL_NAME,
    TARGET_ALIAS,
)
registered_model = copy_model_version_to_registered_model(
    mlflow,
    SOURCE_MODEL_NAME,
    SOURCE_MODEL_VERSION,
    TARGET_MODEL_NAME,
    TARGET_ALIAS,
)

for key, value in {
    "source_environment": SOURCE_ENVIRONMENT,
    "target_environment": TARGET_ENVIRONMENT,
    "client": CLIENT,
    "model_family": MODEL_FAMILY,
}.items():
    if value:
        client.set_model_version_tag(
            name=TARGET_MODEL_NAME,
            version=str(registered_model.version),
            key=key,
            value=value,
        )

logger.info(
    "Promoted %s version %s into %s version %s as alias %s",
    SOURCE_MODEL_NAME,
    SOURCE_MODEL_VERSION,
    TARGET_MODEL_NAME,
    registered_model.version,
    TARGET_ALIAS,
)
