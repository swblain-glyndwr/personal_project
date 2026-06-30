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

from next_ads.ranking.theme_affinity.publish_outputs import (
    parse_table_suffixes,
    publish_theme_affinity_outputs,
)


jobparser = get_job_parser()
jobparser._parse_args()
SOURCE_NAMESPACE = jobparser.get_arg("--source_namespace")
TARGET_NAMESPACE = jobparser.get_arg("--target_namespace")
TABLE_PREFIX = (
    jobparser.get_arg("--table_prefix")
    or "next_uk_nextads_theme_affinity_predict"
)
TARGET_TABLE_PREFIX = jobparser.get_arg("--target_table_prefix") or TABLE_PREFIX
TABLE_SUFFIXES = parse_table_suffixes(jobparser.get_arg("--table_suffixes"))
LOG_LEVEL = jobparser.get_arg("--log_level")

configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()

logger.info(
    "Publishing Theme Affinity outputs from %s to %s: %s",
    SOURCE_NAMESPACE,
    TARGET_NAMESPACE,
    ",".join(TABLE_SUFFIXES),
)
published_tables = publish_theme_affinity_outputs(
    spark,
    source_namespace=SOURCE_NAMESPACE,
    target_namespace=TARGET_NAMESPACE,
    table_prefix=TABLE_PREFIX,
    target_table_prefix=TARGET_TABLE_PREFIX,
    table_suffixes=TABLE_SUFFIXES,
)
if published_tables:
    logger.info("Published Theme Affinity output tables: %s", published_tables)
else:
    logger.info(
        "Theme Affinity publish skipped; source and target table paths match"
    )
