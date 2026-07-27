import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
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
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger

from next_ads.common.paths import load_client_config
from next_ads.ranking.theme_coverage import (
    build_missing_theme_affinity_coverage,
)
from next_ads.common import config_manager, etl


def main(JOB_ENV, CLIENT, LOG_LEVEL, WARN_ONLY=False):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    cfg = load_client_config(CLIENT)
    tbl_args = {
        "catalog": config.catalog_write,
        "schema": config.schema_write,
        "client": CLIENT,
    }

    control_sheet_latest_v1 = etl.map_tbl(
        cfg["tables"]["write"]["control_sheet_latest"],
        **tbl_args,
    )
    control_sheet_latest_v2 = config.tables_write.control_sheet_latest_v2
    theme_affinity_latest = config.theme_affinity_assignment_sources.champion

    logger.info(
        "Checking v1/v2 ad Themes against shared Theme Affinity output: "
        f"{theme_affinity_latest}"
    )
    theme_affinity_scores = spark.table(theme_affinity_latest)
    missing_v1 = build_missing_theme_affinity_coverage(
        spark.table(control_sheet_latest_v1),
        theme_affinity_scores,
        route="v1",
    )
    missing_v2 = build_missing_theme_affinity_coverage(
        spark.table(control_sheet_latest_v2),
        theme_affinity_scores,
        route="v2",
    )
    missing = missing_v1.unionByName(missing_v2)

    missing_count = missing.count()
    if missing_count == 0:
        logger.info("All route ad Themes are present in Theme Affinity output")
        return

    missing.orderBy("route", "Theme").show(200, truncate=False)
    message = (
        f"Theme Affinity coverage validation found {missing_count:,} route "
        "themes with no matching NextTheme in the shared customer-theme output. "
        "Those ads cannot receive customer-ad scores through theme matching."
    )
    logger.warning(message)
    if JOB_ENV.lower() == "prod":
        post_to_webhook(cfg["webhooks"]["DS Warnings"], message)
    if not WARN_ONLY:
        raise ValueError(message)


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "WARN_ONLY": jobparser.has_arg("--warn-only"),
    }


if __name__ == "__main__":
    main(**parse_args())
