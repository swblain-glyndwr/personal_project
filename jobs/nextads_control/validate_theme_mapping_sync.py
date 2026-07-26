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
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils import gcp
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger

from next_ads.common.paths import load_client_config
from next_ads.control.theme_mapping_sync import build_theme_mapping_differences


def main(JOB_ENV, CLIENT, LOG_LEVEL, WARN_ONLY=False):
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    configure_spark()
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    cfg = load_client_config(CLIENT)
    if "theme_mapping_v2" not in cfg:
        raise KeyError(
            "Client config must define theme_mapping_v2 for v1/v2 sync validation"
        )

    logger.info(
        "Comparing copied v1 Theme Mapping against v2 source Theme Mapping"
    )
    v1_theme_mapping = gcp.spark_df_from_sheets(
        url=cfg["theme_mapping"]["url"],
        worksheet_name=cfg["theme_mapping"]["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=cfg["theme_mapping"]["read_schema"],
    )
    v2_theme_mapping = gcp.spark_df_from_sheets(
        url=cfg["theme_mapping_v2"]["url"],
        worksheet_name=cfg["theme_mapping_v2"]["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=cfg["theme_mapping_v2"]["read_schema"],
    )

    differences = build_theme_mapping_differences(v1_theme_mapping, v2_theme_mapping)
    difference_count = differences.count()
    if difference_count == 0:
        logger.info("Theme Mapping tabs match")
        return

    differences.show(200, truncate=False)
    message = (
        f"Theme Mapping sync validation found {difference_count:,} row "
        "differences between the v2 source tab and the copied v1 tab. "
        "Run or check the Google Sheets Apps Script copy before candidate build."
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
