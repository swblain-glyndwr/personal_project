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
from next_ads.control.theme_mapping import normalise_theme_mapping
from next_ads.control.theme_mapping_compare import build_theme_mapping_differences


def load_theme_mapping_sheet(cfg: dict, source_key: str):
    return normalise_theme_mapping(
        gcp.spark_df_from_sheets(
            url=cfg[source_key]["url"],
            worksheet_name=cfg[source_key]["sheet"],
            gcp_scope=cfg["gcp"]["scope"],
            gcp_key=cfg["gcp"]["key"],
            schema=cfg[source_key]["read_schema"],
        )
    )


def main(JOB_ENV, CLIENT, LOG_LEVEL, FAIL_ON_DIFFERENCES=False):
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    configure_spark()

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    cfg = load_client_config(CLIENT)
    v1_mapping = load_theme_mapping_sheet(cfg, "theme_mapping")
    v2_mapping = load_theme_mapping_sheet(cfg, "theme_mapping_v2")
    differences = build_theme_mapping_differences(v1_mapping, v2_mapping)
    difference_count = differences.count()

    if difference_count == 0:
        logger.info("No v1/v2 Theme Mapping differences found")
        return

    sample_rows = differences.orderBy("difference_type", "Theme").limit(20).collect()
    sample_text = "; ".join(
        [
            f"{row['difference_type']}: {row['Theme']}"
            for row in sample_rows
        ]
    )
    message = (
        f"{difference_count:,} v1/v2 Theme Mapping row differences found for "
        f"{CLIENT}. Trade should review before relying on v2 assignments. "
        f"Sample: {sample_text}"
    )
    logger.warning(message)

    webhook_url = cfg.get("webhooks", {}).get("DS Warnings")
    if webhook_url:
        if JOB_ENV.lower() == "prod":
            post_to_webhook(webhook_url, message)
        else:
            logger.info("Skipping webhook post outside prod")

    if FAIL_ON_DIFFERENCES:
        raise ValueError(message)


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "FAIL_ON_DIFFERENCES": jobparser.has_arg("--fail-on-differences"),
    }


if __name__ == "__main__":
    main(**parse_args())
