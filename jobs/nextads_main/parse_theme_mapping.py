import sys
from datetime import date
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
from dsutils.etl import delete_from_and_load, post_to_webhook, truncate_and_load
from dsutils.logtools import configure_logging, get_logger

from next_ads.control.theme_mapping import (
    build_item_themes,
    build_theme_attributes,
    collect_invalid_theme_ranks,
    filter_valid_theme_ranks,
    normalise_theme_mapping,
    rank_item_themes,
    valid_theme_rank_condition,
)
from next_ads.utils import config_manager, etl
from next_ads.common.paths import load_client_config


def main(JOB_ENV, CLIENT, LOG_LEVEL, REFRESH_THEMES_DATE, THEME_RANKING_MODE=None):
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
    logger.info(f"Configuring run for client: {CLIENT}")
    cfg = load_client_config(CLIENT)

    today = date.today().strftime(format="%Y-%m-%d")
    set_theme_attributes = REFRESH_THEMES_DATE == today or False
    if not THEME_RANKING_MODE:
        THEME_RANKING_MODE = "adtype-themetype"
        logger.info(
            "THEME_RANKING_MODE not specified, defaulting to: "
            f"{THEME_RANKING_MODE}"
        )

    tbls = cfg["tables"]["write"]
    schema = config.schema_write
    logger.info(f"Write schema set to {schema}")

    tbl_args = {
        "catalog": config.catalog_write,
        "schema": schema,
        "client": CLIENT,
    }
    theme_mapping = etl.map_tbl(tbls["theme_mapping"], **tbl_args)
    theme_mapping_latest = etl.map_tbl(tbls["theme_mapping_latest"], **tbl_args)
    item_attributes_latest = etl.map_tbl(
        tbls["item_attributes_latest"],
        **tbl_args,
    )
    item_themes_latest = etl.map_tbl(tbls["item_themes_latest"], **tbl_args)
    item_themes = etl.map_tbl(tbls["item_themes"], **tbl_args)

    webhook_url = cfg["webhooks"]["DS Warnings"]

    logger.info(
        "Parsing theme mapping from control sheet tab: "
        f'{cfg["theme_mapping"]["sheet"]}'
    )
    df_themes = normalise_theme_mapping(
        gcp.spark_df_from_sheets(
            url=cfg["theme_mapping"]["url"],
            worksheet_name=cfg["theme_mapping"]["sheet"],
            gcp_scope=cfg["gcp"]["scope"],
            gcp_key=cfg["gcp"]["key"],
            schema=cfg["theme_mapping"]["read_schema"],
        )
    )

    invalid_theme_count = df_themes.filter(~valid_theme_rank_condition()).count()
    if invalid_theme_count > 0:
        invalid_themes = collect_invalid_theme_ranks(df_themes)
        msg_invalid_ranks = (
            f"Filtering out {invalid_theme_count:,} "
            "themes with invalid ThemeTypeRank or AdTypeRank: "
            + ", ".join(invalid_themes)
            + " (ranks must be positive integers)"
        )
        logger.warning(msg_invalid_ranks)
        if JOB_ENV == "prod":
            post_to_webhook(webhook_url, msg_invalid_ranks)

    df_themes = filter_valid_theme_ranks(df_themes)

    if set_theme_attributes:
        logger.info(f"REFRESH_THEMES_DATE matches today ({today})")
        logger.info("Setting theme-to-attribute mapping")
        theme_attributes = build_theme_attributes(df_themes)

        n_themes = theme_attributes.select("Theme").distinct().count()
        n_rows = theme_attributes.count()
        logger.info(f"Parsed {n_themes:,} themes ({n_rows:,} rows)")

        logger.info("Writing theme mapping to output tables")
        truncate_and_load(
            theme_attributes,
            theme_mapping_latest,
            pk_cols=["Theme", "attribute", "value"],
        )

        delete_from_and_load(
            theme_attributes,
            theme_mapping,
            pk_cols=["Theme", "attribute", "value"],
            del_where={"rundate": "current_date()"},
        )

    if not set_theme_attributes:
        logger.info("Reading existing theme mapping for item-theme mapping")
        theme_attributes = spark.table(theme_mapping_latest)
    else:
        logger.info("Using newly refreshed theme mapping for item-theme mapping")

    item_attributes = spark.table(item_attributes_latest)
    item_themes_ranked = rank_item_themes(
        build_item_themes(item_attributes, theme_attributes),
        df_themes,
        THEME_RANKING_MODE,
    )

    logger.info("Writing item-theme mapping to output tables")
    truncate_and_load(
        item_themes_ranked.select("pid", "theme", "theme_rank"),
        item_themes_latest,
        pk_cols=["pid", "theme"],
    )

    delete_from_and_load(
        item_themes_ranked.select("pid", "theme", "theme_rank"),
        item_themes,
        pk_cols=["pid", "theme"],
        del_where={"rundate": "current_date()"},
    )

    logger.info("Run complete")


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "REFRESH_THEMES_DATE": jobparser.get_arg("--refresh_themes_date"),
        "THEME_RANKING_MODE": jobparser.get_arg("--theme-ranking-mode"),
    }


if __name__ == "__main__":
    main(**parse_args())
