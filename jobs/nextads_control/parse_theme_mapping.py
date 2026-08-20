import sys
from datetime import date
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]  # noqa: F821
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

from dsutils import gcp
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import functions as F

from next_ads.control.theme_mapping import (
    build_item_themes,
    build_theme_attributes,
    collect_invalid_theme_ranks,
    filter_valid_theme_ranks,
    normalise_theme_mapping,
    rank_item_themes,
    valid_theme_rank_condition,
)
from next_ads.common import config_manager, etl
from next_ads.common.paths import load_client_config
from next_ads.common.snapshot_writes import (
    capture_run_date,
    publish_history_and_latest,
)


def write_theme_mapping_tables(
    df_theme_mapping,
    history_table,
    latest_table,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_theme_mapping,
        history_table=history_table,
        latest_table=latest_table,
        key_columns=["Theme", "attribute", "value"],
        run_date=run_date,
        columns=["Theme", "attribute", "value", "rundate"],
    )


def write_item_theme_tables(
    df_item_themes,
    history_table,
    latest_table,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_item_themes.select("pid", "theme", "theme_rank"),
        history_table=history_table,
        latest_table=latest_table,
        key_columns=["pid", "theme"],
        run_date=run_date,
        columns=["pid", "theme", "theme_rank", "rundate"],
    )


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False

    normalised = str(value).strip().lower()
    if normalised in {"true", "1", "yes", "y"}:
        return True
    if normalised in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def read_landed_theme_mapping(
    spark,
    *,
    table,
    landing_id,
    mapping_version,
    run_date,
    mapping_columns,
):
    mapping_reader = spark.read
    if mapping_version is not None:
        mapping_reader = mapping_reader.option(
            "versionAsOf",
            int(mapping_version),
        )
    landed_mapping = mapping_reader.table(table).where(
        (F.col("LandingID") == landing_id)
        & (F.col("SourceRole") == "authoritative_v2")
    )
    invalid_landing_date = landed_mapping.where(
        F.col("RunDate").isNull()
        | (F.col("RunDate") != F.lit(run_date))
    ).limit(1)
    if invalid_landing_date.count():
        raise ValueError("Theme Mapping landing has the wrong logical RunDate")
    df_themes = landed_mapping.select(*mapping_columns)
    if df_themes.limit(1).count() == 0:
        raise ValueError(f"Theme Mapping landing {landing_id} is empty")
    return df_themes


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    REFRESH_THEMES_DATE,
    THEME_RANKING_MODE=None,
    REFRESH_THEME_MAPPING=False,
    RUN_DATE=None,
    THEME_MAPPING_CONFIG="theme_mapping",
    THEME_MAPPING_TABLE=None,
    THEME_MAPPING_LANDING_ID=None,
    THEME_MAPPING_VERSION=None,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    run_date = date.fromisoformat(RUN_DATE) if RUN_DATE else capture_run_date(spark)
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    logger.info(f"Configuring run for client: {CLIENT}")
    cfg = load_client_config(CLIENT)

    today = run_date.isoformat()
    set_theme_attributes = (
        REFRESH_THEME_MAPPING or REFRESH_THEMES_DATE == today
    )
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
    theme_mapping_latest = etl.map_tbl(
        tbls["theme_mapping_latest"], **tbl_args
    )
    item_attributes_latest = etl.map_tbl(
        tbls["item_attributes_latest"],
        **tbl_args,
    )
    item_themes_latest = etl.map_tbl(tbls["item_themes_latest"], **tbl_args)
    item_themes = etl.map_tbl(tbls["item_themes"], **tbl_args)

    webhook_url = cfg["webhooks"]["DS Warnings"]

    if THEME_MAPPING_CONFIG not in {"theme_mapping", "theme_mapping_v2"}:
        raise ValueError("Theme Mapping config must be theme_mapping or theme_mapping_v2")
    mapping_config = cfg[THEME_MAPPING_CONFIG]
    if THEME_MAPPING_CONFIG == "theme_mapping_v2" and not mapping_config.get(
        "source_of_truth"
    ):
        raise ValueError("theme_mapping_v2 must be marked as source_of_truth")
    logger.info(
        "Parsing authoritative theme mapping from control sheet tab: "
        f"{mapping_config['sheet']}"
    )
    if bool(THEME_MAPPING_TABLE) != bool(THEME_MAPPING_LANDING_ID):
        raise ValueError(
            "Theme Mapping table and landing ID must be supplied together"
        )
    if THEME_MAPPING_TABLE:
        mapping_columns = [
            column[0] for column in mapping_config["read_schema"]
        ]
        df_themes = read_landed_theme_mapping(
            spark,
            table=THEME_MAPPING_TABLE,
            landing_id=THEME_MAPPING_LANDING_ID,
            mapping_version=THEME_MAPPING_VERSION,
            run_date=run_date,
            mapping_columns=mapping_columns,
        )
    else:
        df_themes = gcp.spark_df_from_sheets(
            url=mapping_config["url"],
            worksheet_name=mapping_config["sheet"],
            gcp_scope=cfg["gcp"]["scope"],
            gcp_key=cfg["gcp"]["key"],
            schema=mapping_config["read_schema"],
        )
    df_themes = normalise_theme_mapping(df_themes)

    invalid_theme_count = df_themes.filter(
        ~valid_theme_rank_condition()
    ).count()
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
        if REFRESH_THEME_MAPPING:
            logger.info("REFRESH_THEME_MAPPING flag set")
        else:
            logger.info(f"REFRESH_THEMES_DATE matches today ({today})")
        logger.info("Setting theme-to-attribute mapping")
        theme_attributes = build_theme_attributes(df_themes)

        n_themes = theme_attributes.select("Theme").distinct().count()
        n_rows = theme_attributes.count()
        logger.info(f"Parsed {n_themes:,} themes ({n_rows:,} rows)")

        logger.info("Writing theme mapping to history and latest tables")
        write_theme_mapping_tables(
            theme_attributes,
            theme_mapping,
            theme_mapping_latest,
            spark=spark,
            run_date=run_date,
        )

    if not set_theme_attributes:
        logger.info("Reading existing theme mapping for item-theme mapping")
        theme_attributes = spark.table(theme_mapping_latest)
    else:
        logger.info(
            "Using newly refreshed theme mapping for item-theme mapping"
        )

    item_attributes = spark.table(item_attributes_latest)
    item_themes_ranked = rank_item_themes(
        build_item_themes(item_attributes, theme_attributes),
        df_themes,
        THEME_RANKING_MODE,
    )

    logger.info("Writing item-theme mapping to history and latest tables")
    write_item_theme_tables(
        item_themes_ranked,
        item_themes,
        item_themes_latest,
        spark=spark,
        run_date=run_date,
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
        "REFRESH_THEME_MAPPING": parse_bool(
            jobparser.get_arg("--refresh_theme_mapping")
        ),
        "RUN_DATE": jobparser.get_arg("--run_date"),
        "THEME_MAPPING_CONFIG": (
            jobparser.get_arg("--theme_mapping_config") or "theme_mapping"
        ),
        "THEME_MAPPING_TABLE": jobparser.get_arg("--theme_mapping_table"),
        "THEME_MAPPING_LANDING_ID": jobparser.get_arg(
            "--theme_mapping_landing_id"
        ),
        "THEME_MAPPING_VERSION": jobparser.get_arg(
            "--theme_mapping_version"
        ),
    }


if __name__ == "__main__":
    main(**parse_args())
