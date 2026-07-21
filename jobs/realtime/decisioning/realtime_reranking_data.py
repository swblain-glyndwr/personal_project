import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[3]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from next_ads.utils import config_manager, etl
from next_ads.common.paths import load_client_config


from next_ads.realtime.decisioning.reranking_data_build import (
    create_realtime_known_reranking_weighting_rules,
    create_central_product_details_by_pid,
    advert_details_build,
    realtime_reranking_preranked_ads_build,
    realtime_reranking_item_weights_build,
)


def main(
    JOB_ENV: str,
    CLIENT: str,
    LOG_LEVEL: str,
    reference_date: str = None,
    advert_matching_threshold: float = 0.1,
):
    from pyspark.sql import functions as F
    from datetime import date

    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    # load configuration
    config = config_manager.load_config(JOB_ENV)
    logger.info(f"Configuring run for client: {CLIENT}")
    cfg = load_client_config(CLIENT)
    SCHEMA = config.schema_write
    CATALOG = config.catalog_write
    logger.info(f"Write schema set to {SCHEMA}")
    logger.info(f"Write catalog set to {CATALOG}")
    tbl_args = {"catalog": CATALOG, "schema": SCHEMA, "client": CLIENT}
    tbls = cfg["tables"]["write"]

    RERANKING_RULES = etl.map_tbl(
        tbls["nextads_realtime_reranking_rules_weighting"], **tbl_args
    )
    REALTIME_PRODUCT_FEATURES = etl.map_tbl(
        tbls["nextads_realtime_reranking_product_features"], **tbl_args
    )
    REALTIME_ADVERT_FEATURES = etl.map_tbl(
        tbls["nextads_realtime_reranking_advert_features"], **tbl_args
    )

    REALTIME_PRERANKED_CUSTOMER_AD_FEATURES = etl.map_tbl(
        tbls["nextads_realtime_reranking_preranked_ads"], **tbl_args
    )
    REALTIME_ITEM_WEIGHTING_RULES = etl.map_tbl(
        tbls["nextads_realtime_reranking_item_weighting_rules"], **tbl_args
    )

    if reference_date:
        try:
            F.lit(reference_date).cast("date")
        except Exception as e:
            logger.warning(
                f"Provided reference date could not be parsed using current date : {e}"
            )
            reference_date = date.today().strftime("%Y-%m-%d")
    else:
        logger.warning("No reference date provided using current date")
        reference_date = date.today().strftime("%Y-%m-%d")

    logger.info(f"Running for date: {reference_date}")

    logger.info("Loading realtime known reranking rules")
    create_realtime_known_reranking_weighting_rules(
        spark, reference_date, RERANKING_RULES
    )

    logger.info("Generating unique product features for realtime reranking")
    create_central_product_details_by_pid(
        spark, config, cfg, reference_date, REALTIME_PRODUCT_FEATURES
    )

    logger.info("Generating unique advert features for realtime reranking")

    advert_details_build(
        spark,
        cfg,
        tbl_args,
        reference_date,
        REALTIME_ADVERT_FEATURES,
        coverage_min_threshold=advert_matching_threshold,
    )
    logger.info(
        "Generating preranked adverts, with RPIDs & features for realtime reranking"
    )
    realtime_reranking_preranked_ads_build(
        spark,
        cfg,
        tbl_args,
        reference_date,
        REALTIME_PRERANKED_CUSTOMER_AD_FEATURES,
    )

    logger.info(
        "Generating items & weighting rules combined data view for realtime reranking"
    )
    realtime_reranking_item_weights_build(
        spark, cfg, tbl_args, reference_date, REALTIME_ITEM_WEIGHTING_RULES
    )

    logger.info("Realtime reranking data build complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(
        JOB_ENV,
        CLIENT,
        LOG_LEVEL,
        reference_date=jobparser.get_arg("--reference-date"),
        advert_matching_threshold=jobparser.get_arg(
            "--advert-matching-threshold"
        ),
    )
