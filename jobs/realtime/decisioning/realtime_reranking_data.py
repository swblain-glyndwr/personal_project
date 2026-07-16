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
    create_realtime_known_reranking_weighting_rules
)


def main(
    JOB_ENV: str,
    CLIENT: str,
    LOG_LEVEL: str,
    reference_date: str = None,
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
    create_realtime_known_reranking_weighting_rules(spark, reference_date, RERANKING_RULES )

    

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
        history_data_weighting=float(
            jobparser.get_arg("--history-data-weighting")
        ),
        lift_threshold=float(jobparser.get_arg("--lift-threshold")),
        ad_perc_coverage_threshold=float(
            jobparser.get_arg("--ad-coverage-threshold")
        ),
    )
