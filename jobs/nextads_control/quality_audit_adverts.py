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
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

import pyspark.sql.functions as F
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from datetime import date
from next_ads.common import config_manager
from next_ads.control.advert_quality_audit import AdvertQualityAudit
from next_ads.common.snapshot_writes import (
    publish_history_and_latest,
)


def main(
    JOB_ENV: str,
    CLIENT: str,
    ROUTE: str,
    RUN_DATE: str,
    LOG_LEVEL: str,
    ITEM_NUM_THRESHOLD: int = 10,
    ITEM_COVERAGE: float = 0.75,
    THEME_COVERAGE: float = 0.5,
    IMAGE_ITEM_COVERAGE: float = 0.7,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)

    if not CLIENT:
        if JOB_ENV.lower() != "dev":
            raise ValueError(
                f"Client must be specified when running in {JOB_ENV}"
            )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    route = ROUTE.lower()
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    spark = configure_spark()

    if RUN_DATE:
        try:
            F.lit(RUN_DATE).cast("date")
        except Exception as e:
            logger.warning(
                f"Provided reference date could not be parsed using current date : {e}"
            )
            RUN_DATE = date.today().strftime("%Y-%m-%d")
    else:
        logger.warning("No reference date provided using current date")
        RUN_DATE = date.today().strftime("%Y-%m-%d")

    logger.info(
        "Auditing %s adverts data for client=%s, environment=%s, run_date=%s",
        route,
        CLIENT,
        JOB_ENV,
        RUN_DATE,
    )

    adqualityaudit = AdvertQualityAudit(
        RUN_DATE,
        ROUTE,
        ITEM_NUM_THRESHOLD,
        ITEM_COVERAGE,
        THEME_COVERAGE,
        IMAGE_ITEM_COVERAGE,
    )
    adqualityaudit.resolve_quality_audit_tables(config)
    adqualityaudit.validate_audit_tables_date(spark)
    adqualityaudit.run_all_validation_checks(spark)
    results = adqualityaudit.format_validation_results(spark)

    publish_history_and_latest(
        spark,
        results,
        history_table=config.tables_write.advert_quality_metrics,
        latest_table=config.tables_write.advert_quality_metrics_latest,
        key_columns=["UniqueAdID", "rundate"],
        run_date=RUN_DATE,
        columns=[*results.columns, "rundate"],
    )

    logger.info(
        "Audit Completed for  %s adverts,  run_date=%s", route, RUN_DATE
    )


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "ROUTE": jobparser.get_arg("--route"),
        "RUN_DATE": jobparser.get_arg("--run_date"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "ITEM_NUM_THRESHOLD": jobparser.get_arg("--item-num-threshold"),
        "ITEM_COVERAGE": jobparser.get_arg("--item-coverage"),
        "THEME_COVERAGE": jobparser.get_arg("--theme-coverage"),
        "IMAGE_ITEM_COVERAGE": jobparser.get_arg("--image-item-coverage"),
    }


if __name__ == "__main__":
    main(**parse_args())
