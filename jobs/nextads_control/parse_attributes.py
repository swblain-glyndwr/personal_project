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
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.control.item_attributes import (
    build_attribute_mappings,
    build_attribute_set,
    build_attributes_master,
    build_bigquery_item_attributes,
    build_item_attribute_catalog,
    build_recent_basket_items,
    build_recent_catalog,
)
from next_ads.common import config_manager, etl
from next_ads.common.output_locations import log_output_location
from next_ads.common.paths import load_client_config
from next_ads.common.snapshot_writes import (
    capture_run_date,
    publish_history_and_latest,
    replace_validated_snapshot,
    with_run_date,
)


def write_attribute_set_tables(
    df_attribute_set,
    history_table,
    latest_table,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_attribute_set,
        history_table=history_table,
        latest_table=latest_table,
        key_columns=["attribute", "value"],
        run_date=run_date,
        columns=["attribute", "value", "rundate"],
    )


def write_item_attributes_latest(
    df_attributes,
    table,
    *,
    spark,
    run_date,
):
    replace_validated_snapshot(
        spark,
        with_run_date(df_attributes, run_date),
        table=table,
        key_columns=["pid", "attribute", "value", "rundate"],
        columns=["pid", "attribute", "value", "rundate"],
    )


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    REFRESH_ATTRIBUTES_DATE,
    BQ_EXPORT=False,
    RUN_DATE=None,
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
    set_attributes = REFRESH_ATTRIBUTES_DATE == today or False

    tbls = cfg["tables"]["write"]
    schema = config.schema_write
    logger.info(f"Write schema set to {schema}")

    product_catalog = cfg["tables"]["read"]["product_catalog"]
    product_catalog_latest = cfg["tables"]["read"]["product_catalog_latest"]
    baskets = cfg["tables"]["read"]["baskets"]
    nov_scores_csv = cfg["attributes"]["nov_scores_csv"]
    bq_options = cfg["big_query"]

    tbl_args = {
        "catalog": config.catalog_write,
        "schema": schema,
        "client": CLIENT,
    }
    attribute_set = etl.map_tbl(tbls["attribute_set"], **tbl_args)
    attribute_set_latest = etl.map_tbl(
        tbls["attribute_set_latest"], **tbl_args
    )
    item_attributes_latest = etl.map_tbl(
        tbls["item_attributes_latest"],
        **tbl_args,
    )

    logger.info(f"Parsing attributes with parameters: {cfg['attributes']}")
    attributes = cfg["attributes"]["active"]
    lookback_days = cfg["attributes"]["lookback_days"]
    frequency_cutoff_pc = cfg["attributes"]["frequency_cutoff_pc"]
    pc_cutoff_col = cfg["attributes"]["pc_cutoff_col"]

    logger.info(f"Fetching item metadata from {product_catalog}")
    df_catalog_full = build_recent_catalog(
        spark.table(product_catalog),
        lookback_days,
        reference_date=run_date,
    )

    logger.info("Parsing metadata into attributes")
    df_catalog = build_item_attribute_catalog(df_catalog_full, attributes)
    df_catalog.cache()

    logger.info(f"Fetching basket data from {baskets}")
    df_baskets = build_recent_basket_items(
        spark.table(baskets),
        lookback_days,
        reference_date=run_date,
    )
    df_baskets.cache()

    if set_attributes:
        logger.info(f"REFRESH_ATTRIBUTES_DATE matches today ({today})")
        logger.info(
            f"Filtering attributes where {pc_cutoff_col} >= "
            f"{frequency_cutoff_pc}%"
        )

    attribute_dfs = build_attribute_mappings(
        spark=spark,
        df_catalog=df_catalog,
        df_baskets=df_baskets,
        attributes=attributes,
        set_attributes=set_attributes,
        attribute_set_latest_table=attribute_set_latest,
        pc_cutoff_col=pc_cutoff_col,
        frequency_cutoff_pc=frequency_cutoff_pc,
    )
    skipped_attributes = sorted(set(attributes).difference(attribute_dfs))
    for attribute in skipped_attributes:
        logger.warning(
            f"Requested attribute {attribute} not found in {attribute_set_latest}"
        )
        logger.warning(f"Skipping {attribute}")

    logger.info(
        f"Combining {len(attribute_dfs)} attributes "
        f"{list(attribute_dfs.keys())} into single dataframe"
    )
    df_attributes_master = build_attributes_master(spark, attribute_dfs)

    if set_attributes:
        df_attribute_set = build_attribute_set(df_attributes_master)

        logger.info("Exporting new attribute set")
        write_attribute_set_tables(
            df_attribute_set,
            attribute_set,
            attribute_set_latest,
            spark=spark,
            run_date=run_date,
        )

        logger.info(
            "Refreshing latest item-attribute mapping (using new attribute set)"
        )
        write_item_attributes_latest(
            df_attributes_master,
            item_attributes_latest,
            spark=spark,
            run_date=run_date,
        )
    else:
        logger.info("Refreshing latest item-attribute mapping")
        write_item_attributes_latest(
            df_attributes_master,
            item_attributes_latest,
            spark=spark,
            run_date=run_date,
        )

        if BQ_EXPORT:
            logger.info("Combining item attributes & NOV score for BQ export")
            bq_item_attributes = build_bigquery_item_attributes(
                spark=spark,
                df_attributes_master=df_attributes_master,
                attributes=attributes,
                nov_scores_csv=nov_scores_csv,
                product_catalog_latest_table=product_catalog_latest,
            )

            target_bq_table = etl.map_tbl(
                bq_options["item_attributes_dashboard"],
                **tbl_args,
            )
            logger.info(
                f"Exporting item attributes to Big Query: {target_bq_table}"
            )
            (
                bq_item_attributes.write.format("bigquery")
                .mode("overwrite")
                .option("temporaryGcsBucket", bq_options["temporaryGcsBucket"])
                .option("parentProject", bq_options["parentProject"])
                .option("table", target_bq_table)
                .save()
            )
            log_output_location(
                f"{bq_options['parentProject']}.{target_bq_table}",
                kind="bigquery_table",
            )

    logger.info("Run complete")


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
        "REFRESH_ATTRIBUTES_DATE": jobparser.get_arg(
            "--refresh_attributes_date"
        ),
        "BQ_EXPORT": jobparser.has_arg("--bq") or False,
        "RUN_DATE": jobparser.get_arg("--run_date"),
    }


if __name__ == "__main__":
    main(**parse_args())
