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
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import functions as F

from next_ads.common import config_manager
from next_ads.common.paths import load_client_config


def _int_arg(value: str | None, default: int) -> int:
    value = str(value or "").strip()
    return int(value) if value else default


def _table_exists(spark, table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def enrich_theme_affinity_inference_log(
    spark,
    inference_log_table: str,
    results_path: str,
    label_window_days: int,
):
    logger = get_logger(__name__)
    if not _table_exists(spark, inference_log_table):
        logger.warning(
            "Theme Affinity inference-log table does not exist, skipping label "
            "enrichment: %s",
            inference_log_table,
        )
        return

    logger.info("Reading results session outcomes from %s", results_path)
    results = spark.read.parquet(results_path)
    max_session_date_row = results.agg(
        F.max("SessionDate").alias("max_session_date")
    ).collect()[0]
    max_session_date = max_session_date_row["max_session_date"]
    if max_session_date is None:
        logger.warning("No result SessionDate values found, skipping label enrichment")
        return

    inference_rows = (
        spark.table(inference_log_table)
        .where(F.col("label").isNull())
        .where(
            F.col("inference_date")
            <= F.date_sub(F.lit(max_session_date), label_window_days)
        )
        .select(
            "inference_date",
            "model_id",
            "account_number",
            "theme",
        )
    )
    if inference_rows.limit(1).count() == 0:
        logger.info(
            "No inference-log rows are eligible for label enrichment through %s",
            max_session_date,
        )
        return

    outcomes = (
        results.select(
            F.col("AccountNumber").alias("account_number"),
            F.col("SessionDate").alias("session_date"),
            F.when(F.col("Revenue") > 0, F.lit(1)).otherwise(F.lit(0)).alias(
                "converted"
            ),
        )
        .where(F.col("account_number").isNotNull())
        .where(F.col("session_date").isNotNull())
    )
    labels = (
        inference_rows.join(
            outcomes,
            on=(
                (inference_rows.account_number == outcomes.account_number)
                & (
                    outcomes.session_date
                    > inference_rows.inference_date
                )
                & (
                    outcomes.session_date
                    <= F.date_add(inference_rows.inference_date, label_window_days)
                )
            ),
            how="left",
        )
        .groupBy(
            inference_rows.inference_date,
            inference_rows.model_id,
            inference_rows.account_number,
            inference_rows.theme,
        )
        .agg(F.coalesce(F.max("converted"), F.lit(0)).cast("int").alias("label"))
        .withColumn(
            "label_observed_until",
            F.date_add(F.col("inference_date"), label_window_days),
        )
        .withColumn("label_updated_timestamp", F.current_timestamp())
    )

    labels.createOrReplaceTempView("theme_affinity_inference_labels")
    spark.sql(
        f"""
        MERGE INTO {inference_log_table} AS target
        USING theme_affinity_inference_labels AS source
        ON target.inference_date = source.inference_date
          AND target.model_id = source.model_id
          AND target.account_number = source.account_number
          AND target.theme = source.theme
        WHEN MATCHED THEN UPDATE SET
          target.label = source.label,
          target.label_observed_until = source.label_observed_until,
          target.label_updated_timestamp = source.label_updated_timestamp
        """
    )
    logger.info(
        "Merged Theme Affinity inference labels into %s through observed date %s",
        inference_log_table,
        max_session_date,
    )


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client") or "next_uk"
    LOG_LEVEL = jobparser.get_arg("--log_level")
    LABEL_WINDOW_DAYS = _int_arg(jobparser.get_arg("--label_window_days"), 28)

    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(JOB_ENV)
    client_config = load_client_config(CLIENT)
    results_path = (
        f"{client_config['dbfs_base_path']}/{JOB_ENV}/tmp/df_sessions_master_meta"
    )
    logger.info(
        "Enriching %s using %s with a %s day label window",
        config.ranking_model_tables.inference_log,
        results_path,
        LABEL_WINDOW_DAYS,
    )
    enrich_theme_affinity_inference_log(
        spark=spark,
        inference_log_table=str(config.ranking_model_tables.inference_log),
        results_path=results_path,
        label_window_days=LABEL_WINDOW_DAYS,
    )
