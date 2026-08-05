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
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import functions as F

from next_ads.common.paths import load_client_config
from next_ads.ranking.scoring_inputs import read_delta_version
from next_ads.ranking.portfolio_resolution import unchanged_provider_themes
from next_ads.ranking.theme_coverage import (
    build_missing_theme_affinity_coverage,
)
from next_ads.common import config_manager, etl


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    ROUTE,
    RUN_DATE,
    PROVIDER_BUILD_ID,
    PROVIDER_SIGNALS_TABLE,
    PROVIDER_SIGNALS_DELTA_VERSION,
    PROVIDER_SOURCE_RUN_DATE,
    PROVIDER_SELECTION_STATUS,
    PROVIDER_INPUT_SNAPSHOT_ID,
    CURRENT_INPUT_SNAPSHOT_ID,
    WARN_ONLY=False,
):
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
    route = (ROUTE or "").lower()
    if route not in {"v1", "v2"}:
        raise ValueError("--route must be v1 or v2")
    try:
        run_date = date.fromisoformat(RUN_DATE)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if not PROVIDER_BUILD_ID:
        raise ValueError("--provider_build_id is required")
    if not PROVIDER_SIGNALS_TABLE:
        raise ValueError("--provider_signals_table is required")
    try:
        provider_source_run_date = date.fromisoformat(
            PROVIDER_SOURCE_RUN_DATE
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--provider_source_run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if not PROVIDER_SELECTION_STATUS:
        raise ValueError("--provider_selection_status is required")
    if not PROVIDER_INPUT_SNAPSHOT_ID or not CURRENT_INPUT_SNAPSHOT_ID:
        raise ValueError(
            "Provider and current input snapshot IDs are required"
        )
    try:
        provider_signals_delta_version = int(
            PROVIDER_SIGNALS_DELTA_VERSION
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--provider_signals_delta_version must be an integer"
        ) from exc
    if provider_signals_delta_version < 0:
        raise ValueError(
            "--provider_signals_delta_version must not be negative"
        )

    tbl_args = {
        "catalog": config.catalog_write,
        "schema": config.schema_write,
        "client": CLIENT,
    }

    control_sheet_latest_v1 = etl.map_tbl(
        cfg["tables"]["write"]["control_sheet_latest"],
        **tbl_args,
    )
    control_sheet_table = (
        control_sheet_latest_v1
        if route == "v1"
        else config.tables_write.control_sheet_latest_v2
    )

    logger.info(
        "Checking %s ad Themes against provider build %s from %s "
        "at Delta version %s (selection=%s, source_run_date=%s, "
        "logical_run_date=%s)",
        route,
        PROVIDER_BUILD_ID,
        PROVIDER_SIGNALS_TABLE,
        provider_signals_delta_version,
        PROVIDER_SELECTION_STATUS,
        provider_source_run_date,
        run_date,
    )
    provider_signals = (
        read_delta_version(
            spark,
            PROVIDER_SIGNALS_TABLE,
            provider_signals_delta_version,
        )
        .where(F.col("ProviderBuildID") == PROVIDER_BUILD_ID)
        .where(F.col("EntityType") == "theme")
        .where(F.col("RunDate") == F.lit(provider_source_run_date))
        .select(F.col("EntityID").alias("NextTheme"))
    )
    if PROVIDER_INPUT_SNAPSHOT_ID != CURRENT_INPUT_SNAPSHOT_ID:
        allowed_themes = unchanged_provider_themes(
            spark,
            item_themes_table=(
                config.tables_write.scoring_input_item_themes
            ),
            provider_input_snapshot_id=PROVIDER_INPUT_SNAPSHOT_ID,
            current_input_snapshot_id=CURRENT_INPUT_SNAPSHOT_ID,
        )
        provider_signals = provider_signals.join(
            F.broadcast(allowed_themes),
            "NextTheme",
            "inner",
        )
    if provider_signals.limit(1).count() == 0:
        raise ValueError(
            "Selected provider build contains no theme signals at its "
            "recorded Delta version"
        )
    missing = build_missing_theme_affinity_coverage(
        spark.table(control_sheet_table),
        provider_signals,
        route=route,
    )

    missing_count = missing.count()
    if missing_count == 0:
        logger.info("All %s ad Themes are present in provider output", route)
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
        "ROUTE": jobparser.get_arg("--route"),
        "RUN_DATE": jobparser.get_arg("--run_date"),
        "PROVIDER_BUILD_ID": jobparser.get_arg("--provider_build_id"),
        "PROVIDER_SIGNALS_TABLE": jobparser.get_arg(
            "--provider_signals_table"
        ),
        "PROVIDER_SIGNALS_DELTA_VERSION": jobparser.get_arg(
            "--provider_signals_delta_version"
        ),
        "PROVIDER_SOURCE_RUN_DATE": jobparser.get_arg(
            "--provider_source_run_date"
        ),
        "PROVIDER_SELECTION_STATUS": jobparser.get_arg(
            "--provider_selection_status"
        ),
        "PROVIDER_INPUT_SNAPSHOT_ID": jobparser.get_arg(
            "--provider_input_snapshot_id"
        ),
        "CURRENT_INPUT_SNAPSHOT_ID": jobparser.get_arg(
            "--current_input_snapshot_id"
        ),
    }


if __name__ == "__main__":
    main(**parse_args())
