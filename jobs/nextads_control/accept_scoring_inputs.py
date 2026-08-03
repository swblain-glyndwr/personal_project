import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

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
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.common.paths import load_client_config
from next_ads.ranking.scoring_input_acceptance import (
    InputSource,
    accept_input_snapshot,
)
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
)


def _set_task_value(key, value):
    get_dbutils().jobs.taskValues.set(key=key, value=value)


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    THEME_MAPPING_LANDING_ID,
    THEME_MAPPING_LANDING_VERSION,
    WARNING_COUNT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    run_date = date.fromisoformat(RUN_DATE)
    task_run_id = int(TASK_RUN_ID)
    execution_count = int(EXECUTION_COUNT)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    cfg = load_client_config(CLIENT)
    if not cfg["theme_mapping_v2"].get("source_of_truth"):
        raise ValueError("theme_mapping_v2 must be marked as source_of_truth")
    landing_version = int(THEME_MAPPING_LANDING_VERSION)
    landed = read_delta_version(
        spark,
        config.tables_write.scoring_input_theme_mapping_raw,
        landing_version,
    ).where(F.col("LandingID") == THEME_MAPPING_LANDING_ID)
    invalid_landing_date = F.col("RunDate").isNull() | (
        F.col("RunDate") != F.lit(run_date)
    )
    if landed.where(invalid_landing_date).limit(1).count():
        raise ValueError("Theme Mapping landing has the wrong logical RunDate")
    landed_v2 = landed.where(F.col("SourceRole") == "authoritative_v2")
    if landed_v2.limit(1).count() == 0:
        raise ValueError(
            f"Theme Mapping landing {THEME_MAPPING_LANDING_ID} is empty"
        )
    warning_count = int(WARNING_COUNT)
    if warning_count:
        logger.warning(
            "The copied v1 Theme Mapping differs from the accepted v2 source "
            "in %s row(s); v2 remains authoritative",
            warning_count,
        )

    tables = config.tables_write
    source_specs = (
        (
            "authoritative_v2_theme_mapping",
            "authoritative_theme_mapping",
            tables.scoring_input_theme_mapping_raw,
            ("Theme",),
            (
                "Theme",
                "TargetingAttributes",
                "ThemeType",
                "ThemeTypeRank",
                "AdType",
                "AdTypeRank",
            ),
            THEME_MAPPING_LANDING_ID,
            "authoritative_v2",
            True,
        ),
        (
            "copied_v1_theme_mapping",
            "compatibility_theme_mapping",
            tables.scoring_input_theme_mapping_raw,
            ("Theme",),
            (
                "Theme",
                "TargetingAttributes",
                "ThemeType",
                "ThemeTypeRank",
                "AdType",
                "AdTypeRank",
            ),
            THEME_MAPPING_LANDING_ID,
            "copied_v1",
            False,
        ),
        (
            "theme_mapping",
            "authoritative_theme_mapping",
            tables.theme_mapping_latest,
            ("Theme", "attribute", "value"),
            ("Theme", "attribute", "value"),
            None,
            None,
            True,
        ),
        (
            "item_attributes",
            "item_attributes",
            tables.item_attributes_latest,
            ("pid", "attribute", "value"),
            ("pid", "attribute", "value"),
            None,
            None,
            True,
        ),
        (
            "item_themes",
            "derived_item_themes",
            tables.item_themes_latest,
            ("pid", "theme"),
            ("pid", "theme", "theme_rank"),
            None,
            None,
            True,
        ),
    )
    sources = []
    for (
        name,
        role,
        table,
        keys,
        columns,
        landing_id,
        source_role,
        is_required,
    ) in source_specs:
        version = (
            landing_version
            if landing_id
            else latest_delta_version(spark, table)
        )
        frame = read_delta_version(spark, table, version)
        if landing_id:
            frame = frame.where(F.col("LandingID") == landing_id)
            frame = frame.where(F.col("SourceRole") == source_role)
        else:
            invalid_date = frame.where(
                F.col("rundate").isNull()
                | (F.col("rundate") != F.lit(run_date))
            ).limit(1)
            if invalid_date.count():
                raise ValueError(
                    f"{name} does not belong to logical run date {run_date}"
                )
        sources.append(
            InputSource(
                name=name,
                role=role,
                table=table,
                delta_version=version,
                frame=frame.select(*columns),
                key_columns=keys,
                content_columns=columns,
                schema_version="v1",
                is_required=is_required,
            )
        )

    completed_at = datetime.now(timezone.utc)
    attempt_id = (
        f"scoring_inputs:{run_date.isoformat()}:"
        f"{task_run_id}:{execution_count}"
    )
    accepted = accept_input_snapshot(
        spark,
        run_date=run_date,
        sources=tuple(sources),
        item_themes_source_name="item_themes",
        item_themes_snapshot_table=tables.scoring_input_item_themes,
        snapshots_table=tables.scoring_input_snapshots,
        snapshot_sources_table=tables.scoring_input_snapshot_sources,
        input_snapshot_attempt_id=attempt_id,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=completed_at,
        warning_count=warning_count,
    )
    _set_task_value("input_snapshot_id", accepted.input_snapshot_id)
    logger.info(
        "Accepted scoring input snapshot %s with status %s",
        accepted.input_snapshot_id,
        accepted.status,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    return {
        "JOB_ENV": parser.get_arg("--job_env"),
        "CLIENT": parser.get_arg("--client") or "next_uk",
        "LOG_LEVEL": parser.get_arg("--log_level"),
        "RUN_DATE": parser.get_arg("--run_date"),
        "TASK_RUN_ID": parser.get_arg("--task_run_id"),
        "EXECUTION_COUNT": parser.get_arg("--execution_count"),
        "THEME_MAPPING_LANDING_ID": parser.get_arg(
            "--theme_mapping_landing_id"
        ),
        "THEME_MAPPING_LANDING_VERSION": parser.get_arg(
            "--theme_mapping_landing_version"
        ),
        "WARNING_COUNT": parser.get_arg("--warning_count"),
    }


if __name__ == "__main__":
    main(**parse_args())
