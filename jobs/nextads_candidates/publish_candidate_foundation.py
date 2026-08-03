import json
import sys
from datetime import timedelta
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    notebook_path = (
        get_dbutils()
        .notebook.entry_point.getDbutils()
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

from next_ads.candidates.foundation import (
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
    parse_run_date,
)
from next_ads.candidates.foundation_manifest import (
    publish_candidate_foundation_manifest,
    verify_output_binding,
)
from next_ads.common import config_manager


def _integer(value, label):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if result < 0:
        raise ValueError(f"{label} must not be negative")
    return result


def _json_list(value, label):
    try:
        result = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be valid JSON") from exc
    if not isinstance(result, list):
        raise ValueError(f"{label} must contain a JSON list")
    return result


def _validated_customer_cell_status(
    run_date,
    source_run_date,
    selection_status,
):
    status = (selection_status or "").strip()
    if status not in {READY_FOR_NEXTADS, FALLBACK_PREVIOUS}:
        raise ValueError(
            "customer_cells_selection_status must be READY_FOR_NEXTADS "
            "or FALLBACK_PREVIOUS"
        )
    if status == READY_FOR_NEXTADS and source_run_date != run_date:
        raise ValueError(
            "READY_FOR_NEXTADS customer cells must match the foundation "
            "run date"
        )
    if (
        status == FALLBACK_PREVIOUS
        and source_run_date != run_date - timedelta(days=1)
    ):
        raise ValueError(
            "FALLBACK_PREVIOUS customer cells must be from the previous "
            "logical day"
        )
    return status


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    FOUNDATION_SNAPSHOT_ID,
    CUSTOMER_CELLS_TABLE,
    CUSTOMER_CELLS_DELTA_VERSION,
    CUSTOMER_CELLS_SOURCE_RUN_DATE,
    CUSTOMER_CELLS_SELECTION_STATUS,
    CUSTOMER_CELLS_ROW_COUNT,
    CUSTOMER_CELLS_CONTENT_CHECKSUM,
    CUSTOMER_CELLS_SCHEMA_CHECKSUM,
    CUSTOMER_CELLS_WARNING_COUNT,
    REPEAT_AD_EXPOSURE_TABLE,
    REPEAT_AD_EXPOSURE_DELTA_VERSION,
    REPEAT_AD_EXPOSURE_ROW_COUNT,
    REPEAT_AD_EXPOSURE_CONTENT_CHECKSUM,
    REPEAT_AD_EXPOSURE_SOURCE_BINDINGS_JSON,
    AD_FEEDBACK_TABLE,
    AD_FEEDBACK_DELTA_VERSION,
    AD_FEEDBACK_ROW_COUNT,
    AD_FEEDBACK_CONTENT_CHECKSUM,
    AD_FEEDBACK_SOURCE_BINDINGS_JSON,
    TASK_RUN_ID,
    EXECUTION_COUNT,
):
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    run_date = parse_run_date(RUN_DATE)
    snapshot_id = (FOUNDATION_SNAPSHOT_ID or "").strip()
    if not snapshot_id:
        raise ValueError("--foundation_snapshot_id is required")
    customer_source_date = parse_run_date(CUSTOMER_CELLS_SOURCE_RUN_DATE)
    customer_selection_status = _validated_customer_cell_status(
        run_date,
        customer_source_date,
        CUSTOMER_CELLS_SELECTION_STATUS,
    )
    customer_version = _integer(
        CUSTOMER_CELLS_DELTA_VERSION,
        "customer_cells_delta_version",
    )
    exposure_version = _integer(
        REPEAT_AD_EXPOSURE_DELTA_VERSION,
        "repeat_ad_exposure_delta_version",
    )
    feedback_version = _integer(
        AD_FEEDBACK_DELTA_VERSION,
        "ad_feedback_delta_version",
    )
    output_bindings = {
        "customer_cells": {
            "table": CUSTOMER_CELLS_TABLE,
            "delta_version": customer_version,
            "schema_version": "customer_cells/v1",
            "source_run_date": customer_source_date.isoformat(),
            "row_count": _integer(
                CUSTOMER_CELLS_ROW_COUNT,
                "customer_cells_row_count",
            ),
            "content_checksum": CUSTOMER_CELLS_CONTENT_CHECKSUM,
        },
        "repeat_ad_exposure": {
            "table": REPEAT_AD_EXPOSURE_TABLE,
            "delta_version": exposure_version,
            "schema_version": "repeat_ad_exposure/v1",
            "source_run_date": run_date.isoformat(),
            "row_count": _integer(
                REPEAT_AD_EXPOSURE_ROW_COUNT,
                "repeat_ad_exposure_row_count",
            ),
            "content_checksum": REPEAT_AD_EXPOSURE_CONTENT_CHECKSUM,
        },
        "ad_feedback": {
            "table": AD_FEEDBACK_TABLE,
            "delta_version": feedback_version,
            "schema_version": "ad_feedback_metrics/v1",
            "source_run_date": run_date.isoformat(),
            "row_count": _integer(
                AD_FEEDBACK_ROW_COUNT,
                "ad_feedback_row_count",
            ),
            "content_checksum": AD_FEEDBACK_CONTENT_CHECKSUM,
        },
    }
    verify_output_binding(
        spark,
        name="customer_cells",
        binding=output_bindings["customer_cells"],
        snapshot_id=snapshot_id,
        run_date=run_date,
        key_columns=("AccountNumber",),
        allow_empty=False,
    )
    verify_output_binding(
        spark,
        name="repeat_ad_exposure",
        binding=output_bindings["repeat_ad_exposure"],
        snapshot_id=snapshot_id,
        run_date=run_date,
        key_columns=(
            "CandidateFoundationSnapshotID",
            "AccountNumber",
            "AdSeen",
        ),
        allow_empty=True,
    )
    verify_output_binding(
        spark,
        name="ad_feedback",
        binding=output_bindings["ad_feedback"],
        snapshot_id=snapshot_id,
        run_date=run_date,
        key_columns=("CandidateFoundationSnapshotID", "UniqueAdID"),
        allow_empty=True,
    )

    source_bindings = [
        {
            "name": "customer_cells",
            "role": "eligibility",
            "table": CUSTOMER_CELLS_TABLE,
            "delta_version": customer_version,
            "schema_version": "customer_cells/v1",
            "schema_checksum": CUSTOMER_CELLS_SCHEMA_CHECKSUM,
            "required": True,
        }
    ]
    source_bindings.extend(
        _json_list(
            REPEAT_AD_EXPOSURE_SOURCE_BINDINGS_JSON,
            "repeat_ad_exposure_source_bindings_json",
        )
    )
    source_bindings.extend(
        _json_list(
            AD_FEEDBACK_SOURCE_BINDINGS_JSON,
            "ad_feedback_source_bindings_json",
        )
    )
    cell_warning_count = _integer(
        CUSTOMER_CELLS_WARNING_COUNT,
        "customer_cells_warning_count",
    )
    status = customer_selection_status
    build = publish_candidate_foundation_manifest(
        spark,
        snapshot_id=snapshot_id,
        run_date=run_date,
        source_bindings=tuple(source_bindings),
        output_bindings=output_bindings,
        warning_count=cell_warning_count,
        status=status,
        task_run_id=_integer(TASK_RUN_ID, "task_run_id"),
        execution_count=_integer(EXECUTION_COUNT, "execution_count"),
        builds_table=config.tables_write.candidate_foundation_builds,
        sources_table=config.tables_write.candidate_foundation_sources,
        fallback_source_snapshot_id=(
            f"customer_cells_delta_{customer_version}"
            if status == FALLBACK_PREVIOUS
            else None
        ),
        fallback_source_run_date=(
            customer_source_date if status == FALLBACK_PREVIOUS else None
        ),
    )
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="foundation_snapshot_id", value=build.snapshot_id)
    task_values.set(key="foundation_attempt_id", value=build.attempt_id)
    task_values.set(key="foundation_status", value=build.status)
    logger.info(
        "Candidate foundation %s accepted with status %s",
        build.snapshot_id,
        build.status,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    keys = {
        "JOB_ENV": "job_env",
        "CLIENT": "client",
        "LOG_LEVEL": "log_level",
        "RUN_DATE": "run_date",
        "FOUNDATION_SNAPSHOT_ID": "foundation_snapshot_id",
        "CUSTOMER_CELLS_TABLE": "customer_cells_table",
        "CUSTOMER_CELLS_DELTA_VERSION": "customer_cells_delta_version",
        "CUSTOMER_CELLS_SOURCE_RUN_DATE": "customer_cells_source_run_date",
        "CUSTOMER_CELLS_SELECTION_STATUS": "customer_cells_selection_status",
        "CUSTOMER_CELLS_ROW_COUNT": "customer_cells_row_count",
        "CUSTOMER_CELLS_CONTENT_CHECKSUM": "customer_cells_content_checksum",
        "CUSTOMER_CELLS_SCHEMA_CHECKSUM": "customer_cells_schema_checksum",
        "CUSTOMER_CELLS_WARNING_COUNT": "customer_cells_warning_count",
        "REPEAT_AD_EXPOSURE_TABLE": "repeat_ad_exposure_table",
        "REPEAT_AD_EXPOSURE_DELTA_VERSION": "repeat_ad_exposure_delta_version",
        "REPEAT_AD_EXPOSURE_ROW_COUNT": "repeat_ad_exposure_row_count",
        "REPEAT_AD_EXPOSURE_CONTENT_CHECKSUM": "repeat_ad_exposure_content_checksum",
        "REPEAT_AD_EXPOSURE_SOURCE_BINDINGS_JSON": "repeat_ad_exposure_source_bindings_json",
        "AD_FEEDBACK_TABLE": "ad_feedback_table",
        "AD_FEEDBACK_DELTA_VERSION": "ad_feedback_delta_version",
        "AD_FEEDBACK_ROW_COUNT": "ad_feedback_row_count",
        "AD_FEEDBACK_CONTENT_CHECKSUM": "ad_feedback_content_checksum",
        "AD_FEEDBACK_SOURCE_BINDINGS_JSON": "ad_feedback_source_bindings_json",
        "TASK_RUN_ID": "task_run_id",
        "EXECUTION_COUNT": "execution_count",
    }
    values = {
        output: parser.get_arg(f"--{argument}")
        for output, argument in keys.items()
    }
    values["CLIENT"] = values["CLIENT"] or "next_uk"
    return values


if __name__ == "__main__":
    main(**parse_args())
