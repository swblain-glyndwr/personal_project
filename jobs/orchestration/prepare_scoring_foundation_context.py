import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
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
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark.sql import Window
from pyspark.sql import functions as F

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ranking.foundation_context import (
    ScoringFoundationContext,
    activate_foundation_context,
    build_foundation_invocation_checksum,
    build_scoring_foundation_build_id,
)
from next_ads.ranking.scoring_inputs import latest_delta_version


EXPECTED_SOURCES = {
    "authoritative_v2_theme_mapping": (
        "authoritative_theme_mapping",
        True,
    ),
    "copied_v1_theme_mapping": ("compatibility_theme_mapping", False),
    "theme_mapping": ("authoritative_theme_mapping", True),
    "item_attributes": ("item_attributes", True),
    "item_themes": ("derived_item_themes", True),
}


def _as_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else dict(value)


def _select_snapshot_id(
    spark,
    table,
    sources_table,
    run_date,
    requested,
):
    candidates = spark.table(table).where(
        (F.col("RunDate") == F.lit(run_date))
        & F.col("Status").isin("READY", "READY_WITH_WARNINGS")
    )
    if requested and requested != "same_day":
        candidates = candidates.where(F.col("InputSnapshotID") == requested)
    window = Window.partitionBy("InputSnapshotID").orderBy(
        F.col("ExecutionCount").desc(),
        F.col("CompletedAt").desc(),
        F.col("TaskRunID").desc(),
    )
    contradictory = (
        candidates.groupBy(
            "InputSnapshotID",
            "ExecutionCount",
            "CompletedAt",
            "TaskRunID",
        )
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if contradictory:
        raise ValueError("Contradictory scoring input attempts found")
    selected = (
        candidates.withColumn("_attempt_rank", F.row_number().over(window))
        .where(F.col("_attempt_rank") == 1)
        .orderBy(F.col("CompletedAt").desc(), F.col("InputSnapshotID"))
        .select(
            "InputSnapshotID",
            "InputSnapshotAttemptID",
            "SourceCount",
        )
        .limit(2)
        .collect()
    )
    if not selected:
        raise ValueError(f"No accepted scoring input snapshot for {run_date}")
    if requested not in {None, "", "same_day"} and len(selected) != 1:
        raise ValueError(f"Input snapshot {requested} is contradictory")
    winner = selected[0]
    if int(winner["SourceCount"]) != len(EXPECTED_SOURCES):
        raise ValueError("Accepted scoring input source count is incomplete")
    source_rows = (
        spark.table(sources_table)
        .where(
            F.col("InputSnapshotAttemptID")
            == winner["InputSnapshotAttemptID"]
        )
        .select(
            "SourceName",
            "SourceRole",
            "SourceTable",
            "DeltaVersion",
            "SchemaVersion",
            "IsRequired",
            "RowCount",
            "DistinctKeyCount",
            "NullKeyCount",
            "DuplicateKeyCount",
            "ContentChecksum",
        )
        .collect()
    )
    observed = {
        row["SourceName"]: (row["SourceRole"], bool(row["IsRequired"]))
        for row in source_rows
    }
    if len(source_rows) != len(observed) or observed != EXPECTED_SOURCES:
        raise ValueError("Accepted scoring input source contract is incomplete")
    invalid_sources = [
        row["SourceName"]
        for row in source_rows
        if (
            int(row["DeltaVersion"]) < 0
            or not row["SchemaVersion"]
            or not row["ContentChecksum"]
            or (
                bool(row["IsRequired"])
                and (
                    int(row["RowCount"]) < 1
                    or int(row["NullKeyCount"]) != 0
                    or int(row["DuplicateKeyCount"]) != 0
                    or int(row["DistinctKeyCount"]) != int(row["RowCount"])
                )
            )
        )
    ]
    if invalid_sources:
        raise ValueError(
            "Accepted scoring input sources are invalid: "
            + ", ".join(sorted(invalid_sources))
        )
    source_bindings = {
        row["SourceName"]: {
            "source_role": row["SourceRole"],
            "source_table": row["SourceTable"],
            "delta_version": int(row["DeltaVersion"]),
            "schema_version": row["SchemaVersion"],
            "is_required": bool(row["IsRequired"]),
            "row_count": int(row["RowCount"]),
            "distinct_key_count": int(row["DistinctKeyCount"]),
            "null_key_count": int(row["NullKeyCount"]),
            "duplicate_key_count": int(row["DuplicateKeyCount"]),
            "content_checksum": row["ContentChecksum"],
        }
        for row in source_rows
    }
    return (
        winner["InputSnapshotID"],
        winner["InputSnapshotAttemptID"],
        source_bindings,
    )


def _wait_for_snapshot_id(
    spark,
    *,
    table,
    sources_table,
    run_date,
    requested,
    wait_seconds,
    poll_seconds,
):
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return _select_snapshot_id(
                spark,
                table,
                sources_table,
                run_date,
                requested,
            )
        except ValueError as error:
            if "No accepted scoring input snapshot" not in str(error):
                raise
            if time.monotonic() >= deadline:
                raise
            remaining = max(0, deadline - time.monotonic())
            time.sleep(min(poll_seconds, remaining))


def _build_bindings(
    foundation,
    input_snapshot_id,
    input_snapshot_attempt_id,
    run_date,
    snapshot_sources,
    input_delta_versions,
):
    bindings = {}
    for name, definition_value in sorted(
        _as_dict(foundation.input_bindings).items()
    ):
        definition = _as_dict(definition_value)
        accepted_source = snapshot_sources.get(name)
        if not isinstance(accepted_source, dict) or (
            accepted_source["schema_version"]
            != definition["schema_version"]
        ):
            raise ValueError(
                f"Accepted source {name} does not match its foundation schema"
            )
        bindings[name] = {
            "table": definition["table"],
            "schema_version": definition["schema_version"],
            "input_snapshot_id": input_snapshot_id,
            "input_snapshot_attempt_id": input_snapshot_attempt_id,
            "run_date": run_date.isoformat(),
            "delta_version": input_delta_versions[name],
        }
    bindings["accepted_sources"] = snapshot_sources
    return json.dumps(bindings, sort_keys=True, separators=(",", ":"))


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    INPUT_SNAPSHOT_ID,
    FOUNDATION_ID,
    CONTEXT_SLOT,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    READINESS_WAIT_SECONDS,
    READINESS_POLL_SECONDS,
    ORCHESTRATION_RUN_ID,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    run_date = date.fromisoformat(RUN_DATE)
    task_run_id = int(TASK_RUN_ID)
    execution_count = int(EXECUTION_COUNT)
    foundation = config.scoring.foundations[FOUNDATION_ID]
    if foundation.foundation_id != FOUNDATION_ID:
        raise ValueError("Foundation key must match foundation_id")
    invocation_checksum = build_foundation_invocation_checksum(foundation)
    (
        input_snapshot_id,
        input_snapshot_attempt_id,
        snapshot_sources,
    ) = _wait_for_snapshot_id(
        spark,
        table=config.tables_write.scoring_input_snapshots,
        sources_table=config.tables_write.scoring_input_snapshot_sources,
        run_date=run_date,
        requested=INPUT_SNAPSHOT_ID,
        wait_seconds=int(READINESS_WAIT_SECONDS),
        poll_seconds=int(READINESS_POLL_SECONDS),
    )
    build_id = build_scoring_foundation_build_id(
        foundation_id=foundation.foundation_id,
        foundation_version=foundation.foundation_version,
        input_snapshot_id=input_snapshot_id,
        invocation_checksum=invocation_checksum,
        run_date=run_date,
    )
    build_attempt_id = f"{build_id}:{task_run_id}:{execution_count}"
    activated_at = datetime.now(timezone.utc)
    context = ScoringFoundationContext(
        context_slot=CONTEXT_SLOT,
        orchestration_run_id=int(ORCHESTRATION_RUN_ID),
        foundation_id=foundation.foundation_id,
        foundation_version=foundation.foundation_version,
        scoring_foundation_build_id=build_id,
        scoring_foundation_build_attempt_id=build_attempt_id,
        input_snapshot_id=input_snapshot_id,
        input_snapshot_attempt_id=input_snapshot_attempt_id,
        run_date=run_date,
        bindings_json=_build_bindings(
            foundation,
            input_snapshot_id,
            input_snapshot_attempt_id,
            run_date,
            snapshot_sources,
            {
                name: latest_delta_version(
                    spark,
                    _as_dict(definition)["table"],
                )
                for name, definition in _as_dict(
                    foundation.input_bindings
                ).items()
            },
        ),
        capability=foundation.capability,
        contract_version=foundation.contract_version,
        invocation_checksum=invocation_checksum,
        expires_at=activated_at + timedelta(hours=8),
    )
    activate_foundation_context(
        spark,
        context_table=config.tables_write.scoring_foundation_run_contexts,
        context=context,
        task_run_id=task_run_id,
        execution_count=execution_count,
        activated_at=activated_at,
    )
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="input_snapshot_id", value=input_snapshot_id)
    task_values.set(key="scoring_foundation_build_id", value=build_id)
    task_values.set(
        key="scoring_foundation_build_attempt_id",
        value=build_attempt_id,
    )
    logger.info(
        "Activated %s for foundation build %s and input %s",
        CONTEXT_SLOT,
        build_id,
        input_snapshot_id,
    )


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--run_date"),
        parser.get_arg("--input_snapshot_id"),
        parser.get_arg("--foundation_id"),
        parser.get_arg("--context_slot"),
        parser.get_arg("--task_run_id"),
        parser.get_arg("--execution_count"),
        parser.get_arg("--readiness_wait_seconds") or "1800",
        parser.get_arg("--readiness_poll_seconds") or "60",
        parser.get_arg("--orchestration_run_id"),
    )
