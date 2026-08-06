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
    build_foundation_invocation_checksum,
)
from next_ads.ranking.provider_context import (
    ProviderContext,
    activate_provider_context,
    build_provider_build_id,
    build_provider_invocation_checksum,
    load_reusable_provider_context,
    pinned_item_themes,
)


def _as_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else dict(value)


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
            F.col("InputSnapshotAttemptID") == winner["InputSnapshotAttemptID"]
        )
        .select(
            "SourceName",
            "SourceRole",
            "IsRequired",
            "AcceptedTable",
            "AcceptedDeltaVersion",
            "AcceptedSchemaChecksum",
            "WriteReceiptID",
        )
        .collect()
    )
    observed = {
        row["SourceName"]: (row["SourceRole"], bool(row["IsRequired"]))
        for row in source_rows
    }
    if len(source_rows) != len(observed) or observed != EXPECTED_SOURCES:
        raise ValueError(
            "Accepted scoring input source contract is incomplete"
        )
    item_themes = next(
        row for row in source_rows if row["SourceName"] == "item_themes"
    )
    if (
        not item_themes["AcceptedTable"]
        or item_themes["AcceptedDeltaVersion"] is None
        or int(item_themes["AcceptedDeltaVersion"]) < 0
        or not item_themes["AcceptedSchemaChecksum"]
        or not item_themes["WriteReceiptID"]
    ):
        raise ValueError("Accepted item-theme receipt is incomplete")
    return (
        winner["InputSnapshotID"],
        winner["InputSnapshotAttemptID"],
        {
            "table": item_themes["AcceptedTable"],
            "input_snapshot_id": winner["InputSnapshotID"],
            "input_snapshot_attempt_id": winner["InputSnapshotAttemptID"],
            "run_date": run_date.isoformat(),
            "delta_version": int(item_themes["AcceptedDeltaVersion"]),
            "schema_checksum": item_themes["AcceptedSchemaChecksum"],
            "write_receipt_id": item_themes["WriteReceiptID"],
        },
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
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))


def _load_foundation_binding(
    spark,
    *,
    config,
    provider,
    input_snapshot_id,
    run_date,
    scoring_foundation_build_id,
    scoring_foundation_build_attempt_id,
):
    foundation_id = provider.foundation_id
    supplied = (
        scoring_foundation_build_id,
        scoring_foundation_build_attempt_id,
    )
    if foundation_id is None:
        if any(supplied):
            raise ValueError(
                "Foundation-free provider received foundation IDs"
            )
        return None
    if not all(supplied):
        raise ValueError(
            "Provider requires an exact scoring foundation attempt"
        )
    foundation = config.scoring.foundations[foundation_id]
    build_rows = (
        spark.table(config.tables_write.scoring_foundation_builds)
        .where(
            (F.col("ScoringFoundationBuildID") == scoring_foundation_build_id)
            & (
                F.col("ScoringFoundationBuildAttemptID")
                == scoring_foundation_build_attempt_id
            )
            & (F.col("Status") == "READY_FOR_PROVIDERS")
        )
        .collect()
    )
    if len(build_rows) != 1:
        raise ValueError("Expected one ready scoring foundation attempt")
    build = build_rows[0]
    try:
        input_bindings = json.loads(build["InputBindingsJSON"])
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            "Scoring foundation input bindings are invalid"
        ) from error
    if not isinstance(input_bindings, dict) or not input_bindings:
        raise ValueError("Scoring foundation input bindings are incomplete")
    expected_build = {
        "InputSnapshotID": input_snapshot_id,
        "RunDate": run_date,
        "FoundationID": foundation_id,
        "FoundationVersion": foundation.foundation_version,
        "Capability": provider.capability,
        "ContractVersion": foundation.contract_version,
        "InvocationChecksum": build_foundation_invocation_checksum(foundation),
    }
    mismatched = [
        field
        for field, value in expected_build.items()
        if build[field] != value
    ]
    if mismatched:
        raise ValueError(
            "Scoring foundation does not match provider requirements: "
            + ", ".join(mismatched)
        )
    output_rows = (
        spark.table(config.tables_write.scoring_foundation_outputs)
        .where(
            F.col("ScoringFoundationBuildAttemptID")
            == scoring_foundation_build_attempt_id
        )
        .collect()
    )
    required_outputs = _as_dict(foundation.required_outputs)
    if tuple(json.loads(build["RequiredOutputsJSON"])) != tuple(
        sorted(required_outputs)
    ):
        raise ValueError(
            "Scoring foundation required outputs do not match config"
        )
    observed = {row["OutputName"]: row for row in output_rows}
    if len(observed) != len(output_rows) or set(observed) != set(
        required_outputs
    ):
        raise ValueError("Scoring foundation output contract is incomplete")
    outputs = {}
    for output_name, schema_version in sorted(required_outputs.items()):
        row = observed[output_name]
        invalid = (
            row["ScoringFoundationBuildID"] != scoring_foundation_build_id
            or row["RunDate"] != run_date
            or row["OutputSchemaVersion"] != schema_version
            or not bool(row["IsRequired"])
            or int(row["RowCount"]) < 1
            or int(row["OutputDeltaVersion"]) < 0
            or not row["WriteReceiptID"]
            or not row["GitCommit"]
            or not row["SourceSchemaChecksum"]
            or not row["OutputSchemaChecksum"]
            or row["SourceSchemaChecksum"] != row["OutputSchemaChecksum"]
        )
        if invalid:
            raise ValueError(
                f"Scoring foundation output {output_name} is invalid"
            )
        outputs[output_name] = {
            "source_table": row["SourceTable"],
            "source_delta_version": row["SourceDeltaVersion"],
            "table": row["OutputTable"],
            "delta_version": int(row["OutputDeltaVersion"]),
            "schema_version": row["OutputSchemaVersion"],
            "schema_checksum": row["OutputSchemaChecksum"],
            "write_receipt_id": row["WriteReceiptID"],
        }
    return {
        "scoring_foundation_build_id": scoring_foundation_build_id,
        "scoring_foundation_build_attempt_id": (
            scoring_foundation_build_attempt_id
        ),
        "input_snapshot_attempt_id": build["InputSnapshotAttemptID"],
        "pipeline_id": build["PipelineID"],
        "pipeline_update_id": build["PipelineUpdateID"],
        "pipeline_update_type": build["PipelineUpdateType"],
        "outputs": outputs,
        "input_bindings": input_bindings,
    }


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    INPUT_SNAPSHOT_ID,
    MODEL_URI,
    CONTEXT_SLOT,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    READINESS_WAIT_SECONDS,
    READINESS_POLL_SECONDS,
    PROVIDER_ID="theme_affinity",
    USE_CASE="theme_ranking",
    ORCHESTRATION_RUN_ID=None,
    SCORING_FOUNDATION_BUILD_ID=None,
    SCORING_FOUNDATION_BUILD_ATTEMPT_ID=None,
    ALLOW_SERIAL_RUN_TAKEOVER=False,
    ACTIVATE_CONTEXT=True,
    EMIT_TASK_VALUES=True,
    REUSE_INCOMPLETE_ATTEMPT=False,
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
    provider = config.scoring.providers[PROVIDER_ID]
    invocation_checksum = build_provider_invocation_checksum(
        provider_id=PROVIDER_ID,
        provider_config=_as_dict(provider),
        provider_implementation=provider.implementation,
        ranking_model_config=(
            _as_dict(config.ranking_model)
            if provider.implementation == "theme_affinity"
            else None
        ),
    )
    (
        input_snapshot_id,
        _input_snapshot_attempt_id,
        accepted_item_themes_binding,
    ) = _wait_for_snapshot_id(
        spark,
        table=config.tables_write.scoring_input_snapshots,
        sources_table=config.tables_write.scoring_input_snapshot_sources,
        run_date=run_date,
        requested=INPUT_SNAPSHOT_ID,
        wait_seconds=int(READINESS_WAIT_SECONDS),
        poll_seconds=int(READINESS_POLL_SECONDS),
    )
    provider_build_id = build_provider_build_id(
        provider_id=provider.provider_id,
        provider_version=provider.provider_version,
        input_snapshot_id=input_snapshot_id,
        model_uri=MODEL_URI,
        invocation_checksum=invocation_checksum,
        run_date=run_date,
        scoring_foundation_build_id=SCORING_FOUNDATION_BUILD_ID,
    )
    provider_build_attempt_id = (
        f"{provider_build_id}:{task_run_id}:{execution_count}"
    )
    activated_at = datetime.now(timezone.utc)
    foundation_binding = _load_foundation_binding(
        spark,
        config=config,
        provider=provider,
        input_snapshot_id=input_snapshot_id,
        run_date=run_date,
        scoring_foundation_build_id=SCORING_FOUNDATION_BUILD_ID,
        scoring_foundation_build_attempt_id=(
            SCORING_FOUNDATION_BUILD_ATTEMPT_ID
        ),
    )
    if foundation_binding is None:
        item_themes_binding = accepted_item_themes_binding
        bindings = {"item_themes": item_themes_binding}
    else:
        item_themes_binding = foundation_binding["input_bindings"].get(
            "item_themes"
        )
        if not isinstance(item_themes_binding, dict):
            raise ValueError(
                "Scoring foundation has no exact item-theme input binding"
            )
        bindings = {
            "item_themes": item_themes_binding,
            "foundation": {
                key: value
                for key, value in foundation_binding.items()
                if key != "input_bindings"
            },
        }
    context = ProviderContext(
        context_slot=CONTEXT_SLOT,
        orchestration_run_id=int(ORCHESTRATION_RUN_ID),
        provider_id=provider.provider_id,
        provider_build_id=provider_build_id,
        provider_build_attempt_id=provider_build_attempt_id,
        input_snapshot_id=input_snapshot_id,
        run_date=run_date,
        model_uri=MODEL_URI,
        bindings_json=json.dumps(
            bindings,
            sort_keys=True,
            separators=(",", ":"),
        ),
        capability=provider.capability,
        use_case=USE_CASE,
        invocation_checksum=invocation_checksum,
        expires_at=activated_at + timedelta(hours=8),
        scoring_foundation_build_id=SCORING_FOUNDATION_BUILD_ID,
        scoring_foundation_build_attempt_id=(
            SCORING_FOUNDATION_BUILD_ATTEMPT_ID
        ),
    )
    if REUSE_INCOMPLETE_ATTEMPT:
        reusable = load_reusable_provider_context(
            spark,
            context_table=config.tables_write.score_provider_run_contexts,
            expected_context=context,
            execution_count=execution_count,
        )
        if reusable is not None:
            context = reusable
            provider_build_attempt_id = context.provider_build_attempt_id
            logger.info(
                "Reusing incomplete provider attempt %s",
                provider_build_attempt_id,
            )
    if EMIT_TASK_VALUES:
        task_values = get_dbutils().jobs.taskValues
        task_values.set(key="input_snapshot_id", value=input_snapshot_id)
        task_values.set(key="provider_build_id", value=provider_build_id)
        task_values.set(
            key="provider_build_attempt_id",
            value=provider_build_attempt_id,
        )
    pinned_item_themes(
        spark,
        context,
        input_table=config.tables_write.scoring_input_item_themes,
    )
    if ACTIVATE_CONTEXT:
        activate_provider_context(
            spark,
            context_table=config.tables_write.score_provider_run_contexts,
            context=context,
            task_run_id=task_run_id,
            execution_count=execution_count,
            activated_at=activated_at,
            allow_serial_run_takeover=ALLOW_SERIAL_RUN_TAKEOVER,
        )
    logger.info(
        "Activated %s for provider build %s and input %s",
        CONTEXT_SLOT,
        provider_build_id,
        input_snapshot_id,
    )
    return context


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--run_date"),
        parser.get_arg("--input_snapshot_id"),
        parser.get_arg("--model_uri"),
        parser.get_arg("--context_slot"),
        parser.get_arg("--task_run_id"),
        parser.get_arg("--execution_count"),
        parser.get_arg("--readiness_wait_seconds") or "1800",
        parser.get_arg("--readiness_poll_seconds") or "60",
        parser.get_arg("--provider_id") or "theme_affinity",
        parser.get_arg("--use_case") or "theme_ranking",
        parser.get_arg("--orchestration_run_id"),
        parser.get_arg("--scoring_foundation_build_id"),
        parser.get_arg("--scoring_foundation_build_attempt_id"),
        parser.has_arg("--allow-serial-run-takeover"),
        REUSE_INCOMPLETE_ATTEMPT=parser.has_arg("--reuse-incomplete-attempt"),
    )
