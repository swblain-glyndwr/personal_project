import inspect
import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import next_ads.ranking.foundation_publication as publication
from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.ranking.foundation_context import ScoringFoundationContext
from next_ads.ranking.foundation_publication import (
    FOUNDATION_BUILD_COLUMNS,
    FOUNDATION_OUTPUT_COLUMNS,
    FoundationOutputSpec,
    foundation_output_bindings_json,
    publish_required_foundation_outputs,
    register_ready_foundation,
    validate_foundation_build_marker,
    validate_foundation_output_manifest_contract,
)
from next_ads.ranking.scoring_manifest import ScoringFoundationOutput


RUN_DATE = date(2026, 8, 3)
NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)


def _context():
    return ScoringFoundationContext(
        context_slot="account_theme_features_v2",
        orchestration_run_id=123,
        foundation_id="account_theme_features",
        foundation_version="account_theme_features/v2",
        scoring_foundation_build_id="foundation-build",
        scoring_foundation_build_attempt_id="foundation-build:456:0",
        input_snapshot_id="input-snapshot",
        input_snapshot_attempt_id="input-snapshot:task:0",
        run_date=RUN_DATE,
        bindings_json='{"item_themes":{"delta_version":42}}',
        capability="account_theme",
        contract_version="account_theme_foundation/v1",
        invocation_checksum="checksum",
        expires_at=NOW + timedelta(hours=8),
    )


def _output(version=42):
    return ScoringFoundationOutput(
        scoring_foundation_build_id="foundation-build",
        scoring_foundation_build_attempt_id="foundation-build:456:0",
        run_date=RUN_DATE,
        output_name="ranked",
        source_table="catalog.pipeline.ranked",
        source_delta_version=None,
        source_schema_checksum="schema-ranked",
        output_table="catalog.schema.ranked",
        output_delta_version=version,
        output_schema_version="account_theme_ranked/v1",
        output_schema_checksum="schema-ranked",
        is_required=True,
        row_count=10,
        write_receipt_id="receipt-ranked",
        git_commit="abc123",
        write_duration_ms=1200,
        retry_count=0,
        published_at=NOW,
    )


def _manifest_schema(columns, *, nullable=()):
    types = {
        "RunDate": DateType(),
        "SourceDeltaVersion": LongType(),
        "OutputDeltaVersion": LongType(),
        "IsRequired": BooleanType(),
        "RowCount": LongType(),
        "WriteDurationMs": LongType(),
        "RetryCount": IntegerType(),
        "PublishedAt": TimestampType(),
        "PipelineTaskRunID": LongType(),
        "WarningCount": LongType(),
        "TaskRunID": LongType(),
        "ExecutionCount": IntegerType(),
        "CompletedAt": TimestampType(),
    }
    return StructType(
        [
            StructField(
                column,
                types.get(column, StringType()),
                nullable=column in nullable,
            )
            for column in columns
        ]
    )


def test_foundation_marker_must_match_the_exact_pipeline_attempt():
    context = _context()
    marker = {
        "ContextSlot": context.context_slot,
        "OrchestrationRunID": context.orchestration_run_id,
        "FoundationID": context.foundation_id,
        "FoundationVersion": context.foundation_version,
        "ScoringFoundationBuildID": context.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            context.scoring_foundation_build_attempt_id
        ),
        "InputSnapshotID": context.input_snapshot_id,
        "InputSnapshotAttemptID": context.input_snapshot_attempt_id,
        "RunDate": context.run_date,
        "InvocationChecksum": context.invocation_checksum,
    }
    spark = SimpleNamespace(
        table=lambda _table: SimpleNamespace(collect=lambda: [marker])
    )
    validate_foundation_build_marker(
        spark,
        context=context,
        marker_table="catalog.schema.marker",
    )
    marker["FoundationVersion"] = "other"
    with pytest.raises(ValueError, match="FoundationVersion"):
        validate_foundation_build_marker(
            spark,
            context=context,
            marker_table="catalog.schema.marker",
        )


def test_foundation_manifest_nullable_fields_are_checked_before_data_copy():
    schemas = {
        "outputs": _manifest_schema(
            FOUNDATION_OUTPUT_COLUMNS,
            nullable=("SourceDeltaVersion",),
        ),
        "builds": _manifest_schema(
            FOUNDATION_BUILD_COLUMNS,
            nullable=("PipelineUpdateID", "PipelineUpdateType"),
        ),
    }
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda table: table in schemas),
        table=lambda table: SimpleNamespace(schema=schemas[table]),
    )
    validate_foundation_output_manifest_contract(
        spark,
        outputs_table="outputs",
        builds_table="builds",
        pipeline_relations=True,
    )

    schemas["outputs"] = _manifest_schema(FOUNDATION_OUTPUT_COLUMNS)
    with pytest.raises(ValueError, match="must allow null"):
        validate_foundation_output_manifest_contract(
            spark,
            outputs_table="outputs",
            builds_table="builds",
            pipeline_relations=True,
        )


def test_ranked_foundation_is_written_once_and_repair_reuses_its_receipt(
    monkeypatch,
):
    schema = StructType(
        [
            StructField("reference_date", DateType()),
            StructField("account_number", StringType()),
            StructField("theme_clean", StringType()),
        ]
    )
    frame = SimpleNamespace(
        columns=[field.name for field in schema], schema=schema
    )
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda _table: True),
        table=lambda _table: frame,
    )
    writes = []
    receipt = DeltaWriteReceipt(
        statement="replace ranked",
        attempts=1,
        receipt_id="receipt-ranked",
        target_table="catalog.target.ranked",
        delta_version=42,
        row_count=10,
        schema_checksum=publication.schema_checksum(frame),
        build_id="foundation-build",
        attempt_id="foundation-build:456:0",
        git_commit="abc123",
    )
    monkeypatch.setattr(
        publication, "find_delta_write_receipt", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        publication,
        "replace_table_by_name",
        lambda *_a, **_k: writes.append("ranked") or receipt,
    )
    spec = FoundationOutputSpec(
        output_name="ranked",
        source_table="catalog.pipeline.ranked",
        target_table="catalog.target.ranked",
        output_schema_version="account_theme_ranked/v1",
        key_columns=("reference_date", "account_number", "theme_clean"),
        account_column="account_number",
        entity_column="theme_clean",
        source_kind="pipeline_relation",
    )

    outputs = publish_required_foundation_outputs(
        spark,
        context=_context(),
        output_specs=(spec,),
        git_commit="abc123",
    )
    assert writes == ["ranked"]
    assert outputs[0].output_delta_version == 42

    monkeypatch.setattr(
        publication,
        "find_delta_write_receipt",
        lambda *_a, **_k: receipt,
    )
    publish_required_foundation_outputs(
        spark,
        context=_context(),
        output_specs=(spec,),
        git_commit="abc123",
    )
    assert writes == ["ranked"]


def test_ready_foundation_manifest_is_typed_and_written_last(monkeypatch):
    operations = []
    captured_rows = {}

    def typed(_spark, table, rows):
        captured_rows[table] = rows
        return SimpleNamespace(columns=list(rows[0]))

    monkeypatch.setattr(publication, "typed_table_frame", typed)
    monkeypatch.setattr(
        publication,
        "replace_scope_by_name",
        lambda _frame, table, *_a, **_k: operations.append(table),
    )
    build = register_ready_foundation(
        "spark",
        context=_context(),
        outputs=(_output(),),
        required_output_names=("ranked",),
        pipeline_update_id=None,
        pipeline_id="pipeline-1",
        pipeline_update_type=None,
        builds_table="builds",
        outputs_table="outputs",
        task_run_id=456,
        execution_count=0,
        pipeline_task_run_id=789,
        git_commit="abc123",
        completed_at=NOW,
    )

    assert operations == ["outputs", "builds"]
    assert captured_rows["builds"][0]["PipelineUpdateID"] is None
    assert captured_rows["builds"][0]["GitCommit"] == "abc123"
    assert build.status == "READY_FOR_PROVIDERS"
    bindings = json.loads(foundation_output_bindings_json(build))
    assert bindings["foundation"]["outputs"]["ranked"]["delta_version"] == 42


def test_critical_foundation_publisher_has_no_whole_table_content_scans():
    source = inspect.getsource(publication)
    assert "countDistinct" not in source
    assert "to_json" not in source
    assert ".cache(" not in source
