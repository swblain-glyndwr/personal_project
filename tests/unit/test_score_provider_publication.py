import inspect
import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import next_ads.ranking.provider_publication as publication
from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.ranking.provider_context import ProviderContext
from next_ads.ranking.provider_publication import (
    PROVIDER_BUILD_COLUMNS,
    PROVIDER_SIGNAL_COLUMNS,
    publish_provider_build,
    register_ready_provider_build,
    stage_provider_signals,
    validate_provider_publication_contract,
)


RUN_DATE = date(2026, 8, 3)
NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
SIGNALS_TABLE = "catalog.schema.score_provider_signals"
BUILDS_TABLE = "catalog.schema.score_provider_builds"


def _context() -> ProviderContext:
    return ProviderContext(
        context_slot="theme_affinity_serving",
        orchestration_run_id=123,
        provider_id="theme_affinity",
        provider_build_id="theme-affinity-build",
        provider_build_attempt_id="theme-affinity-build:task:0",
        input_snapshot_id="scoring-inputs-20260803",
        run_date=RUN_DATE,
        model_uri="models:/catalog.schema.theme_affinity/7",
        bindings_json=json.dumps(
            {
                "foundation": {
                    "pipeline_update_id": None,
                    "scoring_foundation_build_id": "foundation-build",
                    "scoring_foundation_build_attempt_id": (
                        "foundation-build:task:0"
                    ),
                    "outputs": {},
                }
            }
        ),
        capability="account_theme",
        use_case="theme_ranking",
        invocation_checksum="provider-invocation-checksum",
        expires_at=NOW + timedelta(hours=8),
        scoring_foundation_build_id="foundation-build",
        scoring_foundation_build_attempt_id="foundation-build:task:0",
    )


def _receipt() -> DeltaWriteReceipt:
    context = _context()
    return DeltaWriteReceipt(
        statement="replace signals",
        attempts=1,
        receipt_id="receipt-signals",
        target_table=SIGNALS_TABLE,
        delta_version=42,
        row_count=100,
        schema_checksum="signal-schema",
        build_id=context.provider_build_id,
        attempt_id=context.provider_build_attempt_id,
        git_commit="abc123",
        write_duration_ms=1200,
    )


def _schema(columns, *, nullable=()):
    types = {
        "RunDate": DateType(),
        "RawScore": DoubleType(),
        "Score": DoubleType(),
        "ProviderRank": IntegerType(),
        "OutputDeltaVersion": LongType(),
        "RowCount": LongType(),
        "WriteDurationMs": LongType(),
        "RetryCount": IntegerType(),
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


def test_nullable_provider_manifest_contract_fails_before_scoring():
    schemas = {
        SIGNALS_TABLE: _schema(PROVIDER_SIGNAL_COLUMNS),
        BUILDS_TABLE: _schema(
            PROVIDER_BUILD_COLUMNS,
            nullable=(
                "ModelName",
                "ModelVersion",
                "ModelURI",
                "PipelineUpdateID",
                "OutputSnapshotID",
                "OutputTable",
                "OutputDeltaVersion",
                "OutputSchemaChecksum",
                "WriteReceiptID",
                "ScoringFoundationBuildID",
                "ScoringFoundationBuildAttemptID",
            ),
        ),
    }
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda table: table in schemas),
        table=lambda table: SimpleNamespace(schema=schemas[table]),
    )
    validate_provider_publication_contract(
        spark,
        signals_table=SIGNALS_TABLE,
        builds_table=BUILDS_TABLE,
    )

    schemas[BUILDS_TABLE] = _schema(PROVIDER_BUILD_COLUMNS)
    with pytest.raises(ValueError, match="must allow null"):
        validate_provider_publication_contract(
            spark,
            signals_table=SIGNALS_TABLE,
            builds_table=BUILDS_TABLE,
        )


def test_provider_signals_are_written_once_and_repair_reuses_receipt(
    monkeypatch,
):
    context = _context()
    frame = SimpleNamespace(
        columns=list(PROVIDER_SIGNAL_COLUMNS),
        select=lambda *_columns: frame,
    )
    writes = []
    monkeypatch.setattr(
        publication, "_validate_signal_contract", lambda *_a: None
    )
    monkeypatch.setattr(
        publication, "find_delta_write_receipt", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        publication,
        "replace_scope_by_name",
        lambda *_a, **_k: writes.append("signals") or _receipt(),
    )

    receipt = stage_provider_signals(
        "spark",
        frame,
        context=context,
        table=SIGNALS_TABLE,
        git_commit="abc123",
    )
    assert receipt.delta_version == 42
    assert writes == ["signals"]

    monkeypatch.setattr(
        publication,
        "find_delta_write_receipt",
        lambda *_a, **_k: _receipt(),
    )
    stage_provider_signals(
        "spark",
        frame,
        context=context,
        table=SIGNALS_TABLE,
        git_commit="abc123",
    )
    assert writes == ["signals"]


def test_ready_provider_manifest_uses_target_schema_and_is_last(monkeypatch):
    operations = []
    captured = {}

    def typed(_spark, table, rows):
        captured[table] = rows
        return SimpleNamespace(columns=list(rows[0]))

    monkeypatch.setattr(publication, "typed_table_frame", typed)
    monkeypatch.setattr(
        publication,
        "replace_scope_by_name",
        lambda _frame, table, *_a, **_k: operations.append(table),
    )
    build = register_ready_provider_build(
        "spark",
        context=_context(),
        receipt=_receipt(),
        builds_table=BUILDS_TABLE,
        provider_config={"provider_version": "theme_affinity/v1"},
        contract_version="account_entity_scores/v1",
        git_commit="abc123",
        task_run_id=456,
        execution_count=0,
        completed_at=NOW,
    )

    assert operations == [BUILDS_TABLE]
    assert captured[BUILDS_TABLE][0]["PipelineUpdateID"] is None
    assert captured[BUILDS_TABLE][0]["WriteReceiptID"] == "receipt-signals"
    assert build.output_delta_version == 42


def test_provider_publish_binds_receipt_without_reading_signals(monkeypatch):
    operations = []
    monkeypatch.setattr(
        publication,
        "find_delta_write_receipt",
        lambda *_a, **_k: _receipt(),
    )
    monkeypatch.setattr(
        publication,
        "register_ready_provider_build",
        lambda *_a, **_k: operations.append("ready") or "build",
    )

    result = publish_provider_build(
        "spark",
        context=_context(),
        signals_table=SIGNALS_TABLE,
        signals_delta_version=42,
        builds_table=BUILDS_TABLE,
        provider_config={
            "provider_id": "theme_affinity",
            "provider_version": "theme_affinity/v1",
            "capability": "account_theme",
            "entity_type": "theme",
        },
        contract_version="account_entity_scores/v1",
        git_commit="abc123",
        task_run_id=456,
        execution_count=0,
        completed_at=NOW,
    )

    assert operations == ["ready"]
    assert result.build == "build"
    assert result.compatibility_output_versions == {}


def test_provider_publisher_has_no_full_frame_scans_or_driver_write():
    source = inspect.getsource(publication)
    assert "countDistinct" not in source
    assert "to_json" not in source
    assert ".cache(" not in source
    assert "coalesce(1)" not in source
