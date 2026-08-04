import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pyspark.sql.types import DateType, StringType, StructField, StructType

import next_ads.ranking.foundation_publication as publication
from next_ads.ranking.foundation_context import ScoringFoundationContext
from next_ads.ranking.foundation_publication import (
    FoundationOutputSpec,
    FoundationOutputSummary,
    foundation_output_bindings_json,
    publish_required_foundation_outputs,
    register_ready_foundation,
    validate_foundation_build_marker,
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


def _output(name, version):
    return ScoringFoundationOutput(
        scoring_foundation_build_id="foundation-build",
        scoring_foundation_build_attempt_id="foundation-build:456:0",
        run_date=RUN_DATE,
        output_name=name,
        source_table=f"catalog.pipeline.{name}",
        source_delta_version=40,
        source_schema_checksum=f"schema-{name}",
        output_table=f"catalog.schema.{name}",
        output_delta_version=version,
        output_schema_version=f"account_theme_{name}/v1",
        output_schema_checksum=f"schema-{name}",
        is_required=True,
        row_count=10,
        account_count=2,
        entity_count=5,
        null_key_count=0,
        duplicate_key_count=0,
        invalid_value_count=0,
        output_checksum=f"checksum-{name}",
        published_at=NOW,
    )


class _Frame:
    columns = ["reference_date", "account_number", "theme_clean", "rundate"]
    schema = StructType(
        [
            StructField("reference_date", DateType()),
            StructField("account_number", StringType()),
            StructField("theme_clean", StringType()),
            StructField("rundate", DateType()),
        ]
    )

    def persist(self, _level):
        return self

    def unpersist(self):
        return self


class _Spark:
    def __init__(self):
        self.catalog = SimpleNamespace(tableExists=lambda _table: True)
        self.created = []

    def table(self, _table):
        return _Frame()

    def createDataFrame(self, rows):  # noqa: N802
        frame = SimpleNamespace(columns=list(rows[0]), rows=rows)
        self.created.append(frame)
        return frame


class _MarkerSpark:
    def __init__(self, rows):
        self.rows = rows

    def table(self, table):
        assert table == "catalog.schema.build_marker"
        return SimpleNamespace(collect=lambda: self.rows)


def _marker_row(**overrides):
    context = _context()
    values = {
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
    values.update(overrides)
    return values


def test_foundation_marker_must_match_the_exact_pipeline_attempt():
    validate_foundation_build_marker(
        _MarkerSpark([_marker_row()]),
        context=_context(),
        marker_table="catalog.schema.build_marker",
    )
    with pytest.raises(ValueError, match="FoundationVersion"):
        validate_foundation_build_marker(
            _MarkerSpark([_marker_row(FoundationVersion="other")]),
            context=_context(),
            marker_table="catalog.schema.build_marker",
        )


def test_required_outputs_publish_to_exact_deterministic_delta_versions(
    monkeypatch,
):
    writes = []
    monkeypatch.setattr(
        publication,
        "summarise_foundation_output",
        lambda *_args, **_kwargs: FoundationOutputSummary(
            row_count=10,
            account_count=2,
            entity_count=5,
            null_key_count=0,
            duplicate_key_count=0,
            invalid_value_count=0,
            output_checksum="checksum",
        ),
    )
    monkeypatch.setattr(
        publication,
        "replace_table_by_name",
        lambda _frame, table, _columns, *, spark: writes.append(table),
    )
    source_versions = {
        "catalog.source.complete": 31,
        "catalog.source.ranked": 32,
    }
    target_versions = {
        "catalog.target.complete": iter((40, 41)),
        "catalog.target.ranked": iter((41, 42)),
    }

    def latest_version(_spark, table):
        if table in source_versions:
            return source_versions[table]
        return next(target_versions[table])

    monkeypatch.setattr(
        publication,
        "latest_delta_version",
        latest_version,
    )
    monkeypatch.setattr(
        publication,
        "read_delta_version",
        lambda _spark, _table, _version: _Frame(),
    )
    specs = (
        FoundationOutputSpec(
            "complete",
            "catalog.source.complete",
            "catalog.target.complete",
            "account_theme_complete/v1",
            ("reference_date", "account_number", "theme_clean"),
            "account_number",
            "theme_clean",
        ),
        FoundationOutputSpec(
            "ranked",
            "catalog.source.ranked",
            "catalog.target.ranked",
            "account_theme_ranked/v1",
            ("reference_date", "account_number", "theme_clean"),
            "account_number",
            "theme_clean",
        ),
    )

    outputs = publish_required_foundation_outputs(
        _Spark(),
        context=_context(),
        output_specs=specs,
    )

    assert [output.output_name for output in outputs] == ["complete", "ranked"]
    assert [output.output_delta_version for output in outputs] == [41, 42]
    assert [output.source_delta_version for output in outputs] == [31, 32]
    assert set(writes) == {"catalog.target.complete", "catalog.target.ranked"}


def test_output_failure_is_propagated_before_any_ready_manifest(monkeypatch):
    def fail_one(_spark, *, context, spec):
        if spec.output_name == "ranked":
            raise RuntimeError("ranked failed")
        return _output(spec.output_name, 1)

    monkeypatch.setattr(publication, "_publish_one_output", fail_one)
    specs = tuple(
        FoundationOutputSpec(
            name,
            f"catalog.source.{name}",
            f"catalog.target.{name}",
            f"{name}/v1",
            ("reference_date", "account_number", "theme_clean"),
            "account_number",
            "theme_clean",
        )
        for name in ("complete", "ranked")
    )

    with pytest.raises(RuntimeError, match="ranked failed"):
        publish_required_foundation_outputs(
            _Spark(),
            context=_context(),
            output_specs=specs,
        )


def test_ready_manifest_is_written_after_its_exact_output_bindings(monkeypatch):
    operations = []

    def replace(frame, table, scope, columns, *, spark):
        operations.append((table, scope, tuple(columns), frame.rows))

    monkeypatch.setattr(publication, "replace_scope_by_name", replace)
    spark = _Spark()
    outputs = (_output("complete", 41), _output("ranked", 42))

    build = register_ready_foundation(
        spark,
        context=_context(),
        outputs=outputs,
        required_output_names=("complete", "ranked"),
        pipeline_id="pipeline-123",
        pipeline_update_id=None,
        pipeline_update_type=None,
        builds_table="catalog.schema.foundation_builds",
        outputs_table="catalog.schema.foundation_outputs",
        task_run_id=456,
        execution_count=0,
        pipeline_task_run_id=321,
        completed_at=NOW,
    )

    assert [operation[0] for operation in operations] == [
        "catalog.schema.foundation_outputs",
        "catalog.schema.foundation_builds",
    ]
    assert build.pipeline_update_id is None
    assert build.pipeline_id == "pipeline-123"
    assert build.pipeline_update_type is None
    assert build.pipeline_task_run_id == 321
    bindings = json.loads(foundation_output_bindings_json(build))["foundation"]
    assert bindings["scoring_foundation_build_id"] == "foundation-build"
    assert bindings["outputs"]["complete"]["delta_version"] == 41
    assert bindings["outputs"]["ranked"]["delta_version"] == 42
