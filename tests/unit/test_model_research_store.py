from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.features.sql_contracts import extract_create_table_columns
from next_ads.model_development.research_contracts import (
    AutoMLDiscoveryReceipt,
    CandidateEvaluation,
    ModelResearchBuild,
    ModelSelectionDecision,
)
from next_ads.model_development.research_data import (
    RESEARCH_FRAME_COLUMNS,
    ResearchFrameSchemas,
)
from next_ads.model_development import research_store


NOW = datetime(2026, 8, 18, tzinfo=timezone.utc)
DIGEST = "a" * 64


def test_logical_and_attempt_ids_are_separate_and_deterministic():
    logical = research_store.research_build_id(
        model_definition_checksum="a" * 64,
        training_receipt_id="receipt-1",
        research_plan_checksum="b" * 64,
        evaluation_schema_version="binary/v1",
    )
    repeated = research_store.research_build_id(
        model_definition_checksum="a" * 64,
        training_receipt_id="receipt-1",
        research_plan_checksum="b" * 64,
        evaluation_schema_version="binary/v1",
    )
    first_attempt = research_store.attempt_id(
        logical_id=logical,
        invocation_id="job-run-1",
    )
    second_attempt = research_store.attempt_id(
        logical_id=logical,
        invocation_id="job-run-2",
    )

    assert logical == repeated
    assert first_attempt != second_attempt
    assert logical.startswith("research:")
    assert first_attempt.startswith("attempt:")


def test_research_table_contracts_use_composite_attempt_keys_and_no_raw_keys():
    for table, path in research_store.RESEARCH_TABLE_CONTRACTS.items():
        sql = Path(path).read_text()
        assert (
            f"CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{table}" in sql
        )

    build_sql = research_store.RESEARCH_TABLE_CONTRACTS[
        research_store.RESEARCH_BUILD_TABLE
    ].read_text()
    candidate_sql = research_store.RESEARCH_TABLE_CONTRACTS[
        research_store.CANDIDATE_EVALUATION_TABLE
    ].read_text()
    frame_sql = research_store.RESEARCH_TABLE_CONTRACTS[
        research_store.RESEARCH_FRAME_TABLE
    ].read_text()

    assert "PRIMARY KEY (research_build_id, research_attempt_id)" in build_sql
    assert (
        "PRIMARY KEY (candidate_evaluation_id, candidate_attempt_id)"
        in candidate_sql
    )
    assert "account_number" not in frame_sql
    assert "features_json STRING NOT NULL" in frame_sql
    assert "split STRING NOT NULL" in frame_sql


def test_receipt_dataclasses_match_their_sql_contracts():
    contracts = (
        (
            ModelResearchBuild,
            research_store.RESEARCH_BUILD_TABLE,
            {},
        ),
        (
            CandidateEvaluation,
            research_store.CANDIDATE_EVALUATION_TABLE,
            {"metrics": "metrics_json"},
        ),
        (
            ModelSelectionDecision,
            research_store.SELECTION_DECISION_TABLE,
            {},
        ),
        (
            AutoMLDiscoveryReceipt,
            research_store.AUTOML_DISCOVERY_TABLE,
            {},
        ),
    )
    for contract, table, renames in contracts:
        python_columns = {
            renames.get(field.name, field.name) for field in fields(contract)
        }
        sql_columns = {
            name
            for name, _definition in extract_create_table_columns(
                research_store.RESEARCH_TABLE_CONTRACTS[table].read_text()
            )
        }
        assert sql_columns == python_columns


def test_setup_creates_all_research_tables():
    class Spark:
        def __init__(self):
            self.queries = []

        def sql(self, query):
            self.queries.append(query)

    spark = Spark()
    paths = research_store.create_research_tables(
        spark,
        catalog="catalog",
        schema="schema",
    )

    expected_count = len(research_store.RESEARCH_TABLE_CONTRACTS)
    assert len(paths) == expected_count
    assert len(spark.queries) == expected_count
    assert all(
        "CREATE TABLE IF NOT EXISTS" in query for query in spark.queries
    )


def test_candidate_attempts_append_instead_of_replacing_failures(monkeypatch):
    writes = []
    stored = {}
    monkeypatch.setattr(
        research_store,
        "_existing_attempt",
        lambda *_args, **kwargs: stored.get(
            tuple(sorted(kwargs["keys"].items()))
        ),
    )
    monkeypatch.setattr(
        research_store,
        "typed_table_frame",
        lambda _spark, _table, rows: SimpleNamespace(
            columns=tuple(rows[0]), row=rows[0]
        ),
    )
    monkeypatch.setattr(
        research_store,
        "_merge_insert_only",
        lambda _spark, *, frame, keys, **_kwargs: (
            writes.append(
                {
                    "build_id": frame.row[keys[0]],
                    "attempt_id": frame.row[keys[1]],
                }
            ),
            stored.__setitem__(
                tuple(sorted((key, frame.row[key]) for key in keys)),
                frame.row,
            ),
        ),
    )
    values = {
        "candidate_evaluation_id": "candidate-logical",
        "research_build_id": "research-logical",
        "research_attempt_id": "research-attempt",
        "candidate_id": "random_forest",
        "candidate_spec_checksum": DIGEST,
        "required": True,
        "status": "FAILED",
        "created_at": NOW,
        "completed_at": NOW,
        "failure_reason": "fit failed",
    }
    for candidate_attempt_id in ("attempt-1", "attempt-2"):
        research_store.persist_candidate_evaluation(
            object(),
            catalog="catalog",
            schema="schema",
            evaluation=CandidateEvaluation(
                candidate_attempt_id=candidate_attempt_id,
                **values,
            ),
        )

    assert [write["attempt_id"] for write in writes] == [
        "attempt-1",
        "attempt-2",
    ]
    assert all("filters" not in write for write in writes)


def test_immutable_row_uses_insert_only_merge_and_post_write_identity_check(
    monkeypatch,
):
    row = {
        "logical_id": "logical",
        "attempt_id": "attempt",
        "status": "READY",
        "completed_at": NOW,
    }
    calls = []
    outputs = []
    stored = []
    monkeypatch.setattr(
        research_store,
        "_existing_attempt",
        lambda *_args, **_kwargs: stored[0] if stored else None,
    )
    monkeypatch.setattr(
        research_store,
        "typed_table_frame",
        lambda *_args, **_kwargs: SimpleNamespace(columns=tuple(row), row=row),
    )

    def merge(*_args, **kwargs):
        calls.append(kwargs)
        stored.append(row)

    monkeypatch.setattr(research_store, "_merge_insert_only", merge)
    monkeypatch.setattr(
        research_store,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )

    research_store._persist_immutable_row(
        object(),
        table="catalog.schema.table",
        row=row,
        keys=("logical_id", "attempt_id"),
        operation="test",
    )

    assert len(calls) == 1
    assert calls[0]["keys"] == ("logical_id", "attempt_id")
    assert outputs == [
        {
            "destination": "catalog.schema.table",
            "kind": "delta_table",
            "details": {
                "operation": "test",
                "reused": False,
                "status": "READY",
            },
        }
    ]


def test_existing_attempt_cannot_be_changed():
    row = {
        "logical_id": "logical",
        "attempt_id": "attempt",
        "status": "FAILED",
        "completed_at": NOW,
    }

    class Spark:
        pass

    original = research_store._existing_attempt
    try:
        research_store._existing_attempt = lambda *_args, **_kwargs: {
            **row,
            "status": "READY",
        }
        with pytest.raises(ValueError, match="immutable"):
            research_store._persist_immutable_row(
                Spark(),
                table="catalog.schema.table",
                row=row,
                keys=("logical_id", "attempt_id"),
                operation="test",
            )
    finally:
        research_store._existing_attempt = original


def test_identical_attempt_accepts_spark_timestamp_round_trip(monkeypatch):
    writes = []
    outputs = []
    row = {
        "logical_id": "logical",
        "attempt_id": "attempt",
        "status": "READY",
        "completed_at": NOW,
    }
    monkeypatch.setattr(
        research_store,
        "_existing_attempt",
        lambda *_args, **_kwargs: {
            **row,
            "completed_at": NOW.replace(tzinfo=None),
        },
    )
    monkeypatch.setattr(
        research_store,
        "atomic_append_by_name",
        lambda *_args, **_kwargs: writes.append(_kwargs),
    )
    monkeypatch.setattr(
        research_store,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )

    result = research_store._persist_immutable_row(
        object(),
        table="catalog.schema.table",
        row=row,
        keys=("logical_id", "attempt_id"),
        operation="test",
    )

    assert result == "catalog.schema.table"
    assert writes == []
    assert outputs == [
        {
            "destination": "catalog.schema.table",
            "kind": "delta_table",
            "details": {
                "operation": "test",
                "reused": True,
                "status": "READY",
            },
        }
    ]


def test_research_frame_returns_exact_delta_binding(monkeypatch):
    frame = SimpleNamespace(columns=RESEARCH_FRAME_COLUMNS)
    monkeypatch.setattr(
        research_store,
        "validate_replace_source_scope",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        research_store,
        "validate_unique_non_null_keys",
        lambda *_args, **_kwargs: SimpleNamespace(row_count=7),
    )
    monkeypatch.setattr(
        research_store,
        "feature_value_checksum",
        lambda *_args, **_kwargs: "c" * 64,
    )
    monkeypatch.setattr(
        research_store,
        "schema_checksum",
        lambda _frame: "b" * 64,
    )
    monkeypatch.setattr(
        research_store,
        "find_delta_write_receipt",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        research_store,
        "_attempt_has_rows",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        research_store,
        "atomic_append_by_name",
        lambda *_args, **_kwargs: DeltaWriteReceipt(
            statement="",
            attempts=1,
            receipt_id="write-1",
            target_table="catalog.schema.table",
            delta_version=12,
            row_count=7,
            schema_checksum="b" * 64,
        ),
    )
    schemas = ResearchFrameSchemas(
        feature_schema_json=StructType(
            [StructField("advert_ctr", DoubleType(), True)]
        ).json(),
        slice_schema_json=StructType([]).json(),
    )
    binding = research_store.persist_research_frame(
        object(),
        frame,
        catalog="catalog",
        schema="schema",
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        training_receipt_id="training-receipt",
        schemas=schemas,
        git_commit="abc123",
    )

    assert binding.research_frame_delta_version == 12
    assert binding.research_frame_row_count == 7
    assert binding.research_frame_data_checksum == "c" * 64
    assert binding.research_frame_write_receipt_id == "write-1"
    assert (
        binding.research_frame_feature_schema_json
        == schemas.feature_schema_json
    )


def test_research_frame_verified_retry_logs_exact_reused_table(monkeypatch):
    class _Predicate:
        def __eq__(self, _other):
            return self

        def __and__(self, _other):
            return self

    class _StoredFrame:
        def where(self, _condition):
            return self

        def count(self):
            return 7

    frame = SimpleNamespace(columns=RESEARCH_FRAME_COLUMNS)
    stored_frame = _StoredFrame()
    spark = SimpleNamespace(table=lambda _target: stored_frame)
    receipt = DeltaWriteReceipt(
        statement="",
        attempts=1,
        receipt_id="write-1",
        target_table="catalog.schema.next_uk_nextads_model_research_frames",
        delta_version=12,
        row_count=7,
        schema_checksum="b" * 64,
    )
    outputs = []
    monkeypatch.setattr(
        research_store,
        "validate_replace_source_scope",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        research_store,
        "validate_unique_non_null_keys",
        lambda *_args, **_kwargs: SimpleNamespace(row_count=7),
    )
    monkeypatch.setattr(
        research_store,
        "feature_value_checksum",
        lambda *_args, **_kwargs: "c" * 64,
    )
    monkeypatch.setattr(
        research_store,
        "schema_checksum",
        lambda _frame: "b" * 64,
    )
    monkeypatch.setattr(
        research_store,
        "find_delta_write_receipt",
        lambda *_args, **_kwargs: receipt,
    )
    monkeypatch.setattr(
        research_store,
        "atomic_append_by_name",
        lambda *_args, **_kwargs: pytest.fail("retry must reuse its receipt"),
    )
    monkeypatch.setattr(
        research_store,
        "log_output_location",
        lambda destination, **kwargs: outputs.append(
            {"destination": destination, **kwargs}
        ),
    )
    import pyspark.sql.functions as spark_functions

    monkeypatch.setattr(spark_functions, "col", lambda _name: _Predicate())
    monkeypatch.setattr(spark_functions, "lit", lambda _value: _Predicate())
    schemas = ResearchFrameSchemas(
        feature_schema_json=StructType(
            [StructField("advert_ctr", DoubleType(), True)]
        ).json(),
        slice_schema_json=StructType([]).json(),
    )

    binding = research_store.persist_research_frame(
        spark,
        frame,
        catalog="catalog",
        schema="schema",
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        training_receipt_id="training-receipt",
        schemas=schemas,
        git_commit="abc123",
    )

    assert binding.research_frame_write_receipt_id == "write-1"
    assert outputs == [
        {
            "destination": (
                "catalog.schema.next_uk_nextads_model_research_frames"
            ),
            "kind": "delta_table",
            "details": {
                "delta_version": 12,
                "operation": "model_research_frame",
                "receipt_id": "write-1",
                "reused": True,
                "row_count": 7,
            },
        }
    ]


def test_selected_test_requires_exact_persisted_selection(monkeypatch):
    binding = research_store.ResearchFrameBinding(
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        training_receipt_id="training-receipt",
        research_frame_table="catalog.schema.research_frames",
        research_frame_delta_version=12,
        research_frame_row_count=7,
        research_frame_schema_checksum="b" * 64,
        research_frame_data_checksum="c" * 64,
        research_frame_write_receipt_id="write-1",
        research_frame_feature_schema_json="{}",
        research_frame_slice_schema_json="{}",
    )
    decision = ModelSelectionDecision(
        selection_decision_id="decision-logical",
        selection_attempt_id="decision-attempt",
        research_build_id=binding.research_build_id,
        research_attempt_id=binding.research_attempt_id,
        selection_mode="AUTO",
        recommended_candidate_id="random_forest",
        selected_candidate_id="random_forest",
        selected_candidate_evaluation_id="candidate-evaluation",
        reason="deterministic recommendation",
        status="READY",
        created_at=NOW,
        completed_at=NOW,
        model_build_id="model-build",
        registered_model_name="catalog.schema.model",
        decision_code_sha="decision-sha",
    )
    source = object()
    selected = object()
    monkeypatch.setattr(
        research_store,
        "load_ready_selection_decision",
        lambda *_args, **_kwargs: decision,
    )
    monkeypatch.setattr(
        research_store,
        "read_unpacked_research_frame",
        lambda *_args, **_kwargs: source,
    )
    monkeypatch.setattr(
        research_store,
        "selected_test_partition",
        lambda frame, **kwargs: (
            selected
            if frame is source
            and kwargs["selection_decision_id"]
            == decision.selection_decision_id
            else None
        ),
    )

    result = research_store.read_selected_test_frame(
        object(),
        catalog="catalog",
        schema="schema",
        binding=binding,
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id=decision.selected_candidate_id,
        selected_candidate_evaluation_id=(
            decision.selected_candidate_evaluation_id
        ),
    )

    assert result is selected


def test_ready_selection_for_research_attempt_requires_at_most_one(
    monkeypatch,
):
    decision = ModelSelectionDecision(
        selection_decision_id="decision-logical",
        selection_attempt_id="decision-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        selection_mode="REVIEW_REQUIRED",
        recommended_candidate_id="logistic_regression",
        selected_candidate_id="random_forest",
        selected_candidate_evaluation_id="candidate-evaluation",
        reason="Reviewed validation evidence",
        status="READY",
        created_at=NOW,
        completed_at=NOW,
        model_build_id="model-build",
        reviewed_by="reviewer@example.com",
        registered_model_name="catalog.schema.model",
        decision_code_sha="decision-sha",
    )
    rows = [dict(vars(decision))]
    monkeypatch.setattr(
        research_store,
        "_ready_selection_rows_for_research_attempt",
        lambda *_args, **_kwargs: rows,
    )

    loaded = research_store.load_ready_selection_for_research_attempt(
        object(),
        catalog="catalog",
        schema="schema",
        research_build_id=decision.research_build_id,
        research_attempt_id=decision.research_attempt_id,
    )

    assert loaded == decision
    rows.append(dict(vars(decision)))
    with pytest.raises(ValueError, match="More than one READY selection"):
        research_store.load_ready_selection_for_research_attempt(
            object(),
            catalog="catalog",
            schema="schema",
            research_build_id=decision.research_build_id,
            research_attempt_id=decision.research_attempt_id,
        )


def test_selected_test_rejects_missing_or_different_persisted_selection(
    monkeypatch,
):
    binding = research_store.ResearchFrameBinding(
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        training_receipt_id="training-receipt",
        research_frame_table="catalog.schema.research_frames",
        research_frame_delta_version=12,
        research_frame_row_count=7,
        research_frame_schema_checksum="b" * 64,
        research_frame_data_checksum="c" * 64,
        research_frame_write_receipt_id="write-1",
        research_frame_feature_schema_json="{}",
        research_frame_slice_schema_json="{}",
    )
    monkeypatch.setattr(
        research_store,
        "load_ready_selection_decision",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(ValueError, match="persisted READY selection"):
        research_store.read_selected_test_frame(
            object(),
            catalog="catalog",
            schema="schema",
            binding=binding,
            selection_decision_id="decision-logical",
            selected_candidate_id="random_forest",
            selected_candidate_evaluation_id="candidate-evaluation",
        )
