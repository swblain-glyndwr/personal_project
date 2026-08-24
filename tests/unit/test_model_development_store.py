from datetime import date, datetime, timezone
from types import SimpleNamespace

from next_ads.model_development.contracts import (
    DBR_15_4_SPARK_CPU,
    ModelBuild,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)
from next_ads.model_development import store
from next_ads.model_development.external_outputs import (
    ExternalModelComponent,
    ExternalScoreOutputReceipt,
)


NOW = datetime(2026, 8, 17, tzinfo=timezone.utc)


def _receipt():
    return TrainingSetReceipt(
        receipt_id="receipt-1",
        model_name="analytics_pctr",
        model_definition_checksum="a" * 64,
        feature_bindings=(
            TrainingFeatureBinding(
                feature_id="pctr",
                feature_snapshot_id="snapshot",
                feature_snapshot_attempt_id="attempt",
                backing_table="catalog.schema.pctr",
                delta_version=3,
                row_count=10,
                schema_checksum="b" * 64,
                value_checksum="c" * 64,
            ),
        ),
        observation_start=date(2026, 8, 11),
        observation_end=date(2026, 8, 11),
        label_end=date(2026, 8, 11),
        schema_checksum="d" * 64,
        data_checksum="e" * 64,
        code_sha="abc123",
        leakage_status="PASS",
        status="READY",
        created_at=NOW,
        completed_at=NOW,
    )


def _build():
    return ModelBuild(
        model_build_id="build-1",
        model_name="analytics_pctr",
        training_receipt_id="receipt-1",
        model_definition_checksum="a" * 64,
        runtime_profile=DBR_15_4_SPARK_CPU,
        status="READY",
        created_at=NOW,
        mlflow_run_id="run-1",
        registered_model_name="catalog.schema.model",
        registered_model_version=4,
        model_uri="models:/catalog.schema.model/4",
        artifact_digest="f" * 64,
        metrics=(("auc_pr", 0.42),),
        completed_at=NOW,
    )


def _external_receipt():
    return ExternalScoreOutputReceipt(
        receipt_id="external-1",
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table="catalog.schema.predictions",
        source_delta_version=5,
        run_date=date(2026, 8, 11),
        row_count=100,
        schema_checksum="f" * 64,
        producing_run_id="run-5",
        components=(
            ExternalModelComponent(
                role="classifier",
                model_uri="models:/catalog.schema.classifier/2",
                expected_run_id="run-classifier",
            ),
        ),
        created_at=NOW,
    )


def test_setup_creates_receipts_builds_and_scoped_evaluation_candidates():
    class Spark:
        def __init__(self):
            self.queries = []

        def sql(self, query):
            self.queries.append(query)

        def table(self, _table):
            fields = [
                SimpleNamespace(name=column)
                for column in store.MODEL_BUILD_RESEARCH_COLUMNS
            ]
            return SimpleNamespace(schema=SimpleNamespace(fields=fields))

    spark = Spark()
    paths = store.create_model_development_tables(
        spark, catalog="marketingdata_dev", schema="Stephen_Blain"
    )

    assert paths == (
        "marketingdata_dev.Stephen_Blain."
        "next_uk_nextads_training_set_receipts",
        "marketingdata_dev.Stephen_Blain.next_uk_nextads_model_builds",
        "marketingdata_dev.Stephen_Blain."
        "next_uk_nextads_external_score_receipts",
        "marketingdata_dev.Stephen_Blain."
        "next_uk_nextads_model_evaluation_candidates",
    )
    assert len(spark.queries) == 4
    assert all(
        "CREATE TABLE IF NOT EXISTS" in query for query in spark.queries
    )
    assert "route STRING NOT NULL" in spark.queries[-1]
    assert "location STRING NOT NULL" in spark.queries[-1]


def test_model_build_schema_migration_is_additive_and_idempotent():
    class Spark:
        def __init__(self, columns):
            self.columns = columns
            self.queries = []

        def table(self, _table):
            fields = [SimpleNamespace(name=name) for name in self.columns]
            return SimpleNamespace(schema=SimpleNamespace(fields=fields))

        def sql(self, query):
            self.queries.append(query)

    spark = Spark(
        (
            "model_build_id",
            "research_build_id",
            "selection_decision_id",
        )
    )
    missing = store.ensure_model_build_research_columns(
        spark,
        catalog="catalog",
        schema="schema",
    )

    assert missing == (
        "selected_candidate_id",
        "selected_candidate_evaluation_id",
        "registration_code_sha",
    )
    assert spark.queries == [
        "ALTER TABLE `catalog`.`schema`.`next_uk_nextads_model_builds` "
        "ADD COLUMNS (`selected_candidate_id` STRING, "
        "`selected_candidate_evaluation_id` STRING, "
        "`registration_code_sha` STRING)"
    ]

    complete = Spark(store.MODEL_BUILD_RESEARCH_COLUMNS)
    assert (
        store.ensure_model_build_research_columns(
            complete,
            catalog="catalog",
            schema="schema",
        )
        == ()
    )
    assert complete.queries == []


def test_training_receipt_is_replaced_by_deterministic_receipt_id(monkeypatch):
    calls = []
    monkeypatch.setattr(store, "typed_table_frame", lambda _s, _t, rows: rows)
    monkeypatch.setattr(
        store,
        "replace_scope_by_name",
        lambda frame, table, scope, **kwargs: calls.append(
            (frame, table, scope, kwargs)
        ),
    )

    target = store.persist_training_set_receipt(
        object(),
        catalog="catalog",
        schema="schema",
        receipt=_receipt(),
    )

    assert target.endswith(store.TRAINING_RECEIPT_TABLE)
    row, table, scope, kwargs = calls[0]
    assert table == target
    assert scope == {"receipt_id": "receipt-1"}
    assert row[0]["status"] == "READY"
    assert '"delta_version":3' in row[0]["feature_bindings_json"]
    assert kwargs["build_id"] == "receipt-1"


def test_model_build_persists_exact_mlflow_version_and_digest(monkeypatch):
    calls = []
    monkeypatch.setattr(store, "typed_table_frame", lambda _s, _t, rows: rows)
    monkeypatch.setattr(
        store,
        "replace_scope_by_name",
        lambda frame, table, scope, **kwargs: calls.append(
            (frame, table, scope, kwargs)
        ),
    )

    store.persist_model_build(
        object(), catalog="catalog", schema="schema", build=_build()
    )

    row = calls[0][0][0]
    assert row["registered_model_version"] == 4
    assert row["model_uri"] == "models:/catalog.schema.model/4"
    assert row["artifact_digest"] == "f" * 64
    assert row["metrics_json"] == '{"auc_pr":0.42}'
    assert row["research_build_id"] is None
    assert row["selected_candidate_evaluation_id"] is None


def test_model_build_persists_nullable_selected_research_lineage(monkeypatch):
    calls = []
    monkeypatch.setattr(store, "typed_table_frame", lambda _s, _t, rows: rows)
    monkeypatch.setattr(
        store,
        "replace_scope_by_name",
        lambda frame, _table, _scope, **_kwargs: calls.append(frame[0]),
    )
    build = ModelBuild(
        **{
            **_build().__dict__,
            "research_build_id": "research-1",
            "selection_decision_id": "selection-1",
            "selected_candidate_id": "random_forest",
            "selected_candidate_evaluation_id": "evaluation-1",
            "registration_code_sha": "registration-sha",
        }
    )

    store.persist_model_build(
        object(),
        catalog="catalog",
        schema="schema",
        build=build,
    )

    assert calls[0]["research_build_id"] == "research-1"
    assert calls[0]["selection_decision_id"] == "selection-1"
    assert calls[0]["selected_candidate_id"] == "random_forest"
    assert calls[0]["selected_candidate_evaluation_id"] == "evaluation-1"
    assert calls[0]["registration_code_sha"] == "registration-sha"


def test_external_score_receipt_persists_component_model_versions(monkeypatch):
    calls = []
    monkeypatch.setattr(store, "typed_table_frame", lambda _s, _t, rows: rows)
    monkeypatch.setattr(
        store,
        "replace_scope_by_name",
        lambda frame, table, scope, **kwargs: calls.append(
            (frame, table, scope, kwargs)
        ),
    )

    target = store.persist_external_score_output_receipt(
        object(),
        catalog="catalog",
        schema="schema",
        receipt=_external_receipt(),
    )

    row = calls[0][0][0]
    assert target.endswith(store.EXTERNAL_SCORE_RECEIPT_TABLE)
    assert row["source_delta_version"] == 5
    assert (
        '"model_uri":"models:/catalog.schema.classifier/2"'
        in row["components_json"]
    )


def test_ready_receipt_loader_reconstructs_feature_bindings(monkeypatch):
    receipt = _receipt()
    row = {
        "receipt_id": receipt.receipt_id,
        "model_name": receipt.model_name,
        "model_definition_checksum": receipt.model_definition_checksum,
        "feature_bindings_json": (
            '[{"backing_table":"catalog.schema.pctr",'
            '"delta_version":3,"feature_id":"pctr",'
            '"feature_snapshot_attempt_id":"attempt",'
            '"feature_snapshot_id":"snapshot","row_count":10,'
            f'"schema_checksum":"{"b" * 64}",'
            f'"value_checksum":"{"c" * 64}"}}]'
        ),
        "observation_start": receipt.observation_start,
        "observation_end": receipt.observation_end,
        "label_end": receipt.label_end,
        "schema_checksum": receipt.schema_checksum,
        "data_checksum": receipt.data_checksum,
        "code_sha": receipt.code_sha,
        "leakage_status": receipt.leakage_status,
        "status": receipt.status,
        "created_at": receipt.created_at,
        "completed_at": receipt.completed_at,
        "failure_reason": None,
    }

    class Frame:
        def where(self, *_args):
            return self

        def limit(self, _value):
            return self

        def collect(self):
            return [SimpleNamespace(asDict=lambda: row)]

    spark = SimpleNamespace(table=lambda _table: Frame())
    loaded = store.load_ready_training_set_receipt(
        spark, catalog="catalog", schema="schema", receipt_id="receipt-1"
    )

    assert loaded == receipt
