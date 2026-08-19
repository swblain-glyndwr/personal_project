from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from next_ads.model_development.contracts import (
    DBR_15_4_SPARK_CPU,
    FeatureLookupSpec,
    ModelDefinition,
    TrainingFeatureBinding,
    TrainingObservationSpec,
    TrainingSetReceipt,
)
from next_ads.model_development.registry import load_model_research_plan
from next_ads.model_development.research_contracts import (
    AUTO,
    AWAITING_SELECTION,
    FAILED,
    READY,
    CandidateEvaluation,
    CandidateSpec,
    ModelResearchBuild,
    ModelSelectionDecision,
)
from next_ads.model_development.research_plugins import (
    resolve_candidate_plugin,
)
from next_ads.model_development import research_runtime
from next_ads.model_development import spark_research
from next_ads.model_development.research_runtime import plan_observation_dates
from jobs.model.research import run_declared_research as research_job
from next_ads.model_development.research_selection import (
    recommend_candidate,
    register_selected_candidate,
    selected_model_build_id,
)


DIGEST = "a" * 64
SCHEMA_JSON = (
    '{"fields":[{"metadata":{},"name":"feature","nullable":true,'
    '"type":"double"}],"type":"struct"}'
)


def _definition() -> ModelDefinition:
    return ModelDefinition(
        model_name="example",
        provider_id="example_provider",
        problem_statement="Predict a binary outcome.",
        prediction_entity="entity",
        prediction_time="observation time",
        label="label",
        observation_keys=("row_key",),
        success_metrics=("auc_pr",),
        runtime_profile=DBR_15_4_SPARK_CPU,
        training_observation=TrainingObservationSpec(
            feature_id="observations",
            selected_columns=("row_key", "observed_at", "label"),
            observation_timestamp="observed_at",
        ),
        feature_lookups=(
            FeatureLookupSpec(
                feature_id="features",
                selected_columns=("feature",),
                key_mapping=(("entity_id", "row_key"),),
                observation_timestamp="observed_at",
            ),
        ),
        trainer="trainer",
        score_provider="scores",
        candidate_adapter="adapter",
    )


def _receipt(definition: ModelDefinition) -> TrainingSetReceipt:
    now = datetime.now(timezone.utc)
    return TrainingSetReceipt(
        receipt_id="receipt",
        model_name=definition.model_name,
        model_definition_checksum=definition.checksum,
        feature_bindings=(
            TrainingFeatureBinding(
                feature_id="features",
                feature_snapshot_id="snapshot",
                feature_snapshot_attempt_id="attempt",
                backing_table="catalog.schema.features",
                delta_version=1,
                row_count=10,
                schema_checksum=DIGEST,
                value_checksum=DIGEST,
            ),
        ),
        observation_start=date(2026, 8, 5),
        observation_end=date(2026, 8, 11),
        label_end=date(2026, 8, 12),
        schema_checksum=DIGEST,
        data_checksum=DIGEST,
        code_sha="sha",
        leakage_status="PASS",
        status=READY,
        created_at=now,
        completed_at=now,
    )


def _research_build(definition: ModelDefinition, receipt: TrainingSetReceipt):
    now = datetime.now(timezone.utc)
    return ModelResearchBuild(
        research_build_id="research",
        research_attempt_id="research-attempt",
        model_name=definition.model_name,
        training_receipt_id=receipt.receipt_id,
        model_definition_checksum=definition.checksum,
        research_plan_checksum=DIGEST,
        evaluation_schema_version="binary/v1",
        code_sha="sha",
        research_frame_id="frame",
        research_frame_attempt_id="frame-attempt",
        research_frame_table="catalog.schema.frame",
        research_frame_delta_version=1,
        research_frame_row_count=10,
        research_frame_schema_checksum=DIGEST,
        research_frame_data_checksum=DIGEST,
        research_frame_write_receipt_id="write",
        research_frame_feature_schema_json=SCHEMA_JSON,
        research_frame_slice_schema_json='{"fields":[],"type":"struct"}',
        candidate_count=1,
        successful_candidate_count=1,
        status=AWAITING_SELECTION,
        created_at=now,
        completed_at=now,
        mlflow_experiment_id="experiment",
        mlflow_parent_run_id="parent",
        automatic_candidate_id="candidate",
        artifact_manifest_digest=DIGEST,
    )


def _candidate(
    required=True, candidate_id="candidate", auc_pr=0.2, log_loss=0.1
):
    now = datetime.now(timezone.utc)
    return CandidateEvaluation(
        candidate_evaluation_id=f"evaluation-{candidate_id}",
        candidate_attempt_id=f"attempt-{candidate_id}",
        research_build_id="research",
        research_attempt_id="research-attempt",
        candidate_id=candidate_id,
        candidate_spec_checksum=DIGEST,
        required=required,
        status=READY,
        created_at=now,
        completed_at=now,
        mlflow_run_id=f"run-{candidate_id}",
        model_uri=f"runs:/run-{candidate_id}/model",
        metrics=(("auc_pr", auc_pr), ("log_loss", log_loss)),
        artifact_manifest_digest=DIGEST,
        explanation_status=READY,
    )


def test_declared_shopping_bag_plan_has_exact_seven_date_split():
    plan = load_model_research_plan("shopping_bag_pctr")
    assert plan is not None
    assert plan_observation_dates(plan) == tuple(
        date(2026, 8, day) for day in range(5, 12)
    )
    assert [candidate.plugin for candidate in plan.candidates] == [
        "spark_logistic_regression",
        "spark_random_forest",
        "spark_gradient_boosted_trees",
        "spark_xgboost",
    ]


def test_research_entrypoint_requires_one_prior_feature_date_per_observation():
    plan = load_model_research_plan("shopping_bag_pctr")
    assert plan is not None
    expected = tuple(date(2026, 8, day) for day in range(4, 11))

    research_job._assert_declared_feature_dates(plan, expected)

    with pytest.raises(
        ValueError,
        match=(
            r"exactly one day before.*missing=\['2026-08-04'\].*"
            r"unexpected=\['2026-08-11'\]"
        ),
    ):
        research_job._assert_declared_feature_dates(
            plan,
            tuple(date(2026, 8, day) for day in range(5, 12)),
        )


def test_research_split_uses_session_date_for_cross_midnight_exposures(
    monkeypatch,
):
    from pyspark.sql import functions as F

    base_definition = _definition()
    definition = replace(
        base_definition,
        training_observation=replace(
            base_definition.training_observation,
            selected_columns=(
                *base_definition.training_observation.selected_columns,
                "session_date",
            ),
            observation_date_column="session_date",
        ),
    )
    plan = replace(
        load_model_research_plan("shopping_bag_pctr"),
        slices=(),
    )

    class Frame:
        def __init__(self, rows, columns=None):
            self.rows = tuple(dict(row) for row in rows)
            self.columns = list(
                columns if columns is not None else rows[0].keys()
            )

        def withColumn(self, name, expression):  # noqa: N802
            kind, source = expression
            assert kind == "to_date"
            rows = []
            for row in self.rows:
                value = row[source]
                logical_date = (
                    value.date() if isinstance(value, datetime) else value
                )
                rows.append({**row, name: logical_date})
            columns = (
                (*self.columns, name)
                if name not in self.columns
                else self.columns
            )
            return Frame(rows, columns)

        def select(self, *columns):
            return Frame(
                tuple(
                    {column: row[column] for column in columns}
                    for row in self.rows
                ),
                columns,
            )

        def distinct(self):
            unique = []
            seen = set()
            for row in self.rows:
                identity = tuple(row[column] for column in self.columns)
                if identity not in seen:
                    unique.append(row)
                    seen.add(identity)
            return Frame(unique, self.columns)

        def collect(self):
            return list(self.rows)

    rows = tuple(
        {
            "row_key": f"row-{day}",
            "session_date": date(2026, 8, day),
            "observed_at": (
                datetime(2026, 8, 8, 22, 46, tzinfo=timezone.utc)
                if day == 9
                else datetime(2026, 8, day, 12, 0, tzinfo=timezone.utc)
            ),
            "label": day % 2,
            "feature": float(day),
        }
        for day in range(5, 12)
    )
    captured = {}

    monkeypatch.setattr(F, "col", lambda name: ("column", name))
    monkeypatch.setattr(
        F,
        "to_date",
        lambda expression: ("to_date", expression[1]),
    )

    def pack(frame, *, plan, **_kwargs):
        captured["frame"] = frame
        captured["plan"] = plan
        return frame

    monkeypatch.setattr(research_runtime, "pack_research_frame", pack)
    monkeypatch.setattr(
        research_runtime,
        "declared_research_schemas",
        lambda *_args, **_kwargs: "schemas",
    )

    packed, schemas, frame_plan = research_runtime.prepare_research_frame(
        Frame(rows),
        definition=definition,
        plan=plan,
        logical_research_build_id="research",
        research_attempt_id="research-attempt",
        logical_research_frame_id="frame",
        research_frame_attempt_id="frame-attempt",
        training_receipt_id="receipt",
    )

    assert packed is captured["frame"]
    assert schemas == "schemas"
    validation_row = captured["frame"].rows[4]
    assert validation_row["observed_at"].date() == date(2026, 8, 8)
    assert validation_row["observation_date"] == date(2026, 8, 9)
    assert "2026-08-09" in frame_plan.validation_dates


def test_runtime_preserves_declared_slice_values_and_thresholds():
    plan = load_model_research_plan("shopping_bag_pctr")
    assert plan is not None

    specs = research_runtime._reporting_slice_specs(
        plan,
        SimpleNamespace(columns=["location", "device"]),
    )

    location = specs[0]
    assert location.slice_id == "shopping_bag_location"
    assert location.values == ("SB1", "SB2")
    assert location.minimum_rows == 100


def test_supplied_candidate_alias_resolves_both_interfaces():
    candidate = CandidateSpec(
        candidate_id="lr",
        plugin="spark_logistic_regression",
    )
    resolved = resolve_candidate_plugin(candidate)
    assert callable(resolved.trainer.fit)
    assert callable(resolved.prediction_adapter.predict)
    assert callable(resolved.prediction_adapter.model_for_persistence)


def test_custom_plugin_outside_package_is_rejected():
    with pytest.raises(ValueError, match="plug-in alias or a next_ads"):
        CandidateSpec(candidate_id="custom", plugin="other.module.Plugin")


def test_custom_candidate_mapping_uses_declared_feature_names(monkeypatch):
    definition = _definition()
    monkeypatch.setattr(
        research_runtime,
        "readable_feature_mapping",
        lambda *_args: pytest.fail(
            "custom candidates must not require vectors"
        ),
    )

    mapping = research_runtime._candidate_feature_mapping(
        "next_ads.custom.Candidate",
        object(),
        object(),
        definition,
    )

    assert [(item.vector_index, item.source_column) for item in mapping] == [
        (0, "feature")
    ]


def test_claim_owner_is_stable_across_task_attempts():
    assert research_runtime._claim_owner_id("job-7:task-1:0") == "job-7"
    assert research_runtime._claim_owner_id("job-7:task-2:1") == "job-7"


def test_terminal_tagged_child_recovers_the_same_candidate_receipt():
    candidate = CandidateSpec(
        candidate_id="custom",
        plugin="next_ads.custom.Candidate",
    )
    tags = {
        research_runtime._RESEARCH_BUILD_TAG: "research",
        research_runtime._RESEARCH_ATTEMPT_TAG: "research-attempt",
        research_runtime._CANDIDATE_ID_TAG: candidate.candidate_id,
        research_runtime._CANDIDATE_EVALUATION_TAG: "evaluation",
        research_runtime._CANDIDATE_ATTEMPT_TAG: "candidate-attempt",
        research_runtime._CANDIDATE_STATUS_TAG: READY,
        research_runtime._CANDIDATE_MODEL_PATH_TAG: "model_attempts/run-1",
        research_runtime._CANDIDATE_MANIFEST_TAG: DIGEST,
        research_runtime._CANDIDATE_EXPLANATION_TAG: READY,
    }
    run = SimpleNamespace(
        info=SimpleNamespace(
            run_id="child-run",
            start_time=1_786_000_000_000,
            end_time=1_786_000_001_000,
        ),
        data=SimpleNamespace(
            tags=tags,
            metrics={"validation_auc_pr": 0.2, "validation_log_loss": 0.1},
        ),
    )

    recovered = research_runtime._recover_tagged_candidate(
        run,
        candidate=candidate,
        logical_candidate_id="evaluation",
        candidate_attempt="candidate-attempt",
        logical_build_id="research",
        research_attempt="research-attempt",
    )

    assert recovered is not None
    assert recovered.model_uri == "runs:/child-run/model_attempts/run-1"
    assert dict(recovered.metrics) == {"auc_pr": 0.2, "log_loss": 0.1}


def test_persisted_research_model_rejects_non_double_scores():
    frame = SimpleNamespace(
        schema=SimpleNamespace(
            fields=(
                SimpleNamespace(
                    name="prediction",
                    dataType=SimpleNamespace(simpleString=lambda: "double"),
                ),
                SimpleNamespace(
                    name="score",
                    dataType=SimpleNamespace(simpleString=lambda: "float"),
                ),
            )
        )
    )

    with pytest.raises(ValueError, match="DOUBLE prediction and score"):
        spark_research._validate_research_model_output(frame)


def test_recommendation_uses_pr_auc_then_log_loss_then_id():
    plan = load_model_research_plan("shopping_bag_pctr")
    assert plan is not None
    evaluations = (
        _candidate(
            candidate_id="logistic_regression", auc_pr=0.2, log_loss=0.2
        ),
        _candidate(candidate_id="random_forest", auc_pr=0.3, log_loss=0.4),
        _candidate(
            candidate_id="gradient_boosted_trees",
            auc_pr=0.3,
            log_loss=0.1,
        ),
        _candidate(candidate_id="spark_xgboost", auc_pr=0.3, log_loss=0.1),
    )
    assert recommend_candidate(plan, evaluations).candidate_id == (
        "gradient_boosted_trees"
    )


class _Client:
    def __init__(self):
        self.tags = []
        self.aliases = []

    def set_model_version_tag(self, **kwargs):
        self.tags.append(kwargs)

    def set_registered_model_alias(self, **kwargs):
        self.aliases.append(kwargs)

    def search_model_versions(self, _filter):
        return []


class _Mlflow:
    def __init__(self):
        self.registrations = []

    def register_model(self, **kwargs):
        self.registrations.append(kwargs)
        return SimpleNamespace(version="7")


def test_selected_registration_has_research_lineage_and_scalar_model_identity():
    definition = _definition()
    receipt = _receipt(definition)
    research = _research_build(definition, receipt)
    candidate = _candidate()
    now = datetime.now(timezone.utc)
    decision = ModelSelectionDecision(
        selection_decision_id="decision",
        selection_attempt_id="decision-attempt",
        research_build_id=research.research_build_id,
        research_attempt_id=research.research_attempt_id,
        selection_mode=AUTO,
        recommended_candidate_id=candidate.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        reason="automatic",
        status=READY,
        created_at=now,
        completed_at=now,
        registered_model_name="catalog.schema.model",
        decision_code_sha="registration-sha",
    )
    decision = replace(
        decision,
        model_build_id=selected_model_build_id(
            definition, receipt, research, candidate, decision
        ),
    )
    mlflow = _Mlflow()
    client = _Client()
    build = register_selected_candidate(
        definition,
        receipt,
        research,
        candidate,
        decision,
        registered_model_name="catalog.schema.model",
        selection_execution_code_sha="registration-sha",
        selected_metrics={"test_auc_pr": 0.2},
        mlflow_module=mlflow,
        client=client,
        digest_fn=lambda _uri: DIGEST,
    )
    assert build.model_build_id == selected_model_build_id(
        definition, receipt, research, candidate, decision
    )
    assert build.model_uri == "models:/catalog.schema.model/7"
    assert build.research_build_id == research.research_build_id
    assert build.selection_decision_id == decision.selection_decision_id
    assert build.selected_candidate_id == candidate.candidate_id
    assert build.registration_code_sha == "registration-sha"
    assert mlflow.registrations == [
        {"model_uri": candidate.model_uri, "name": "catalog.schema.model"}
    ]
    assert client.aliases == []
    assert all("." not in tag["key"] for tag in client.tags)


def test_selected_registration_adopts_one_partially_tagged_child_version():
    definition = _definition()
    receipt = _receipt(definition)
    research = _research_build(definition, receipt)
    candidate = _candidate()
    now = datetime.now(timezone.utc)
    decision = ModelSelectionDecision(
        selection_decision_id="decision",
        selection_attempt_id="decision-attempt",
        research_build_id=research.research_build_id,
        research_attempt_id=research.research_attempt_id,
        selection_mode=AUTO,
        recommended_candidate_id=candidate.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        reason="automatic",
        status=READY,
        created_at=now,
        completed_at=now,
        registered_model_name="catalog.schema.model",
        decision_code_sha="original-registration-sha",
    )
    decision = replace(
        decision,
        model_build_id=selected_model_build_id(
            definition, receipt, research, candidate, decision
        ),
    )
    client = _Client()
    client.search_model_versions = lambda _filter: [
        SimpleNamespace(
            version="6",
            run_id=candidate.mlflow_run_id,
            tags={
                "nextads_research_build_id": research.research_build_id,
                "nextads_registration_code_sha": "original-registration-sha",
            },
        )
    ]
    mlflow = _Mlflow()

    build = register_selected_candidate(
        definition,
        receipt,
        research,
        candidate,
        decision,
        registered_model_name="catalog.schema.model",
        selection_execution_code_sha="retry-registration-sha",
        selected_metrics={"test_auc_pr": 0.2},
        mlflow_module=mlflow,
        client=client,
        digest_fn=lambda _uri: DIGEST,
    )

    assert build.registered_model_version == 6
    assert build.registration_code_sha == "original-registration-sha"
    assert mlflow.registrations == []
    assert client.aliases == []


def test_registration_retry_preserves_sha_and_rejects_a_new_target():
    definition = _definition()
    receipt = _receipt(definition)
    research = _research_build(definition, receipt)
    candidate = _candidate()
    now = datetime.now(timezone.utc)
    decision = ModelSelectionDecision(
        selection_decision_id="decision",
        selection_attempt_id="decision-attempt",
        research_build_id=research.research_build_id,
        research_attempt_id=research.research_attempt_id,
        selection_mode=AUTO,
        recommended_candidate_id=candidate.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        reason="automatic",
        status=READY,
        created_at=now,
        completed_at=now,
        registered_model_name="catalog.schema.model_a",
        decision_code_sha="registration-sha-a",
    )
    decision = replace(
        decision,
        model_build_id=selected_model_build_id(
            definition, receipt, research, candidate, decision
        ),
    )
    mlflow = _Mlflow()
    client = _Client()

    first = register_selected_candidate(
        definition,
        receipt,
        research,
        candidate,
        decision,
        registered_model_name=" catalog.schema.model_a ",
        selection_execution_code_sha="registration-sha-a",
        selected_metrics={"test_auc_pr": 0.2},
        mlflow_module=mlflow,
        client=client,
        digest_fn=lambda _uri: DIGEST,
    )
    version_tags = {item["key"]: item["value"] for item in client.tags}
    client.search_model_versions = lambda _filter: [
        SimpleNamespace(
            version="7",
            run_id=candidate.mlflow_run_id,
            tags=version_tags,
        )
    ]

    retry = register_selected_candidate(
        definition,
        receipt,
        research,
        candidate,
        decision,
        registered_model_name="catalog.schema.model_a",
        selection_execution_code_sha="fix-sha-b",
        selected_metrics={"test_auc_pr": 0.2},
        mlflow_module=mlflow,
        client=client,
        digest_fn=lambda _uri: DIGEST,
    )

    assert retry.model_build_id == first.model_build_id
    assert retry.registration_code_sha == "registration-sha-a"
    assert len(mlflow.registrations) == 1
    assert (
        sum(
            item["key"] == "nextads_registration_code_sha"
            for item in client.tags
        )
        == 1
    )
    with pytest.raises(ValueError, match="locked decision"):
        register_selected_candidate(
            definition,
            receipt,
            research,
            candidate,
            decision,
            registered_model_name="catalog.schema.model_b",
            selection_execution_code_sha="fix-sha-b",
            selected_metrics={"test_auc_pr": 0.2},
            mlflow_module=mlflow,
            client=client,
            digest_fn=lambda _uri: DIGEST,
        )
    assert len(mlflow.registrations) == 1


def test_registration_retry_repairs_a_crash_before_the_first_version_tag():
    definition = _definition()
    receipt = _receipt(definition)
    research = _research_build(definition, receipt)
    candidate = _candidate()
    now = datetime.now(timezone.utc)
    decision = ModelSelectionDecision(
        selection_decision_id="decision",
        selection_attempt_id="decision-attempt",
        research_build_id=research.research_build_id,
        research_attempt_id=research.research_attempt_id,
        selection_mode=AUTO,
        recommended_candidate_id=candidate.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        reason="automatic",
        status=READY,
        created_at=now,
        completed_at=now,
        registered_model_name="catalog.schema.model",
        decision_code_sha="registration-intent-sha",
    )
    decision = replace(
        decision,
        model_build_id=selected_model_build_id(
            definition, receipt, research, candidate, decision
        ),
    )
    mlflow = _Mlflow()
    client = _Client()
    first_tag = [True]

    def set_tag(**kwargs):
        if first_tag[0]:
            first_tag[0] = False
            raise RuntimeError("simulated crash before first version tag")
        client.tags.append(kwargs)

    client.set_model_version_tag = set_tag
    client.search_model_versions = lambda _filter: (
        [
            SimpleNamespace(
                version="7",
                run_id=candidate.mlflow_run_id,
                tags={},
            )
        ]
        if mlflow.registrations
        else []
    )

    with pytest.raises(RuntimeError, match="simulated crash"):
        register_selected_candidate(
            definition,
            receipt,
            research,
            candidate,
            decision,
            registered_model_name="catalog.schema.model",
            selection_execution_code_sha="registration-intent-sha",
            selected_metrics={"test_auc_pr": 0.2},
            mlflow_module=mlflow,
            client=client,
            digest_fn=lambda _uri: DIGEST,
        )

    recovered = register_selected_candidate(
        definition,
        receipt,
        research,
        candidate,
        decision,
        registered_model_name="catalog.schema.model",
        selection_execution_code_sha="fix-sha",
        selected_metrics={"test_auc_pr": 0.2},
        mlflow_module=mlflow,
        client=client,
        digest_fn=lambda _uri: DIGEST,
    )

    assert len(mlflow.registrations) == 1
    assert recovered.registration_code_sha == "registration-intent-sha"
    assert client.tags[0] == {
        "name": "catalog.schema.model",
        "version": 7,
        "key": "nextads_registration_code_sha",
        "value": "registration-intent-sha",
    }


def test_automatic_retry_reloads_exact_selection_and_model(monkeypatch):
    plan = replace(
        load_model_research_plan("shopping_bag_pctr"),
        selection_policy=AUTO,
    )
    evaluations = (
        _candidate(
            candidate_id="logistic_regression", auc_pr=0.10, log_loss=0.30
        ),
        _candidate(candidate_id="random_forest", auc_pr=0.20, log_loss=0.20),
        _candidate(
            candidate_id="gradient_boosted_trees",
            auc_pr=0.30,
            log_loss=0.10,
        ),
        _candidate(candidate_id="spark_xgboost", auc_pr=0.25, log_loss=0.15),
    )
    selected = evaluations[2]
    definition = _definition()
    receipt = _receipt(definition)
    build = replace(
        _research_build(definition, receipt),
        status=READY,
        candidate_count=4,
        successful_candidate_count=4,
        automatic_candidate_id=selected.candidate_id,
    )
    decision_id = research_runtime.selection_decision_id(
        research_build_id=build.research_build_id,
        selection_mode=AUTO,
        recommended_candidate_id=selected.candidate_id,
        selected_candidate_id=selected.candidate_id,
        reason=research_runtime.AUTOMATIC_SELECTION_REASON,
    )
    now = datetime.now(timezone.utc)
    decision = ModelSelectionDecision(
        selection_decision_id=decision_id,
        selection_attempt_id="decision-attempt",
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        selection_mode=AUTO,
        recommended_candidate_id=selected.candidate_id,
        selected_candidate_id=selected.candidate_id,
        selected_candidate_evaluation_id=selected.candidate_evaluation_id,
        reason=research_runtime.AUTOMATIC_SELECTION_REASON,
        status=READY,
        created_at=now,
        completed_at=now,
        model_build_id="model-build",
        registered_model_name="catalog.schema.model",
        decision_code_sha="decision-sha",
    )
    model_build = SimpleNamespace(
        model_build_id="model-build",
        research_build_id=build.research_build_id,
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id=selected.candidate_id,
        selected_candidate_evaluation_id=selected.candidate_evaluation_id,
        registered_model_name="catalog.schema.model",
        registration_code_sha="registration-sha",
        status=READY,
    )
    monkeypatch.setattr(
        research_runtime,
        "_load_attempt_candidates",
        lambda *_args, **_kwargs: evaluations,
    )
    monkeypatch.setattr(
        research_runtime,
        "load_ready_selection_decision",
        lambda *_args, **_kwargs: decision,
    )
    monkeypatch.setattr(
        research_runtime,
        "load_ready_model_build",
        lambda *_args, **_kwargs: model_build,
    )
    validated = []
    parent_updates = []
    client = SimpleNamespace(
        set_tag=lambda *args: parent_updates.append(("tag", *args)),
        set_terminated=lambda *args, **kwargs: parent_updates.append(
            ("terminated", *args, kwargs)
        ),
    )
    monkeypatch.setattr(
        research_runtime,
        "validate_registered_model_build",
        lambda _client, value: validated.append(value),
    )

    result = research_runtime._reused_research_result(
        object(),
        definition=definition,
        plan=plan,
        build=build,
        catalog="catalog",
        schema="schema",
        registered_model_name="catalog.schema.model",
        mlflow_client=client,
    )

    assert result.reused is True
    assert result.selection_decision is decision
    assert result.model_build is model_build
    assert validated == [model_build]
    assert (
        "tag",
        build.mlflow_parent_run_id,
        "nextads_model_research_status",
        READY,
    ) in parent_updates
    assert (
        "terminated",
        build.mlflow_parent_run_id,
        {"status": "FINISHED"},
    ) in parent_updates
    with pytest.raises(ValueError, match="automatic selection"):
        research_runtime._reused_research_result(
            object(),
            definition=definition,
            plan=plan,
            build=build,
            catalog="catalog",
            schema="schema",
            registered_model_name="catalog.schema.other_model",
            mlflow_client=client,
        )


def test_review_retry_restores_awaiting_parent_status(monkeypatch):
    plan = load_model_research_plan("shopping_bag_pctr")
    definition = _definition()
    receipt = _receipt(definition)
    evaluations = (
        _candidate(candidate_id="logistic_regression", auc_pr=0.10),
        _candidate(candidate_id="random_forest", auc_pr=0.20),
        _candidate(candidate_id="gradient_boosted_trees", auc_pr=0.30),
        _candidate(candidate_id="spark_xgboost", auc_pr=0.25),
    )
    build = replace(
        _research_build(definition, receipt),
        candidate_count=4,
        successful_candidate_count=4,
        automatic_candidate_id="gradient_boosted_trees",
    )
    parent_updates = []
    client = SimpleNamespace(
        set_tag=lambda *args: parent_updates.append(("tag", *args)),
        set_terminated=lambda *args, **kwargs: parent_updates.append(
            ("terminated", *args, kwargs)
        ),
    )
    monkeypatch.setattr(
        research_runtime,
        "_load_attempt_candidates",
        lambda *_args, **_kwargs: evaluations,
    )
    monkeypatch.setattr(
        research_runtime,
        "load_ready_selection_for_research_attempt",
        lambda *_args, **_kwargs: None,
    )

    result = research_runtime._reused_research_result(
        object(),
        definition=definition,
        plan=plan,
        build=build,
        catalog="catalog",
        schema="schema",
        registered_model_name="catalog.schema.model",
        mlflow_client=client,
    )

    assert result.reused is True
    assert result.selection_decision is None
    assert (
        "tag",
        build.mlflow_parent_run_id,
        "nextads_model_research_status",
        AWAITING_SELECTION,
    ) in parent_updates
    assert (
        "terminated",
        build.mlflow_parent_run_id,
        {"status": "FINISHED"},
    ) in parent_updates


def test_custom_evidence_producer_records_complete_status(monkeypatch):
    plan = replace(
        load_model_research_plan("shopping_bag_pctr"),
        evidence_producers=("next_ads.custom.Evidence",),
    )
    received = []

    def produce(*args):
        received.append(args[3])
        return {"summary": {"rows": 10}}

    producer = SimpleNamespace(produce=produce)
    monkeypatch.setattr(
        research_runtime,
        "resolve_evidence_producer",
        lambda _identifier: producer,
    )

    evidence = research_runtime._optional_evidence(
        plan,
        _definition(),
        _candidate(),
        object(),
        {"validation": {"metrics": {"auc_pr": 0.2}}},
        ("feature",),
    )

    assert evidence == {
        "next_ads.custom.Evidence": {
            "status": "COMPLETE",
            "evidence": {"summary": {"rows": 10}},
        }
    }
    assert received == [{"validation": {"metrics": {"auc_pr": 0.2}}}]
    assert not hasattr(received[0], "columns")


def test_custom_evidence_producer_can_record_not_applicable(monkeypatch):
    plan = replace(
        load_model_research_plan("shopping_bag_pctr"),
        evidence_producers=("next_ads.custom.Evidence",),
    )
    producer = SimpleNamespace(
        produce=lambda *_args: {
            "status": "NOT_APPLICABLE",
            "reason": "Candidate does not expose local contributions",
        }
    )
    monkeypatch.setattr(
        research_runtime,
        "resolve_evidence_producer",
        lambda _identifier: producer,
    )

    evidence = research_runtime._optional_evidence(
        plan,
        _definition(),
        _candidate(),
        object(),
        {"validation": {"metrics": {"auc_pr": 0.2}}},
        ("feature",),
    )

    assert evidence == {
        "next_ads.custom.Evidence": {
            "status": "NOT_APPLICABLE",
            "reason": "Candidate does not expose local contributions",
        }
    }


def test_optional_evidence_failure_never_persists_exception_text(monkeypatch):
    plan = replace(
        load_model_research_plan("shopping_bag_pctr"),
        evidence_producers=("next_ads.custom.Evidence",),
    )

    def fail(*_args):
        raise RuntimeError("account_number=12345678")

    monkeypatch.setattr(
        research_runtime,
        "resolve_evidence_producer",
        lambda _identifier: SimpleNamespace(produce=fail),
    )

    evidence = research_runtime._optional_evidence(
        plan,
        _definition(),
        _candidate(),
        object(),
        {"validation": {"metrics": {"auc_pr": 0.2}}},
        ("feature",),
    )

    reason = evidence["next_ads.custom.Evidence"]["reason"]
    assert "12345678" not in reason
    assert "account_number" not in reason
    assert "message_sha256" in reason


def test_parent_run_is_marked_failed_when_later_runtime_work_raises(
    monkeypatch,
):
    calls = []
    client = SimpleNamespace(
        set_tag=lambda *args: calls.append(("tag", *args)),
        set_terminated=lambda *args, **kwargs: calls.append(
            ("terminated", *args, kwargs)
        ),
    )

    def fail(*_args, **kwargs):
        kwargs["_parent_run_state"]["run_id"] = "parent-run"
        raise RuntimeError("registration failed")

    monkeypatch.setattr(research_runtime, "_run_model_research_impl", fail)

    with pytest.raises(RuntimeError, match="registration failed"):
        research_runtime.run_model_research(
            object(),
            definition=_definition(),
            plan=load_model_research_plan("shopping_bag_pctr"),
            training=object(),
            catalog="catalog",
            schema="schema",
            registered_model_name="catalog.schema.model",
            experiment_path="/experiment",
            code_sha="sha",
            invocation_id="run:task:0",
            mlflow_module=object(),
            mlflow_client=client,
        )

    assert (
        "tag",
        "parent-run",
        "nextads_model_research_status",
        FAILED,
    ) in calls
    assert ("terminated", "parent-run", {"status": "FAILED"}) in calls
