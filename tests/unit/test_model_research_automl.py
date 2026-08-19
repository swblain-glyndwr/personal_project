import argparse
from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
import inspect
import json
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from jobs.model.research import run_automl_discovery as discovery
from next_ads.model_development import automl_claims
from next_ads.model_development.research_contracts import (
    AWAITING_SELECTION,
    FAILED,
    READY,
    AutoMLDiscoveryReceipt,
    ModelResearchBuild,
)


NOW = datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc)
DIGEST = "a" * 64
FEATURE_SCHEMA = StructType(
    [
        StructField("advert_ctr", DoubleType(), True),
        StructField("device_type", StringType(), True),
    ]
).json()
SLICE_SCHEMA = StructType([]).json()


def _leaderboard_receipt_fields(
    *,
    discovery_id: str = "discovery-logical",
    research_build_id: str = "research-logical",
    experiment_id: str = "automl-experiment",
    trial_ids: tuple[str, ...] = (
        "best-run",
        "trial-2",
        "trial-3",
        "trial-4",
    ),
) -> dict[str, object]:
    rows = [
        {
            "rank": rank,
            "trial_id": trial_id,
            "primary_metric_value": 1.0 - rank / 10,
            "notebook_artifact_uri": f"runs:/{trial_id}/notebook",
            "notebook_path": None,
            "notebook_url": (
                "https://workspace/notebook/1" if rank == 1 else None
            ),
            "is_best_trial": rank == 1,
        }
        for rank, trial_id in enumerate(trial_ids, start=1)
    ]
    payload = {
        "schema_version": discovery.AUTOML_LEADERBOARD_SCHEMA_VERSION,
        "research_build_id": research_build_id,
        "discovery_id": discovery_id,
        "research_parent_run_id": "research-parent-run",
        "experiment_id": experiment_id,
        "primary_metric": discovery.AUTOML_PRIMARY_METRIC,
        "trial_count": len(rows),
        "best_trial_id": trial_ids[0],
        "trials": rows,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "experiment_id": experiment_id,
        "best_trial_id": trial_ids[0],
        "primary_metric": discovery.AUTOML_PRIMARY_METRIC,
        "trial_count": len(rows),
        "trial_evidence_json": encoded,
        "leaderboard_run_id": "leaderboard-run",
        "leaderboard_artifact_sha256": hashlib.sha256(
            encoded.encode("utf-8")
        ).hexdigest(),
        "leaderboard_artifact_uri": (
            "runs:/leaderboard-run/automl_discovery/leaderboard.json"
        ),
        "recipe_artifact_uri": "https://workspace/notebook/1",
    }


@pytest.fixture(scope="module")
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("nextads-model-research-automl-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as error:
        pytest.skip(f"Local Spark unavailable: {error}")
    yield spark


def _build() -> ModelResearchBuild:
    return ModelResearchBuild(
        research_build_id="research-logical",
        research_attempt_id="research-attempt",
        model_name="shopping_bag_pctr",
        training_receipt_id="training-receipt",
        model_definition_checksum="b" * 64,
        research_plan_checksum="c" * 64,
        evaluation_schema_version="binary_classifier_evidence/v1",
        code_sha="abc123",
        research_frame_id="frame-logical",
        research_frame_attempt_id="frame-attempt",
        research_frame_table="catalog.schema.research_frame",
        research_frame_delta_version=12,
        research_frame_row_count=8,
        research_frame_schema_checksum="d" * 64,
        research_frame_data_checksum="e" * 64,
        research_frame_write_receipt_id="frame-write-receipt",
        research_frame_feature_schema_json=FEATURE_SCHEMA,
        research_frame_slice_schema_json=SLICE_SCHEMA,
        candidate_count=4,
        successful_candidate_count=4,
        status=AWAITING_SELECTION,
        created_at=NOW,
        completed_at=NOW,
        mlflow_experiment_id="research-experiment",
        mlflow_parent_run_id="research-parent-run",
        automatic_candidate_id="gradient_boosted_trees",
        artifact_manifest_digest="f" * 64,
    )


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        enabled=True,
        research_build_id="research-logical",
        model_catalog="catalog",
        model_schema="schema",
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
        orchestration_run_id="job-run-1",
        task_run_id="task-run-1",
        execution_count=0,
        log_level="INFO",
    )


def _ready_receipt() -> AutoMLDiscoveryReceipt:
    binding = discovery._frame_binding(_build())
    return AutoMLDiscoveryReceipt(
        discovery_id="discovery-logical",
        discovery_attempt_id="discovery-attempt",
        request_checksum=discovery._request_checksum(
            timeout_minutes=30,
            experiment_path="/Shared/model-research/automl",
            code_sha="abc123",
        ),
        research_build_id=binding.research_build_id,
        research_attempt_id=binding.research_attempt_id,
        research_frame_id=binding.research_frame_id,
        research_frame_attempt_id=binding.research_frame_attempt_id,
        research_frame_table=binding.research_frame_table,
        research_frame_delta_version=binding.research_frame_delta_version,
        research_frame_schema_checksum=(
            binding.research_frame_schema_checksum
        ),
        research_frame_data_checksum=binding.research_frame_data_checksum,
        research_frame_write_receipt_id=(
            binding.research_frame_write_receipt_id
        ),
        research_frame_feature_schema_json=(
            binding.research_frame_feature_schema_json
        ),
        research_frame_slice_schema_json=(
            binding.research_frame_slice_schema_json
        ),
        status=READY,
        timeout_minutes=30,
        created_at=NOW,
        completed_at=NOW,
        **_leaderboard_receipt_fields(),
    )


def _mock_definition_and_plan(monkeypatch, build: ModelResearchBuild) -> None:
    monkeypatch.setattr(
        discovery,
        "load_model_definition",
        lambda _name: SimpleNamespace(
            checksum=build.model_definition_checksum
        ),
    )
    monkeypatch.setattr(
        discovery,
        "_effective_research_plan",
        lambda *_args: SimpleNamespace(
            candidate_search=SimpleNamespace(
                plugin=discovery.AUTOML_CANDIDATE_SEARCH_PLUGIN
            )
        ),
    )


def _mock_new_claim(monkeypatch) -> list[automl_claims.AutoMLDiscoveryClaim]:
    state = []
    monkeypatch.setattr(
        discovery,
        "load_automl_claim",
        lambda *a, **k: state[0] if state else None,
    )

    def claim(*_args, **kwargs):
        stored = automl_claims.AutoMLDiscoveryClaim(
            discovery_id=kwargs["discovery_id"],
            discovery_attempt_id=kwargs["discovery_attempt_id"],
            request_checksum=kwargs["request_checksum"],
            research_build_id=kwargs["research_build_id"],
            research_attempt_id=kwargs["research_attempt_id"],
            research_frame_id=kwargs["research_frame_id"],
            research_frame_attempt_id=kwargs["research_frame_attempt_id"],
            research_frame_delta_version=(
                kwargs["research_frame_delta_version"]
            ),
            timeout_minutes=kwargs["timeout_minutes"],
            experiment_path=kwargs["experiment_path"],
            code_sha=kwargs["code_sha"],
            owner_invocation_id=kwargs["owner_invocation_id"],
            lease_token="lease-token",
            lease_expires_at=NOW,
            checkpoint=automl_claims.CLAIMED,
            checkpoint_version=0,
            experiment_id=None,
            trial_count=None,
            best_trial_id=None,
            primary_metric=None,
            trial_evidence_json=None,
            leaderboard_run_id=None,
            leaderboard_artifact_sha256=None,
            leaderboard_artifact_uri=None,
            recipe_artifact_uri=None,
            failure_reason=None,
            created_at=NOW,
            updated_at=NOW,
        )
        state.append(stored)
        return stored

    def transition(checkpoint, **changes):
        stored = replace(
            changes.pop("claim"),
            checkpoint=checkpoint,
            checkpoint_version=state[0].checkpoint_version + 1,
            updated_at=NOW,
            **changes,
        )
        state[0] = stored
        return stored

    monkeypatch.setattr(discovery, "claim_automl_discovery", claim)
    monkeypatch.setattr(
        discovery,
        "start_automl_discovery",
        lambda *_args, **kwargs: transition(
            automl_claims.RUNNING,
            claim=kwargs["claim"],
        ),
    )
    monkeypatch.setattr(
        discovery,
        "record_automl_evidence",
        lambda *_args, **kwargs: transition(
            automl_claims.EVIDENCE_READY,
            claim=kwargs["claim"],
            experiment_id=kwargs["experiment_id"],
            trial_count=kwargs["trial_count"],
            best_trial_id=kwargs["best_trial_id"],
            primary_metric=kwargs["primary_metric"],
            trial_evidence_json=kwargs["trial_evidence_json"],
            leaderboard_run_id=kwargs["leaderboard_run_id"],
            leaderboard_artifact_sha256=(
                kwargs["leaderboard_artifact_sha256"]
            ),
            leaderboard_artifact_uri=kwargs["leaderboard_artifact_uri"],
            recipe_artifact_uri=kwargs["recipe_artifact_uri"],
        ),
    )
    monkeypatch.setattr(
        discovery,
        "complete_automl_claim",
        lambda *_args, **kwargs: transition(
            automl_claims.COMPLETE,
            claim=kwargs["claim"],
        ),
    )
    monkeypatch.setattr(
        discovery,
        "fail_automl_claim",
        lambda *_args, **kwargs: transition(
            automl_claims.FAILED,
            claim=kwargs["claim"],
            failure_reason=kwargs["failure_reason"],
        ),
    )
    return state


def _existing_claim(
    checkpoint: str,
    *,
    with_evidence: bool = False,
) -> automl_claims.AutoMLDiscoveryClaim:
    build = _build()
    binding = discovery._frame_binding(build)
    request_checksum = discovery._request_checksum(
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
    )
    discovery_id = discovery.automl_discovery_id(
        research_build_id=build.research_build_id,
        research_frame_id=binding.research_frame_id,
        research_frame_delta_version=binding.research_frame_delta_version,
        request_checksum=request_checksum,
    )
    result_fields: dict[str, object] = {
        "experiment_id": None,
        "trial_count": None,
        "best_trial_id": None,
        "primary_metric": None,
        "trial_evidence_json": None,
        "leaderboard_run_id": None,
        "leaderboard_artifact_sha256": None,
        "leaderboard_artifact_uri": None,
        "recipe_artifact_uri": None,
    }
    if with_evidence:
        result_fields.update(
            _leaderboard_receipt_fields(discovery_id=discovery_id)
        )
    return automl_claims.AutoMLDiscoveryClaim(
        discovery_id=discovery_id,
        discovery_attempt_id="discovery-attempt",
        request_checksum=request_checksum,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        research_frame_id=binding.research_frame_id,
        research_frame_attempt_id=binding.research_frame_attempt_id,
        research_frame_delta_version=binding.research_frame_delta_version,
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
        owner_invocation_id="job-run-1:task-run-1:0",
        lease_token="lease-token",
        lease_expires_at=NOW,
        checkpoint=checkpoint,
        checkpoint_version=2,
        failure_reason=(
            "RuntimeError: discovery service failed"
            if checkpoint == automl_claims.FAILED
            else None
        ),
        created_at=NOW,
        updated_at=NOW,
        **result_fields,
    )


def test_cli_requires_explicit_enablement_and_bounded_timeout():
    assert discovery.parse_enabled("true") is True
    assert discovery.parse_enabled("false") is False
    for invalid in ("TRUE", "False", "1", "yes", ""):
        with pytest.raises(argparse.ArgumentTypeError):
            discovery.parse_enabled(invalid)

    assert discovery.parse_timeout_minutes("1") == 1
    assert discovery.parse_timeout_minutes("120") == 120
    for invalid in ("0", "121", "1.5", "thirty"):
        with pytest.raises(argparse.ArgumentTypeError):
            discovery.parse_timeout_minutes(invalid)


def test_discovery_request_identity_pins_runtime_bound_path_and_code():
    base = discovery._request_checksum(
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
    )

    assert base == discovery._request_checksum(
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
    )
    assert base != discovery._request_checksum(
        timeout_minutes=60,
        experiment_path="/Shared/model-research/automl",
        code_sha="abc123",
    )
    assert base != discovery._request_checksum(
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl-v2",
        code_sha="abc123",
    )
    assert base != discovery._request_checksum(
        timeout_minutes=30,
        experiment_path="/Shared/model-research/automl",
        code_sha="def456",
    )


def test_effective_plan_resolves_both_reviewed_and_automatic_builds(
    monkeypatch,
):
    declared = discovery.load_model_research_plan("shopping_bag_pctr")
    assert declared is not None
    monkeypatch.setattr(
        discovery,
        "load_model_research_plan",
        lambda _model_name: declared,
    )
    automatic = replace(declared, selection_policy="AUTO")

    assert (
        discovery._effective_research_plan(
            "shopping_bag_pctr", declared.checksum
        ).selection_policy
        == declared.selection_policy
    )
    assert (
        discovery._effective_research_plan(
            "shopping_bag_pctr", automatic.checksum
        ).selection_policy
        == "AUTO"
    )


def test_entrypoint_bootstraps_without_project_or_custom_library_imports():
    source = inspect.getsource(discovery)
    bootstrap = source.index("sys.path.insert(0")
    project_import = source.index("from next_ads")

    assert bootstrap < project_import
    pre_bootstrap = source[:bootstrap]
    assert "dsutils" not in pre_bootstrap
    assert "dynaconf" not in pre_bootstrap.lower()
    assert "databricks.automl" not in pre_bootstrap
    assert "register_model" not in source
    assert "set_registered_model_alias" not in source


def test_entrypoint_resolves_bundle_root_for_python_and_databricks_exec():
    expected_python_root = (
        discovery.Path(discovery.__file__).resolve().parents[3]
    )

    assert discovery.resolve_project_root(discovery.__file__) == (
        expected_python_root
    )
    databricks_root = discovery.resolve_project_root(
        None,
        (
            "/Users/test/.bundle/next-ads/DEV/test/files/"
            "jobs/model/research/run_automl_discovery.py"
        ),
    )

    assert databricks_root.as_posix() == (
        "/Workspace/Users/test/.bundle/next-ads/DEV/test/files"
    )


def test_entrypoint_uses_injected_databricks_handle_without_custom_libraries():
    value = SimpleNamespace(
        get=lambda: (
            "/Users/test/.bundle/next-ads/DEV/test/files/"
            "jobs/model/research/run_automl_discovery.py"
        )
    )
    context = SimpleNamespace(notebookPath=lambda: value)
    notebook = SimpleNamespace(getContext=lambda: context)
    entry_point = SimpleNamespace(
        getDbutils=lambda: SimpleNamespace(notebook=lambda: notebook)
    )
    dbutils_handle = SimpleNamespace(
        notebook=SimpleNamespace(entry_point=entry_point)
    )

    root = discovery.resolve_project_root(
        None,
        dbutils_handle=dbutils_handle,
    )

    assert root.as_posix() == (
        "/Workspace/Users/test/.bundle/next-ads/DEV/test/files"
    )


def test_entrypoint_requires_databricks_handle_when_file_is_unavailable(
    monkeypatch,
):
    monkeypatch.delitem(discovery.__dict__, "dbutils", raising=False)

    with pytest.raises(
        RuntimeError,
        match="Databricks execution requires an injected dbutils handle",
    ):
        discovery.resolve_project_root(None)


def test_frame_binding_uses_every_recorded_delta_receipt_field():
    build = _build()
    binding = discovery._frame_binding(build)

    assert binding.research_build_id == build.research_build_id
    assert binding.research_attempt_id == build.research_attempt_id
    assert binding.training_receipt_id == build.training_receipt_id
    assert binding.research_frame_id == build.research_frame_id
    assert binding.research_frame_attempt_id == build.research_frame_attempt_id
    assert binding.research_frame_table == build.research_frame_table
    assert binding.research_frame_delta_version == 12
    assert binding.research_frame_row_count == 8
    assert binding.research_frame_schema_checksum == "d" * 64
    assert binding.research_frame_data_checksum == "e" * 64
    assert binding.research_frame_write_receipt_id == "frame-write-receipt"


def test_fixed_validation_dates_use_the_later_date_as_automl_test():
    assert discovery._validation_holdout_date(
        [date(2026, 8, 9), date(2026, 8, 10)]
    ) == date(2026, 8, 10)
    assert discovery._validation_holdout_date([date(2026, 8, 9)]) is None


def test_automl_frame_excludes_main_test_and_only_exposes_model_inputs(
    local_spark,
):
    schema = StructType(
        [
            StructField("row_id", StringType(), False),
            StructField("observation_date", DateType(), False),
            StructField("split", StringType(), False),
            StructField("label", DoubleType(), False),
            StructField("advert_ctr", DoubleType(), True),
            StructField("device_type", StringType(), True),
            StructField("location", StringType(), True),
        ]
    )
    rows = [
        ("t1", date(2026, 8, 5), "train", 0.0, 0.1, "app", "SB1"),
        ("t2", date(2026, 8, 6), "train", 1.0, 0.2, "web", "SB2"),
        ("v1", date(2026, 8, 9), "validate", 0.0, 0.3, "app", "SB1"),
        ("v2", date(2026, 8, 10), "validate", 1.0, 0.4, "web", "SB2"),
    ]
    frame = local_spark.createDataFrame(rows, schema)

    prepared, counts = discovery._prepare_automl_frame(
        frame,
        binding=discovery._frame_binding(_build()),
    )

    assert prepared.columns == [
        "advert_ctr",
        "device_type",
        "label",
        "automl_split",
    ]
    assert counts == {"train": 2, "validate": 1, "test": 1}
    assert "row_id" not in prepared.columns
    assert "observation_date" not in prepared.columns
    assert "split" not in prepared.columns
    assert "location" not in prepared.columns


def test_main_test_split_is_rejected_even_before_feature_selection(
    local_spark,
):
    schema = StructType(
        [
            StructField("row_id", StringType(), False),
            StructField("observation_date", DateType(), False),
            StructField("split", StringType(), False),
            StructField("label", DoubleType(), False),
            StructField("advert_ctr", DoubleType(), True),
            StructField("device_type", StringType(), True),
        ]
    )
    frame = local_spark.createDataFrame(
        [
            ("t1", date(2026, 8, 5), "train", 0.0, 0.1, "app"),
            ("v1", date(2026, 8, 9), "validate", 1.0, 0.2, "web"),
            ("x1", date(2026, 8, 11), "test", 1.0, 0.3, "app"),
        ],
        schema,
    )

    with pytest.raises(ValueError, match="withheld or unknown"):
        discovery._prepare_automl_frame(
            frame,
            binding=discovery._frame_binding(_build()),
        )


def test_summary_fields_build_sorted_bounded_trial_evidence():
    best = SimpleNamespace(
        mlflow_run_id="best-run",
        evaluation_metric_score=0.81,
        artifact_uri="runs:/best-run/notebook",
        notebook_url="https://workspace/notebook/1",
        params={"unused_training_parameter": "not persisted"},
        model_description="not persisted",
    )
    other = SimpleNamespace(
        mlflow_run_id="other-run",
        evaluation_metric_score=0.72,
        artifact_uri="runs:/other-run/notebook",
        notebook_url=None,
        notebook_path=None,
    )
    summary = SimpleNamespace(
        experiment=SimpleNamespace(experiment_id="experiment-1"),
        trials=(other, best),
        best_trial=best,
    )

    fields = discovery._summary_receipt_fields(
        summary,
        research_build_id="research-logical",
        discovery_id="discovery-logical",
        research_parent_run_id="research-parent-run",
    )
    leaderboard = json.loads(fields["trial_evidence_json"])

    assert fields["experiment_id"] == "experiment-1"
    assert fields["best_trial_id"] == "best-run"
    assert fields["trial_count"] == 2
    assert fields["primary_metric"] == "roc_auc"
    assert fields["recipe_artifact_uri"] == "https://workspace/notebook/1"
    assert (
        fields["leaderboard_artifact_sha256"]
        == hashlib.sha256(
            fields["trial_evidence_json"].encode("utf-8")
        ).hexdigest()
    )
    assert [row["trial_id"] for row in leaderboard["trials"]] == [
        "best-run",
        "other-run",
    ]
    assert [row["rank"] for row in leaderboard["trials"]] == [1, 2]
    assert leaderboard["research_build_id"] == "research-logical"
    assert leaderboard["discovery_id"] == "discovery-logical"
    assert set(leaderboard["trials"][0]) == {
        "rank",
        "trial_id",
        "primary_metric_value",
        "notebook_artifact_uri",
        "notebook_path",
        "notebook_url",
        "is_best_trial",
    }
    assert "params" not in fields["trial_evidence_json"]
    assert "model_description" not in fields["trial_evidence_json"]
    assert "artifact_location" not in fields


def test_leaderboard_is_a_real_mlflow_artifact_and_trials_are_linked(
    monkeypatch,
):
    calls: list[tuple[object, ...]] = []

    class Client:
        def create_run(self, *, experiment_id, tags):
            calls.append(("create_run", experiment_id, tags))
            return SimpleNamespace(
                info=SimpleNamespace(run_id="leaderboard-run")
            )

        def log_text(self, run_id, text, artifact_path):
            calls.append(("log_text", run_id, text, artifact_path))

        def log_metric(self, run_id, key, value):
            calls.append(("log_metric", run_id, key, value))

        def set_tag(self, run_id, key, value):
            calls.append(("set_tag", run_id, key, value))

        def set_terminated(self, run_id, *, status):
            calls.append(("set_terminated", run_id, status))

    fields = _leaderboard_receipt_fields(
        experiment_id="experiment-1",
        trial_ids=("trial-high", "trial-low"),
    )
    fields.pop("leaderboard_run_id")
    fields.pop("leaderboard_artifact_uri")
    monkeypatch.setattr(discovery, "_mlflow_client", Client)

    result = discovery._log_leaderboard_artifact(
        fields,
        research_build_id="research-logical",
        discovery_id="discovery-logical",
        research_parent_run_id="research-parent-run",
    )

    assert result == {
        "leaderboard_run_id": "leaderboard-run",
        "leaderboard_artifact_uri": (
            "runs:/leaderboard-run/automl_discovery/leaderboard.json"
        ),
    }
    log_text = next(call for call in calls if call[0] == "log_text")
    assert log_text[2] == fields["trial_evidence_json"]
    assert log_text[3] == "automl_discovery/leaderboard.json"
    create = next(call for call in calls if call[0] == "create_run")
    assert create[2]["nextads_research_build_id"] == "research-logical"
    assert create[2]["nextads_automl_discovery_id"] == "discovery-logical"
    linked_trials = {
        call[1]
        for call in calls
        if call[:1] == ("set_tag",)
        and call[2] == "nextads_automl_discovery_id"
    }
    assert linked_trials == {"trial-high", "trial-low"}
    assert calls[-1] == ("set_terminated", "leaderboard-run", "FINISHED")


def test_summary_rejects_a_trial_without_generated_notebook_evidence():
    trial = SimpleNamespace(
        mlflow_run_id="trial-without-notebook",
        evaluation_metric_score=0.7,
    )
    summary = SimpleNamespace(
        experiment=SimpleNamespace(experiment_id="experiment-1"),
        trials=(trial,),
        best_trial=trial,
    )

    with pytest.raises(ValueError, match="generated notebook association"):
        discovery._summary_receipt_fields(
            summary,
            research_build_id="research-logical",
            discovery_id="discovery-logical",
            research_parent_run_id="research-parent-run",
        )


def test_discovery_classifies_exact_frame_without_registering(monkeypatch):
    writes = []
    classify_calls = []
    build = _build()
    source_frame = object()
    candidate_frame = object()
    _mock_definition_and_plan(monkeypatch, build)
    claim_state = _mock_new_claim(monkeypatch)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: build,
    )
    definition = SimpleNamespace(checksum=build.model_definition_checksum)
    search = SimpleNamespace(
        plugin=discovery.AUTOML_CANDIDATE_SEARCH_PLUGIN,
    )
    plan = SimpleNamespace(
        checksum=build.research_plan_checksum,
        candidate_search=search,
    )
    monkeypatch.setattr(
        discovery, "load_model_definition", lambda _name: definition
    )
    monkeypatch.setattr(
        discovery, "load_model_research_plan", lambda _name: plan
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        discovery,
        "read_automl_discovery_frame",
        lambda *a, **k: source_frame,
    )
    monkeypatch.setattr(
        discovery,
        "_prepare_automl_frame",
        lambda frame, **kwargs: (
            candidate_frame,
            {"train": 4, "validate": 2, "test": 2},
        ),
    )

    best = SimpleNamespace(
        mlflow_run_id="best-run",
        evaluation_metric_score=0.81,
        artifact_uri="runs:/best-run/notebook",
        notebook_url="https://workspace/notebook/1",
    )

    def classify(**kwargs):
        classify_calls.append(kwargs)
        return SimpleNamespace(
            experiment=SimpleNamespace(
                experiment_id="automl-experiment",
                artifact_location="dbfs:/automl/experiment",
            ),
            trials=(best,),
            best_trial=best,
        )

    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: SimpleNamespace(classify=classify),
    )
    monkeypatch.setattr(
        discovery,
        "_log_leaderboard_artifact",
        lambda *_args, **_kwargs: {
            "leaderboard_run_id": "leaderboard-run",
            "leaderboard_artifact_uri": (
                "runs:/leaderboard-run/automl_discovery/leaderboard.json"
            ),
        },
    )
    monkeypatch.setattr(
        discovery,
        "persist_automl_discovery_receipt",
        lambda *a, **kwargs: writes.append(kwargs["receipt"]),
    )

    evidence = discovery.run_discovery(_args(), spark=object())

    assert len(classify_calls) == 1
    assert classify_calls[0] == {
        "dataset": candidate_frame,
        "target_col": "label",
        "split_col": "automl_split",
        "primary_metric": "roc_auc",
        "timeout_minutes": 30,
        "experiment_dir": "/Shared/model-research/automl",
    }
    assert len(writes) == 1
    assert writes[0].status == READY
    assert writes[0].research_frame_delta_version == 12
    assert evidence["registration_performed"] is False
    assert evidence["main_test_rows_exposed"] == 0
    assert evidence["split_counts"] == {
        "train": 4,
        "validate": 2,
        "test": 2,
    }
    assert claim_state[0].checkpoint == automl_claims.COMPLETE
    assert claim_state[0].experiment_id == "automl-experiment"


def test_identical_ready_discovery_is_reused_before_automl(monkeypatch):
    prior = _ready_receipt()
    build = _build()
    _mock_definition_and_plan(monkeypatch, build)
    _mock_new_claim(monkeypatch)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: _build(),
    )
    monkeypatch.setattr(
        discovery,
        "load_model_definition",
        lambda _name: SimpleNamespace(
            checksum=build.model_definition_checksum
        ),
    )
    monkeypatch.setattr(
        discovery,
        "load_model_research_plan",
        lambda _name: SimpleNamespace(
            checksum=build.research_plan_checksum,
            candidate_search=SimpleNamespace(
                plugin=discovery.AUTOML_CANDIDATE_SEARCH_PLUGIN
            ),
        ),
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: prior,
    )
    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: pytest.fail("AutoML must not run for a READY retry"),
    )

    evidence = discovery.run_discovery(_args(), spark=object())

    assert evidence["reused"] is True
    assert evidence["discovery_attempt_id"] == "discovery-attempt"


def test_unknown_running_claim_fails_closed_without_duplicate(monkeypatch):
    build = _build()
    claim = _existing_claim(automl_claims.RUNNING)
    _mock_definition_and_plan(monkeypatch, build)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: build,
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        discovery,
        "load_automl_claim",
        lambda *a, **k: claim,
    )
    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: pytest.fail("AutoML must not run for an unresolved claim"),
    )

    with pytest.raises(
        automl_claims.AutoMLClaimConflictError,
        match="duplicate experiment will not be launched",
    ):
        discovery.run_discovery(_args(), spark=object())


def test_evidence_ready_claim_recovers_receipt_without_rerun(monkeypatch):
    build = _build()
    claim = _existing_claim(
        automl_claims.EVIDENCE_READY,
        with_evidence=True,
    )
    writes = []
    completions = []
    _mock_definition_and_plan(monkeypatch, build)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: build,
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        discovery,
        "load_automl_claim",
        lambda *a, **k: claim,
    )
    monkeypatch.setattr(
        discovery,
        "persist_automl_discovery_receipt",
        lambda *a, **kwargs: writes.append(kwargs["receipt"]),
    )
    monkeypatch.setattr(
        discovery,
        "complete_automl_claim",
        lambda *a, **kwargs: completions.append(kwargs["claim"]),
    )
    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: pytest.fail("AutoML must not rerun during receipt recovery"),
    )

    evidence = discovery.run_discovery(_args(), spark=object())

    assert len(writes) == 1
    assert writes[0].status == READY
    assert writes[0].experiment_id == "automl-experiment"
    assert completions == [claim]
    assert evidence["reused"] is True


def test_failed_claim_restores_terminal_receipt_without_rerun(monkeypatch):
    build = _build()
    claim = _existing_claim(automl_claims.FAILED)
    writes = []
    _mock_definition_and_plan(monkeypatch, build)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: build,
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        discovery,
        "load_automl_claim",
        lambda *a, **k: claim,
    )
    monkeypatch.setattr(
        discovery,
        "persist_automl_discovery_receipt",
        lambda *a, **kwargs: writes.append(kwargs["receipt"]),
    )
    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: pytest.fail("AutoML must not rerun after a failed claim"),
    )

    with pytest.raises(
        automl_claims.AutoMLClaimConflictError,
        match="failure_reason=RuntimeError: discovery service failed",
    ):
        discovery.run_discovery(_args(), spark=object())

    assert len(writes) == 1
    assert writes[0].status == FAILED
    assert writes[0].failure_reason == claim.failure_reason


def test_failed_discovery_is_persisted_and_re_raised(monkeypatch):
    writes = []
    build = _build()
    _mock_definition_and_plan(monkeypatch, build)
    claim_state = _mock_new_claim(monkeypatch)
    monkeypatch.setattr(
        discovery, "create_research_tables", lambda *a, **k: ()
    )
    monkeypatch.setattr(
        discovery,
        "load_selectable_research_build",
        lambda *a, **k: _build(),
    )
    monkeypatch.setattr(
        discovery,
        "load_model_definition",
        lambda _name: SimpleNamespace(
            checksum=build.model_definition_checksum
        ),
    )
    monkeypatch.setattr(
        discovery,
        "load_model_research_plan",
        lambda _name: SimpleNamespace(
            checksum=build.research_plan_checksum,
            candidate_search=SimpleNamespace(
                plugin=discovery.AUTOML_CANDIDATE_SEARCH_PLUGIN
            ),
        ),
    )
    monkeypatch.setattr(
        discovery,
        "load_ready_automl_discovery_receipt",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        discovery,
        "read_automl_discovery_frame",
        lambda *a, **k: object(),
    )
    monkeypatch.setattr(
        discovery,
        "_prepare_automl_frame",
        lambda *a, **k: (object(), {"train": 4, "validate": 2, "test": 2}),
    )
    monkeypatch.setattr(
        discovery,
        "_load_automl",
        lambda: SimpleNamespace(
            classify=lambda **kwargs: (_ for _ in ()).throw(
                RuntimeError("discovery service failed")
            )
        ),
    )
    monkeypatch.setattr(
        discovery,
        "persist_automl_discovery_receipt",
        lambda *a, **kwargs: writes.append(kwargs["receipt"]),
    )

    with pytest.raises(RuntimeError, match="discovery service failed"):
        discovery.run_discovery(_args(), spark=object())

    assert len(writes) == 1
    assert writes[0].status == FAILED
    assert writes[0].trial_count == 0
    failure = json.loads(writes[0].failure_reason)
    assert failure["error_type"] == "RuntimeError"
    assert failure["stage"] == "automl_classification"
    assert len(failure["message_sha256"]) == 64
    assert "discovery service failed" not in writes[0].failure_reason
    assert claim_state[0].checkpoint == automl_claims.FAILED


def test_evidence_marker_is_bounded_and_contains_no_observation_rows():
    payload = discovery._evidence(
        _ready_receipt(),
        code_sha="abc123",
        reused=False,
        split_counts={"train": 4, "validate": 2, "test": 2},
    )
    encoded = json.dumps(payload, sort_keys=True)

    assert len(encoded) < 2000
    assert "account" not in encoded.lower()
    assert "observation" not in encoded.lower()
    assert payload["registration_performed"] is False
