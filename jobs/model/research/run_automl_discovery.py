"""Run bounded AutoML discovery against one immutable research frame."""

from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import importlib
import json
import logging
import math
from pathlib import Path
import sys
from typing import Any


def resolve_project_root(
    script_file: str | None,
    notebook_path: str | None = None,
    dbutils_handle: Any | None = None,
) -> Path:
    """Resolve the bundle root for Python and Databricks execution."""
    if script_file:
        return Path(script_file).resolve().parents[3]
    if notebook_path is None:
        if dbutils_handle is None:
            dbutils_handle = globals().get("dbutils")
        if dbutils_handle is None:
            raise RuntimeError(
                "Databricks execution requires an injected dbutils handle"
            )
        notebook_path = (
            dbutils_handle.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    return Path(notebook_path).parents[3]


PROJECT_ROOT = resolve_project_root(globals().get("__file__"))
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.model_development.research_contracts import (
    AUTO,
    FAILED,
    READY,
    REVIEW_REQUIRED,
    AutoMLDiscoveryReceipt,
    ModelResearchBuild,
)
from next_ads.model_development.research_failures import safe_failure_reason
from next_ads.model_development.automl_claims import (
    COMPLETE as CLAIM_COMPLETE,
    EVIDENCE_READY,
    AutoMLDiscoveryClaim,
    automl_claim_recovery_error,
    claim_automl_discovery,
    complete_automl_claim,
    fail_automl_claim,
    load_automl_claim,
    record_automl_evidence,
    start_automl_discovery,
    validate_automl_claim_identity,
)
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_research_plan,
)
from next_ads.model_development.research_store import (
    ResearchFrameBinding,
    attempt_id,
    automl_discovery_id,
    create_research_tables,
    load_ready_automl_discovery_receipt,
    load_selectable_research_build,
    persist_automl_discovery_receipt,
    read_automl_discovery_frame,
)


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_RESEARCH_AUTOML_DISCOVERY="
AUTOML_REQUEST_SCHEMA_VERSION = "databricks_automl_classification/v1"
AUTOML_CANDIDATE_SEARCH_PLUGIN = "databricks_automl_classification"
AUTOML_PRIMARY_METRIC = "roc_auc"
AUTOML_LEADERBOARD_SCHEMA_VERSION = "nextads_automl_leaderboard/v1"
AUTOML_LEADERBOARD_ARTIFACT_PATH = "automl_discovery/leaderboard.json"
MAX_AUTOML_TRIALS = 512
MAX_AUTOML_LEADERBOARD_BYTES = 1_000_000


def _effective_research_plan(model_name: str, checksum: str) -> Any:
    """Resolve the exact declared plan for either supported selection mode."""
    declared = load_model_research_plan(model_name)
    if declared is None:
        raise ValueError("Research build model has no declared research plan")
    matches = [
        candidate
        for candidate in (
            replace(declared, selection_policy=REVIEW_REQUIRED),
            replace(declared, selection_policy=AUTO),
        )
        if candidate.checksum == checksum
    ]
    if len(matches) != 1:
        raise ValueError("Research build plan is missing or has drifted")
    return matches[0]


def _request_checksum(
    *,
    timeout_minutes: int,
    experiment_path: str,
    code_sha: str,
    candidate_search_plugin: str = AUTOML_CANDIDATE_SEARCH_PLUGIN,
) -> str:
    """Pin the exact bounded discovery request used for READY reuse."""
    payload = {
        "code_sha": str(code_sha).strip(),
        "experiment_path": str(experiment_path).strip(),
        "candidate_search_plugin": str(candidate_search_plugin).strip(),
        "operation": "databricks.automl.classify",
        "primary_metric": AUTOML_PRIMARY_METRIC,
        "leaderboard_schema_version": AUTOML_LEADERBOARD_SCHEMA_VERSION,
        "request_schema_version": AUTOML_REQUEST_SCHEMA_VERSION,
        "split_policy": "research_train_validate_only/v1",
        "timeout_minutes": int(timeout_minutes),
    }
    if not all(
        payload[name]
        for name in ("code_sha", "experiment_path", "candidate_search_plugin")
    ):
        raise ValueError(
            "AutoML request needs code_sha, experiment_path and a search plug-in"
        )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def parse_enabled(value: str) -> bool:
    """Accept only explicit lower-case true or false values."""
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("enabled must be exactly true or false")


def parse_timeout_minutes(value: str) -> int:
    """Parse the supported one-to-120-minute AutoML bound."""
    try:
        timeout = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "timeout_minutes must be an integer from 1 to 120"
        ) from error
    if timeout < 1 or timeout > 120:
        raise argparse.ArgumentTypeError(
            "timeout_minutes must be an integer from 1 to 120"
        )
    return timeout


def parse_execution_count(value: str) -> int:
    """Require the non-negative Databricks task execution count."""
    try:
        count = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "execution_count must be a non-negative integer"
        ) from error
    if count < 0:
        raise argparse.ArgumentTypeError(
            "execution_count must be a non-negative integer"
        )
    return count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the manual DEV discovery job parameters."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--enabled", type=parse_enabled, default="false")
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--research_build_id", required=True)
    parser.add_argument("--model_catalog", required=True)
    parser.add_argument("--model_schema", required=True)
    parser.add_argument(
        "--timeout_minutes",
        type=parse_timeout_minutes,
        default="30",
    )
    parser.add_argument("--experiment_path", required=True)
    parser.add_argument("--code_sha", required=True)
    parser.add_argument("--orchestration_run_id", required=True)
    parser.add_argument("--task_run_id", required=True)
    parser.add_argument(
        "--execution_count",
        type=parse_execution_count,
        required=True,
    )
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def _frame_binding(build: ModelResearchBuild) -> ResearchFrameBinding:
    """Construct an exact frame binding only from the research receipt."""
    return ResearchFrameBinding(
        research_frame_id=build.research_frame_id,
        research_frame_attempt_id=build.research_frame_attempt_id,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        training_receipt_id=build.training_receipt_id,
        research_frame_table=build.research_frame_table,
        research_frame_delta_version=build.research_frame_delta_version,
        research_frame_row_count=build.research_frame_row_count,
        research_frame_schema_checksum=build.research_frame_schema_checksum,
        research_frame_data_checksum=build.research_frame_data_checksum,
        research_frame_write_receipt_id=(
            build.research_frame_write_receipt_id
        ),
        research_frame_feature_schema_json=(
            build.research_frame_feature_schema_json
        ),
        research_frame_slice_schema_json=(
            build.research_frame_slice_schema_json
        ),
    )


def _declared_feature_names(binding: ResearchFrameBinding) -> tuple[str, ...]:
    """Read the declared model inputs from the immutable frame receipt."""
    try:
        schema = json.loads(binding.research_frame_feature_schema_json)
        fields = schema["fields"]
        names = tuple(field["name"] for field in fields)
    except (TypeError, KeyError, json.JSONDecodeError) as error:
        raise ValueError(
            "Research frame feature schema is not a Spark StructType receipt"
        ) from error
    if (
        not names
        or any(not isinstance(name, str) or not name for name in names)
        or len(names) != len(set(names))
    ):
        raise ValueError(
            "Research frame feature schema needs unique named model inputs"
        )
    reserved = sorted(set(names).intersection({"label", "automl_split"}))
    if reserved:
        raise ValueError(
            "Declared feature names collide with AutoML columns: "
            + ", ".join(reserved)
        )
    return names


def _validation_holdout_date(values: list[Any]) -> Any | None:
    """Use the latest validation date as AutoML's internal test period."""
    dates = sorted(set(values))
    return dates[-1] if len(dates) >= 2 else None


def _prepare_automl_frame(
    frame: Any,
    *,
    binding: ResearchFrameBinding,
) -> tuple[Any, dict[str, int]]:
    """Create AutoML's three splits without releasing the research test."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    required = {"row_id", "observation_date", "split", "label"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            "AutoML discovery frame is missing receipt columns: "
            + ", ".join(missing)
        )
    unexpected = [
        row["split"]
        for row in frame.select("split").distinct().collect()
        if row["split"] not in {"train", "validate"}
    ]
    if unexpected:
        raise ValueError(
            "AutoML discovery received a withheld or unknown research split"
        )

    validation = frame.where(F.col("split") == F.lit("validate"))
    validation_dates = [
        row["observation_date"]
        for row in validation.select("observation_date").distinct().collect()
    ]
    holdout_date = _validation_holdout_date(validation_dates)
    if holdout_date is not None:
        prepared = frame.withColumn(
            "automl_split",
            F.when(F.col("split") == F.lit("train"), F.lit("train"))
            .when(
                F.col("observation_date") == F.lit(holdout_date),
                F.lit("test"),
            )
            .otherwise(F.lit("validate")),
        )
    else:
        validation_count = validation.count()
        if validation_count < 2:
            raise ValueError(
                "AutoML discovery needs two validation rows or two dates"
            )
        validation_cutoff = validation_count // 2
        ranked_validation = validation.withColumn(
            "_automl_validation_rank",
            F.row_number().over(
                Window.orderBy(F.xxhash64("row_id"), F.col("row_id"))
            ),
        ).withColumn(
            "automl_split",
            F.when(
                F.col("_automl_validation_rank") <= F.lit(validation_cutoff),
                F.lit("validate"),
            ).otherwise(F.lit("test")),
        )
        prepared = (
            frame.where(F.col("split") == F.lit("train"))
            .withColumn("automl_split", F.lit("train"))
            .unionByName(
                ranked_validation.drop("_automl_validation_rank"),
                allowMissingColumns=False,
            )
        )

    counts = {
        row["automl_split"]: int(row["count"])
        for row in prepared.groupBy("automl_split").count().collect()
    }
    if set(counts) != {"train", "validate", "test"} or any(
        count < 1 for count in counts.values()
    ):
        raise ValueError(
            "AutoML discovery needs non-empty train, validate and test splits"
        )
    feature_names = _declared_feature_names(binding)
    missing_features = sorted(set(feature_names).difference(prepared.columns))
    if missing_features:
        raise ValueError(
            "AutoML discovery is missing declared model features: "
            + ", ".join(missing_features)
        )
    invalid_label = prepared.where(
        F.col("label").isNull() | ~F.col("label").isin(0.0, 1.0)
    ).limit(1)
    if invalid_label.count():
        raise ValueError(
            "AutoML discovery label must contain only binary 0/1 values"
        )
    return (
        prepared.select(
            *[F.col(name) for name in feature_names],
            F.col("label").cast("int").alias("label"),
            F.col("automl_split"),
        ),
        counts,
    )


def _object_value(value: Any, *names: str) -> Any | None:
    """Read an attribute or mapping value from an AutoML summary object."""
    for name in names:
        if isinstance(value, dict) and value.get(name) is not None:
            return value[name]
        attribute = getattr(value, name, None)
        if attribute is not None:
            return attribute
    return None


def _required_text(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"AutoML summary did not expose {field_name}")
    return text


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    text = _required_text(value, field_name)
    if len(text) > 4096:
        raise ValueError(f"AutoML {field_name} is unexpectedly long")
    return text


def _finite_metric(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"AutoML {field_name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"AutoML {field_name} must be numeric") from error
    if not math.isfinite(number):
        raise ValueError(f"AutoML {field_name} must be finite")
    return number


def _trial_evidence(
    trial: Any,
    *,
    primary_metric: str,
    best_trial_id: str,
) -> dict[str, Any]:
    """Extract only bounded, aggregate evidence from one AutoML trial."""
    trial_id = _required_text(
        _object_value(trial, "mlflow_run_id", "run_id", "trial_id"),
        "a trial run ID",
    )
    metric = _finite_metric(
        _object_value(trial, "evaluation_metric_score"),
        f"{primary_metric} score for trial {trial_id}",
    )
    notebook_artifact_uri = _optional_text(
        _object_value(trial, "artifact_uri"),
        f"notebook artifact URI for trial {trial_id}",
    )
    notebook_path = _optional_text(
        _object_value(trial, "notebook_path"),
        f"notebook path for trial {trial_id}",
    )
    notebook_url = _optional_text(
        _object_value(trial, "notebook_url"),
        f"notebook URL for trial {trial_id}",
    )
    if not any((notebook_artifact_uri, notebook_path, notebook_url)):
        raise ValueError(
            "AutoML summary did not expose a generated notebook association "
            f"for trial {trial_id}"
        )
    return {
        "trial_id": trial_id,
        "primary_metric_value": metric,
        "notebook_artifact_uri": notebook_artifact_uri,
        "notebook_path": notebook_path,
        "notebook_url": notebook_url,
        "is_best_trial": trial_id == best_trial_id,
    }


def _summary_receipt_fields(
    summary: Any,
    *,
    research_build_id: str,
    discovery_id: str,
    research_parent_run_id: str,
    primary_metric: str = AUTOML_PRIMARY_METRIC,
) -> dict[str, Any]:
    """Build durable trial evidence and a deterministic leaderboard payload."""
    experiment = _object_value(summary, "experiment")
    experiment_id = _required_text(
        _object_value(experiment, "experiment_id")
        or _object_value(summary, "experiment_id"),
        "an experiment ID",
    )
    trials = tuple(_object_value(summary, "trials") or ())
    if not trials:
        raise ValueError("AutoML summary did not expose any completed trials")
    if len(trials) > MAX_AUTOML_TRIALS:
        raise ValueError(
            "AutoML summary exceeded the bounded trial evidence limit: "
            f"{len(trials)} > {MAX_AUTOML_TRIALS}"
        )
    best_trial = _object_value(summary, "best_trial")
    if best_trial is None:
        raise ValueError("AutoML summary did not expose a best trial")
    best_trial_id = _required_text(
        _object_value(best_trial, "mlflow_run_id", "run_id", "trial_id"),
        "a best-trial run ID",
    )
    rows = [
        _trial_evidence(
            trial,
            primary_metric=primary_metric,
            best_trial_id=best_trial_id,
        )
        for trial in trials
    ]
    trial_ids = [row["trial_id"] for row in rows]
    if len(trial_ids) != len(set(trial_ids)):
        raise ValueError("AutoML summary contains duplicate trial run IDs")
    if best_trial_id not in set(trial_ids):
        raise ValueError("AutoML best trial is absent from completed trials")
    rows.sort(key=lambda row: (-row["primary_metric_value"], row["trial_id"]))
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    recipe_uri = _object_value(
        best_trial,
        "notebook_url",
        "notebook_path",
        "recipe_artifact_uri",
        "artifact_uri",
    )
    leaderboard = {
        "schema_version": AUTOML_LEADERBOARD_SCHEMA_VERSION,
        "research_build_id": _required_text(
            research_build_id, "a research build ID"
        ),
        "discovery_id": _required_text(discovery_id, "a discovery ID"),
        "research_parent_run_id": _required_text(
            research_parent_run_id, "a research parent run ID"
        ),
        "experiment_id": experiment_id,
        "primary_metric": _required_text(primary_metric, "a primary metric"),
        "trial_count": len(rows),
        "best_trial_id": best_trial_id,
        "trials": rows,
    }
    trial_evidence_json = json.dumps(
        leaderboard,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(trial_evidence_json.encode("utf-8")) > MAX_AUTOML_LEADERBOARD_BYTES:
        raise ValueError(
            "AutoML leaderboard exceeds the bounded artifact size"
        )
    return {
        "experiment_id": experiment_id,
        "best_trial_id": best_trial_id,
        "trial_count": len(rows),
        "primary_metric": primary_metric,
        "trial_evidence_json": trial_evidence_json,
        "leaderboard_artifact_sha256": hashlib.sha256(
            trial_evidence_json.encode("utf-8")
        ).hexdigest(),
        "recipe_artifact_uri": _required_text(
            recipe_uri,
            "a generated recipe association",
        ),
    }


def _mlflow_client() -> Any:
    """Create a tracking client only inside the dedicated ML task."""
    from mlflow.tracking import MlflowClient

    return MlflowClient()


def _log_leaderboard_artifact(
    fields: dict[str, Any],
    *,
    research_build_id: str,
    discovery_id: str,
    research_parent_run_id: str,
) -> dict[str, str]:
    """Log the exact sorted leaderboard and link every trial to research."""
    client = _mlflow_client()
    tags = {
        "mlflow.runName": f"AutoML discovery evidence {discovery_id[:12]}",
        "nextads_automl_discovery_id": discovery_id,
        "nextads_automl_evidence_role": "leaderboard",
        "nextads_model_registration_performed": "false",
        "nextads_research_build_id": research_build_id,
        "nextads_research_parent_run_id": research_parent_run_id,
    }
    evidence_run = client.create_run(
        experiment_id=fields["experiment_id"],
        tags=tags,
    )
    evidence_run_id = _required_text(
        _object_value(_object_value(evidence_run, "info"), "run_id"),
        "an AutoML evidence run ID",
    )
    try:
        client.log_text(
            evidence_run_id,
            fields["trial_evidence_json"],
            AUTOML_LEADERBOARD_ARTIFACT_PATH,
        )
        client.log_metric(
            evidence_run_id,
            "trial_count",
            float(fields["trial_count"]),
        )
        leaderboard = json.loads(fields["trial_evidence_json"])
        client.log_metric(
            evidence_run_id,
            f"best_{fields['primary_metric']}",
            float(leaderboard["trials"][0]["primary_metric_value"]),
        )
        for row in leaderboard["trials"]:
            trial_id = row["trial_id"]
            client.set_tag(
                trial_id,
                "nextads_automl_discovery_id",
                discovery_id,
            )
            client.set_tag(
                trial_id,
                "nextads_research_build_id",
                research_build_id,
            )
        client.set_terminated(evidence_run_id, status="FINISHED")
    except Exception:
        try:
            client.set_terminated(evidence_run_id, status="FAILED")
        except Exception:
            LOGGER.exception(
                "Could not terminate failed AutoML evidence run %s",
                evidence_run_id,
            )
        raise
    return {
        "leaderboard_run_id": evidence_run_id,
        "leaderboard_artifact_uri": (
            f"runs:/{evidence_run_id}/{AUTOML_LEADERBOARD_ARTIFACT_PATH}"
        ),
    }


def _load_automl() -> Any:
    """Import AutoML only inside the dedicated ML-runtime task."""
    return importlib.import_module("databricks.automl")


def _spark_session() -> Any:
    """Use the task's active Spark session without shared job helpers."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    )


def _failure_reason(error: Exception, *, stage: str) -> str:
    """Return a durable reference without copying source values into evidence."""
    return safe_failure_reason(error, stage=f"automl_{stage}")


def _evidence(
    receipt: AutoMLDiscoveryReceipt | None,
    *,
    code_sha: str,
    reused: bool,
    split_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Return a bounded, PII-free job evidence marker."""
    if receipt is None:
        return {
            "code_sha": code_sha,
            "enabled": False,
            "registration_performed": False,
            "status": "DISABLED",
        }
    return {
        "best_trial_id": receipt.best_trial_id,
        "code_sha": code_sha,
        "discovery_attempt_id": receipt.discovery_attempt_id,
        "discovery_id": receipt.discovery_id,
        "enabled": True,
        "experiment_id": receipt.experiment_id,
        "leaderboard_artifact_sha256": (receipt.leaderboard_artifact_sha256),
        "leaderboard_artifact_uri": receipt.leaderboard_artifact_uri,
        "leaderboard_run_id": receipt.leaderboard_run_id,
        "main_test_rows_exposed": 0,
        "primary_metric": receipt.primary_metric,
        "registration_performed": False,
        "request_checksum": receipt.request_checksum,
        "research_build_id": receipt.research_build_id,
        "reused": reused,
        "split_counts": split_counts,
        "status": receipt.status,
        "timeout_minutes": receipt.timeout_minutes,
        "trial_count": receipt.trial_count,
    }


def _validate_claim_request(
    claim: AutoMLDiscoveryClaim,
    *,
    request_checksum: str,
    build: ModelResearchBuild,
    binding: ResearchFrameBinding,
    timeout_minutes: int,
    experiment_path: str,
    code_sha: str,
) -> None:
    """Require a stored control claim to match the exact discovery request."""
    validate_automl_claim_identity(
        claim,
        request_checksum=request_checksum,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        research_frame_id=binding.research_frame_id,
        research_frame_attempt_id=binding.research_frame_attempt_id,
        research_frame_delta_version=binding.research_frame_delta_version,
        timeout_minutes=timeout_minutes,
        experiment_path=experiment_path,
        code_sha=code_sha,
    )


def _ready_receipt_from_claim(
    claim: AutoMLDiscoveryClaim,
    *,
    build: ModelResearchBuild,
    binding: ResearchFrameBinding,
) -> AutoMLDiscoveryReceipt:
    """Recover the immutable READY receipt from locked external evidence."""
    if claim.checkpoint not in {EVIDENCE_READY, CLAIM_COMPLETE}:
        raise automl_claim_recovery_error(claim)
    return AutoMLDiscoveryReceipt(
        discovery_id=claim.discovery_id,
        discovery_attempt_id=claim.discovery_attempt_id,
        request_checksum=claim.request_checksum,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
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
        timeout_minutes=claim.timeout_minutes,
        trial_count=claim.trial_count or 0,
        created_at=claim.created_at,
        completed_at=claim.updated_at,
        experiment_id=claim.experiment_id,
        best_trial_id=claim.best_trial_id,
        primary_metric=claim.primary_metric,
        trial_evidence_json=claim.trial_evidence_json,
        leaderboard_run_id=claim.leaderboard_run_id,
        leaderboard_artifact_sha256=(claim.leaderboard_artifact_sha256),
        leaderboard_artifact_uri=claim.leaderboard_artifact_uri,
        recipe_artifact_uri=claim.recipe_artifact_uri,
    )


def _failed_receipt_from_claim(
    claim: AutoMLDiscoveryClaim,
    *,
    build: ModelResearchBuild,
    binding: ResearchFrameBinding,
) -> AutoMLDiscoveryReceipt:
    """Create the existing terminal failure receipt from its durable claim."""
    return AutoMLDiscoveryReceipt(
        discovery_id=claim.discovery_id,
        discovery_attempt_id=claim.discovery_attempt_id,
        request_checksum=claim.request_checksum,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
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
        status=FAILED,
        timeout_minutes=claim.timeout_minutes,
        trial_count=0,
        created_at=claim.created_at,
        completed_at=claim.updated_at,
        failure_reason=claim.failure_reason,
    )


def _validate_ready_receipt_claim(
    receipt: AutoMLDiscoveryReceipt,
    claim: AutoMLDiscoveryClaim,
) -> None:
    """Ensure a READY receipt is the terminal output of this exact claim."""
    expected = {
        "discovery_id": receipt.discovery_id,
        "discovery_attempt_id": receipt.discovery_attempt_id,
        "request_checksum": receipt.request_checksum,
        "experiment_id": receipt.experiment_id,
        "trial_count": receipt.trial_count,
        "best_trial_id": receipt.best_trial_id,
        "primary_metric": receipt.primary_metric,
        "trial_evidence_json": receipt.trial_evidence_json,
        "leaderboard_run_id": receipt.leaderboard_run_id,
        "leaderboard_artifact_sha256": (receipt.leaderboard_artifact_sha256),
        "leaderboard_artifact_uri": receipt.leaderboard_artifact_uri,
        "recipe_artifact_uri": receipt.recipe_artifact_uri,
    }
    changed = sorted(
        name
        for name, value in expected.items()
        if getattr(claim, name) != value
    )
    if changed:
        raise ValueError(
            "READY AutoML receipt differs from its durable claim: "
            + ", ".join(changed)
        )


def run_discovery(
    args: argparse.Namespace,
    *,
    spark: Any,
) -> dict[str, Any]:
    """Run or reuse one frame-pinned, non-registering discovery attempt."""
    build = load_selectable_research_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        research_build_id=args.research_build_id,
    )
    if build is None:
        raise ValueError(
            "No completed research build is available for AutoML discovery: "
            f"{args.research_build_id}"
        )
    expected_model_name = getattr(args, "model_name", build.model_name)
    if build.model_name != expected_model_name:
        raise ValueError(
            "Research build does not belong to the requested model"
        )
    definition = load_model_definition(build.model_name)
    plan = _effective_research_plan(
        build.model_name,
        build.research_plan_checksum,
    )
    if definition.checksum != build.model_definition_checksum:
        raise ValueError(
            "Research build model definition checksum has drifted"
        )
    search = plan.candidate_search
    if search is None or search.plugin != AUTOML_CANDIDATE_SEARCH_PLUGIN:
        raise ValueError(
            "Research plan does not declare Databricks AutoML discovery"
        )
    create_research_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    binding = _frame_binding(build)
    request_checksum = _request_checksum(
        timeout_minutes=args.timeout_minutes,
        experiment_path=args.experiment_path,
        code_sha=args.code_sha,
        candidate_search_plugin=search.plugin,
    )
    discovery_id = automl_discovery_id(
        research_build_id=build.research_build_id,
        research_frame_id=binding.research_frame_id,
        research_frame_delta_version=binding.research_frame_delta_version,
        request_checksum=request_checksum,
    )
    prior = load_ready_automl_discovery_receipt(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        discovery_id=discovery_id,
    )
    existing_claim = load_automl_claim(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        discovery_id=discovery_id,
    )
    if prior is not None:
        if prior.request_checksum != request_checksum:
            raise ValueError("READY AutoML discovery request checksum differs")
        if existing_claim is not None:
            _validate_claim_request(
                existing_claim,
                request_checksum=request_checksum,
                build=build,
                binding=binding,
                timeout_minutes=args.timeout_minutes,
                experiment_path=args.experiment_path,
                code_sha=args.code_sha,
            )
            _validate_ready_receipt_claim(prior, existing_claim)
            if existing_claim.checkpoint == EVIDENCE_READY:
                complete_automl_claim(
                    spark,
                    catalog=args.model_catalog,
                    schema=args.model_schema,
                    claim=existing_claim,
                )
            elif existing_claim.checkpoint != CLAIM_COMPLETE:
                raise automl_claim_recovery_error(existing_claim)
        return _evidence(prior, code_sha=args.code_sha, reused=True)

    invocation_id = ":".join(
        (
            str(args.orchestration_run_id),
            str(args.task_run_id),
            str(args.execution_count),
        )
    )
    discovery_attempt_id = attempt_id(
        logical_id=discovery_id,
        invocation_id=invocation_id,
    )
    if existing_claim is not None:
        _validate_claim_request(
            existing_claim,
            request_checksum=request_checksum,
            build=build,
            binding=binding,
            timeout_minutes=args.timeout_minutes,
            experiment_path=args.experiment_path,
            code_sha=args.code_sha,
        )
        if existing_claim.checkpoint in {EVIDENCE_READY, CLAIM_COMPLETE}:
            receipt = _ready_receipt_from_claim(
                existing_claim,
                build=build,
                binding=binding,
            )
            persist_automl_discovery_receipt(
                spark,
                catalog=args.model_catalog,
                schema=args.model_schema,
                receipt=receipt,
            )
            if existing_claim.checkpoint == EVIDENCE_READY:
                complete_automl_claim(
                    spark,
                    catalog=args.model_catalog,
                    schema=args.model_schema,
                    claim=existing_claim,
                )
            return _evidence(receipt, code_sha=args.code_sha, reused=True)
        if existing_claim.checkpoint == FAILED:
            receipt = _failed_receipt_from_claim(
                existing_claim,
                build=build,
                binding=binding,
            )
            persist_automl_discovery_receipt(
                spark,
                catalog=args.model_catalog,
                schema=args.model_schema,
                receipt=receipt,
            )
        raise automl_claim_recovery_error(existing_claim)

    claim = claim_automl_discovery(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        discovery_id=discovery_id,
        discovery_attempt_id=discovery_attempt_id,
        request_checksum=request_checksum,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        research_frame_id=binding.research_frame_id,
        research_frame_attempt_id=binding.research_frame_attempt_id,
        research_frame_delta_version=binding.research_frame_delta_version,
        timeout_minutes=args.timeout_minutes,
        experiment_path=args.experiment_path,
        code_sha=args.code_sha,
        owner_invocation_id=invocation_id,
    )
    split_counts: dict[str, int] | None = None
    try:
        source = read_automl_discovery_frame(spark, binding=binding)
        dataset, split_counts = _prepare_automl_frame(
            source,
            binding=binding,
        )
    except Exception as error:
        failed_claim = fail_automl_claim(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            claim=claim,
            failure_reason=_failure_reason(error, stage="frame_preparation"),
        )
        receipt = _failed_receipt_from_claim(
            failed_claim,
            build=build,
            binding=binding,
        )
        persist_automl_discovery_receipt(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            receipt=receipt,
        )
        LOGGER.error(
            "%s%s",
            EVIDENCE_PREFIX,
            json.dumps(
                _evidence(
                    receipt,
                    code_sha=args.code_sha,
                    reused=False,
                    split_counts=split_counts,
                ),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        raise

    claim = start_automl_discovery(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        claim=claim,
    )
    try:
        summary = _load_automl().classify(
            dataset=dataset,
            target_col="label",
            split_col="automl_split",
            primary_metric=AUTOML_PRIMARY_METRIC,
            timeout_minutes=args.timeout_minutes,
            experiment_dir=args.experiment_path,
        )
        fields = _summary_receipt_fields(
            summary,
            research_build_id=build.research_build_id,
            discovery_id=discovery_id,
            research_parent_run_id=build.mlflow_parent_run_id,
        )
        fields.update(
            _log_leaderboard_artifact(
                fields,
                research_build_id=build.research_build_id,
                discovery_id=discovery_id,
                research_parent_run_id=build.mlflow_parent_run_id,
            )
        )
    except Exception as error:
        failed_claim = fail_automl_claim(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            claim=claim,
            failure_reason=_failure_reason(error, stage="classification"),
        )
        receipt = _failed_receipt_from_claim(
            failed_claim,
            build=build,
            binding=binding,
        )
        persist_automl_discovery_receipt(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            receipt=receipt,
        )
        LOGGER.error(
            "%s%s",
            EVIDENCE_PREFIX,
            json.dumps(
                _evidence(
                    receipt,
                    code_sha=args.code_sha,
                    reused=False,
                    split_counts=split_counts,
                ),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        raise

    claim = record_automl_evidence(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        claim=claim,
        experiment_id=fields["experiment_id"],
        trial_count=fields["trial_count"],
        best_trial_id=fields["best_trial_id"],
        primary_metric=fields["primary_metric"],
        trial_evidence_json=fields["trial_evidence_json"],
        leaderboard_run_id=fields["leaderboard_run_id"],
        leaderboard_artifact_sha256=(fields["leaderboard_artifact_sha256"]),
        leaderboard_artifact_uri=fields["leaderboard_artifact_uri"],
        recipe_artifact_uri=fields["recipe_artifact_uri"],
    )
    receipt = _ready_receipt_from_claim(
        claim,
        build=build,
        binding=binding,
    )
    persist_automl_discovery_receipt(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        receipt=receipt,
    )
    complete_automl_claim(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        claim=claim,
    )
    return _evidence(
        receipt,
        code_sha=args.code_sha,
        reused=False,
        split_counts=split_counts,
    )


def main(argv: list[str] | None = None) -> None:
    """Run the explicitly enabled discovery task and emit bounded evidence."""
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    if not args.enabled:
        evidence = _evidence(None, code_sha=args.code_sha, reused=False)
    else:
        evidence = run_discovery(args, spark=_spark_session())
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()
