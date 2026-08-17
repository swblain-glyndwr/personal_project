"""Typed, model-neutral contracts for research, training and scoring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import re
from typing import Any, Protocol, runtime_checkable


DBR_15_4_SPARK_CPU = "dbr_15_4_spark_cpu"
DBR_18_1_THEME_GPU = "dbr_18_1_theme_gpu"
ALLOWED_RUNTIME_PROFILES = frozenset(
    {DBR_15_4_SPARK_CPU, DBR_18_1_THEME_GPU}
)
EVALUATE = "EVALUATE"
READY = "READY"
FAILED = "FAILED"
VALID_RECEIPT_STATUSES = frozenset({READY, FAILED})
VALID_MODEL_BUILD_STATUSES = frozenset(
    {"TRAINING", "READY", "FAILED"}
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DEFAULT_VALUE_TYPES = (str, int, float, bool, type(None))


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _text(value, field_name)


def _digest(value: object, field_name: str) -> str:
    text = _text(value, field_name)
    if _SHA256.fullmatch(text) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return text


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


def _date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError(f"{field_name} must be a date")
    return value


def _names(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    result = tuple(_text(value, field_name) for value in values)
    if len(result) != len(set(result)):
        raise ValueError(f"{field_name} must contain unique values")
    return result


def _pairs(
    values: tuple[tuple[str, Any], ...],
    field_name: str,
) -> tuple[tuple[str, Any], ...]:
    result = tuple(values)
    keys = []
    for value in result:
        if not isinstance(value, tuple) or len(value) != 2:
            raise ValueError(f"{field_name} must contain name/value pairs")
        keys.append(_text(value[0], field_name))
    if len(keys) != len(set(keys)):
        raise ValueError(f"{field_name} names must be unique")
    return result


@dataclass(frozen=True)
class FeatureLookupSpec:
    """One point-in-time lookup declared by logical feature name."""

    feature_id: str
    selected_columns: tuple[str, ...]
    key_mapping: tuple[tuple[str, str], ...]
    observation_timestamp: str
    renames: tuple[tuple[str, str], ...] = ()
    defaults: tuple[tuple[str, str | int | float | bool | None], ...] = ()

    def __post_init__(self) -> None:
        """Validate lookup keys, selected values and time semantics."""
        object.__setattr__(self, "feature_id", _text(self.feature_id, "feature_id"))
        selected = _names(self.selected_columns, "selected_columns")
        object.__setattr__(self, "selected_columns", selected)
        object.__setattr__(
            self,
            "observation_timestamp",
            _text(self.observation_timestamp, "observation_timestamp"),
        )
        mapping = _pairs(self.key_mapping, "key_mapping")
        if not mapping:
            raise ValueError("key_mapping must contain at least one key")
        for feature_key, observation_key in mapping:
            _text(feature_key, "feature key")
            _text(observation_key, "observation key")
        object.__setattr__(self, "key_mapping", mapping)
        renames = _pairs(self.renames, "renames")
        for source, target in renames:
            _text(source, "rename source")
            _text(target, "rename target")
        unknown_renames = sorted(
            set(source for source, _target in renames).difference(selected)
        )
        if unknown_renames:
            raise ValueError(
                "renames reference unselected columns: "
                + ", ".join(unknown_renames)
            )
        rename_targets = [target for _source, target in renames]
        if len(rename_targets) != len(set(rename_targets)):
            raise ValueError("rename targets must be unique")
        object.__setattr__(self, "renames", renames)
        defaults = _pairs(self.defaults, "defaults")
        unknown_defaults = sorted(
            set(column for column, _value in defaults).difference(selected)
        )
        if unknown_defaults:
            raise ValueError(
                "defaults reference unselected columns: "
                + ", ".join(unknown_defaults)
            )
        if any(
            not isinstance(value, _DEFAULT_VALUE_TYPES)
            for _column, value in defaults
        ):
            raise ValueError("feature defaults must be JSON scalar values")
        object.__setattr__(self, "defaults", defaults)

    def as_dict(self) -> dict[str, object]:
        return {
            "feature_id": self.feature_id,
            "selected_columns": self.selected_columns,
            "key_mapping": self.key_mapping,
            "observation_timestamp": self.observation_timestamp,
            "renames": self.renames,
            "defaults": self.defaults,
        }


@dataclass(frozen=True)
class ModelDefinition:
    """The author-owned declaration for one evaluation model."""

    model_name: str
    provider_id: str
    problem_statement: str
    prediction_entity: str
    prediction_time: str
    label: str
    observation_keys: tuple[str, ...]
    success_metrics: tuple[str, ...]
    runtime_profile: str
    feature_lookups: tuple[FeatureLookupSpec, ...]
    trainer: str
    score_provider: str
    candidate_adapter: str
    activation_mode: str = EVALUATE

    def __post_init__(self) -> None:
        """Keep definitions model-neutral, reproducible and evaluate-only."""
        for field_name in (
            "model_name",
            "provider_id",
            "problem_statement",
            "prediction_entity",
            "prediction_time",
            "label",
            "trainer",
            "score_provider",
            "candidate_adapter",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        metrics = _names(self.success_metrics, "success_metrics")
        object.__setattr__(self, "success_metrics", metrics)
        observation_keys = _names(
            self.observation_keys,
            "observation_keys",
        )
        object.__setattr__(self, "observation_keys", observation_keys)
        if self.runtime_profile not in ALLOWED_RUNTIME_PROFILES:
            raise ValueError(
                f"Unsupported model runtime profile: {self.runtime_profile}"
            )
        lookups = tuple(self.feature_lookups)
        if not lookups:
            raise ValueError("A model definition needs at least one feature lookup")
        feature_ids = [lookup.feature_id for lookup in lookups]
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("A model definition cannot repeat a feature lookup")
        object.__setattr__(self, "feature_lookups", lookups)
        if self.activation_mode != EVALUATE:
            raise ValueError(
                "New model definitions must remain EVALUATE until a separate "
                "activation policy is approved"
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "model_name": self.model_name,
            "provider_id": self.provider_id,
            "problem_statement": self.problem_statement,
            "prediction_entity": self.prediction_entity,
            "prediction_time": self.prediction_time,
            "label": self.label,
            "observation_keys": self.observation_keys,
            "success_metrics": self.success_metrics,
            "runtime_profile": self.runtime_profile,
            "feature_lookups": tuple(
                lookup.as_dict() for lookup in self.feature_lookups
            ),
            "trainer": self.trainer,
            "score_provider": self.score_provider,
            "candidate_adapter": self.candidate_adapter,
            "activation_mode": self.activation_mode,
        }

    @property
    def checksum(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self.as_dict(),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class TrainingFeatureBinding:
    """Exact READY feature version included in one training set."""

    feature_id: str
    feature_snapshot_id: str
    feature_snapshot_attempt_id: str
    backing_table: str
    delta_version: int
    row_count: int
    schema_checksum: str
    value_checksum: str

    def __post_init__(self) -> None:
        """Validate one exact READY feature input binding."""
        for field_name in (
            "feature_id",
            "feature_snapshot_id",
            "feature_snapshot_attempt_id",
            "backing_table",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if self.delta_version < 0 or self.row_count < 0:
            raise ValueError("Feature Delta version and row count cannot be negative")
        _digest(self.schema_checksum, "schema_checksum")
        _digest(self.value_checksum, "value_checksum")


@dataclass(frozen=True)
class TrainingSetReceipt:
    """Reproducible proof written before a trainer can run."""

    receipt_id: str
    model_name: str
    model_definition_checksum: str
    feature_bindings: tuple[TrainingFeatureBinding, ...]
    observation_start: date
    observation_end: date
    label_end: date
    schema_checksum: str
    data_checksum: str
    code_sha: str
    leakage_status: str
    status: str
    created_at: datetime
    completed_at: datetime
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Ensure READY receipts have complete, leakage-safe evidence."""
        for field_name in ("receipt_id", "model_name", "code_sha"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        for field_name in (
            "model_definition_checksum",
            "schema_checksum",
            "data_checksum",
        ):
            _digest(getattr(self, field_name), field_name)
        bindings = tuple(self.feature_bindings)
        if not bindings:
            raise ValueError("A training receipt needs feature bindings")
        identities = [
            (
                binding.feature_id,
                binding.feature_snapshot_id,
                binding.feature_snapshot_attempt_id,
            )
            for binding in bindings
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("Training receipt feature bindings must be unique")
        object.__setattr__(self, "feature_bindings", bindings)
        for field_name in ("observation_start", "observation_end", "label_end"):
            _date(getattr(self, field_name), field_name)
        if not self.observation_start <= self.observation_end <= self.label_end:
            raise ValueError(
                "Training receipt dates must be observation_start <= "
                "observation_end <= label_end"
            )
        if self.leakage_status not in {"PASS", "FAIL"}:
            raise ValueError("leakage_status must be PASS or FAIL")
        if self.status not in VALID_RECEIPT_STATUSES:
            raise ValueError(f"Unsupported training receipt status: {self.status}")
        _timestamp(self.created_at, "created_at")
        _timestamp(self.completed_at, "completed_at")
        if self.completed_at < self.created_at:
            raise ValueError("completed_at cannot predate created_at")
        failure = _optional_text(self.failure_reason, "failure_reason")
        object.__setattr__(self, "failure_reason", failure)
        if self.status == READY:
            if self.leakage_status != "PASS" or failure is not None:
                raise ValueError(
                    "A READY training receipt must pass leakage checks"
                )
        elif failure is None:
            raise ValueError("A failed training receipt needs failure_reason")


@dataclass(frozen=True)
class ModelBuild:
    """The exact MLflow artifact created from one READY training receipt."""

    model_build_id: str
    model_name: str
    training_receipt_id: str
    model_definition_checksum: str
    runtime_profile: str
    status: str
    created_at: datetime
    mlflow_run_id: str | None = None
    registered_model_name: str | None = None
    registered_model_version: int | None = None
    model_uri: str | None = None
    artifact_digest: str | None = None
    metrics: tuple[tuple[str, float], ...] = ()
    completed_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Ensure READY builds identify one exact registered artifact."""
        for field_name in (
            "model_build_id",
            "model_name",
            "training_receipt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _digest(self.model_definition_checksum, "model_definition_checksum")
        if self.runtime_profile not in ALLOWED_RUNTIME_PROFILES:
            raise ValueError(
                f"Unsupported model runtime profile: {self.runtime_profile}"
            )
        if self.status not in VALID_MODEL_BUILD_STATUSES:
            raise ValueError(f"Unsupported model build status: {self.status}")
        _timestamp(self.created_at, "created_at")
        metrics = _pairs(self.metrics, "metrics")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for _name, value in metrics):
            raise ValueError("Model metrics must be numeric")
        object.__setattr__(
            self,
            "metrics",
            tuple((name, float(value)) for name, value in metrics),
        )
        terminal_fields = (
            self.mlflow_run_id,
            self.registered_model_name,
            self.registered_model_version,
            self.model_uri,
            self.artifact_digest,
            self.completed_at,
        )
        failure = _optional_text(self.failure_reason, "failure_reason")
        object.__setattr__(self, "failure_reason", failure)
        if self.status == "TRAINING":
            if any(value is not None for value in terminal_fields) or failure:
                raise ValueError("A TRAINING model build cannot be terminal")
            return
        if self.completed_at is None:
            raise ValueError("A terminal model build needs completed_at")
        if self.completed_at < self.created_at:
            raise ValueError("completed_at cannot predate created_at")
        if self.status == FAILED:
            if failure is None:
                raise ValueError("A failed model build needs failure_reason")
            return
        if failure is not None:
            raise ValueError("A READY model build cannot have failure_reason")
        required_ready = terminal_fields[:-1]
        if any(value is None for value in required_ready):
            raise ValueError(
                "A READY model build needs the exact MLflow artifact identity"
            )
        if (
            isinstance(self.registered_model_version, bool)
            or not isinstance(self.registered_model_version, int)
            or self.registered_model_version < 1
        ):
            raise ValueError("Registered model version must be positive")
        _digest(self.artifact_digest, "artifact_digest")


@runtime_checkable
class Trainer(Protocol):
    def train(
        self,
        definition: ModelDefinition,
        training_receipt: TrainingSetReceipt,
        training_frame: Any,
    ) -> ModelBuild: ...


@runtime_checkable
class ScoreProvider(Protocol):
    def score(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
    ) -> Any: ...


@runtime_checkable
class CandidateAdapter(Protocol):
    def apply(
        self,
        provider_scores: Any,
        eligible_candidates: Any,
    ) -> Any: ...


__all__ = [
    "ALLOWED_RUNTIME_PROFILES",
    "DBR_15_4_SPARK_CPU",
    "DBR_18_1_THEME_GPU",
    "CandidateAdapter",
    "FeatureLookupSpec",
    "ModelBuild",
    "ModelDefinition",
    "ScoreProvider",
    "Trainer",
    "TrainingFeatureBinding",
    "TrainingSetReceipt",
]
