"""Typed contracts for repeatable model research from Feature Store data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
import re
from typing import Any, Mapping, Protocol, runtime_checkable

from next_ads.model_development.contracts import (
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)


AUTO = "AUTO"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
VALID_SELECTION_MODES = frozenset({AUTO, REVIEW_REQUIRED})

RESEARCHING = "RESEARCHING"
AWAITING_SELECTION = "AWAITING_SELECTION"
READY = "READY"
FAILED = "FAILED"
VALID_RESEARCH_BUILD_STATUSES = frozenset(
    {RESEARCHING, AWAITING_SELECTION, READY, FAILED}
)
VALID_CANDIDATE_STATUSES = frozenset({RESEARCHING, READY, FAILED})
VALID_SELECTION_STATUSES = frozenset({READY, FAILED})
VALID_DISCOVERY_STATUSES = frozenset({RESEARCHING, READY, FAILED})
VALID_EXPLANATION_STATUSES = frozenset({READY, FAILED, "NOT_APPLICABLE"})

STANDARD_PREDICTION_FIELDS = (
    "score",
    "prediction",
    "split",
    "observation_date",
    "row_id",
)
MANDATORY_BINARY_METRICS = (
    "auc_pr",
    "prevalence",
    "auc_roc",
    "log_loss",
    "observed_click_rate",
    "predicted_click_rate",
    "calibration_gap",
    "precision_at_1_percent",
    "recall_at_1_percent",
    "lift_at_1_percent",
    "precision_at_5_percent",
    "recall_at_5_percent",
    "lift_at_5_percent",
    "precision_at_10_percent",
    "recall_at_10_percent",
    "lift_at_10_percent",
)
MANDATORY_BINARY_EVIDENCE = (
    "precision_recall_curve",
    "roc_curve",
    "calibration",
    "lift_and_cumulative_gain",
    "score_distributions",
    "top_fraction_confusion",
    "slice_metrics",
    "feature_importance",
    "missingness_and_default_coverage",
    "candidate_comparison",
)
MANDATORY_EXPLANATION_REQUIREMENTS = (
    "global_feature_importance",
    "readable_feature_names",
    "model_specific_or_permutation",
)

# These values belong to research orchestration, not candidate estimators.
PROTECTED_CANDIDATE_PARAMETERS = frozenset(
    {
        "alias",
        "features_col",
        "label_col",
        "mlflow_experiment",
        "mlflow_experiment_id",
        "mlflow_run_id",
        "model_alias",
        "model_name",
        "output_table",
        "prediction_col",
        "probability_col",
        "provider_id",
        "publish",
        "publish_scores",
        "random_seed",
        "random_state",
        "raw_prediction_col",
        "register_model",
        "registered_model_name",
        "score_provider",
        "score_table",
        "seed",
        "set_alias",
        "split",
        "split_col",
        "split_column",
        "split_seed",
        "test_end",
        "test_start",
        "train_end",
        "train_start",
        "validation_end",
        "validation_start",
    }
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_ALIAS = re.compile(r"[a-z][a-z0-9_]*")
_PYTHON_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_JSON_SCALARS = (str, int, float, bool, type(None))


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _text(value, field_name)


def standard_prediction_columns(label_column: str) -> tuple[str, ...]:
    """Return the standard prediction contract for a declared label."""
    return (_text(label_column, "label_column"), *STANDARD_PREDICTION_FIELDS)


def _digest(value: object, field_name: str) -> str:
    text = _text(value, field_name)
    if _DIGEST.fullmatch(text) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return text


def _optional_digest(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _digest(value, field_name)


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


def _date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError(f"{field_name} must be a date")
    return value


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
    ):
        raise ValueError(f"{field_name} must be an integer >= {minimum}")
    return value


def _names(
    values: tuple[str, ...],
    field_name: str,
    *,
    required: bool = True,
) -> tuple[str, ...]:
    result = tuple(_text(value, field_name) for value in values)
    if required and not result:
        raise ValueError(f"{field_name} must contain at least one value")
    if len(result) != len(set(result)):
        raise ValueError(f"{field_name} must contain unique values")
    return result


def _plugin_identifier(value: object, field_name: str) -> str:
    identifier = _text(value, field_name)
    if _ALIAS.fullmatch(identifier):
        return identifier
    parts = identifier.split(".")
    if (
        len(parts) >= 3
        and parts[0] == "next_ads"
        and all(_PYTHON_PART.fullmatch(part) for part in parts)
    ):
        return identifier
    raise ValueError(
        f"{field_name} must be a plug-in alias or a next_ads.* class"
    )


def _normalised_parameter_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _json_copy(value: object, field_name: str) -> object:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be JSON-safe") from error
    return json.loads(encoded)


def _json_text(value: object, field_name: str) -> str:
    text = _text(value, field_name)
    try:
        json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f"{field_name} must contain valid JSON") from error
    return text


def _parameters(
    value: Mapping[str, Any] | tuple[tuple[str, Any], ...],
) -> tuple[tuple[str, object], ...]:
    if isinstance(value, Mapping):
        items = tuple(value.items())
    elif isinstance(value, tuple):
        items = value
    else:
        raise ValueError("parameters must be a mapping")
    result: list[tuple[str, object]] = []
    names: list[str] = []
    for item in items:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("parameters must contain name/value pairs")
        name = _text(item[0], "parameter name")
        normalised = _normalised_parameter_name(name)
        if normalised in PROTECTED_CANDIDATE_PARAMETERS:
            raise ValueError(
                f"Candidate parameter is controlled by orchestration: {name}"
            )
        names.append(name)
        result.append((name, _json_copy(item[1], f"parameters.{name}")))
    if len(names) != len(set(names)):
        raise ValueError("Candidate parameter names must be unique")
    return tuple(sorted(result, key=lambda item: item[0]))


def _metrics(
    values: tuple[tuple[str, float], ...],
) -> tuple[tuple[str, float], ...]:
    result: list[tuple[str, float]] = []
    names: list[str] = []
    for item in values:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("metrics must contain name/value pairs")
        name = _text(item[0], "metric name")
        value = item[1]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise ValueError("Candidate metrics must be finite numbers")
        names.append(name)
        result.append((name, float(value)))
    if len(names) != len(set(names)):
        raise ValueError("Candidate metric names must be unique")
    return tuple(sorted(result, key=lambda item: item[0]))


@dataclass(frozen=True)
class CandidateSpec:
    """One declared estimator plug-in and its reproducible parameters."""

    candidate_id: str
    plugin: str
    parameters: Mapping[str, Any] | tuple[tuple[str, Any], ...] = ()
    seed: int = 1729
    failure_allowed: bool = False

    def __post_init__(self) -> None:
        """Validate estimator identity, parameters and reproducibility."""
        candidate_id = _text(self.candidate_id, "candidate_id")
        if _ALIAS.fullmatch(candidate_id) is None:
            raise ValueError("candidate_id must be lower snake case")
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(
            self,
            "plugin",
            _plugin_identifier(self.plugin, "plugin"),
        )
        object.__setattr__(self, "parameters", _parameters(self.parameters))
        _integer(self.seed, "seed")
        if not isinstance(self.failure_allowed, bool):
            raise ValueError("failure_allowed must be true or false")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "plugin": self.plugin,
            "parameters": {
                name: _json_copy(value, f"parameters.{name}")
                for name, value in self.parameters
            },
            "seed": self.seed,
            "failure_allowed": self.failure_allowed,
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
class SliceSpec:
    """A reporting slice whose low-volume results must be suppressed."""

    slice_id: str
    column: str
    values: tuple[str | int | float | bool, ...] = ()
    if_present: bool = False
    minimum_rows: int = 100

    def __post_init__(self) -> None:
        """Validate reporting identity and suppression threshold."""
        slice_id = _text(self.slice_id, "slice_id")
        if _ALIAS.fullmatch(slice_id) is None:
            raise ValueError("slice_id must be lower snake case")
        object.__setattr__(self, "slice_id", slice_id)
        object.__setattr__(self, "column", _text(self.column, "column"))
        values = tuple(self.values)
        for value in values:
            if (
                not isinstance(value, _JSON_SCALARS[:-1])
                or isinstance(value, float)
                and not math.isfinite(value)
            ):
                raise ValueError("Slice values must be finite JSON scalars")
        if len(values) != len(set(values)):
            raise ValueError("Slice values must be unique")
        object.__setattr__(self, "values", values)
        if not isinstance(self.if_present, bool):
            raise ValueError("if_present must be true or false")
        _integer(self.minimum_rows, "minimum_rows", minimum=1)

    def as_dict(self) -> dict[str, object]:
        return {
            "slice_id": self.slice_id,
            "column": self.column,
            "values": self.values,
            "if_present": self.if_present,
            "minimum_rows": self.minimum_rows,
        }


@dataclass(frozen=True)
class EvaluationRules:
    """Consistent evidence rules applied to every binary candidate."""

    required_metrics: tuple[str, ...] = MANDATORY_BINARY_METRICS
    required_evidence: tuple[str, ...] = MANDATORY_BINARY_EVIDENCE
    top_fractions: tuple[float, ...] = (0.01, 0.05, 0.10)
    confidence_interval_metrics: tuple[str, ...] = (
        "auc_pr",
        "lift_at_5_percent",
    )
    confidence_level: float = 0.95
    confidence_interval_resamples: int = 1000
    confidence_interval_seed: int = 1729
    minimum_slice_rows: int = 100
    prevalence_baseline: bool = True

    def __post_init__(self) -> None:
        """Retain the standard metrics and evidence for every candidate."""
        metrics = _names(self.required_metrics, "required_metrics")
        missing_metrics = sorted(
            set(MANDATORY_BINARY_METRICS).difference(metrics)
        )
        if missing_metrics:
            raise ValueError(
                "required_metrics omit standard binary metrics: "
                + ", ".join(missing_metrics)
            )
        object.__setattr__(self, "required_metrics", metrics)
        evidence = _names(self.required_evidence, "required_evidence")
        missing_evidence = sorted(
            set(MANDATORY_BINARY_EVIDENCE).difference(evidence)
        )
        if missing_evidence:
            raise ValueError(
                "required_evidence omit standard binary evidence: "
                + ", ".join(missing_evidence)
            )
        object.__setattr__(self, "required_evidence", evidence)
        fractions = tuple(float(value) for value in self.top_fractions)
        if any(
            not math.isfinite(value) or value <= 0 or value > 1
            for value in fractions
        ) or len(fractions) != len(set(fractions)):
            raise ValueError("top_fractions must be unique values in (0, 1]")
        missing_fractions = {0.01, 0.05, 0.10}.difference(fractions)
        if missing_fractions:
            raise ValueError("top_fractions must include 1%, 5% and 10%")
        object.__setattr__(self, "top_fractions", tuple(sorted(fractions)))
        confidence_metrics = _names(
            self.confidence_interval_metrics,
            "confidence_interval_metrics",
        )
        unknown = sorted(set(confidence_metrics).difference(metrics))
        if unknown:
            raise ValueError(
                "Confidence interval metrics are not required metrics: "
                + ", ".join(unknown)
            )
        object.__setattr__(
            self,
            "confidence_interval_metrics",
            confidence_metrics,
        )
        if (
            isinstance(self.confidence_level, bool)
            or not isinstance(self.confidence_level, (int, float))
            or not 0 < float(self.confidence_level) < 1
        ):
            raise ValueError("confidence_level must be between zero and one")
        object.__setattr__(
            self, "confidence_level", float(self.confidence_level)
        )
        _integer(
            self.confidence_interval_resamples,
            "confidence_interval_resamples",
            minimum=1,
        )
        _integer(self.confidence_interval_seed, "confidence_interval_seed")
        _integer(self.minimum_slice_rows, "minimum_slice_rows", minimum=1)
        if not isinstance(self.prevalence_baseline, bool):
            raise ValueError("prevalence_baseline must be true or false")

    def as_dict(self) -> dict[str, object]:
        return {
            "required_metrics": self.required_metrics,
            "required_evidence": self.required_evidence,
            "top_fractions": self.top_fractions,
            "confidence_interval_metrics": self.confidence_interval_metrics,
            "confidence_level": self.confidence_level,
            "confidence_interval_resamples": self.confidence_interval_resamples,
            "confidence_interval_seed": self.confidence_interval_seed,
            "minimum_slice_rows": self.minimum_slice_rows,
            "prevalence_baseline": self.prevalence_baseline,
        }


@dataclass(frozen=True)
class TemporalSplitSpec:
    """Exact, non-overlapping train, validation and untouched test dates."""

    train_start: date
    train_end: date
    validate_start: date
    validate_end: date
    test_start: date
    test_end: date

    def __post_init__(self) -> None:
        """Reject overlapping or incorrectly ordered temporal periods."""
        for field_name in (
            "train_start",
            "train_end",
            "validate_start",
            "validate_end",
            "test_start",
            "test_end",
        ):
            _date(getattr(self, field_name), field_name)
        if not (
            self.train_start
            <= self.train_end
            < self.validate_start
            <= self.validate_end
            < self.test_start
            <= self.test_end
        ):
            raise ValueError(
                "Temporal splits must be ordered, non-overlapping train, "
                "validate and test dates"
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "train": {
                "start": self.train_start.isoformat(),
                "end": self.train_end.isoformat(),
            },
            "validate": {
                "start": self.validate_start.isoformat(),
                "end": self.validate_end.isoformat(),
            },
            "test": {
                "start": self.test_start.isoformat(),
                "end": self.test_end.isoformat(),
            },
        }


@dataclass(frozen=True)
class CandidateSearchSpec:
    """Optional, isolated discovery tool configuration."""

    plugin: str
    enabled: bool = False
    timeout_minutes: int = 30

    def __post_init__(self) -> None:
        """Keep optional discovery disabled and bounded by declaration."""
        object.__setattr__(
            self,
            "plugin",
            _plugin_identifier(self.plugin, "candidate_search.plugin"),
        )
        if not isinstance(self.enabled, bool):
            raise ValueError("candidate_search.enabled must be true or false")
        timeout = _integer(
            self.timeout_minutes,
            "candidate_search.timeout_minutes",
            minimum=1,
        )
        if timeout > 120:
            raise ValueError("candidate search cannot exceed 120 minutes")

    def as_dict(self) -> dict[str, object]:
        return {
            "plugin": self.plugin,
            "enabled": self.enabled,
            "timeout_minutes": self.timeout_minutes,
        }


@dataclass(frozen=True)
class ResearchPlan:
    """One immutable, model-neutral candidate comparison declaration."""

    candidates: tuple[CandidateSpec, ...]
    temporal_split: TemporalSplitSpec
    evaluation_rules: EvaluationRules
    slices: tuple[SliceSpec, ...]
    selection_policy: str
    explanation_requirements: tuple[str, ...]
    evaluation_schema_version: str = "binary_classifier_evidence/v1"
    minimum_successful_candidates: int = 1
    evidence_producers: tuple[str, ...] = ()
    candidate_search: CandidateSearchSpec | None = None

    def __post_init__(self) -> None:
        """Validate the complete reproducible candidate comparison plan."""
        candidates = tuple(self.candidates)
        if not candidates or any(
            not isinstance(candidate, CandidateSpec)
            for candidate in candidates
        ):
            raise ValueError("Research plans need at least one CandidateSpec")
        candidate_ids = [candidate.candidate_id for candidate in candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("Research candidate IDs must be unique")
        object.__setattr__(self, "candidates", candidates)
        if not isinstance(self.temporal_split, TemporalSplitSpec):
            raise ValueError("temporal_split must be a TemporalSplitSpec")
        if not isinstance(self.evaluation_rules, EvaluationRules):
            raise ValueError("evaluation_rules must be EvaluationRules")
        slices = tuple(self.slices)
        if any(not isinstance(slice_spec, SliceSpec) for slice_spec in slices):
            raise ValueError("slices must contain SliceSpec values")
        slice_ids = [slice_spec.slice_id for slice_spec in slices]
        if len(slice_ids) != len(set(slice_ids)):
            raise ValueError("Research slice IDs must be unique")
        object.__setattr__(self, "slices", slices)
        if self.selection_policy not in VALID_SELECTION_MODES:
            raise ValueError(
                f"Unsupported selection policy: {self.selection_policy}"
            )
        requirements = _names(
            self.explanation_requirements,
            "explanation_requirements",
        )
        missing_requirements = sorted(
            set(MANDATORY_EXPLANATION_REQUIREMENTS).difference(requirements)
        )
        if missing_requirements:
            raise ValueError(
                "explanation_requirements omit standard explanations: "
                + ", ".join(missing_requirements)
            )
        object.__setattr__(self, "explanation_requirements", requirements)
        object.__setattr__(
            self,
            "evaluation_schema_version",
            _text(self.evaluation_schema_version, "evaluation_schema_version"),
        )
        minimum = _integer(
            self.minimum_successful_candidates,
            "minimum_successful_candidates",
            minimum=1,
        )
        required_count = sum(
            not candidate.failure_allowed for candidate in candidates
        )
        if minimum > len(candidates) or minimum < required_count:
            raise ValueError(
                "minimum_successful_candidates must cover every required "
                "candidate and cannot exceed the candidate count"
            )
        producers = tuple(
            _plugin_identifier(value, "evidence_producers")
            for value in self.evidence_producers
        )
        if len(producers) != len(set(producers)):
            raise ValueError("evidence_producers must be unique")
        object.__setattr__(self, "evidence_producers", producers)
        if self.candidate_search is not None and not isinstance(
            self.candidate_search,
            CandidateSearchSpec,
        ):
            raise ValueError("candidate_search must be a CandidateSearchSpec")

    @property
    def selection_mode(self) -> str:
        """Expose the policy using the runtime selection terminology."""
        return self.selection_policy

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": tuple(
                candidate.as_dict() for candidate in self.candidates
            ),
            "temporal_split": self.temporal_split.as_dict(),
            "evaluation_rules": self.evaluation_rules.as_dict(),
            "slices": tuple(
                slice_spec.as_dict() for slice_spec in self.slices
            ),
            "selection_policy": self.selection_policy,
            "explanation_requirements": self.explanation_requirements,
            "evaluation_schema_version": self.evaluation_schema_version,
            "minimum_successful_candidates": self.minimum_successful_candidates,
            "evidence_producers": self.evidence_producers,
            "candidate_search": (
                None
                if self.candidate_search is None
                else self.candidate_search.as_dict()
            ),
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
class ModelResearchBuild:
    """Immutable identity and receipt for one research experiment attempt."""

    research_build_id: str
    research_attempt_id: str
    model_name: str
    training_receipt_id: str
    model_definition_checksum: str
    research_plan_checksum: str
    evaluation_schema_version: str
    code_sha: str
    research_frame_id: str
    research_frame_attempt_id: str
    research_frame_table: str
    research_frame_delta_version: int
    research_frame_row_count: int
    research_frame_schema_checksum: str
    research_frame_data_checksum: str
    research_frame_write_receipt_id: str
    research_frame_feature_schema_json: str
    research_frame_slice_schema_json: str
    candidate_count: int
    successful_candidate_count: int
    status: str
    created_at: datetime
    completed_at: datetime | None = None
    mlflow_experiment_id: str | None = None
    mlflow_parent_run_id: str | None = None
    automatic_candidate_id: str | None = None
    artifact_manifest_digest: str | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate research identity, frame lineage and terminal evidence."""
        for field_name in (
            "research_build_id",
            "research_attempt_id",
            "model_name",
            "training_receipt_id",
            "evaluation_schema_version",
            "code_sha",
            "research_frame_id",
            "research_frame_attempt_id",
            "research_frame_table",
            "research_frame_write_receipt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        for field_name in (
            "research_frame_feature_schema_json",
            "research_frame_slice_schema_json",
        ):
            object.__setattr__(
                self,
                field_name,
                _json_text(getattr(self, field_name), field_name),
            )
        _digest(self.model_definition_checksum, "model_definition_checksum")
        _digest(self.research_plan_checksum, "research_plan_checksum")
        _digest(
            self.research_frame_schema_checksum,
            "research_frame_schema_checksum",
        )
        _digest(
            self.research_frame_data_checksum,
            "research_frame_data_checksum",
        )
        _integer(
            self.research_frame_delta_version, "research_frame_delta_version"
        )
        _integer(self.research_frame_row_count, "research_frame_row_count")
        count = _integer(self.candidate_count, "candidate_count", minimum=1)
        successful = _integer(
            self.successful_candidate_count,
            "successful_candidate_count",
        )
        if successful > count:
            raise ValueError(
                "successful_candidate_count exceeds candidate_count"
            )
        if self.status not in VALID_RESEARCH_BUILD_STATUSES:
            raise ValueError(
                f"Unsupported research build status: {self.status}"
            )
        _timestamp(self.created_at, "created_at")
        completed = self.completed_at
        if completed is not None:
            _timestamp(completed, "completed_at")
            if completed < self.created_at:
                raise ValueError("completed_at cannot predate created_at")
        for field_name in (
            "mlflow_experiment_id",
            "mlflow_parent_run_id",
            "automatic_candidate_id",
            "failure_reason",
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "artifact_manifest_digest",
            _optional_digest(
                self.artifact_manifest_digest,
                "artifact_manifest_digest",
            ),
        )
        if self.status == FAILED:
            if completed is None or self.failure_reason is None:
                raise ValueError(
                    "A failed research build needs completed_at and failure_reason"
                )
        elif self.failure_reason is not None:
            raise ValueError("Only a failed research build has failure_reason")
        if self.status in {AWAITING_SELECTION, READY}:
            required = (
                completed,
                self.mlflow_experiment_id,
                self.mlflow_parent_run_id,
                self.automatic_candidate_id,
                self.artifact_manifest_digest,
            )
            if any(value is None for value in required):
                raise ValueError(
                    "A completed research build needs its MLflow and evidence "
                    "identity"
                )


@dataclass(frozen=True)
class CandidateEvaluation:
    """One candidate attempt and its comparable validation evidence."""

    candidate_evaluation_id: str
    candidate_attempt_id: str
    research_build_id: str
    research_attempt_id: str
    candidate_id: str
    candidate_spec_checksum: str
    required: bool
    status: str
    created_at: datetime
    completed_at: datetime | None = None
    mlflow_run_id: str | None = None
    model_uri: str | None = None
    metrics: tuple[tuple[str, float], ...] = ()
    artifact_manifest_digest: str | None = None
    explanation_status: str | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate one candidate attempt and its evidence state."""
        for field_name in (
            "candidate_evaluation_id",
            "candidate_attempt_id",
            "research_build_id",
            "research_attempt_id",
            "candidate_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _digest(self.candidate_spec_checksum, "candidate_spec_checksum")
        if not isinstance(self.required, bool):
            raise ValueError("required must be true or false")
        if self.status not in VALID_CANDIDATE_STATUSES:
            raise ValueError(f"Unsupported candidate status: {self.status}")
        _timestamp(self.created_at, "created_at")
        completed = self.completed_at
        if completed is not None:
            _timestamp(completed, "completed_at")
            if completed < self.created_at:
                raise ValueError("completed_at cannot predate created_at")
        object.__setattr__(
            self,
            "mlflow_run_id",
            _optional_text(self.mlflow_run_id, "mlflow_run_id"),
        )
        object.__setattr__(
            self,
            "model_uri",
            _optional_text(self.model_uri, "model_uri"),
        )
        object.__setattr__(self, "metrics", _metrics(self.metrics))
        object.__setattr__(
            self,
            "artifact_manifest_digest",
            _optional_digest(
                self.artifact_manifest_digest,
                "artifact_manifest_digest",
            ),
        )
        explanation = _optional_text(
            self.explanation_status,
            "explanation_status",
        )
        if (
            explanation is not None
            and explanation not in VALID_EXPLANATION_STATUSES
        ):
            raise ValueError(f"Unsupported explanation status: {explanation}")
        object.__setattr__(self, "explanation_status", explanation)
        failure = _optional_text(self.failure_reason, "failure_reason")
        object.__setattr__(self, "failure_reason", failure)
        if self.status == FAILED:
            if completed is None or failure is None:
                raise ValueError(
                    "A failed candidate needs completed_at and failure_reason"
                )
        elif failure is not None:
            raise ValueError("Only a failed candidate has failure_reason")
        if self.status == READY:
            required = (
                completed,
                self.mlflow_run_id,
                self.model_uri,
                self.artifact_manifest_digest,
            )
            if (
                any(value is None for value in required)
                or explanation != READY
            ):
                raise ValueError(
                    "A READY candidate needs its model, artifact manifest and "
                    "explanation evidence"
                )


@dataclass(frozen=True)
class ModelSelectionDecision:
    """Automatic or reviewed selection of one exact candidate attempt."""

    selection_decision_id: str
    selection_attempt_id: str
    research_build_id: str
    research_attempt_id: str
    selection_mode: str
    recommended_candidate_id: str
    selected_candidate_id: str
    selected_candidate_evaluation_id: str
    reason: str
    status: str
    created_at: datetime
    completed_at: datetime
    reviewed_by: str | None = None
    model_build_id: str | None = None
    registered_model_name: str | None = None
    decision_code_sha: str | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate a deterministic automatic or reviewed choice."""
        for field_name in (
            "selection_decision_id",
            "selection_attempt_id",
            "research_build_id",
            "research_attempt_id",
            "recommended_candidate_id",
            "selected_candidate_id",
            "selected_candidate_evaluation_id",
            "reason",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if self.selection_mode not in VALID_SELECTION_MODES:
            raise ValueError(
                f"Unsupported selection mode: {self.selection_mode}"
            )
        if self.status not in VALID_SELECTION_STATUSES:
            raise ValueError(f"Unsupported selection status: {self.status}")
        _timestamp(self.created_at, "created_at")
        _timestamp(self.completed_at, "completed_at")
        if self.completed_at < self.created_at:
            raise ValueError("completed_at cannot predate created_at")
        reviewer = _optional_text(self.reviewed_by, "reviewed_by")
        object.__setattr__(self, "reviewed_by", reviewer)
        object.__setattr__(
            self,
            "model_build_id",
            _optional_text(self.model_build_id, "model_build_id"),
        )
        object.__setattr__(
            self,
            "registered_model_name",
            _optional_text(
                self.registered_model_name,
                "registered_model_name",
            ),
        )
        object.__setattr__(
            self,
            "decision_code_sha",
            _optional_text(self.decision_code_sha, "decision_code_sha"),
        )
        failure = _optional_text(self.failure_reason, "failure_reason")
        object.__setattr__(self, "failure_reason", failure)
        if self.status == FAILED:
            if failure is None:
                raise ValueError("A failed selection needs failure_reason")
            return
        if failure is not None:
            raise ValueError("A READY selection cannot have failure_reason")
        if (
            self.registered_model_name is None
            or self.decision_code_sha is None
        ):
            raise ValueError(
                "A READY selection must name its registration target and "
                "decision code SHA"
            )
        if self.selection_mode == AUTO:
            if self.selected_candidate_id != self.recommended_candidate_id:
                raise ValueError("AUTO must select the recommended candidate")
            if reviewer is not None:
                raise ValueError("AUTO selection cannot have a reviewer")
        elif reviewer is None:
            raise ValueError("REVIEW_REQUIRED selection needs reviewed_by")


@dataclass(frozen=True)
class AutoMLDiscoveryReceipt:
    """Bounded discovery evidence linked to one immutable research frame."""

    discovery_id: str
    discovery_attempt_id: str
    request_checksum: str
    research_build_id: str
    research_attempt_id: str
    research_frame_id: str
    research_frame_attempt_id: str
    research_frame_table: str
    research_frame_delta_version: int
    research_frame_schema_checksum: str
    research_frame_data_checksum: str
    research_frame_write_receipt_id: str
    research_frame_feature_schema_json: str
    research_frame_slice_schema_json: str
    status: str
    timeout_minutes: int
    trial_count: int
    created_at: datetime
    completed_at: datetime | None = None
    experiment_id: str | None = None
    best_trial_id: str | None = None
    primary_metric: str | None = None
    trial_evidence_json: str | None = None
    leaderboard_run_id: str | None = None
    leaderboard_artifact_sha256: str | None = None
    leaderboard_artifact_uri: str | None = None
    recipe_artifact_uri: str | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate one bounded and frame-pinned discovery receipt."""
        for field_name in (
            "discovery_id",
            "discovery_attempt_id",
            "research_build_id",
            "research_attempt_id",
            "research_frame_id",
            "research_frame_attempt_id",
            "research_frame_table",
            "research_frame_write_receipt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _digest(self.request_checksum, "request_checksum")
        for field_name in (
            "research_frame_feature_schema_json",
            "research_frame_slice_schema_json",
        ):
            object.__setattr__(
                self,
                field_name,
                _json_text(getattr(self, field_name), field_name),
            )
        _integer(
            self.research_frame_delta_version, "research_frame_delta_version"
        )
        _digest(
            self.research_frame_schema_checksum,
            "research_frame_schema_checksum",
        )
        _digest(
            self.research_frame_data_checksum,
            "research_frame_data_checksum",
        )
        if self.status not in VALID_DISCOVERY_STATUSES:
            raise ValueError(f"Unsupported discovery status: {self.status}")
        timeout = _integer(self.timeout_minutes, "timeout_minutes", minimum=1)
        if timeout > 120:
            raise ValueError("AutoML discovery cannot exceed 120 minutes")
        trials = _integer(self.trial_count, "trial_count")
        _timestamp(self.created_at, "created_at")
        completed = self.completed_at
        if completed is not None:
            _timestamp(completed, "completed_at")
            if completed < self.created_at:
                raise ValueError("completed_at cannot predate created_at")
        for field_name in (
            "experiment_id",
            "best_trial_id",
            "primary_metric",
            "trial_evidence_json",
            "leaderboard_run_id",
            "leaderboard_artifact_uri",
            "recipe_artifact_uri",
            "failure_reason",
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), field_name),
            )
        artifact_digest = _optional_digest(
            self.leaderboard_artifact_sha256,
            "leaderboard_artifact_sha256",
        )
        object.__setattr__(
            self,
            "leaderboard_artifact_sha256",
            artifact_digest,
        )
        if self.status == FAILED:
            if completed is None or self.failure_reason is None:
                raise ValueError(
                    "Failed AutoML discovery needs completed_at and failure_reason"
                )
        elif self.failure_reason is not None:
            raise ValueError("Only failed AutoML discovery has failure_reason")
        if self.status == READY:
            required = (
                completed,
                self.experiment_id,
                self.best_trial_id,
                self.primary_metric,
                self.trial_evidence_json,
                self.leaderboard_run_id,
                self.leaderboard_artifact_sha256,
                self.leaderboard_artifact_uri,
                self.recipe_artifact_uri,
            )
            if trials < 1 or any(value is None for value in required):
                raise ValueError(
                    "READY AutoML discovery needs trials and linked artifacts"
                )
            self._validate_leaderboard(trials)

    def _validate_leaderboard(self, trial_count: int) -> None:
        """Require the durable JSON to be the exact bounded leaderboard."""
        assert self.trial_evidence_json is not None
        assert self.leaderboard_artifact_sha256 is not None
        assert self.leaderboard_artifact_uri is not None
        assert self.leaderboard_run_id is not None
        assert self.primary_metric is not None
        assert self.experiment_id is not None
        assert self.best_trial_id is not None
        encoded = self.trial_evidence_json.encode("utf-8")
        if len(encoded) > 1_000_000:
            raise ValueError("AutoML leaderboard evidence exceeds 1 MB")
        if hashlib.sha256(encoded).hexdigest() != (
            self.leaderboard_artifact_sha256
        ):
            raise ValueError("AutoML leaderboard artifact checksum differs")
        expected_uri = (
            f"runs:/{self.leaderboard_run_id}/"
            "automl_discovery/leaderboard.json"
        )
        if self.leaderboard_artifact_uri != expected_uri:
            raise ValueError(
                "AutoML leaderboard URI must identify the logged leaderboard"
            )
        payload = json.loads(self.trial_evidence_json)
        expected_top_level = {
            "schema_version",
            "research_build_id",
            "discovery_id",
            "research_parent_run_id",
            "experiment_id",
            "primary_metric",
            "trial_count",
            "best_trial_id",
            "trials",
        }
        if not isinstance(payload, dict) or set(payload) != expected_top_level:
            raise ValueError("AutoML leaderboard has unexpected fields")
        expected_values = {
            "schema_version": "nextads_automl_leaderboard/v1",
            "research_build_id": self.research_build_id,
            "discovery_id": self.discovery_id,
            "experiment_id": self.experiment_id,
            "primary_metric": self.primary_metric,
            "trial_count": trial_count,
            "best_trial_id": self.best_trial_id,
        }
        changed = sorted(
            key
            for key, expected in expected_values.items()
            if payload.get(key) != expected
        )
        if changed:
            raise ValueError(
                "AutoML leaderboard differs from its receipt: "
                + ", ".join(changed)
            )
        if not isinstance(payload["research_parent_run_id"], str) or not (
            payload["research_parent_run_id"].strip()
        ):
            raise ValueError("AutoML leaderboard needs a research parent run")
        rows = payload["trials"]
        if not isinstance(rows, list) or len(rows) != trial_count:
            raise ValueError("AutoML leaderboard trial count differs")
        expected_row_fields = {
            "rank",
            "trial_id",
            "primary_metric_value",
            "notebook_artifact_uri",
            "notebook_path",
            "notebook_url",
            "is_best_trial",
        }
        if any(
            not isinstance(row, dict) or set(row) != expected_row_fields
            for row in rows
        ):
            raise ValueError("AutoML leaderboard trial fields differ")
        trial_ids = [row["trial_id"] for row in rows]
        if any(
            not isinstance(value, str) or not value for value in trial_ids
        ) or len(trial_ids) != len(set(trial_ids)):
            raise ValueError("AutoML leaderboard trial IDs must be unique")
        metrics = [row["primary_metric_value"] for row in rows]
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in metrics
        ):
            raise ValueError("AutoML leaderboard metrics must be finite")
        ranks = [row["rank"] for row in rows]
        if any(
            isinstance(rank, bool) or not isinstance(rank, int)
            for rank in ranks
        ):
            raise ValueError("AutoML leaderboard ranks must be integers")
        if ranks != list(range(1, trial_count + 1)):
            raise ValueError("AutoML leaderboard ranks must be consecutive")
        expected_order = sorted(
            rows,
            key=lambda row: (
                -float(row["primary_metric_value"]),
                row["trial_id"],
            ),
        )
        if rows != expected_order:
            raise ValueError("AutoML leaderboard is not sorted")
        best_rows = [row for row in rows if row["is_best_trial"] is True]
        if len(best_rows) != 1 or (
            best_rows[0]["trial_id"] != self.best_trial_id
        ):
            raise ValueError("AutoML leaderboard best-trial marker differs")
        for row in rows:
            links = (
                row["notebook_artifact_uri"],
                row["notebook_path"],
                row["notebook_url"],
            )
            if any(
                value is not None
                and (not isinstance(value, str) or not value.strip())
                for value in links
            ):
                raise ValueError(
                    "AutoML trial notebook links must be non-empty strings"
                )
            if row["is_best_trial"] is True and not any(links):
                raise ValueError(
                    "The best AutoML trial needs a generated notebook link"
                )
        best_links = (
            best_rows[0]["notebook_artifact_uri"],
            best_rows[0]["notebook_path"],
            best_rows[0]["notebook_url"],
        )
        if self.recipe_artifact_uri not in best_links:
            raise ValueError(
                "AutoML recipe URI must identify the best trial notebook"
            )


@runtime_checkable
class CandidateTrainer(Protocol):
    """Fits one declared candidate without owning data splits or registration."""

    def fit(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        training_frame: Any,
    ) -> Any: ...


@runtime_checkable
class PredictionAdapter(Protocol):
    """Returns declared-label predictions and a standard-output model.

    ``predict`` retains ``definition.label`` and emits the standard score,
    prediction, split, observation-date and hashed-row fields.
    """

    def predict(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        fitted_model: Any,
        evaluation_frame: Any,
    ) -> Any: ...

    def model_for_persistence(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        fitted_model: Any,
    ) -> Any:
        """Return an MLWritable model whose transform emits score/prediction."""
        ...


@runtime_checkable
class EvidenceProducer(Protocol):
    """Adds bounded evidence from aggregate standard outputs only.

    Producers never receive observation-level predictions. The supplied
    mapping contains the standard validation aggregates, feature coverage and
    global explanation already produced by the orchestration route.
    """

    def produce(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        fitted_model: Any,
        aggregate_evidence: Mapping[str, Any],
        feature_names: tuple[str, ...],
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class CandidateSearch(Protocol):
    """Runs optional discovery without registering or activating a model."""

    def search(
        self,
        definition: ModelDefinition,
        research_plan: ResearchPlan,
        research_build: ModelResearchBuild,
        discovery_frame: Any,
    ) -> AutoMLDiscoveryReceipt: ...


@runtime_checkable
class ModelSelector(Protocol):
    """Registers only the candidate identified by an exact selection receipt."""

    def select(
        self,
        definition: ModelDefinition,
        training_receipt: TrainingSetReceipt,
        research_build: ModelResearchBuild,
        candidate_evaluation: CandidateEvaluation,
        selection_decision: ModelSelectionDecision,
    ) -> ModelBuild: ...


__all__ = [
    "AUTO",
    "AWAITING_SELECTION",
    "FAILED",
    "MANDATORY_BINARY_EVIDENCE",
    "MANDATORY_BINARY_METRICS",
    "MANDATORY_EXPLANATION_REQUIREMENTS",
    "PROTECTED_CANDIDATE_PARAMETERS",
    "READY",
    "RESEARCHING",
    "REVIEW_REQUIRED",
    "STANDARD_PREDICTION_FIELDS",
    "AutoMLDiscoveryReceipt",
    "CandidateEvaluation",
    "CandidateSearch",
    "CandidateSearchSpec",
    "CandidateSpec",
    "CandidateTrainer",
    "EvaluationRules",
    "EvidenceProducer",
    "ModelResearchBuild",
    "ModelSelectionDecision",
    "ModelSelector",
    "PredictionAdapter",
    "ResearchPlan",
    "SliceSpec",
    "TemporalSplitSpec",
    "standard_prediction_columns",
]
