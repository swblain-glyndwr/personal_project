"""Repository registry for generic NextAds model definitions."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from next_ads.model_development.contracts import (
    FeatureLookupSpec,
    ModelDefinition,
    TrainingObservationSpec,
)
from next_ads.model_development.research_contracts import (
    CandidateSearchSpec,
    CandidateSpec,
    EvaluationRules,
    ResearchPlan,
    SliceSpec,
    TemporalSplitSpec,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODEL_REGISTRY_PATH = (
    PROJECT_ROOT / "configs" / "models" / ("nextads_models.yaml")
)


def _pairs(value: Any, field_name: str) -> tuple[tuple[str, Any], ...]:
    if value is None:
        return ()
    if isinstance(value, dict):
        return tuple((str(key), item) for key, item in value.items())
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a mapping or list of pairs")
    result = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"from", "to"}:
            raise ValueError(f"{field_name} list entries require from and to")
        result.append((str(item["from"]), item["to"]))
    return tuple(result)


def _lookup(raw: dict[str, Any]) -> FeatureLookupSpec:
    return FeatureLookupSpec(
        feature_id=raw["feature_id"],
        selected_columns=tuple(raw["selected_columns"]),
        key_mapping=_pairs(raw["key_mapping"], "key_mapping"),
        observation_timestamp=raw["observation_timestamp"],
        availability_lag_days=raw.get("availability_lag_days", 0),
        renames=_pairs(raw.get("renames"), "renames"),
        defaults=_pairs(raw.get("defaults"), "defaults"),
    )


def _observation(raw: dict[str, Any]) -> TrainingObservationSpec:
    return TrainingObservationSpec(
        feature_id=raw["feature_id"],
        selected_columns=tuple(raw["selected_columns"]),
        observation_timestamp=raw["observation_timestamp"],
        context_features=tuple(raw.get("context_features", ())),
        label_maturity_column=raw.get("label_maturity_column"),
        filters=_pairs(raw.get("filters"), "filters"),
        observation_date_column=raw.get("observation_date_column"),
    )


def _scope(value: Any) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if value is None:
        return ()
    if not isinstance(value, dict):
        raise ValueError("evaluation_scope must be a mapping")
    result = []
    for column, allowed in value.items():
        if not isinstance(allowed, list):
            raise ValueError("evaluation_scope values must be lists")
        result.append((str(column), tuple(str(item) for item in allowed)))
    return tuple(result)


def _iso_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a date, not a timestamp")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO date") from error


def _candidate(raw: dict[str, Any]) -> CandidateSpec:
    parameters = raw.get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("research candidate parameters must be a mapping")
    return CandidateSpec(
        candidate_id=raw["candidate_id"],
        plugin=raw["plugin"],
        parameters=parameters,
        seed=raw.get("seed", 1729),
        failure_allowed=raw.get("failure_allowed", False),
    )


def _slice(raw: dict[str, Any]) -> SliceSpec:
    values = raw.get("values", ())
    if not isinstance(values, (list, tuple)):
        raise ValueError("research slice values must be a list")
    return SliceSpec(
        slice_id=raw["slice_id"],
        column=raw["column"],
        values=tuple(values),
        if_present=raw.get("if_present", False),
        minimum_rows=raw.get("minimum_rows", 100),
    )


def _temporal_split(raw: dict[str, Any]) -> TemporalSplitSpec:
    expected = {"train", "validate", "test"}
    if set(raw) != expected:
        raise ValueError(
            "temporal_split must contain exactly train, validate and test"
        )
    for split_name in expected:
        if not isinstance(raw[split_name], dict) or set(raw[split_name]) != {
            "start",
            "end",
        }:
            raise ValueError(
                f"temporal_split.{split_name} must contain start and end"
            )
    return TemporalSplitSpec(
        train_start=_iso_date(raw["train"]["start"], "train.start"),
        train_end=_iso_date(raw["train"]["end"], "train.end"),
        validate_start=_iso_date(
            raw["validate"]["start"],
            "validate.start",
        ),
        validate_end=_iso_date(raw["validate"]["end"], "validate.end"),
        test_start=_iso_date(raw["test"]["start"], "test.start"),
        test_end=_iso_date(raw["test"]["end"], "test.end"),
    )


def _evaluation_rules(raw: dict[str, Any]) -> EvaluationRules:
    defaults = EvaluationRules()
    return EvaluationRules(
        required_metrics=tuple(
            raw.get("required_metrics", defaults.required_metrics)
        ),
        required_evidence=tuple(
            raw.get("required_evidence", defaults.required_evidence)
        ),
        top_fractions=tuple(raw.get("top_fractions", defaults.top_fractions)),
        confidence_interval_metrics=tuple(
            raw.get(
                "confidence_interval_metrics",
                defaults.confidence_interval_metrics,
            )
        ),
        confidence_level=raw.get("confidence_level", 0.95),
        confidence_interval_resamples=raw.get(
            "confidence_interval_resamples",
            1000,
        ),
        confidence_interval_seed=raw.get("confidence_interval_seed", 1729),
        minimum_slice_rows=raw.get("minimum_slice_rows", 100),
        prevalence_baseline=raw.get("prevalence_baseline", True),
    )


def _candidate_search(raw: Any) -> CandidateSearchSpec | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("candidate_search must be a mapping")
    return CandidateSearchSpec(
        plugin=raw["plugin"],
        enabled=raw.get("enabled", False),
        timeout_minutes=raw.get("timeout_minutes", 30),
    )


def _research_plan(raw: dict[str, Any]) -> ResearchPlan:
    candidates = raw.get("candidates", ())
    slices = raw.get("slices", ())
    if not isinstance(candidates, list):
        raise ValueError("research candidates must be a list")
    if not isinstance(slices, list):
        raise ValueError("research slices must be a list")
    rules = raw.get("evaluation_rules")
    split = raw.get("temporal_split")
    if not isinstance(rules, dict):
        raise ValueError("evaluation_rules must be a mapping")
    if not isinstance(split, dict):
        raise ValueError("temporal_split must be a mapping")
    producers = raw.get("evidence_producers", [])
    requirements = raw.get("explanation_requirements", [])
    if not isinstance(producers, list):
        raise ValueError("evidence_producers must be a list")
    if not isinstance(requirements, list):
        raise ValueError("explanation_requirements must be a list")
    return ResearchPlan(
        candidates=tuple(_candidate(item) for item in candidates),
        temporal_split=_temporal_split(split),
        evaluation_rules=_evaluation_rules(rules),
        slices=tuple(_slice(item) for item in slices),
        selection_policy=raw["selection_policy"],
        explanation_requirements=tuple(requirements),
        evaluation_schema_version=raw.get(
            "evaluation_schema_version",
            "binary_classifier_evidence/v1",
        ),
        minimum_successful_candidates=raw.get(
            "minimum_successful_candidates",
            1,
        ),
        evidence_producers=tuple(producers),
        candidate_search=_candidate_search(raw.get("candidate_search")),
    )


def _definition(raw: dict[str, Any]) -> ModelDefinition:
    research = raw.get("research")
    if research is not None and not isinstance(research, dict):
        raise ValueError("research must be a mapping")
    return ModelDefinition(
        model_name=raw["model_name"],
        provider_id=raw["provider_id"],
        problem_statement=raw["problem_statement"],
        prediction_entity=raw["prediction_entity"],
        prediction_time=raw["prediction_time"],
        label=raw["label"],
        observation_keys=tuple(raw["observation_keys"]),
        success_metrics=tuple(raw["success_metrics"]),
        runtime_profile=raw["runtime_profile"],
        training_observation=_observation(raw["training_observation"]),
        feature_lookups=tuple(
            _lookup(item) for item in raw["feature_lookups"]
        ),
        trainer=raw["trainer"],
        score_provider=raw["score_provider"],
        candidate_adapter=raw["candidate_adapter"],
        evaluation_use_case=raw.get("evaluation_use_case", "advert_ranking"),
        evaluation_scope=_scope(raw.get("evaluation_scope")),
        activation_mode=raw.get("activation_mode", "EVALUATE"),
        research=None if research is None else _research_plan(research),
    )


def load_model_definitions(
    path: str | Path = DEFAULT_MODEL_REGISTRY_PATH,
) -> tuple[ModelDefinition, ...]:
    """Load all model adopters and reject ambiguous model/provider names."""
    registry_path = Path(path)
    raw = yaml.safe_load(registry_path.read_text()) or {}
    definitions = tuple(_definition(item) for item in raw.get("models", ()))
    if not definitions:
        raise ValueError("The model registry must contain at least one model")
    model_names = [definition.model_name for definition in definitions]
    provider_ids = [definition.provider_id for definition in definitions]
    duplicate_models = sorted(
        name for name in set(model_names) if model_names.count(name) > 1
    )
    duplicate_providers = sorted(
        name for name in set(provider_ids) if provider_ids.count(name) > 1
    )
    if duplicate_models:
        raise ValueError(
            "Duplicate model definitions: " + ", ".join(duplicate_models)
        )
    if duplicate_providers:
        raise ValueError(
            "Duplicate model provider IDs: " + ", ".join(duplicate_providers)
        )
    return definitions


def load_model_definition(
    model_name: str,
    path: str | Path = DEFAULT_MODEL_REGISTRY_PATH,
) -> ModelDefinition:
    """Return one named model without adding model-specific orchestration."""
    matches = [
        definition
        for definition in load_model_definitions(path)
        if definition.model_name == model_name
    ]
    if len(matches) != 1:
        raise KeyError(f"Unknown model definition: {model_name}")
    return matches[0]


def load_model_research_plans(
    path: str | Path = DEFAULT_MODEL_REGISTRY_PATH,
) -> tuple[tuple[str, ResearchPlan], ...]:
    """Return the optional typed research plans on model definitions."""
    return tuple(
        (definition.model_name, definition.research)
        for definition in load_model_definitions(path)
        if definition.research is not None
    )


def load_model_research_plan(
    model_name: str,
    path: str | Path = DEFAULT_MODEL_REGISTRY_PATH,
) -> ResearchPlan | None:
    """Return one model's optional research plan or None when undeclared."""
    return load_model_definition(model_name, path).research


__all__ = [
    "DEFAULT_MODEL_REGISTRY_PATH",
    "load_model_definition",
    "load_model_definitions",
    "load_model_research_plan",
    "load_model_research_plans",
]
