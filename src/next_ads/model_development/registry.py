"""Repository registry for generic NextAds model definitions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from next_ads.model_development.contracts import (
    FeatureLookupSpec,
    ModelDefinition,
    TrainingObservationSpec,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODEL_REGISTRY_PATH = PROJECT_ROOT / "configs" / "models" / (
    "nextads_models.yaml"
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
            raise ValueError(
                f"{field_name} list entries require from and to"
            )
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


def _definition(raw: dict[str, Any]) -> ModelDefinition:
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
    )


def load_model_definitions(
    path: str | Path = DEFAULT_MODEL_REGISTRY_PATH,
) -> tuple[ModelDefinition, ...]:
    """Load all model adopters and reject ambiguous model/provider names."""
    registry_path = Path(path)
    raw = yaml.safe_load(registry_path.read_text()) or {}
    definitions = tuple(
        _definition(item) for item in raw.get("models", ())
    )
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


__all__ = [
    "DEFAULT_MODEL_REGISTRY_PATH",
    "load_model_definition",
    "load_model_definitions",
]
