"""Secure plug-in resolution for declared model research candidates."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
from typing import Any

from next_ads.model_development.research_contracts import (
    CandidateSpec,
    CandidateTrainer,
    EvidenceProducer,
    PredictionAdapter,
)
from next_ads.model_development.spark_research import (
    BUILTIN_CANDIDATES,
    SparkResearchCandidatePlugin,
)


@dataclass(frozen=True)
class ResolvedCandidatePlugin:
    """Fit and prediction interfaces resolved for one declaration."""

    trainer: CandidateTrainer
    prediction_adapter: PredictionAdapter


def _custom_plugin(identifier: str) -> Any:
    module_name, separator, class_name = identifier.rpartition(".")
    if not separator or not module_name.startswith("next_ads."):
        raise ValueError("Custom research plug-ins must live under next_ads.*")
    module = importlib.import_module(module_name)
    plugin_type = getattr(module, class_name, None)
    if plugin_type is None or not isinstance(plugin_type, type):
        raise ValueError(f"Research plug-in is not a class: {identifier}")
    try:
        return plugin_type()
    except TypeError as exc:
        raise ValueError(
            "Research plug-ins must have a no-argument constructor: "
            f"{identifier}"
        ) from exc


def resolve_candidate_plugin(
    candidate: CandidateSpec,
) -> ResolvedCandidatePlugin:
    """Resolve a supplied alias or reviewed repository class."""
    if candidate.plugin in BUILTIN_CANDIDATES:
        plugin: Any = SparkResearchCandidatePlugin(candidate.plugin)
    else:
        plugin = _custom_plugin(candidate.plugin)
    if not isinstance(plugin, CandidateTrainer):
        raise ValueError(
            f"Candidate plug-in does not implement fit: {candidate.plugin}"
        )
    if not isinstance(plugin, PredictionAdapter):
        raise ValueError(
            f"Candidate plug-in does not implement predict: {candidate.plugin}"
        )
    return ResolvedCandidatePlugin(plugin, plugin)


def resolve_evidence_producer(identifier: str) -> EvidenceProducer:
    """Resolve one optional reviewed evidence extension under next_ads.*."""
    producer = _custom_plugin(identifier)
    if not isinstance(producer, EvidenceProducer):
        raise ValueError(
            f"Evidence plug-in does not implement produce: {identifier}"
        )
    return producer


__all__ = [
    "ResolvedCandidatePlugin",
    "resolve_candidate_plugin",
    "resolve_evidence_producer",
]
