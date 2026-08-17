"""Supported contracts for developing NextAds challenger models."""

from next_ads.model_development.contracts import (
    ALLOWED_RUNTIME_PROFILES,
    DBR_15_4_SPARK_CPU,
    DBR_18_1_THEME_GPU,
    CandidateAdapter,
    FeatureLookupSpec,
    ModelBuild,
    ModelDefinition,
    ScoreProvider,
    Trainer,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)

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
