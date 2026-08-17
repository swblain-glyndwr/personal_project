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
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_definitions,
)
from next_ads.model_development.training_sets import (
    TrainingSetBuildResult,
    build_training_set,
    validate_snapshot_time_boundary,
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
    "TrainingSetBuildResult",
    "build_training_set",
    "load_model_definition",
    "load_model_definitions",
    "validate_snapshot_time_boundary",
]
