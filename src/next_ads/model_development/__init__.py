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
from next_ads.model_development.store import (
    create_model_development_tables,
    load_ready_model_build,
    load_external_score_output_receipt,
    load_ready_training_set_receipt,
    persist_model_build,
    persist_external_score_output_receipt,
    persist_training_set_receipt,
)
from next_ads.model_development.runtime import (
    model_build_id,
    train_or_reuse_model,
)
from next_ads.model_development.external_outputs import (
    ExternalModelComponent,
    ExternalScoreOutputReceipt,
    adapt_external_advert_scores,
    bind_external_score_output,
)

__all__ = [
    "ALLOWED_RUNTIME_PROFILES",
    "DBR_15_4_SPARK_CPU",
    "DBR_18_1_THEME_GPU",
    "ExternalModelComponent",
    "ExternalScoreOutputReceipt",
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
    "adapt_external_advert_scores",
    "bind_external_score_output",
    "create_model_development_tables",
    "load_ready_model_build",
    "load_external_score_output_receipt",
    "load_ready_training_set_receipt",
    "load_model_definition",
    "load_model_definitions",
    "model_build_id",
    "persist_model_build",
    "persist_external_score_output_receipt",
    "persist_training_set_receipt",
    "train_or_reuse_model",
    "validate_snapshot_time_boundary",
]
