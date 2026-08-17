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
    TrainingObservationSpec,
    TrainingSetReceipt,
)
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_definitions,
)
from next_ads.model_development.training_sets import (
    TrainingSetBuildResult,
    build_training_set,
    build_training_set_from_feature_store,
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
from next_ads.model_development.spark_training import (
    SparkBinaryClassifierTrainer,
    artifact_directory_digest,
    deterministic_train_validation_split,
)
from next_ads.model_development.external_outputs import (
    ExternalModelComponent,
    ExternalScoreOutputReceipt,
    adapt_external_advert_scores,
    bind_external_score_output,
    verify_external_model_components,
)
from next_ads.model_development.plugins import (
    AccountAdvertCandidateAdapter,
    ExternalAnalyticsScoreProvider,
    ModelPluginRegistry,
    SparkAccountAdvertScoreProvider,
)
from next_ads.model_development.promotion import (
    ModelPromotionReceipt,
    promote_exact_model_build,
)

__all__ = [
    "ALLOWED_RUNTIME_PROFILES",
    "AccountAdvertCandidateAdapter",
    "DBR_15_4_SPARK_CPU",
    "DBR_18_1_THEME_GPU",
    "ExternalModelComponent",
    "ExternalScoreOutputReceipt",
    "ExternalAnalyticsScoreProvider",
    "CandidateAdapter",
    "FeatureLookupSpec",
    "ModelBuild",
    "ModelDefinition",
    "ModelPluginRegistry",
    "ModelPromotionReceipt",
    "ScoreProvider",
    "SparkBinaryClassifierTrainer",
    "SparkAccountAdvertScoreProvider",
    "Trainer",
    "TrainingFeatureBinding",
    "TrainingObservationSpec",
    "TrainingSetReceipt",
    "artifact_directory_digest",
    "TrainingSetBuildResult",
    "build_training_set",
    "build_training_set_from_feature_store",
    "deterministic_train_validation_split",
    "adapt_external_advert_scores",
    "bind_external_score_output",
    "verify_external_model_components",
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
    "promote_exact_model_build",
    "train_or_reuse_model",
    "validate_snapshot_time_boundary",
]
