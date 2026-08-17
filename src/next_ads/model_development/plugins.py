"""Model plug-ins that keep training and scoring out of core orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from next_ads.model_development.contracts import (
    ModelBuild,
    ModelDefinition,
)
from next_ads.model_development.external_outputs import (
    ExternalScoreOutputReceipt,
    adapt_external_advert_scores,
)
from next_ads.model_development.spark_training import (
    SparkBinaryClassifierTrainer,
)
from next_ads.ranking.provider_signals import adapt_account_entity_scores


@dataclass(frozen=True)
class SparkAccountAdvertScoreProvider:
    """Score an exact Spark model and emit account_entity_scores/v1."""

    run_date: date
    account_column: str = "account_number"
    advert_column: str = "advert_id"
    probability_column: str = "probability"

    def score(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
    ) -> Any:
        """Load the numeric registered version recorded by ModelBuild."""
        if model_build.status != "READY" or not model_build.model_uri:
            raise ValueError("Scoring requires a READY exact model build")
        if (
            model_build.model_name != definition.model_name
            or model_build.model_definition_checksum != definition.checksum
        ):
            raise ValueError("Model build does not match the score definition")
        import mlflow
        from pyspark.ml.functions import vector_to_array
        from pyspark.sql import functions as F

        model = mlflow.spark.load_model(model_build.model_uri)
        predictions = model.transform(feature_frame).withColumn(
            "__model_pctr",
            vector_to_array(F.col(self.probability_column)).getItem(1),
        )
        return adapt_account_entity_scores(
            predictions,
            provider_build_id=model_build.model_build_id,
            provider_id=definition.provider_id,
            entity_type="advert",
            run_date=self.run_date,
            account_column=self.account_column,
            entity_column=self.advert_column,
            raw_score_column="__model_pctr",
            score_column="__model_pctr",
        )


@dataclass(frozen=True)
class ExternalAnalyticsScoreProvider:
    """Adapt the pinned two-stage Analytics output without retraining it."""

    receipt: ExternalScoreOutputReceipt
    account_column: str = "account_number"
    advert_column: str = "UniqueAdID"
    score_column: str = "combined_weighted_score"

    def score(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
    ) -> Any:
        """Return the external output through the same provider contract."""
        if definition.model_name != self.receipt.model_name:
            raise ValueError("External score receipt belongs to another model")
        if model_build.model_name != definition.model_name:
            raise ValueError("External model build belongs to another model")
        return adapt_external_advert_scores(
            feature_frame,
            self.receipt,
            provider_build_id=model_build.model_build_id,
            account_column=self.account_column,
            advert_column=self.advert_column,
            raw_score_column=self.score_column,
            score_column=self.score_column,
        )


@dataclass(frozen=True)
class AccountAdvertCandidateAdapter:
    """Apply canonical advert scores to eligible rows deterministically."""

    account_column: str = "AccountNumber"
    advert_column: str = "UniqueAdID"

    def apply(self, provider_scores: Any, eligible_candidates: Any) -> Any:
        """Filter scores to eligibility and rerank after that filtering."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        required_scores = {
            "AccountNumber",
            "EntityID",
            "ProviderBuildID",
            "ProviderID",
            "RawScore",
            "Score",
        }
        missing_scores = sorted(required_scores.difference(provider_scores.columns))
        missing_candidates = sorted(
            {self.account_column, self.advert_column}.difference(
                eligible_candidates.columns
            )
        )
        if missing_scores or missing_candidates:
            raise ValueError(
                "Candidate adaptation is missing columns: "
                + ", ".join([*missing_scores, *missing_candidates])
            )
        eligible = eligible_candidates.alias("eligible")
        scores = provider_scores.alias("scores")
        joined = eligible.join(
            scores,
            on=(
                F.col(f"eligible.{self.account_column}").cast("string")
                == F.col("scores.AccountNumber")
            )
            & (
                F.col(f"eligible.{self.advert_column}").cast("string")
                == F.col("scores.EntityID")
            ),
            how="inner",
        )
        rank = Window.partitionBy(self.account_column).orderBy(
            F.col("Score").desc_nulls_last(),
            F.col(self.advert_column).cast("string").asc(),
        )
        return joined.select(
            *[F.col(f"eligible.{column}") for column in eligible_candidates.columns],
            F.col("scores.ProviderBuildID"),
            F.col("scores.ProviderID"),
            F.col("scores.RawScore"),
            F.col("scores.Score"),
        ).withColumn("ProviderRank", F.row_number().over(rank))


class ModelPluginRegistry:
    """Resolve declared plug-ins without changing the job graph."""

    def __init__(self) -> None:
        self._trainers: dict[str, Callable[..., Any]] = {
            "shopping_bag_pctr_spark": SparkBinaryClassifierTrainer,
        }
        self._score_providers: dict[str, Callable[..., Any]] = {
            "shopping_bag_pctr_scores": SparkAccountAdvertScoreProvider,
            "analytics_pctr_two_stage_scores": ExternalAnalyticsScoreProvider,
        }
        self._candidate_adapters: dict[str, Callable[..., Any]] = {
            "account_advert_ranked_candidates": AccountAdvertCandidateAdapter,
        }

    @staticmethod
    def _resolve(
        plugins: dict[str, Callable[..., Any]],
        name: str,
        kind: str,
        **kwargs: Any,
    ) -> Any:
        try:
            factory = plugins[name]
        except KeyError as exc:
            raise ValueError(f"Unknown {kind} plug-in: {name}") from exc
        return factory(**kwargs)

    def trainer(self, definition: ModelDefinition, **kwargs: Any) -> Any:
        return self._resolve(
            self._trainers,
            definition.trainer,
            "trainer",
            **kwargs,
        )

    def score_provider(
        self,
        definition: ModelDefinition,
        **kwargs: Any,
    ) -> Any:
        return self._resolve(
            self._score_providers,
            definition.score_provider,
            "score provider",
            **kwargs,
        )

    def candidate_adapter(
        self,
        definition: ModelDefinition,
        **kwargs: Any,
    ) -> Any:
        return self._resolve(
            self._candidate_adapters,
            definition.candidate_adapter,
            "candidate adapter",
            **kwargs,
        )


__all__ = [
    "AccountAdvertCandidateAdapter",
    "ExternalAnalyticsScoreProvider",
    "ModelPluginRegistry",
    "SparkAccountAdvertScoreProvider",
]
