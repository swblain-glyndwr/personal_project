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

    def _predictions(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
    ) -> Any:
        """Load the numeric registered version and score declared rows once."""
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
        return model.transform(feature_frame).withColumn(
            "__model_pctr",
            vector_to_array(F.col(self.probability_column)).getItem(1),
        )

    def _latest_predictions(
        self,
        definition: ModelDefinition,
        predictions: Any,
        *,
        scope_columns: tuple[str, ...] = (),
        tie_break_columns: tuple[str, ...] | None = None,
    ) -> Any:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        observation_timestamp = (
            definition.training_observation.observation_timestamp
        )
        resolved_tie_break_columns = (
            definition.observation_keys
            if tie_break_columns is None
            else tie_break_columns
        )
        required = {
            self.account_column,
            self.advert_column,
            observation_timestamp,
            *scope_columns,
            *resolved_tie_break_columns,
        }
        missing = sorted(required.difference(predictions.columns))
        if missing:
            raise ValueError(
                "Shopping Bag scoring is missing exposure columns: "
                + ", ".join(missing)
            )
        latest_order = [
            F.col(observation_timestamp).desc_nulls_last(),
            *(
                F.col(column).cast("string").desc_nulls_last()
                for column in resolved_tie_break_columns
            ),
        ]
        latest = Window.partitionBy(
            self.account_column,
            self.advert_column,
            *scope_columns,
        ).orderBy(*latest_order)
        return (
            predictions.withColumn(
                "__model_latest_exposure",
                F.row_number().over(latest),
            )
            .where(F.col("__model_latest_exposure") == F.lit(1))
            .drop("__model_latest_exposure")
        )

    def score(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
    ) -> Any:
        """Emit one account-advert signal for the canonical provider contract."""
        predictions = self._latest_predictions(
            definition,
            self._predictions(definition, model_build, feature_frame),
        )
        return adapt_account_entity_scores(
            predictions,
            provider_build_id=model_build.model_build_id,
            provider_id=definition.provider_id,
            entity_type="ad",
            run_date=self.run_date,
            account_column=self.account_column,
            entity_column=self.advert_column,
            raw_score_column="__model_pctr",
            score_column="__model_pctr",
        )

    def score_with_evaluation_scope(
        self,
        definition: ModelDefinition,
        model_build: ModelBuild,
        feature_frame: Any,
        *,
        scope_columns: tuple[str, ...],
    ) -> tuple[Any, Any]:
        """Return canonical signals plus location-preserving EVALUATE scores."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        predictions = self._predictions(definition, model_build, feature_frame)
        scoped_predictions = self._latest_predictions(
            definition,
            predictions,
            scope_columns=scope_columns,
            # Current candidates are label-free and intentionally do not have
            # historical exposure IDs or label horizons. Their candidate
            # contract is already unique within the declared route and scope.
            tie_break_columns=(),
        ).persist()
        canonical_predictions = self._latest_predictions(
            definition,
            scoped_predictions,
            # A canonical provider row drops the evaluation scope. Use that
            # retained scope as the deterministic tie-break when the same ad
            # appears in more than one Shopping Bag placement.
            tie_break_columns=scope_columns,
        )
        canonical = adapt_account_entity_scores(
            canonical_predictions,
            provider_build_id=model_build.model_build_id,
            provider_id=definition.provider_id,
            entity_type="ad",
            run_date=self.run_date,
            account_column=self.account_column,
            entity_column=self.advert_column,
            raw_score_column="__model_pctr",
            score_column="__model_pctr",
        )
        scoped = scoped_predictions
        rank = Window.partitionBy(
            self.account_column,
            *scope_columns,
        ).orderBy(
            F.col("__model_pctr").desc_nulls_last(),
            F.col(self.advert_column).cast("string").asc(),
        )
        scoped = scoped.withColumn(
            "ProviderRank",
            F.row_number().over(rank),
        ).select(
            F.lit(model_build.model_build_id).alias("ProviderBuildID"),
            F.col(self.account_column).cast("string").alias("AccountNumber"),
            F.lit("ad").alias("EntityType"),
            F.col(self.advert_column).cast("string").alias("EntityID"),
            F.lit(definition.provider_id).alias("ProviderID"),
            F.lit(self.run_date).cast("date").alias("RunDate"),
            F.col("__model_pctr").cast("double").alias("RawScore"),
            F.col("__model_pctr").cast("double").alias("Score"),
            "ProviderRank",
            *scope_columns,
        )
        return canonical, scoped


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
    scope_filters: tuple[tuple[str, tuple[str, ...]], ...] = ()

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
            *(column for column, _values in self.scope_filters),
        }
        missing_scores = sorted(
            required_scores.difference(provider_scores.columns)
        )
        scope_columns = tuple(column for column, _values in self.scope_filters)
        missing_candidates = sorted(
            {
                self.account_column,
                self.advert_column,
                *scope_columns,
            }.difference(eligible_candidates.columns)
        )
        if missing_scores or missing_candidates:
            raise ValueError(
                "Candidate adaptation is missing columns: "
                + ", ".join([*missing_scores, *missing_candidates])
            )
        eligible = eligible_candidates
        for column, allowed_values in self.scope_filters:
            if not allowed_values:
                raise ValueError(f"Candidate scope is empty for {column}")
            eligible = eligible.where(F.col(column).isin(*allowed_values))
        eligible = eligible.alias("eligible")
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
        for column in scope_columns:
            joined = joined.where(
                F.col(f"eligible.{column}").cast("string")
                == F.col(f"scores.{column}").cast("string")
            )
        rank = Window.partitionBy(
            self.account_column,
            *scope_columns,
        ).orderBy(
            F.col("Score").desc_nulls_last(),
            F.col(self.advert_column).cast("string").asc(),
        )
        return joined.select(
            *[
                F.col(f"eligible.{column}")
                for column in eligible_candidates.columns
            ],
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
