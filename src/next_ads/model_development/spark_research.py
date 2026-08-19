"""Spark candidate plug-ins for the reusable binary research route."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from next_ads.model_development.contracts import ModelDefinition
from next_ads.model_development.research_contracts import CandidateSpec
from next_ads.model_development.research_scoring import (
    PositiveClassScoreTransformer,
    ensure_scalar_score,
)


RESEARCH_SIGNATURE_OUTPUTS = ("prediction", "score")
BUILTIN_CANDIDATES = frozenset(
    {
        "spark_logistic_regression",
        "spark_random_forest",
        "spark_gradient_boosted_trees",
        "spark_xgboost",
    }
)


def _resolved_parameters(candidate: CandidateSpec) -> dict[str, Any]:
    return dict(candidate.parameters)


def _model_fields(
    frame: Any, definition: ModelDefinition
) -> tuple[list[str], list[str]]:
    from pyspark.sql.types import NumericType, StringType

    fields = [
        frame.schema[column] for column in definition.model_feature_columns
    ]
    strings = [
        field.name
        for field in fields
        if isinstance(field.dataType, StringType)
    ]
    numerics = [
        field.name
        for field in fields
        if isinstance(field.dataType, NumericType)
    ]
    unsupported = sorted(
        field.name
        for field in fields
        if not isinstance(field.dataType, (StringType, NumericType))
    )
    if unsupported:
        raise ValueError(
            "Research candidates do not support declared feature types: "
            + ", ".join(unsupported)
        )
    if not strings and not numerics:
        raise ValueError("Research candidate has no supported model features")
    return strings, numerics


def _preprocessing_stages(
    frame: Any, definition: ModelDefinition
) -> list[Any]:
    from pyspark.ml.feature import (
        Imputer,
        OneHotEncoder,
        StringIndexer,
        VectorAssembler,
    )

    string_columns, numeric_columns = _model_fields(frame, definition)
    stages: list[Any] = []
    indexed: list[str] = []
    encoded: list[str] = []
    for column in string_columns:
        index_column = f"__research_index_{column}"
        encoded_column = f"__research_encoded_{column}"
        stages.append(
            StringIndexer(
                inputCol=column,
                outputCol=index_column,
                handleInvalid="keep",
                stringOrderType="alphabetAsc",
            )
        )
        indexed.append(index_column)
        encoded.append(encoded_column)
    if indexed:
        stages.append(
            OneHotEncoder(
                inputCols=indexed,
                outputCols=encoded,
                handleInvalid="keep",
                dropLast=False,
            )
        )
    imputed = [f"__research_numeric_{column}" for column in numeric_columns]
    if numeric_columns:
        stages.append(
            Imputer(
                inputCols=numeric_columns,
                outputCols=imputed,
                strategy="median",
            )
        )
    stages.append(
        VectorAssembler(
            inputCols=[*imputed, *encoded],
            outputCol="features",
            handleInvalid="keep",
        )
    )
    return stages


def _estimator(candidate: CandidateSpec, label_column: str) -> Any:
    parameters = _resolved_parameters(candidate)
    common = {
        "featuresCol": "features",
        "labelCol": label_column,
        "predictionCol": "prediction",
    }
    probabilistic_common = {
        **common,
        "probabilityCol": "probability",
        "rawPredictionCol": "rawPrediction",
    }
    if candidate.plugin == "spark_logistic_regression":
        from pyspark.ml.classification import LogisticRegression

        defaults = {"maxIter": 50, "regParam": 0.01, "elasticNetParam": 0.0}
        return LogisticRegression(
            **probabilistic_common,
            **{**defaults, **parameters},
        )
    if candidate.plugin == "spark_random_forest":
        from pyspark.ml.classification import RandomForestClassifier

        defaults = {
            "numTrees": 120,
            "maxDepth": 8,
            "minInstancesPerNode": 20,
            "seed": candidate.seed,
        }
        return RandomForestClassifier(
            **probabilistic_common,
            **{**defaults, **parameters},
        )
    if candidate.plugin == "spark_gradient_boosted_trees":
        from pyspark.ml.classification import GBTClassifier

        defaults = {
            "maxIter": 60,
            "maxDepth": 5,
            "stepSize": 0.05,
            "seed": candidate.seed,
        }
        # Spark GBT inherits the probability and rawPrediction output defaults,
        # but its Python constructor does not accept either column argument.
        return GBTClassifier(**common, **{**defaults, **parameters})
    if candidate.plugin == "spark_xgboost":
        from xgboost.spark import SparkXGBClassifier

        defaults = {
            "eval_metric": "aucpr",
            "max_depth": 6,
            "learning_rate": 0.05,
            "n_estimators": 150,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "num_workers": 4,
            "seed": candidate.seed,
        }
        xgb_common = {
            "features_col": "features",
            "label_col": label_column,
            "prediction_col": "prediction",
            "probability_col": "probability",
            "raw_prediction_col": "rawPrediction",
        }
        return SparkXGBClassifier(**xgb_common, **{**defaults, **parameters})
    raise ValueError(
        f"Unknown supplied research candidate: {candidate.plugin}"
    )


@dataclass(frozen=True)
class SparkResearchCandidatePlugin:
    """Fit and score one supplied Spark candidate without owning its split."""

    plugin_name: str

    def __post_init__(self) -> None:
        """Require one of the supplied, reviewed Spark candidate aliases."""
        if self.plugin_name not in BUILTIN_CANDIDATES:
            raise ValueError(
                f"Unknown supplied research candidate: {self.plugin_name}"
            )

    def fit(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        training_frame: Any,
    ) -> Any:
        """Fit on the exact frame supplied by orchestration."""
        from pyspark.ml import Pipeline

        if candidate.plugin != self.plugin_name:
            raise ValueError("Candidate plug-in resolution changed before fit")
        required = {definition.label, *definition.model_feature_columns}
        missing = sorted(required.difference(training_frame.columns))
        if missing:
            raise ValueError(
                "Candidate training frame is missing: " + ", ".join(missing)
            )
        stages = _preprocessing_stages(training_frame, definition)
        stages.extend(
            (
                _estimator(candidate, definition.label),
                PositiveClassScoreTransformer(
                    inputCol="probability",
                    outputCol="score",
                    predictionCol="prediction",
                ),
            )
        )
        return Pipeline(stages=stages).fit(training_frame)

    def predict(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        fitted_model: Any,
        evaluation_frame: Any,
    ) -> Any:
        """Return standard scalar outputs while retaining bounded audit fields."""
        if candidate.plugin != self.plugin_name:
            raise ValueError(
                "Candidate plug-in resolution changed before score"
            )
        predicted = fitted_model.transform(evaluation_frame)
        return ensure_scalar_score(predicted)

    def model_for_persistence(
        self,
        definition: ModelDefinition,
        candidate: CandidateSpec,
        fitted_model: Any,
    ) -> Any:
        """Return the Spark pipeline that already exposes scalar score."""
        del definition
        if candidate.plugin != self.plugin_name:
            raise ValueError(
                "Candidate plug-in resolution changed before persistence"
            )
        return fitted_model


def vector_feature_names(
    fitted_model: Any, example_frame: Any
) -> tuple[str, ...]:
    """Read Spark vector metadata and return stable, human-readable names."""
    transformed = fitted_model.transform(example_frame.limit(1))
    metadata: Mapping[str, Any] = transformed.schema["features"].metadata
    attributes = metadata.get("ml_attr", {}).get("attrs", {})
    indexed = []
    for group in sorted(attributes):
        for attribute in attributes[group]:
            index = int(attribute["idx"])
            name = str(attribute.get("name") or "").strip()
            if not name:
                raise ValueError(
                    "Feature vector metadata contains an unnamed value"
                )
            indexed.append((index, name))
    indexed.sort()
    if not indexed or [index for index, _name in indexed] != list(
        range(len(indexed))
    ):
        raise ValueError(
            "Feature vector metadata is incomplete or non-contiguous"
        )
    return tuple(name for _index, name in indexed)


def readable_feature_mapping(
    fitted_model: Any,
    example_frame: Any,
    definition: ModelDefinition,
) -> tuple[Any, ...]:
    """Map vector positions back to declared columns and fitted categories."""
    from next_ads.model_development.research_explainability import (
        FeatureNameMapping,
    )

    names = vector_feature_names(fitted_model, example_frame)
    source_columns = sorted(
        definition.model_feature_columns,
        key=len,
        reverse=True,
    )
    mappings = []
    for index, encoded_name in enumerate(names):
        cleaned = encoded_name
        for prefix in ("__research_numeric_", "__research_encoded_"):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :]
                break
        source = next(
            (
                column
                for column in source_columns
                if cleaned == column
                or cleaned.startswith(column + "_")
                or cleaned.startswith(column + "=")
            ),
            None,
        )
        if source is None:
            raise ValueError(
                "Could not map fitted vector value to a declared feature: "
                + encoded_name
            )
        category = cleaned[len(source) :].lstrip("_=") or None
        mappings.append(
            FeatureNameMapping(
                vector_index=index,
                source_column=source,
                category=category,
            )
        )
    return tuple(mappings)


def bounded_xgboost_contribution_frame(
    fitted_model: Any,
    frame: Any,
    *,
    row_id_column: str = "row_id",
    max_rows: int = 1000,
) -> Any:
    """Compute bounded SHAP-style contributions without changing the model."""
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    if not 1 <= max_rows <= 10_000:
        raise ValueError("max_rows must be between 1 and 10000")
    if row_id_column not in frame.columns:
        raise ValueError("Contribution evidence needs a hashed row identity")
    stages = list(fitted_model.stages)
    classifier_index = next(
        (
            index
            for index in range(len(stages) - 1, -1, -1)
            if hasattr(stages[index], "get_booster")
        ),
        None,
    )
    if classifier_index is None:
        raise ValueError("Fitted XGBoost pipeline has no booster stage")
    prepared = PipelineModel(stages=stages[:classifier_index]).transform(
        frame.orderBy(F.col(row_id_column).cast("string").asc()).limit(
            max_rows
        )
    )
    classifier = stages[classifier_index]
    if not classifier.hasParam("pred_contrib_col"):
        raise ValueError("Fitted XGBoost model has no contribution output")
    contribution_model = classifier.copy(
        {classifier.getParam("pred_contrib_col"): "contributions"}
    )
    contributed = contribution_model.transform(prepared).select(
        F.col(row_id_column).cast("string").alias("row_id_hash"),
        "contributions",
    )
    if not contributed.limit(1).collect():
        raise ValueError("Contribution evidence frame is empty")
    return contributed


def validate_research_model_signature(
    definition: ModelDefinition,
    signature: Any,
) -> None:
    """Require exact declared inputs and genuine prediction/score outputs."""
    input_names = tuple(signature.inputs.input_names())
    output_names = tuple(signature.outputs.input_names())
    if input_names != definition.model_feature_columns:
        raise ValueError(
            "Research model signature inputs do not match declared features"
        )
    if output_names != RESEARCH_SIGNATURE_OUTPUTS:
        raise ValueError(
            "Research model signature must expose prediction and score"
        )


def _validate_research_model_output(frame: Any) -> None:
    """Require genuine scalar DOUBLE outputs before signature inference."""
    field_types = {
        field.name: field.dataType.simpleString()
        for field in frame.schema.fields
    }
    expected = {name: "double" for name in RESEARCH_SIGNATURE_OUTPUTS}
    if field_types != expected:
        raise ValueError(
            "Research model outputs must be DOUBLE prediction and score: "
            f"found={field_types}"
        )


def log_research_model_with_signature(
    mlflow_module: Any,
    fitted_model: Any,
    definition: ModelDefinition,
    signature_frame: Any,
    *,
    artifact_path: str = "model",
    infer_signature_fn: Any | None = None,
) -> Any:
    """Log the fitted candidate with the standard two-output signature."""
    model_input = signature_frame.select(*definition.model_feature_columns)
    model_output = fitted_model.transform(model_input).select(
        *RESEARCH_SIGNATURE_OUTPUTS
    )
    _validate_research_model_output(model_output)
    if infer_signature_fn is None:
        from mlflow.models import infer_signature

        infer_signature_fn = infer_signature
    signature = infer_signature_fn(model_input, model_output)
    validate_research_model_signature(definition, signature)
    mlflow_module.spark.log_model(
        fitted_model,
        artifact_path=artifact_path,
        signature=signature,
    )
    return signature


__all__ = [
    "BUILTIN_CANDIDATES",
    "RESEARCH_SIGNATURE_OUTPUTS",
    "SparkResearchCandidatePlugin",
    "bounded_xgboost_contribution_frame",
    "log_research_model_with_signature",
    "readable_feature_mapping",
    "validate_research_model_signature",
    "vector_feature_names",
]
