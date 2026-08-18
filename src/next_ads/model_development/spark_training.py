"""DBR 15.4 Spark trainer used by declared binary classification models."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from next_ads.model_development.contracts import (
    DBR_15_4_SPARK_CPU,
    MODEL_VERSION_TAG_ARTIFACT_DIGEST,
    MODEL_VERSION_TAG_BUILD_ID,
    MODEL_VERSION_TAG_TRAINING_RECEIPT_ID,
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)
from next_ads.model_development.runtime import model_build_id


MODEL_EVALUATION_METRICS = (
    "auc_pr",
    "auc_roc",
    "log_loss",
    "calibration_gap",
    "lift_at_5_percent",
)

MODEL_SIGNATURE_OUTPUTS = ("prediction",)


def artifact_directory_digest(path: str | Path) -> str:
    """Hash artifact paths and bytes so promotion can verify exact identity."""
    root = Path(path)
    if not root.is_dir():
        raise ValueError(f"MLflow artifact directory does not exist: {root}")
    digest = hashlib.sha256()
    files = sorted(item for item in root.rglob("*") if item.is_file())
    if not files:
        raise ValueError("MLflow artifact directory is empty")
    for item in files:
        relative = item.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with item.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _signature_column_names(schema: Any, field_name: str) -> tuple[str, ...]:
    """Return named MLflow schema columns or reject an unusable signature."""
    if schema is None or not hasattr(schema, "input_names"):
        raise ValueError(f"MLflow model signature has no {field_name} schema")
    names = tuple(schema.input_names())
    if not names or any(
        not isinstance(name, str) or not name.strip() for name in names
    ):
        raise ValueError(
            f"MLflow model signature {field_name} columns must be named"
        )
    return names


def validate_spark_model_signature(
    definition: ModelDefinition,
    signature: Any,
) -> None:
    """Require the registered signature to expose only declared model inputs."""
    input_names = _signature_column_names(signature.inputs, "input")
    if input_names != definition.model_feature_columns:
        raise ValueError(
            "MLflow model signature inputs do not match the declared model "
            f"features: expected={definition.model_feature_columns}, "
            f"found={input_names}"
        )
    output_names = _signature_column_names(signature.outputs, "output")
    if output_names != MODEL_SIGNATURE_OUTPUTS:
        raise ValueError(
            "MLflow model signature output must be the Spark prediction: "
            f"found={output_names}"
        )


def log_spark_model_with_signature(
    mlflow_module: Any,
    model: Any,
    definition: ModelDefinition,
    signature_frame: Any,
    *,
    infer_signature_fn: Any | None = None,
) -> Any:
    """Log a Spark pipeline with the named signature required by UC models."""
    model_input = signature_frame.select(*definition.model_feature_columns)
    model_output = model.transform(model_input).select(
        *MODEL_SIGNATURE_OUTPUTS
    )
    if infer_signature_fn is None:
        from mlflow.models import infer_signature

        infer_signature_fn = infer_signature
    signature = infer_signature_fn(model_input, model_output)
    validate_spark_model_signature(definition, signature)
    mlflow_module.spark.log_model(
        model,
        artifact_path="model",
        signature=signature,
    )
    return signature


def deterministic_train_validation_split(
    frame: Any,
    *,
    keys: tuple[str, ...],
    validation_percent: int = 20,
) -> tuple[Any, Any]:
    """Split by stable row identity instead of Spark partition order."""
    from pyspark.sql import functions as F

    if not 1 <= validation_percent <= 50:
        raise ValueError("validation_percent must be between 1 and 50")
    missing = sorted(set(keys).difference(frame.columns))
    if missing:
        raise ValueError(
            "Training split is missing keys: " + ", ".join(missing)
        )
    bucket = F.pmod(
        F.xxhash64(*[F.col(column) for column in keys]),
        F.lit(100),
    )
    split = frame.withColumn("_model_validation_bucket", bucket)
    train = split.where(
        F.col("_model_validation_bucket") >= F.lit(validation_percent)
    ).drop("_model_validation_bucket")
    validation = split.where(
        F.col("_model_validation_bucket") < F.lit(validation_percent)
    ).drop("_model_validation_bucket")
    if not train.limit(1).collect() or not validation.limit(1).collect():
        raise ValueError("Deterministic split produced an empty dataset")
    return train, validation


def temporal_validation_cutoff(
    observation_dates: tuple[Any, ...],
    *,
    validation_percent: int = 20,
) -> Any:
    """Choose whole latest dates for validation, never random future leakage."""
    if not 1 <= validation_percent <= 50:
        raise ValueError("validation_percent must be between 1 and 50")
    dates = tuple(sorted(set(observation_dates)))
    if len(dates) < 2:
        raise ValueError(
            "Temporal validation requires observations from at least two dates"
        )
    validation_dates = max(
        1,
        math.ceil(len(dates) * validation_percent / 100),
    )
    validation_dates = min(validation_dates, len(dates) - 1)
    return dates[-validation_dates]


def temporal_train_validation_split(
    frame: Any,
    *,
    timestamp_column: str,
    validation_percent: int = 20,
) -> tuple[Any, Any, Any]:
    """Train on earlier exposure dates and validate on later exposure dates."""
    from pyspark.sql import functions as F

    if timestamp_column not in frame.columns:
        raise ValueError(
            f"Temporal validation is missing timestamp: {timestamp_column}"
        )
    rows = (
        frame.select(
            F.to_date(F.col(timestamp_column)).alias("observation_date")
        )
        .where(F.col("observation_date").isNotNull())
        .distinct()
        .orderBy("observation_date")
        .collect()
    )
    cutoff = temporal_validation_cutoff(
        tuple(row["observation_date"] for row in rows),
        validation_percent=validation_percent,
    )
    observation_date = F.to_date(F.col(timestamp_column))
    train = frame.where(observation_date < F.lit(cutoff))
    validation = frame.where(observation_date >= F.lit(cutoff))
    if not train.limit(1).collect() or not validation.limit(1).collect():
        raise ValueError("Temporal split produced an empty dataset")
    return train, validation, cutoff


def _probability_metrics(
    predictions: Any,
    *,
    label_column: str,
    probability_column: str,
) -> dict[str, float]:
    """Return interpretable pCTR checks from one temporal holdout."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    scored = predictions.withColumn(
        "__model_probability",
        vector_to_array(F.col(probability_column)).getItem(1).cast("double"),
    )
    probability = F.least(
        F.lit(1.0 - 1e-15),
        F.greatest(F.lit(1e-15), F.col("__model_probability")),
    )
    label = F.col(label_column).cast("double")
    row = scored.agg(
        F.count(F.lit(1)).alias("rows"),
        F.avg(label).alias("observed_rate"),
        F.avg(probability).alias("predicted_rate"),
        F.avg(
            -(label * F.log(probability))
            - ((F.lit(1.0) - label) * F.log(F.lit(1.0) - probability))
        ).alias("log_loss"),
    ).first()
    if row is None or not row["rows"]:
        raise ValueError("Validation predictions are empty")
    observed_rate = float(row["observed_rate"])
    predicted_rate = float(row["predicted_rate"])
    top_rows = max(1, math.ceil(int(row["rows"]) * 0.05))
    top = (
        scored.orderBy(F.col("__model_probability").desc())
        .limit(top_rows)
        .agg(F.avg(label).alias("top_rate"))
        .first()
    )
    top_rate = float(top["top_rate"])
    return {
        "log_loss": float(row["log_loss"]),
        "calibration_gap": abs(predicted_rate - observed_rate),
        "observed_click_rate": observed_rate,
        "predicted_click_rate": predicted_rate,
        "lift_at_5_percent": top_rate / observed_rate,
    }


class SparkBinaryClassifierTrainer:
    """Compare approved Spark classifiers and register the best PR-AUC model."""

    def __init__(
        self,
        *,
        registered_model_name: str,
        validation_percent: int = 20,
        seed: int = 1729,
    ) -> None:
        if not registered_model_name.strip():
            raise ValueError("registered_model_name must not be empty")
        self.registered_model_name = registered_model_name.strip()
        self.validation_percent = validation_percent
        self.seed = seed

    def train(
        self,
        definition: ModelDefinition,
        training_receipt: TrainingSetReceipt,
        training_frame: Any,
    ) -> ModelBuild:
        """Fit candidates, log evidence and return one exact registered version."""
        if definition.runtime_profile != DBR_15_4_SPARK_CPU:
            raise ValueError(
                "Spark binary trainer requires DBR 15.4 Spark/CPU"
            )
        if training_receipt.status != "READY":
            raise ValueError("Spark binary trainer requires a READY receipt")

        import mlflow
        from mlflow.tracking import MlflowClient
        from pyspark.ml import Pipeline
        from pyspark.ml.classification import (
            GBTClassifier,
            LogisticRegression,
        )
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        from pyspark.ml.feature import (
            Imputer,
            OneHotEncoder,
            StringIndexer,
            VectorAssembler,
        )
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType, StringType

        missing = sorted(
            {definition.label, *definition.observation_keys}.difference(
                training_frame.columns
            )
        )
        if missing:
            raise ValueError(
                "Training frame is missing: " + ", ".join(missing)
            )
        train, validation, validation_start = temporal_train_validation_split(
            training_frame,
            timestamp_column=(
                definition.training_observation.observation_timestamp
            ),
            validation_percent=self.validation_percent,
        )
        from next_ads.model_development.training_sets import (
            summarise_binary_labels,
        )

        summarise_binary_labels(train, definition.label)
        summarise_binary_labels(validation, definition.label)
        feature_columns = definition.model_feature_columns
        feature_fields = [
            training_frame.schema[field_name] for field_name in feature_columns
        ]
        string_columns = [
            field.name
            for field in feature_fields
            if isinstance(field.dataType, StringType)
        ]
        numeric_columns = [
            field.name
            for field in feature_fields
            if isinstance(field.dataType, NumericType)
        ]
        if not string_columns and not numeric_columns:
            raise ValueError("Training frame has no supported model features")
        for column in numeric_columns:
            train = train.withColumn(column, F.col(column).cast("double"))
            validation = validation.withColumn(
                column, F.col(column).cast("double")
            )

        stages = []
        indexed = []
        encoded = []
        for column in string_columns:
            index_column = f"__model_index_{column}"
            encoded_column = f"__model_encoded_{column}"
            stages.append(
                StringIndexer(
                    inputCol=column,
                    outputCol=index_column,
                    handleInvalid="keep",
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
                )
            )
        imputed_numeric = [
            f"__model_numeric_{column}" for column in numeric_columns
        ]
        if numeric_columns:
            stages.append(
                Imputer(
                    inputCols=numeric_columns,
                    outputCols=imputed_numeric,
                    strategy="median",
                )
            )
        stages.append(
            VectorAssembler(
                inputCols=[*imputed_numeric, *encoded],
                outputCol="features",
                handleInvalid="keep",
            )
        )
        candidates = (
            (
                "logistic_regression",
                LogisticRegression(
                    featuresCol="features",
                    labelCol=definition.label,
                    maxIter=50,
                    regParam=0.01,
                ),
            ),
            (
                "gradient_boosted_trees",
                GBTClassifier(
                    featuresCol="features",
                    labelCol=definition.label,
                    maxIter=60,
                    maxDepth=5,
                    stepSize=0.05,
                    seed=self.seed,
                ),
            ),
        )
        pr_evaluator = BinaryClassificationEvaluator(
            labelCol=definition.label,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR",
        )
        roc_evaluator = BinaryClassificationEvaluator(
            labelCol=definition.label,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )

        build_id = model_build_id(definition, training_receipt)
        started_at = datetime.now(timezone.utc)
        with mlflow.start_run(
            run_name=f"{definition.model_name}_{build_id[:12]}"
        ) as run:
            mlflow.log_params(
                {
                    "model_build_id": build_id,
                    "model_definition_checksum": definition.checksum,
                    "runtime_profile": definition.runtime_profile,
                    "training_receipt_id": training_receipt.receipt_id,
                    "validation_strategy": "latest_observation_dates",
                    "validation_start_date": validation_start.isoformat(),
                }
            )
            mlflow.log_text(
                json.dumps(
                    {
                        "definition": definition.as_dict(),
                        "feature_bindings": [
                            binding.__dict__
                            for binding in training_receipt.feature_bindings
                        ],
                        "receipt_id": training_receipt.receipt_id,
                    },
                    default=str,
                    sort_keys=True,
                ),
                "training_receipt.json",
            )
            best = None
            metrics = {}
            for candidate_name, estimator in candidates:
                fitted = Pipeline(stages=[*stages, estimator]).fit(train)
                predictions = fitted.transform(validation)
                pr_auc = float(pr_evaluator.evaluate(predictions))
                roc_auc = float(roc_evaluator.evaluate(predictions))
                metrics[f"{candidate_name}_auc_pr"] = pr_auc
                metrics[f"{candidate_name}_auc_roc"] = roc_auc
                for metric_name, metric_value in _probability_metrics(
                    predictions,
                    label_column=definition.label,
                    probability_column="probability",
                ).items():
                    metrics[f"{candidate_name}_{metric_name}"] = metric_value
                if best is None or pr_auc > best[0]:
                    best = (pr_auc, candidate_name, fitted)
            assert best is not None
            for metric_name in MODEL_EVALUATION_METRICS:
                metrics[metric_name] = metrics[f"{best[1]}_{metric_name}"]
            mlflow.log_metrics(metrics)
            mlflow.log_param("selected_candidate", best[1])
            log_spark_model_with_signature(
                mlflow,
                best[2],
                definition,
                validation,
            )
            run_id = run.info.run_id

        model_uri = f"runs:/{run_id}/model"
        registered = mlflow.register_model(
            model_uri=model_uri,
            name=self.registered_model_name,
        )
        version = int(registered.version)
        client = MlflowClient()
        artifact_path = client.download_artifacts(run_id, "model")
        digest = artifact_directory_digest(artifact_path)
        for key, value in {
            MODEL_VERSION_TAG_ARTIFACT_DIGEST: digest,
            MODEL_VERSION_TAG_BUILD_ID: build_id,
            MODEL_VERSION_TAG_TRAINING_RECEIPT_ID: (
                training_receipt.receipt_id
            ),
        }.items():
            client.set_model_version_tag(
                name=self.registered_model_name,
                version=version,
                key=key,
                value=value,
            )
        client.set_registered_model_alias(
            name=self.registered_model_name,
            alias="dev_candidate",
            version=version,
        )
        completed_at = datetime.now(timezone.utc)
        return ModelBuild(
            model_build_id=build_id,
            model_name=definition.model_name,
            training_receipt_id=training_receipt.receipt_id,
            model_definition_checksum=definition.checksum,
            runtime_profile=definition.runtime_profile,
            status="READY",
            created_at=started_at,
            mlflow_run_id=run_id,
            registered_model_name=self.registered_model_name,
            registered_model_version=version,
            model_uri=(f"models:/{self.registered_model_name}/{version}"),
            artifact_digest=digest,
            metrics=tuple(sorted(metrics.items())),
            completed_at=completed_at,
        )


__all__ = [
    "SparkBinaryClassifierTrainer",
    "MODEL_EVALUATION_METRICS",
    "artifact_directory_digest",
    "deterministic_train_validation_split",
    "log_spark_model_with_signature",
    "temporal_train_validation_split",
    "temporal_validation_cutoff",
    "validate_spark_model_signature",
]
