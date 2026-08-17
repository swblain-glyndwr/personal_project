"""DBR 15.4 Spark trainer used by declared binary classification models."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from next_ads.model_development.contracts import (
    DBR_15_4_SPARK_CPU,
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)
from next_ads.model_development.runtime import model_build_id


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
        raise ValueError("Training split is missing keys: " + ", ".join(missing))
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
            raise ValueError("Spark binary trainer requires DBR 15.4 Spark/CPU")
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
            raise ValueError("Training frame is missing: " + ", ".join(missing))
        train, validation = deterministic_train_validation_split(
            training_frame,
            keys=definition.observation_keys,
            validation_percent=self.validation_percent,
        )
        excluded = set(definition.observation_keys).union({definition.label})
        feature_fields = [
            field for field in training_frame.schema.fields if field.name not in excluded
        ]
        string_columns = [
            field.name for field in feature_fields if isinstance(field.dataType, StringType)
        ]
        numeric_columns = [
            field.name for field in feature_fields if isinstance(field.dataType, NumericType)
        ]
        if not string_columns and not numeric_columns:
            raise ValueError("Training frame has no supported model features")
        for column in numeric_columns:
            train = train.withColumn(column, F.col(column).cast("double"))
            validation = validation.withColumn(column, F.col(column).cast("double"))

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
        imputed_numeric = [f"__model_numeric_{column}" for column in numeric_columns]
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
        with mlflow.start_run(run_name=f"{definition.model_name}_{build_id[:12]}") as run:
            mlflow.log_params(
                {
                    "model_build_id": build_id,
                    "model_definition_checksum": definition.checksum,
                    "runtime_profile": definition.runtime_profile,
                    "training_receipt_id": training_receipt.receipt_id,
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
                if best is None or pr_auc > best[0]:
                    best = (pr_auc, candidate_name, fitted)
            assert best is not None
            mlflow.log_metrics(metrics)
            mlflow.log_param("selected_candidate", best[1])
            mlflow.spark.log_model(best[2], artifact_path="model")
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
    "artifact_directory_digest",
    "deterministic_train_validation_split",
]
