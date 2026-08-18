"""Stable scalar-score output for binary research models."""

from __future__ import annotations

from typing import Any

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.types import DoubleType, StructField, StructType


STANDARD_LINEAGE_COLUMNS = ("row_id", "observation_date", "split")


class PositiveClassScoreTransformer(
    Transformer,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Expose the positive-class probability as a durable scalar score.

    Spark classifiers normally expose a probability vector.  Appending this
    transformer to a fitted pipeline preserves that vector while guaranteeing
    the registered model also returns ``prediction: DOUBLE`` and
    ``score: DOUBLE``.  Default parameter persistence makes the transformer
    safe to save as part of an MLWritable pipeline.
    """

    predictionCol = Param(
        Params._dummy(),
        "predictionCol",
        "binary prediction output column",
        typeConverter=TypeConverters.toString,
    )
    threshold = Param(
        Params._dummy(),
        "threshold",
        "score threshold used only when prediction is not already present",
        typeConverter=TypeConverters.toFloat,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: str = "probability",
        outputCol: str = "score",
        predictionCol: str = "prediction",
        threshold: float = 0.5,
    ) -> None:
        super().__init__()
        self._setDefault(
            inputCol="probability",
            outputCol="score",
            predictionCol="prediction",
            threshold=0.5,
        )
        self.setParams(**self._input_kwargs)

    @keyword_only
    def setParams(  # noqa: N802 - Spark API convention
        self,
        *,
        inputCol: str = "probability",
        outputCol: str = "score",
        predictionCol: str = "prediction",
        threshold: float = 0.5,
    ) -> PositiveClassScoreTransformer:
        """Set all persisted transformer parameters."""
        if not 0.0 <= float(threshold) <= 1.0:
            raise ValueError("threshold must be between 0 and 1")
        return self._set(**self._input_kwargs)

    def getPredictionCol(self) -> str:  # noqa: N802 - Spark API convention
        """Return the persisted prediction column name."""
        return self.getOrDefault(self.predictionCol)

    def getThreshold(self) -> float:  # noqa: N802 - Spark API convention
        """Return the persisted score threshold."""
        return float(self.getOrDefault(self.threshold))

    def _transform(self, dataset: Any) -> Any:
        return ensure_scalar_score(
            dataset,
            probability_column=self.getInputCol(),
            score_column=self.getOutputCol(),
            prediction_column=self.getPredictionCol(),
            threshold=self.getThreshold(),
            prefer_existing_score=False,
        )

    def transformSchema(self, schema: StructType) -> StructType:  # noqa: N802
        """Validate the probability input and declare the two scalar outputs."""
        input_column = self.getInputCol()
        if input_column not in schema.fieldNames():
            raise ValueError(f"Binary model output is missing {input_column}")
        output_column = self.getOutputCol()
        prediction_column = self.getPredictionCol()
        fields = [
            field
            for field in schema.fields
            if field.name not in {output_column, prediction_column}
        ]
        fields.extend(
            (
                StructField(prediction_column, DoubleType(), nullable=False),
                StructField(output_column, DoubleType(), nullable=False),
            )
        )
        return StructType(fields)


def ensure_scalar_score(
    frame: Any,
    *,
    probability_column: str = "probability",
    score_column: str = "score",
    prediction_column: str = "prediction",
    threshold: float = 0.5,
    prefer_existing_score: bool = True,
) -> Any:
    """Return a frame with scalar double prediction and positive-class score.

    A research model can already expose ``score``.  Legacy Spark models expose
    only ``probability`` and continue to work through the vector fallback.
    """
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    if not 0.0 <= float(threshold) <= 1.0:
        raise ValueError("threshold must be between 0 and 1")
    columns = set(frame.columns)
    if prefer_existing_score and score_column in columns:
        score = F.col(score_column).cast("double")
    elif probability_column in columns:
        score = (
            vector_to_array(F.col(probability_column))
            .getItem(1)
            .cast("double")
        )
    else:
        raise ValueError(
            "Binary model output must contain either scalar score or "
            f"probability: score={score_column}, "
            f"probability={probability_column}"
        )
    scored = frame.withColumn(score_column, score)
    if prediction_column in columns:
        prediction = F.col(prediction_column).cast("double")
    else:
        prediction = (F.col(score_column) >= F.lit(float(threshold))).cast(
            "double"
        )
    return scored.withColumn(prediction_column, prediction)


def _require_columns(frame: Any, columns: set[str], *, context: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")


def _row_profile(frame: Any, *, row_id_column: str) -> tuple[int, int, int]:
    from pyspark.sql import functions as F

    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.col(row_id_column)).alias("distinct_rows"),
        F.sum(
            F.when(
                F.col(row_id_column).isNull()
                | (F.length(F.col(row_id_column).cast("string")) == F.lit(0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("missing_rows"),
    ).first()
    if row is None:
        return 0, 0, 0
    return (
        int(row["rows"] or 0),
        int(row["distinct_rows"] or 0),
        int(row["missing_rows"] or 0),
    )


def _assert_exact_lineage(
    expected: Any,
    actual: Any,
    *,
    row_id_column: str,
    compared_columns: tuple[str, ...],
    context: str,
) -> None:
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as F

    expected_projection = expected.select(
        F.col(row_id_column),
        F.lit(1).alias("__expected_present"),
        *[
            F.col(column).alias(f"__expected_{index}")
            for index, column in enumerate(compared_columns)
        ],
    )
    actual_projection = actual.select(
        F.col(row_id_column),
        F.lit(1).alias("__actual_present"),
        *[
            F.col(column).alias(f"__actual_{index}")
            for index, column in enumerate(compared_columns)
        ],
    )
    joined = expected_projection.join(
        actual_projection,
        on=row_id_column,
        how="full",
    )
    mismatches = [
        F.col("__expected_present").isNull(),
        F.col("__actual_present").isNull(),
        *[
            ~F.col(f"__expected_{index}").eqNullSafe(
                F.col(f"__actual_{index}")
            )
            for index in range(len(compared_columns))
        ],
    ]
    if joined.where(reduce(or_, mismatches)).limit(1).count():
        raise ValueError(
            f"{context} does not preserve the exact supplied row lineage"
        )


def validate_prediction_adapter_output(
    source_frame: Any,
    predictions: Any,
    *,
    label_column: str,
    slice_columns: tuple[str, ...] = (),
    row_id_column: str = "row_id",
    observation_date_column: str = "observation_date",
    split_column: str = "split",
    prediction_column: str = "prediction",
    score_column: str = "score",
    context: str = "Candidate prediction adapter",
) -> None:
    """Prove a plug-in scored every supplied row without changing lineage."""
    from pyspark.sql import functions as F

    lineage_columns = tuple(
        dict.fromkeys(
            (
                label_column,
                observation_date_column,
                split_column,
                *slice_columns,
            )
        )
    )
    source_required = {row_id_column, *lineage_columns}
    prediction_required = {
        *source_required,
        prediction_column,
        score_column,
    }
    _require_columns(source_frame, source_required, context="Supplied frame")
    _require_columns(predictions, prediction_required, context=context)
    output_types = {
        column: predictions.schema[column].dataType.simpleString()
        for column in (prediction_column, score_column)
    }
    if output_types != {prediction_column: "double", score_column: "double"}:
        raise ValueError(
            f"{context} must emit DOUBLE prediction and score outputs"
        )
    for column in (row_id_column, *lineage_columns):
        expected_type = source_frame.schema[column].dataType.simpleString()
        actual_type = predictions.schema[column].dataType.simpleString()
        if actual_type != expected_type:
            raise ValueError(f"{context} changed the supplied {column} type")
    source_rows, source_distinct, source_missing = _row_profile(
        source_frame,
        row_id_column=row_id_column,
    )
    actual_rows, actual_distinct, actual_missing = _row_profile(
        predictions,
        row_id_column=row_id_column,
    )
    if source_rows < 1:
        raise ValueError("Supplied candidate frame is empty")
    if source_missing or source_distinct != source_rows:
        raise ValueError("Supplied candidate frame row IDs are not unique")
    if actual_missing or actual_distinct != actual_rows:
        raise ValueError(f"{context} row IDs are not unique and non-null")
    if actual_rows != source_rows:
        raise ValueError(f"{context} changed the supplied row count")
    invalid = predictions.agg(
        F.sum(
            F.when(
                F.col(score_column).isNull()
                | F.isnan(F.col(score_column))
                | (F.col(score_column) < F.lit(0.0))
                | (F.col(score_column) > F.lit(1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_scores"),
        F.sum(
            F.when(
                F.col(prediction_column).isNull()
                | F.isnan(F.col(prediction_column))
                | (~F.col(prediction_column).isin(0.0, 1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_predictions"),
    ).first()
    if invalid is None or int(invalid["invalid_scores"] or 0):
        raise ValueError(f"{context} contains invalid scores")
    if int(invalid["invalid_predictions"] or 0):
        raise ValueError(f"{context} contains invalid predictions")
    _assert_exact_lineage(
        source_frame,
        predictions,
        row_id_column=row_id_column,
        compared_columns=lineage_columns,
        context=context,
    )


def validate_persisted_prediction_equivalence(
    source_frame: Any,
    adapter_predictions: Any,
    persisted_predictions: Any,
    *,
    label_column: str,
    slice_columns: tuple[str, ...] = (),
    context: str = "Persisted candidate model",
) -> None:
    """Prove the model artifact scores exactly like the evaluated adapter."""
    validate_prediction_adapter_output(
        source_frame,
        persisted_predictions,
        label_column=label_column,
        slice_columns=slice_columns,
        context=context,
    )
    _assert_exact_lineage(
        adapter_predictions,
        persisted_predictions,
        row_id_column="row_id",
        compared_columns=("prediction", "score"),
        context=context,
    )


__all__ = [
    "STANDARD_LINEAGE_COLUMNS",
    "PositiveClassScoreTransformer",
    "ensure_scalar_score",
    "validate_persisted_prediction_equivalence",
    "validate_prediction_adapter_output",
]
