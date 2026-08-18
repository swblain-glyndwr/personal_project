from datetime import date

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from next_ads.model_development.research_scoring import (
    validate_persisted_prediction_equivalence,
    validate_prediction_adapter_output,
)


@pytest.fixture(scope="module")
def local_spark():
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("nextads-model-research-scoring-contract-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as error:
        pytest.skip(f"Local Spark unavailable: {error}")


def _source(local_spark):
    return local_spark.createDataFrame(
        [
            ("row-a", date(2026, 8, 9), "validate", 0.0, "SB1"),
            ("row-b", date(2026, 8, 10), "validate", 1.0, "SB2"),
        ],
        StructType(
            [
                StructField("row_id", StringType(), False),
                StructField("observation_date", DateType(), False),
                StructField("split", StringType(), False),
                StructField("clicked", DoubleType(), False),
                StructField("location", StringType(), False),
            ]
        ),
    )


def _predictions(source):
    return source.withColumn(
        "score",
        F.when(F.col("row_id") == F.lit("row-a"), F.lit(0.2)).otherwise(
            F.lit(0.8)
        ),
    ).withColumn(
        "prediction",
        (F.col("score") >= F.lit(0.5)).cast("double"),
    )


def test_prediction_adapter_must_preserve_every_supplied_row(local_spark):
    source = _source(local_spark)
    predictions = _predictions(source)

    validate_prediction_adapter_output(
        source,
        predictions,
        label_column="clicked",
        slice_columns=("location",),
    )

    with pytest.raises(ValueError, match="changed the supplied row count"):
        validate_prediction_adapter_output(
            source,
            predictions.where(F.col("row_id") == F.lit("row-a")),
            label_column="clicked",
            slice_columns=("location",),
        )
    with pytest.raises(ValueError, match="not unique"):
        validate_prediction_adapter_output(
            source,
            predictions.unionByName(predictions.limit(1)),
            label_column="clicked",
            slice_columns=("location",),
        )


def test_prediction_adapter_cannot_change_labels_splits_or_output_types(
    local_spark,
):
    source = _source(local_spark)
    predictions = _predictions(source)
    changed = predictions.withColumn(
        "clicked",
        F.when(F.col("row_id") == F.lit("row-a"), F.lit(1.0)).otherwise(
            F.col("clicked")
        ),
    )
    with pytest.raises(ValueError, match="exact supplied row lineage"):
        validate_prediction_adapter_output(
            source,
            changed,
            label_column="clicked",
            slice_columns=("location",),
        )
    with pytest.raises(ValueError, match="DOUBLE prediction and score"):
        validate_prediction_adapter_output(
            source,
            predictions.withColumn("score", F.col("score").cast("float")),
            label_column="clicked",
            slice_columns=("location",),
        )


def test_persisted_model_must_match_evaluated_adapter_scores(local_spark):
    source = _source(local_spark)
    predictions = _predictions(source)
    changed = predictions.withColumn(
        "score",
        F.when(F.col("row_id") == F.lit("row-a"), F.lit(0.3)).otherwise(
            F.col("score")
        ),
    )

    with pytest.raises(ValueError, match="does not preserve"):
        validate_persisted_prediction_equivalence(
            source,
            predictions,
            changed,
            label_column="clicked",
            slice_columns=("location",),
        )
