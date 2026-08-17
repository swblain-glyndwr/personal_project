from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from next_ads.features import product_embedding_inference as inference
from next_ads.features.product_embedding_inference import (
    build_product_embeddings_frame,
    validate_promoted_model_provenance,
)


MODEL_NAME = "marketingdata_dev.nextads_integration.product_embedding"
MODEL_VERSION = 2
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
SOURCE_MODEL_NAME = "marketingdata_dev.user.product_embedding"
SOURCE_MODEL_VERSION = 11
SOURCE_RUN_ID = "a" * 32
ARTIFACT_SHA256 = "b" * 64


@pytest.fixture(scope="module")
def local_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        pytest.skip(f"PySpark unavailable: {exc}")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("product-embedding-inference-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark
    spark.stop()


def _binding():
    return SimpleNamespace(
        model=SimpleNamespace(
            registered_model_name=MODEL_NAME,
            registered_model_version=MODEL_VERSION,
            model_uri=MODEL_URI,
        ),
        source_registered_model_name=SOURCE_MODEL_NAME,
        source_registered_model_version=SOURCE_MODEL_VERSION,
        source_run_id=SOURCE_RUN_ID,
        artifact_sha256=ARTIFACT_SHA256,
        inference_partitions=2,
        inference_batch_size=8,
    )


def _vector():
    return [1.0] + [0.0] * 383


def _existing_frame(local_spark, rows):
    return local_spark.createDataFrame(
        rows,
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_model_uri string, "
        "embedding_source_run_id string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int, "
        "embedding_text_hash string, embedding_text string, "
        "created_at timestamp, updated_at timestamp",
    )


def _existing_row(item_id, text_hash, *, model_uri=MODEL_URI, run_id=SOURCE_RUN_ID):
    timestamp = datetime(2026, 8, 1, 12, 0, 0)
    return (
        item_id,
        MODEL_NAME,
        str(MODEL_VERSION),
        model_uri,
        run_id,
        ARTIFACT_SHA256,
        _vector(),
        384,
        text_hash,
        f"text for {item_id}",
        timestamp,
        timestamp,
    )


def _install_fake_encoder(monkeypatch):
    def encode(missing, **_kwargs):
        from pyspark.sql import functions as F

        return missing.withColumn(
            "embedding",
            F.concat(
                F.array(F.lit(1.0)),
                F.array_repeat(F.lit(0.0), 383),
            ),
        )

    monkeypatch.setattr(inference, "_encode_missing_products", encode)


def test_complete_snapshot_reuses_exact_rows_generates_changes_and_drops_deletes(
    local_spark,
    monkeypatch,
):
    _install_fake_encoder(monkeypatch)
    source = local_spark.createDataFrame(
        [
            ("item-a", "text a", "a" * 64),
            ("item-b", "text b", "b" * 64),
        ],
        "item_id string, embedding_text string, embedding_text_hash string",
    )
    existing = _existing_frame(
        local_spark,
        [
            _existing_row("item-a", "a" * 64),
            _existing_row("deleted-item", "d" * 64),
        ],
    )

    output, evidence = build_product_embeddings_frame(
        source,
        existing,
        binding=_binding(),
        model_path=Path("unused"),
    )

    rows = {row.item_id: row for row in output.collect()}
    assert set(rows) == {"item-a", "item-b"}
    assert rows["item-a"].created_at == datetime(2026, 8, 1, 12, 0, 0)
    assert rows["item-b"].embedding_model_uri == MODEL_URI
    assert rows["item-b"].embedding_source_run_id == SOURCE_RUN_ID
    assert evidence.source_row_count == 2
    assert evidence.reused_row_count == 1
    assert evidence.generated_row_count == 1
    assert evidence.output_row_count == 2


def test_cache_row_with_wrong_provenance_is_regenerated(local_spark, monkeypatch):
    _install_fake_encoder(monkeypatch)
    source = local_spark.createDataFrame(
        [("item-a", "text a", "a" * 64)],
        "item_id string, embedding_text string, embedding_text_hash string",
    )
    existing = _existing_frame(
        local_spark,
        [_existing_row("item-a", "a" * 64, run_id="c" * 32)],
    )

    output, evidence = build_product_embeddings_frame(
        source,
        existing,
        binding=_binding(),
        model_path=Path("unused"),
    )

    assert output.collect()[0].embedding_source_run_id == SOURCE_RUN_ID
    assert evidence.reused_row_count == 0
    assert evidence.generated_row_count == 1


def test_duplicate_exact_cache_keys_are_rejected(local_spark):
    source = local_spark.createDataFrame(
        [("item-a", "text a", "a" * 64)],
        "item_id string, embedding_text string, embedding_text_hash string",
    )
    row = _existing_row("item-a", "a" * 64)
    existing = _existing_frame(local_spark, [row, row])

    with pytest.raises(ValueError, match="duplicate exact model key"):
        build_product_embeddings_frame(
            source,
            existing,
            binding=_binding(),
            model_path=Path("unused"),
        )


def test_promoted_model_provenance_must_match_recorded_source():
    binding = _binding()
    valid = SimpleNamespace(
        tags={
            "source_registered_model_name": SOURCE_MODEL_NAME,
            "source_model_version": str(SOURCE_MODEL_VERSION),
        }
    )
    validate_promoted_model_provenance(valid, binding)

    invalid = SimpleNamespace(tags={"source_model_version": "12"})
    with pytest.raises(ValueError, match="provenance does not match"):
        validate_promoted_model_provenance(invalid, binding)
