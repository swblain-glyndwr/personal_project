import ast
from datetime import date
import hashlib
import inspect
from math import sqrt

import pytest

from next_ads.features import product_embedding_transforms as transforms
from next_ads.features.product_embedding_transforms import (
    ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS,
    PRODUCT_TEXT_OUTPUT_COLUMNS,
    ProductEmbeddingLineage,
    build_advert_product_profile_frame,
    build_current_product_text_source,
    resolve_product_catalog_columns,
)


MODEL_NAME = "marketingdata_dev.nextads_models.product_embedding"
MODEL_VERSION = "7"
ARTIFACT_SHA256 = "a" * 64
REFERENCE_DATE = date(2026, 8, 1)


def _vector(*values: float) -> list[float]:
    return list(values) + [0.0] * (384 - len(values))


@pytest.fixture(scope="module")
def local_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        pytest.skip(f"PySpark unavailable: {exc}")
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-product-embedding-transform-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def test_module_keeps_pyspark_imports_inside_transform_functions():
    tree = ast.parse(inspect.getsource(transforms))
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]

    assert all(
        not (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("pyspark")
        )
        and not (
            isinstance(node, ast.Import)
            and any(alias.name.startswith("pyspark") for alias in node.names)
        )
        for node in top_level_imports
    )


def test_transforms_publish_the_revised_contract_columns():
    assert PRODUCT_TEXT_OUTPUT_COLUMNS == (
        "item_id",
        "embedding_text",
        "embedding_text_hash",
    )
    assert ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS == (
        "advert_id",
        "feature_date",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_artifact_sha256",
        "advert_product_item_count",
        "advert_product_embedded_item_count",
        "advert_product_embedding_coverage",
        "advert_product_embedding",
        "advert_product_embedding_dimension",
        "created_at",
        "updated_at",
    )


def test_catalog_column_resolution_matches_experiment_precedence():
    binding = resolve_product_catalog_columns(
        [
            "productSku",
            "itemno",
            "pid",
            "Brand",
            "brand",
            "name",
            "item_title",
            "colour",
            "color",
            "product_description",
            "start_date",
            "end_date",
        ]
    )

    assert binding.item_id == "pid"
    assert binding.descriptor("brand") == "brand"
    assert binding.descriptor("title") == "item_title"
    assert binding.descriptor("primary_colour") == "colour"
    assert binding.descriptor("description") == "product_description"
    assert binding.descriptor("material") is None
    assert binding.start_date == "start_date"
    assert binding.end_date == "end_date"


def test_catalog_column_resolution_requires_a_supported_product_id():
    with pytest.raises(ValueError, match="product identifier column"):
        resolve_product_catalog_columns(["title", "brand"])


def test_embedding_lineage_is_exact_and_normalises_numeric_version():
    lineage = ProductEmbeddingLineage(
        embedding_model_name=f" {MODEL_NAME} ",
        embedding_model_version=7,
        embedding_artifact_sha256=ARTIFACT_SHA256,
        embedding_dimension=384,
    )

    assert lineage.embedding_model_name == MODEL_NAME
    assert lineage.embedding_model_version == MODEL_VERSION
    assert lineage.embedding_artifact_sha256 == ARTIFACT_SHA256
    assert lineage.embedding_dimension == 384


@pytest.mark.parametrize(
    ("version", "digest", "dimension", "message"),
    [
        ("latest", ARTIFACT_SHA256, 384, "positive numeric version"),
        (0, ARTIFACT_SHA256, 384, "positive numeric version"),
        (MODEL_VERSION, "A" * 64, 384, "lowercase hexadecimal"),
        (MODEL_VERSION, "a" * 63, 384, "64-character"),
        (MODEL_VERSION, ARTIFACT_SHA256, 383, "exactly 384"),
        (MODEL_VERSION, ARTIFACT_SHA256, 384.5, "exactly 384"),
    ],
)
def test_embedding_lineage_rejects_inexact_artifacts(
    version,
    digest,
    dimension,
    message,
):
    with pytest.raises(ValueError, match=message):
        ProductEmbeddingLineage(
            embedding_model_name=MODEL_NAME,
            embedding_model_version=version,
            embedding_artifact_sha256=digest,
            embedding_dimension=dimension,
        )


def test_weighted_vector_reweights_missing_items_and_l2_normalises():
    result = transforms._weighted_l2_embedding(
        [
            {"weight": 0.5, "embedding": _vector(1.0, 0.0)},
            {"weight": 0.25, "embedding": _vector(0.0, 1.0)},
            {"weight": 0.25, "embedding": None},
        ]
    )

    assert result is not None
    assert result[:2] == pytest.approx([2 / sqrt(5), 1 / sqrt(5)])
    assert result[2:] == [0.0] * 382
    assert (
        transforms._weighted_l2_embedding([{"weight": 1.0, "embedding": None}])
        is None
    )


def test_profile_transform_checks_required_columns_before_spark_work():
    class MissingBridgeColumn:
        columns = ["advert_id", "feature_date", "item_id"]

    with pytest.raises(ValueError, match="item_weight"):
        build_advert_product_profile_frame(MissingBridgeColumn(), None)


def test_current_product_text_uses_precedence_and_latest_effective_row(
    local_spark,
):
    history = local_spark.createDataFrame(
        [
            (
                " AB-12 ",
                "ignored-id",
                date(2025, 1, 1),
                date(2026, 12, 31),
                "old brand",
                "old title",
                "old",
                "black",
            ),
            (
                " AB-12 ",
                "ignored-id",
                date(2026, 1, 1),
                date(2027, 12, 31),
                "  NEXT ",
                "Blue   Coat",
                "Women|Coats",
                "Navy",
            ),
            (
                "future",
                "ignored-id",
                date(2026, 9, 1),
                date(2027, 12, 31),
                "future",
                "future",
                "future",
                "future",
            ),
            (
                "empty",
                "ignored-id",
                date(2026, 1, 1),
                date(2027, 12, 31),
                "NaN",
                "null",
                "none",
                "n/a",
            ),
        ],
        "pid string, itemno string, start_date date, end_date date, "
        "brand string, item_title string, crumbs string, colour string",
    )

    rows = build_current_product_text_source(
        history,
        reference_date=REFERENCE_DATE,
    ).collect()

    assert len(rows) == 1
    assert rows[0].item_id == "ab12"
    assert rows[0].embedding_text == "next blue coat women coats navy"
    assert (
        rows[0].embedding_text_hash
        == hashlib.sha256(rows[0].embedding_text.encode()).hexdigest()
    )


def test_current_product_text_prefers_latest_open_ended_start(local_spark):
    history = local_spark.createDataFrame(
        [
            (
                "A-1",
                date(2025, 1, 1),
                date(2027, 12, 31),
                "older finite row",
            ),
            (
                "A-1",
                date(2026, 1, 1),
                None,
                "newer open row",
            ),
        ],
        "pid string, start_date date, end_date date, title string",
    )

    rows = build_current_product_text_source(
        history,
        reference_date=REFERENCE_DATE,
    ).collect()

    assert len(rows) == 1
    assert rows[0].embedding_text == "newer open row"


def test_current_product_text_rejects_equally_latest_conflicts(local_spark):
    history = local_spark.createDataFrame(
        [
            ("A-1", REFERENCE_DATE, date(2026, 12, 31), "one"),
            ("a1", REFERENCE_DATE, date(2026, 12, 31), "two"),
        ],
        "pid string, start_date date, end_date date, brand string",
    )

    with pytest.raises(ValueError, match="equally-latest text"):
        build_current_product_text_source(
            history,
            reference_date=REFERENCE_DATE,
        )


def test_current_product_text_rejects_an_empty_latest_snapshot(local_spark):
    history = local_spark.createDataFrame(
        [("A-1", REFERENCE_DATE, "none")],
        "pid string, start_date date, title string",
    )

    with pytest.raises(ValueError, match="no current product text rows"):
        build_current_product_text_source(
            history,
            reference_date=REFERENCE_DATE,
        )


def _profile_frames(local_spark, *, second_model_version=MODEL_VERSION):
    bridge = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "AB-12",
                1,
                0.75,
                "source",
                REFERENCE_DATE,
            ),
            (
                "advert-a",
                REFERENCE_DATE,
                "missing-34",
                2,
                0.25,
                "source",
                REFERENCE_DATE,
            ),
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    embeddings = local_spark.createDataFrame(
        [
            (
                "ab12",
                MODEL_NAME,
                second_model_version,
                ARTIFACT_SHA256,
                _vector(1.0),
                384,
            )
        ],
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int",
    )
    return bridge, embeddings


def test_advert_product_profile_matches_the_revised_contract(local_spark):
    bridge, embeddings = _profile_frames(local_spark)

    result = build_advert_product_profile_frame(bridge, embeddings)
    row = result.collect()[0]

    assert tuple(result.columns) == ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS
    assert row.advert_id == "advert-a"
    assert row.feature_date == REFERENCE_DATE
    assert row.embedding_model_name == MODEL_NAME
    assert row.embedding_model_version == MODEL_VERSION
    assert row.embedding_artifact_sha256 == ARTIFACT_SHA256
    assert row.advert_product_item_count == 2
    assert row.advert_product_embedded_item_count == 1
    assert row.advert_product_embedding_coverage == 0.5
    assert row.advert_product_embedding == _vector(1.0)
    assert row.advert_product_embedding_dimension == 384
    assert row.created_at == row.updated_at


def test_advert_product_profile_uses_spark_native_vector_aggregation():
    source = inspect.getsource(build_advert_product_profile_frame)

    assert "F.udf" not in source
    assert "aggregate(" in source
    assert "zip_with(" in source


def test_advert_product_profile_rejects_bad_weight_sums(local_spark):
    bridge, embeddings = _profile_frames(local_spark)
    invalid_bridge = bridge.withColumn("item_weight", bridge.item_weight / 2)

    with pytest.raises(ValueError, match="item_weight must sum to 1"):
        build_advert_product_profile_frame(invalid_bridge, embeddings)


def test_advert_product_profile_rejects_bad_vectors(local_spark):
    from pyspark.sql import functions as F

    bridge, embeddings = _profile_frames(local_spark)
    invalid_embeddings = embeddings.withColumn(
        "embedding",
        F.slice("embedding", 1, 3),
    )

    with pytest.raises(ValueError, match="384-value finite embedding"):
        build_advert_product_profile_frame(bridge, invalid_embeddings)


def test_advert_product_profile_rejects_mixed_model_lineage(local_spark):
    bridge, embeddings = _profile_frames(local_spark)
    mixed = embeddings.unionByName(
        local_spark.createDataFrame(
            [
                (
                    "other",
                    MODEL_NAME,
                    "8",
                    ARTIFACT_SHA256,
                    _vector(0.0, 1.0),
                    384,
                )
            ],
            embeddings.schema,
        )
    )

    with pytest.raises(ValueError, match="exactly one model artifact"):
        build_advert_product_profile_frame(bridge, mixed)
