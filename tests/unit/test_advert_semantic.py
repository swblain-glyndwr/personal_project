import ast
from datetime import date
import hashlib
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from next_ads.features import advert_semantic
from next_ads.features.advert_semantic import (
    ADVERT_SEMANTIC_PROFILE_COLUMNS,
    ADVERT_SEMANTIC_TEXT_COLUMNS,
    ADVERT_SEMANTIC_VECTOR_COLUMNS,
    MAX_NEIGHBOURS_PER_ADVERT,
    AdvertEmbeddingLineage,
    build_advert_image_flags,
    build_advert_semantic_profile_frame,
    build_advert_semantic_text_source,
    normalise_advert_text,
    select_exact_product_text,
)


REFERENCE_DATE = date(2026, 8, 1)
MODEL_NAME = "marketingdata_dev.nextads_integration.advert_semantic"
MODEL_VERSION = "4"
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
SOURCE_RUN_ID = "95be978bd9e24783afe4e68def0c9845"
ARTIFACT_SHA256 = "a" * 64


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
            .appName("next-ads-advert-semantic-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def test_module_keeps_pyspark_imports_inside_transform_functions():
    tree = ast.parse(inspect.getsource(advert_semantic))
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


def test_contract_columns_match_the_declared_table():
    assert MAX_NEIGHBOURS_PER_ADVERT == 20
    assert ADVERT_SEMANTIC_TEXT_COLUMNS == (
        "advert_id",
        "feature_date",
        "advert_text_corpus",
        "advert_text_hash",
        "advert_has_destination_image",
    )
    assert ADVERT_SEMANTIC_PROFILE_COLUMNS == (
        "advert_id",
        "feature_date",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "advert_text_corpus",
        "advert_text_hash",
        "advert_embedding",
        "advert_embedding_dimension",
        "advert_semantic_token_count",
        "advert_semantic_unique_token_count",
        "advert_has_destination_image",
        "advert_embedding_neighbour_count",
        "advert_embedding_top_similarity",
        "advert_embedding_avg_similarity",
        "created_at",
        "updated_at",
    )
    assert ADVERT_SEMANTIC_VECTOR_COLUMNS == (
        "advert_id",
        "feature_date",
        "advert_text_hash",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "embedding",
        "embedding_dimension",
    )


def test_text_normalisation_matches_the_existing_experiment():
    assert (
        normalise_advert_text(
            "  Shop HTTPS://example.test/a?b=1 -- Blue & WHITE!  "
        )
        == "shop blue white"
    )
    assert normalise_advert_text(None) == ""


def test_advert_embedding_lineage_requires_an_exact_model_version():
    lineage = AdvertEmbeddingLineage(
        embedding_model_name=f" {MODEL_NAME} ",
        embedding_model_version=4,
        embedding_model_uri=MODEL_URI,
        embedding_source_run_id=SOURCE_RUN_ID,
        embedding_artifact_sha256=ARTIFACT_SHA256,
        embedding_dimension=384,
    )

    assert lineage.embedding_model_name == MODEL_NAME
    assert lineage.embedding_model_version == MODEL_VERSION
    assert lineage.embedding_model_uri == MODEL_URI
    assert lineage.embedding_source_run_id == SOURCE_RUN_ID
    assert lineage.embedding_artifact_sha256 == ARTIFACT_SHA256
    assert lineage.embedding_dimension == 384


@pytest.mark.parametrize(
    ("version", "dimension", "message"),
    [
        ("latest", 384, "positive numeric version"),
        (0, 384, "positive numeric version"),
        (MODEL_VERSION, 383, "exactly 384"),
        (MODEL_VERSION, 384.5, "exactly 384"),
    ],
)
def test_advert_embedding_lineage_rejects_mutable_or_wrong_models(
    version,
    dimension,
    message,
):
    with pytest.raises(ValueError, match=message):
        AdvertEmbeddingLineage(
            embedding_model_name=MODEL_NAME,
            embedding_model_version=version,
            embedding_model_uri=(
                f"models:/{MODEL_NAME}/{version}"
            ),
            embedding_source_run_id=SOURCE_RUN_ID,
            embedding_artifact_sha256=ARTIFACT_SHA256,
            embedding_dimension=dimension,
        )


def test_advert_embedding_lineage_rejects_an_inexact_uri_or_digest():
    with pytest.raises(ValueError, match="exact registered model"):
        AdvertEmbeddingLineage(
            embedding_model_name=MODEL_NAME,
            embedding_model_version=MODEL_VERSION,
            embedding_model_uri=f"models:/{MODEL_NAME}/5",
            embedding_source_run_id=SOURCE_RUN_ID,
            embedding_artifact_sha256=ARTIFACT_SHA256,
            embedding_dimension=384,
        )
    with pytest.raises(ValueError, match="64-character lowercase"):
        AdvertEmbeddingLineage(
            embedding_model_name=MODEL_NAME,
            embedding_model_version=MODEL_VERSION,
            embedding_model_uri=MODEL_URI,
            embedding_source_run_id=SOURCE_RUN_ID,
            embedding_artifact_sha256="A" * 64,
            embedding_dimension=384,
        )


def test_table_sql_persists_the_exact_model_artifact():
    project_root = Path(__file__).resolve().parents[2]
    ddl = (
        project_root
        / "sql"
        / "features"
        / "nextads"
        / "create_table_next_uk_nextads_fs_advert_semantic_profile_daily.sql"
    ).read_text(encoding="utf-8")

    for definition in (
        "embedding_model_name STRING NOT NULL",
        "embedding_model_version STRING NOT NULL",
        "embedding_model_uri STRING NOT NULL",
        "embedding_source_run_id STRING NOT NULL",
        "embedding_artifact_sha256 STRING NOT NULL",
        "advert_embedding ARRAY<DOUBLE>",
        "advert_embedding_dimension INT",
    ):
        assert definition in ddl


def test_text_transform_checks_source_columns_before_spark_work():
    class MissingCoreColumns:
        columns = ["advert_id", "feature_date"]

    with pytest.raises(ValueError, match="location"):
        build_advert_semantic_text_source(
            MissingCoreColumns(),
            None,
            None,
            None,
            None,
        )


def _text_frames(local_spark, *, conflicting_core=False, include_image=True):
    second_headline = "Different copy" if conflicting_core else "The LOOK!"
    core = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "shopping_bag_1",
                "Summer Edit",
                "The LOOK!",
                "Blue + white",
                "Buy now",
                REFERENCE_DATE,
            ),
            (
                "advert-a",
                REFERENCE_DATE,
                "shopping_bag_2",
                "Summer Edit",
                second_headline,
                "Blue + white",
                "Buy now",
                REFERENCE_DATE,
            ),
        ],
        "advert_id string, feature_date date, location string, "
        "advert_title string, headline string, subtext string, cta string, "
        "source_rundate date",
    )
    attributes = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "NEXT",
                "Holiday",
                "Blue",
                "Relaxed",
                "Dresses",
                "Women",
                "Female",
            )
        ],
        "advert_id string, feature_date date, top_brand string, "
        "top_use string, top_colour string, top_style string, "
        "top_category string, top_department string, top_gender string",
    )
    bridge = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "SKU-1",
                1,
                1.0,
                "v2_sort_history",
                REFERENCE_DATE,
            )
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    product_text_value = "NEXT - linen dress!"
    product_text = local_spark.createDataFrame(
        [
            (
                "sku1",
                product_text_value,
                hashlib.sha256(product_text_value.encode()).hexdigest(),
            )
        ],
        "item_id string, embedding_text string, embedding_text_hash string",
    )
    image_rows = [("advert-a", REFERENCE_DATE, True)] if include_image else []
    images = local_spark.createDataFrame(
        image_rows,
        "advert_id string, feature_date date, "
        "advert_has_destination_image boolean",
    )
    return core, attributes, bridge, product_text, images


def test_text_source_uses_advert_attributes_and_ranked_item_text(local_spark):
    frames = _text_frames(local_spark)

    result = build_advert_semantic_text_source(*frames)
    row = result.collect()[0]

    assert tuple(result.columns) == ADVERT_SEMANTIC_TEXT_COLUMNS
    assert row.advert_id == "advert-a"
    assert row.feature_date == REFERENCE_DATE
    assert row.advert_has_destination_image is True
    assert row.advert_text_corpus == (
        "the look the look blue white buy now next holiday blue relaxed "
        "dresses women female next linen dress advert image blue next "
        "holiday relaxed dresses women female"
    )
    assert (
        row.advert_text_hash
        == hashlib.sha256(row.advert_text_corpus.encode()).hexdigest()
    )


def test_image_flags_use_any_repository_control_sheet_image(local_spark):
    control = local_spark.createDataFrame(
        [
            (
                "advert-a",
                date(2026, 1, 1),
                date(2026, 12, 31),
                None,
                " ",
                None,
            ),
            (
                "advert-a",
                date(2026, 1, 1),
                date(2026, 12, 31),
                "https://images.test/a.jpg",
                None,
                None,
            ),
            (
                "advert-b",
                date(2026, 1, 1),
                date(2026, 12, 31),
                "null",
                "none",
                "n/a",
            ),
            (
                "advert-future",
                date(2026, 9, 1),
                None,
                "https://images.test/future.jpg",
                None,
                None,
            ),
        ],
        "UniqueAdID string, StartDate date, EndDate date, "
        "BackgroundImage string, MobileImage string, FlatJPG string",
    )

    rows = {
        row.advert_id: row.advert_has_destination_image
        for row in build_advert_image_flags(control, REFERENCE_DATE).collect()
    }

    assert rows == {"advert-a": True, "advert-b": False}


def test_product_text_is_pinned_to_the_approved_embedding_artifact(
    local_spark,
):
    product_text_value = "NEXT - linen dress!"
    products = local_spark.createDataFrame(
        [
            (
                "sku1",
                product_text_value,
                hashlib.sha256(product_text_value.encode()).hexdigest(),
                MODEL_NAME,
                MODEL_VERSION,
                MODEL_URI,
                SOURCE_RUN_ID,
                ARTIFACT_SHA256,
                384,
            )
        ],
        "item_id string, embedding_text string, embedding_text_hash string, "
        "embedding_model_name string, embedding_model_version string, "
        "embedding_model_uri string, embedding_source_run_id string, "
        "embedding_artifact_sha256 string, embedding_dimension int",
    )
    binding = SimpleNamespace(
        model=SimpleNamespace(
            registered_model_name=MODEL_NAME,
            registered_model_version=int(MODEL_VERSION),
            model_uri=MODEL_URI,
        ),
        source_run_id=SOURCE_RUN_ID,
        artifact_sha256=ARTIFACT_SHA256,
    )

    selected = select_exact_product_text(products, binding).collect()

    assert len(selected) == 1
    assert selected[0].embedding_text == product_text_value

    mismatched = SimpleNamespace(
        model=binding.model,
        source_run_id="0" * 32,
        artifact_sha256=ARTIFACT_SHA256,
    )
    with pytest.raises(ValueError, match="approved model artifact"):
        select_exact_product_text(products, mismatched)


def test_text_source_rejects_conflicting_copy_across_locations(local_spark):
    frames = _text_frames(local_spark, conflicting_core=True)

    with pytest.raises(ValueError, match="conflicting text across locations"):
        build_advert_semantic_text_source(*frames)


def test_text_source_requires_explicit_image_coverage(local_spark):
    frames = _text_frames(local_spark, include_image=False)

    with pytest.raises(ValueError, match="does not cover advert_id"):
        build_advert_semantic_text_source(*frames)


def _semantic_profile_frames(local_spark):
    corpora = ["summer blue dress", "blue summer dress"]
    text_source = local_spark.createDataFrame(
        [
            (
                f"advert-{suffix}",
                REFERENCE_DATE,
                corpus,
                hashlib.sha256(corpus.encode()).hexdigest(),
                suffix == "a",
            )
            for suffix, corpus in zip(("a", "b"), corpora, strict=True)
        ],
        "advert_id string, feature_date date, advert_text_corpus string, "
        "advert_text_hash string, advert_has_destination_image boolean",
    )
    embeddings = local_spark.createDataFrame(
        [
            (
                f"advert-{suffix}",
                REFERENCE_DATE,
                hashlib.sha256(corpus.encode()).hexdigest(),
                MODEL_NAME,
                MODEL_VERSION,
                MODEL_URI,
                SOURCE_RUN_ID,
                ARTIFACT_SHA256,
                _vector(1.0),
                384,
            )
            for suffix, corpus in zip(("a", "b"), corpora, strict=True)
        ],
        "advert_id string, feature_date date, advert_text_hash string, "
        "embedding_model_name string, embedding_model_version string, "
        "embedding_model_uri string, embedding_source_run_id string, "
        "embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int",
    )
    return text_source, embeddings


def test_profile_uses_exact_vectors_and_deterministic_neighbours(local_spark):
    text_source, embeddings = _semantic_profile_frames(local_spark)

    result = build_advert_semantic_profile_frame(text_source, embeddings)
    rows = {row.advert_id: row for row in result.collect()}

    assert tuple(result.columns) == ADVERT_SEMANTIC_PROFILE_COLUMNS
    assert set(rows) == {"advert-a", "advert-b"}
    for row in rows.values():
        assert row.embedding_model_name == MODEL_NAME
        assert row.embedding_model_version == MODEL_VERSION
        assert row.embedding_model_uri == MODEL_URI
        assert row.embedding_source_run_id == SOURCE_RUN_ID
        assert row.embedding_artifact_sha256 == ARTIFACT_SHA256
        assert row.advert_embedding == _vector(1.0)
        assert row.advert_embedding_dimension == 384
        assert row.advert_semantic_token_count == 3
        assert row.advert_semantic_unique_token_count == 3
        assert row.advert_embedding_neighbour_count == 1
        assert row.advert_embedding_top_similarity == pytest.approx(1.0)
        assert row.advert_embedding_avg_similarity == pytest.approx(1.0)
        assert row.created_at == row.updated_at


def test_profile_rejects_vectors_for_stale_text(local_spark):
    from pyspark.sql import functions as F

    text_source, embeddings = _semantic_profile_frames(local_spark)
    stale = embeddings.withColumn(
        "advert_text_hash",
        F.when(F.col("advert_id") == "advert-a", F.lit("0" * 64)).otherwise(
            F.col("advert_text_hash")
        ),
    )

    with pytest.raises(
        ValueError, match="do not cover the exact current text"
    ):
        build_advert_semantic_profile_frame(text_source, stale)


def test_profile_rejects_wrong_dimension_vectors(local_spark):
    from pyspark.sql import functions as F

    text_source, embeddings = _semantic_profile_frames(local_spark)
    invalid = embeddings.withColumn("embedding", F.slice("embedding", 1, 3))

    with pytest.raises(ValueError, match="384-value L2-normalised vector"):
        build_advert_semantic_profile_frame(text_source, invalid)
