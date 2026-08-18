from datetime import date
from math import sqrt
from types import SimpleNamespace

import pytest

from next_ads.features.embedding_contract import (
    EXPECTED_EMBEDDING_DIMENSION,
)
from next_ads.features.product_embedding_features import (
    AdvertProductEmbeddingInput,
    ExistingProductEmbedding,
    ProductEmbeddingKey,
    ProductEmbeddingSource,
    build_advert_product_profiles,
    plan_product_embedding_snapshot,
)
from next_ads.features.product_embedding_transforms import (
    _weighted_l2_embedding,
    build_advert_product_profile_frame,
    build_current_product_text_source,
)


MODEL_NAME = "marketingdata_dev.nextads_models.product_embedding"
MODEL_VERSION = "7"
ARTIFACT_SHA256 = "a" * 64
FEATURE_DATE = date(2026, 8, 1)


def _key(item_id: str, version: str = MODEL_VERSION) -> ProductEmbeddingKey:
    return ProductEmbeddingKey(item_id, MODEL_NAME, version)


def _existing(
    item_id: str,
    text_hash: str,
    *,
    version: str = MODEL_VERSION,
    dimension: int = EXPECTED_EMBEDDING_DIMENSION,
    has_embedding: bool = True,
) -> ExistingProductEmbedding:
    return ExistingProductEmbedding(
        key=_key(item_id, version),
        embedding_text_hash=text_hash,
        embedding_dimension=dimension,
        has_embedding=has_embedding,
    )


def _vector(*values: float) -> tuple[float, ...]:
    return tuple(values) + (0.0,) * (
        EXPECTED_EMBEDDING_DIMENSION - len(values)
    )


def _build_profiles(rows):
    return build_advert_product_profiles(
        rows,
        embedding_model_name=MODEL_NAME,
        embedding_model_version=MODEL_VERSION,
        embedding_artifact_sha256=ARTIFACT_SHA256,
    )


def test_snapshot_plan_reuses_only_exact_current_rows_and_removes_deletes():
    sources = [
        ProductEmbeddingSource("item-a", "hash-a"),
        ProductEmbeddingSource("item-b", "hash-b-new"),
        ProductEmbeddingSource("item-c", "hash-c"),
        ProductEmbeddingSource("item-e", "hash-e"),
    ]
    existing = [
        _existing("item-a", "hash-a"),
        _existing("item-b", "hash-b-old"),
        _existing("item-d", "hash-d"),
        _existing("item-e", "hash-e", dimension=12),
        _existing("item-a", "old-version-hash", version="6"),
    ]

    plan = plan_product_embedding_snapshot(
        reversed(sources),
        reversed(existing),
        embedding_model_name=MODEL_NAME,
        embedding_model_version=MODEL_VERSION,
    )

    assert plan.expected_output_keys == (
        _key("item-a"),
        _key("item-b"),
        _key("item-c"),
        _key("item-e"),
    )
    assert plan.reuse_keys == (_key("item-a"),)
    assert plan.generate_keys == (
        _key("item-b"),
        _key("item-c"),
        _key("item-e"),
    )
    assert plan.replace_keys == (_key("item-b"), _key("item-e"))
    assert plan.obsolete_keys == (_key("item-a", "6"), _key("item-d"))


def test_snapshot_plan_refuses_ambiguous_or_empty_current_sources():
    with pytest.raises(ValueError, match="duplicate item_id item-a"):
        plan_product_embedding_snapshot(
            [
                ProductEmbeddingSource("item-a", "hash-a"),
                ProductEmbeddingSource("item-a", "hash-a"),
            ],
            [],
            embedding_model_name=MODEL_NAME,
            embedding_model_version=MODEL_VERSION,
        )

    with pytest.raises(ValueError, match="source is empty"):
        plan_product_embedding_snapshot(
            [],
            [_existing("item-a", "hash-a")],
            embedding_model_name=MODEL_NAME,
            embedding_model_version=MODEL_VERSION,
        )


def test_snapshot_plan_refuses_duplicate_existing_keys():
    with pytest.raises(ValueError, match="duplicate key"):
        plan_product_embedding_snapshot(
            [ProductEmbeddingSource("item-a", "hash-a")],
            [
                _existing("item-a", "hash-a"),
                _existing("item-a", "hash-a"),
            ],
            embedding_model_name=MODEL_NAME,
            embedding_model_version=MODEL_VERSION,
        )


def test_weighted_profiles_renormalise_available_vectors_and_l2_normalise():
    rows = [
        AdvertProductEmbeddingInput(
            advert_id="advert-a",
            feature_date=FEATURE_DATE,
            item_id="item-a",
            item_weight=0.5,
            embedding=_vector(1.0, 0.0),
        ),
        AdvertProductEmbeddingInput(
            advert_id="advert-a",
            feature_date=FEATURE_DATE,
            item_id="item-b",
            item_weight=0.25,
            embedding=_vector(0.0, 1.0),
        ),
        AdvertProductEmbeddingInput(
            advert_id="advert-a",
            feature_date=FEATURE_DATE,
            item_id="item-c",
            item_weight=0.25,
            embedding=None,
        ),
    ]

    profile = _build_profiles(reversed(rows))[0]

    assert profile.advert_id == "advert-a"
    assert profile.feature_date == FEATURE_DATE
    assert profile.advert_product_item_count == 3
    assert profile.advert_product_embedded_item_count == 2
    assert profile.advert_product_embedding_coverage == pytest.approx(2 / 3)
    assert profile.advert_product_embedding is not None
    assert len(profile.advert_product_embedding) == 384
    assert profile.advert_product_embedding[:2] == pytest.approx(
        (2 / sqrt(5), 1 / sqrt(5))
    )
    assert profile.advert_product_embedding[2:] == (0.0,) * 382
    assert profile.embedding_model_name == MODEL_NAME
    assert profile.embedding_model_version == MODEL_VERSION
    assert profile.embedding_artifact_sha256 == ARTIFACT_SHA256
    assert profile.advert_product_embedding_dimension == 384


def test_weighted_profiles_are_order_independent_and_keep_empty_coverage():
    rows = [
        AdvertProductEmbeddingInput(
            "advert-b",
            "2026-08-01",
            "item-b",
            0.5,
            None,
        ),
        AdvertProductEmbeddingInput(
            "advert-a",
            "2026-08-01",
            "item-a",
            1.0,
            _vector(1.0),
        ),
        AdvertProductEmbeddingInput(
            "advert-b",
            "2026-08-01",
            "item-a",
            0.5,
            None,
        ),
    ]

    forward = _build_profiles(rows)
    backward = _build_profiles(reversed(rows))

    assert forward == backward
    assert tuple(profile.advert_id for profile in forward) == (
        "advert-a",
        "advert-b",
    )
    assert forward[1].advert_product_embedded_item_count == 0
    assert forward[1].advert_product_embedding_coverage == 0.0
    assert forward[1].advert_product_embedding is None


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            [
                AdvertProductEmbeddingInput(
                    "advert-a",
                    FEATURE_DATE,
                    "item-a",
                    0.75,
                    _vector(1.0),
                )
            ],
            "item_weight must sum to 1",
        ),
        (
            [
                AdvertProductEmbeddingInput(
                    "advert-a",
                    FEATURE_DATE,
                    "item-a",
                    0.5,
                    _vector(1.0),
                ),
                AdvertProductEmbeddingInput(
                    "advert-a",
                    FEATURE_DATE,
                    "item-a",
                    0.5,
                    _vector(1.0),
                ),
            ],
            "duplicate key",
        ),
    ],
)
def test_weighted_profiles_reject_invalid_advert_item_contract(rows, message):
    with pytest.raises(ValueError, match=message):
        _build_profiles(rows)


@pytest.mark.parametrize(
    ("model_version", "artifact_sha256", "message"),
    [
        (0, ARTIFACT_SHA256, "positive numeric version"),
        ("latest", ARTIFACT_SHA256, "positive numeric version"),
        (MODEL_VERSION, "A" * 64, "lowercase hexadecimal"),
        (MODEL_VERSION, "a" * 63, "64-character"),
    ],
)
def test_weighted_profiles_require_exact_model_artifact_lineage(
    model_version,
    artifact_sha256,
    message,
):
    with pytest.raises(ValueError, match=message):
        build_advert_product_profiles(
            [],
            embedding_model_name=MODEL_NAME,
            embedding_model_version=model_version,
            embedding_artifact_sha256=artifact_sha256,
        )


def test_weighted_profiles_require_an_exact_model_name():
    with pytest.raises(
        ValueError, match="embedding_model_name cannot be empty"
    ):
        build_advert_product_profiles(
            [],
            embedding_model_name=" ",
            embedding_model_version=MODEL_VERSION,
            embedding_artifact_sha256=ARTIFACT_SHA256,
        )


def test_advert_product_input_rejects_invalid_embedding_shape_and_values():
    with pytest.raises(ValueError, match="exactly 384 values"):
        AdvertProductEmbeddingInput(
            "advert-a",
            FEATURE_DATE,
            "item-a",
            1.0,
            (1.0, 2.0),
        )

    with pytest.raises(ValueError, match="non-finite"):
        AdvertProductEmbeddingInput(
            "advert-a",
            FEATURE_DATE,
            "item-a",
            1.0,
            _vector(float("nan")),
        )


def test_zero_weighted_vector_preserves_existing_aggregation_behaviour():
    profile = _build_profiles(
        [
            AdvertProductEmbeddingInput(
                "advert-a",
                FEATURE_DATE,
                "item-a",
                1.0,
                _vector(),
            )
        ]
    )[0]

    assert profile.advert_product_embedding == (0.0,) * 384


def test_runtime_weighted_profile_rejects_non_unit_and_cancelled_vectors():
    with pytest.raises(ValueError, match="L2-normalised"):
        _weighted_l2_embedding(
            [{"weight": 1.0, "embedding": _vector(2.0)}]
        )

    with pytest.raises(ValueError, match="zero-norm"):
        _weighted_l2_embedding(
            [
                {"weight": 0.5, "embedding": _vector(1.0)},
                {"weight": 0.5, "embedding": _vector(-1.0)},
            ]
        )


def test_product_text_rejects_malformed_non_null_catalog_dates(spark):
    history = spark.createDataFrame(
        [("item-a", "not-a-date", "2026-12-31", "shoe")],
        "pid string, start_date string, end_date string, title string",
    )

    with pytest.raises(ValueError, match="malformed non-null"):
        build_current_product_text_source(
            history,
            reference_date=FEATURE_DATE,
        )


def test_advert_profile_requires_the_approved_model_lineage(spark):
    source_run_id = "b" * 32
    model_uri = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
    approved_binding = SimpleNamespace(
        model=SimpleNamespace(
            registered_model_name=MODEL_NAME,
            registered_model_version=int(MODEL_VERSION),
            model_uri=model_uri,
        ),
        source_run_id=source_run_id,
        artifact_sha256=ARTIFACT_SHA256,
    )
    bridge = spark.createDataFrame(
        [
            (
                "advert-a",
                FEATURE_DATE,
                "item-a",
                1,
                1.0,
                "v2_control",
                FEATURE_DATE,
            )
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    embeddings = spark.createDataFrame(
        [
            (
                "item-a",
                MODEL_NAME,
                MODEL_VERSION,
                model_uri,
                source_run_id,
                "c" * 64,
                _vector(1.0),
                EXPECTED_EMBEDDING_DIMENSION,
            )
        ],
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_model_uri string, "
        "embedding_source_run_id string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int",
    )

    with pytest.raises(ValueError, match="approved materialization binding"):
        build_advert_product_profile_frame(
            bridge,
            embeddings,
            approved_binding=approved_binding,
        )


def test_advert_profile_rejects_a_non_unit_product_vector(spark):
    bridge = spark.createDataFrame(
        [
            (
                "advert-a",
                FEATURE_DATE,
                "item-a",
                1,
                1.0,
                "v2_control",
                FEATURE_DATE,
            )
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    embeddings = spark.createDataFrame(
        [
            (
                "item-a",
                MODEL_NAME,
                MODEL_VERSION,
                ARTIFACT_SHA256,
                _vector(2.0),
                EXPECTED_EMBEDDING_DIMENSION,
            )
        ],
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int",
    )

    with pytest.raises(ValueError, match="L2 norm 1"):
        build_advert_product_profile_frame(bridge, embeddings)
