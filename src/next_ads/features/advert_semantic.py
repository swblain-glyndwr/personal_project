"""Deterministic advert text and semantic-profile transforms."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
import re

from next_ads.features.advert_items import CANONICAL_ADVERT_ITEM_COLUMNS
from next_ads.features.embedding_contract import EXPECTED_EMBEDDING_DIMENSION


ADVERT_CORE_TEXT_COLUMNS = (
    "advert_id",
    "feature_date",
    "location",
    "advert_title",
    "headline",
    "subtext",
    "cta",
    "source_rundate",
)
ADVERT_ATTRIBUTE_TEXT_COLUMNS = (
    "advert_id",
    "feature_date",
    "top_brand",
    "top_use",
    "top_colour",
    "top_style",
    "top_category",
    "top_department",
    "top_gender",
)
ADVERT_IMAGE_FLAG_COLUMNS = (
    "advert_id",
    "feature_date",
    "advert_has_destination_image",
)
CONTROL_SHEET_IMAGE_COLUMNS = (
    "UniqueAdID",
    "StartDate",
    "EndDate",
    "BackgroundImage",
    "MobileImage",
    "FlatJPG",
)
PRODUCT_TEXT_COLUMNS = (
    "item_id",
    "embedding_text",
    "embedding_text_hash",
)
PRODUCT_EMBEDDING_SOURCE_COLUMNS = (
    *PRODUCT_TEXT_COLUMNS,
    "embedding_model_name",
    "embedding_model_version",
    "embedding_model_uri",
    "embedding_source_run_id",
    "embedding_artifact_sha256",
    "embedding_dimension",
)
ADVERT_SEMANTIC_TEXT_COLUMNS = (
    "advert_id",
    "feature_date",
    "advert_text_corpus",
    "advert_text_hash",
    "advert_has_destination_image",
)
ADVERT_SEMANTIC_VECTOR_COLUMNS = (
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
ADVERT_SEMANTIC_PROFILE_COLUMNS = (
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

NEIGHBOUR_DISTANCE_THRESHOLD = 1.25
NEIGHBOUR_COSINE_THRESHOLD = 1.0 - (NEIGHBOUR_DISTANCE_THRESHOLD**2 / 2.0)
MAX_NEIGHBOURS_PER_ADVERT = 20

_MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]*")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_NON_TEXT_PATTERN = re.compile(r"[^A-Za-z0-9]+")
_URL_PATTERN = re.compile(r"https?://\S+", flags=re.IGNORECASE)


@dataclass(frozen=True)
class AdvertEmbeddingLineage:
    """One exact registered embedding model represented by a vector frame."""

    embedding_model_name: str
    embedding_model_version: str | int
    embedding_model_uri: str
    embedding_source_run_id: str
    embedding_artifact_sha256: str
    embedding_dimension: int

    def __post_init__(self) -> None:
        """Normalise and validate the immutable model coordinates."""
        if not isinstance(self.embedding_model_name, str):
            raise ValueError("embedding_model_name must be text")
        model_name = self.embedding_model_name.strip()
        if not model_name:
            raise ValueError("embedding_model_name cannot be empty")

        raw_version = self.embedding_model_version
        if isinstance(raw_version, bool):
            version = ""
        elif isinstance(raw_version, int):
            version = str(raw_version)
        elif isinstance(raw_version, str):
            version = raw_version.strip()
        else:
            version = ""
        if _MODEL_VERSION_PATTERN.fullmatch(version) is None:
            raise ValueError(
                "embedding_model_version must be a positive numeric version"
            )

        model_uri = self.embedding_model_uri
        if not isinstance(model_uri, str):
            raise ValueError("embedding_model_uri must be text")
        model_uri = model_uri.strip()
        if model_uri != f"models:/{model_name}/{version}":
            raise ValueError(
                "embedding_model_uri must identify the exact registered "
                "model name and numeric version"
            )
        source_run_id = self.embedding_source_run_id
        if not isinstance(source_run_id, str) or not source_run_id.strip():
            raise ValueError("embedding_source_run_id cannot be empty")
        source_run_id = source_run_id.strip()
        artifact_sha256 = self.embedding_artifact_sha256
        if (
            not isinstance(artifact_sha256, str)
            or _SHA256_PATTERN.fullmatch(artifact_sha256.strip()) is None
        ):
            raise ValueError(
                "embedding_artifact_sha256 must be a 64-character lowercase "
                "hexadecimal SHA-256 digest"
            )
        artifact_sha256 = artifact_sha256.strip()

        raw_dimension = self.embedding_dimension
        if isinstance(raw_dimension, bool):
            dimension = -1
        else:
            try:
                numeric_dimension = float(raw_dimension)
                dimension = int(raw_dimension)
            except (TypeError, ValueError, OverflowError):
                numeric_dimension = float("nan")
                dimension = -1
        if (
            not isfinite(numeric_dimension)
            or numeric_dimension != dimension
            or dimension != EXPECTED_EMBEDDING_DIMENSION
        ):
            raise ValueError(
                "embedding_dimension must be exactly "
                f"{EXPECTED_EMBEDDING_DIMENSION}"
            )

        object.__setattr__(self, "embedding_model_name", model_name)
        object.__setattr__(self, "embedding_model_version", version)
        object.__setattr__(self, "embedding_model_uri", model_uri)
        object.__setattr__(self, "embedding_source_run_id", source_run_id)
        object.__setattr__(
            self,
            "embedding_artifact_sha256",
            artifact_sha256,
        )
        object.__setattr__(self, "embedding_dimension", dimension)


def normalise_advert_text(value: object) -> str:
    """Apply the repository experiment's URL and punctuation normalisation."""
    if value is None:
        return ""
    without_urls = _URL_PATTERN.sub(" ", str(value))
    return " ".join(_NON_TEXT_PATTERN.sub(" ", without_urls).lower().split())


def build_advert_image_flags(control_sheet, reference_date):
    """Derive one explicit destination-image flag per active advert."""
    from pyspark.sql import functions as F

    _require_columns(
        control_sheet,
        "control_sheet",
        CONTROL_SHEET_IMAGE_COLUMNS,
    )
    feature_date = F.lit(reference_date).cast("date")
    active = (
        control_sheet.where(F.col("UniqueAdID").isNotNull())
        .where(
            F.col("StartDate").isNull()
            | (F.to_date("StartDate") <= feature_date)
        )
        .where(
            F.col("EndDate").isNull()
            | (F.to_date("EndDate") >= feature_date)
        )
    )

    def has_value(column_name: str):
        value = F.lower(F.trim(F.col(column_name).cast("string")))
        return (
            value.isNotNull()
            & (value != "")
            & (~value.isin("nan", "null", "none", "n/a"))
        )

    any_image = (
        has_value("FlatJPG")
        | has_value("MobileImage")
        | has_value("BackgroundImage")
    )
    return (
        active.select(
            F.trim(F.col("UniqueAdID").cast("string")).alias("advert_id"),
            any_image.cast("int").alias("_has_image"),
        )
        .where(F.col("advert_id") != "")
        .groupBy("advert_id")
        .agg(F.max("_has_image").alias("_has_image"))
        .select(
            "advert_id",
            feature_date.alias("feature_date"),
            (F.col("_has_image") == F.lit(1)).alias(
                "advert_has_destination_image"
            ),
        )
        .select(*ADVERT_IMAGE_FLAG_COLUMNS)
    )


def _require_columns(
    frame, source_name: str, required: tuple[str, ...]
) -> None:
    missing = sorted(set(required).difference(frame.columns))
    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {', '.join(missing)}"
        )


def _clean_text(F, column):
    return F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.coalesce(column.cast("string"), F.lit("")),
                    r"(?i)https?://\S+",
                    " ",
                ),
                r"[^A-Za-z0-9]+",
                " ",
            )
        )
    )


def _normalise_item(F, column):
    return F.regexp_replace(
        F.lower(F.trim(F.coalesce(column.cast("string"), F.lit("")))),
        r"[^a-z0-9]",
        "",
    )


def _raise_for_duplicate_keys(frame, source_name: str, keys: tuple[str, ...]):
    from pyspark.sql import functions as F

    duplicate = (
        frame.groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
        .orderBy(*keys)
        .limit(1)
        .collect()
    )
    if duplicate:
        values = ", ".join(f"{key}={duplicate[0][key]}" for key in keys)
        raise ValueError(f"{source_name} contains duplicate key: {values}")


def _prepare_core_text(advert_core):
    from pyspark.sql import functions as F

    _require_columns(advert_core, "advert_core", ADVERT_CORE_TEXT_COLUMNS)
    selected = advert_core.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        F.trim(F.col("location").cast("string")).alias("location"),
        *(
            _clean_text(F, F.col(column_name)).alias(column_name)
            for column_name in ("advert_title", "headline", "subtext", "cta")
        ),
        F.to_date("source_rundate").alias("source_rundate"),
    )
    invalid = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("location").isNull()
        | (F.col("location") == "")
        | F.col("source_rundate").isNull()
        | (F.col("source_rundate") > F.col("feature_date"))
    )
    if selected.where(invalid).limit(1).collect():
        raise ValueError(
            "advert_core contains a blank key, missing source date, or "
            "source_rundate after feature_date"
        )
    _raise_for_duplicate_keys(
        selected,
        "advert_core",
        ("advert_id", "location", "feature_date"),
    )

    signature_columns = ("advert_title", "headline", "subtext", "cta")
    signatures = selected.withColumn(
        "_text_signature",
        F.concat_ws("||", *(F.col(name) for name in signature_columns)),
    )
    conflict = (
        signatures.groupBy("advert_id", "feature_date")
        .agg(F.countDistinct("_text_signature").alias("_signature_count"))
        .where(F.col("_signature_count") > 1)
        .orderBy("advert_id", "feature_date")
        .limit(1)
        .collect()
    )
    if conflict:
        row = conflict[0]
        raise ValueError(
            "advert_core has conflicting text across locations for "
            f"advert_id={row['advert_id']} and "
            f"feature_date={row['feature_date']}"
        )
    return signatures.groupBy("advert_id", "feature_date").agg(
        *(F.first(name).alias(name) for name in signature_columns)
    )


def _prepare_attributes(advert_attributes):
    from pyspark.sql import functions as F

    _require_columns(
        advert_attributes,
        "advert_attributes",
        ADVERT_ATTRIBUTE_TEXT_COLUMNS,
    )
    prepared = advert_attributes.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        *(
            _clean_text(F, F.col(column_name)).alias(column_name)
            for column_name in ADVERT_ATTRIBUTE_TEXT_COLUMNS[2:]
        ),
    )
    invalid = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
    )
    if prepared.where(invalid).limit(1).collect():
        raise ValueError("advert_attributes contains a blank or null key")
    _raise_for_duplicate_keys(
        prepared,
        "advert_attributes",
        ("advert_id", "feature_date"),
    )
    return prepared


def _prepare_image_flags(advert_image_flags):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    _require_columns(
        advert_image_flags,
        "advert_image_flags",
        ADVERT_IMAGE_FLAG_COLUMNS,
    )
    if not isinstance(
        advert_image_flags.schema["advert_has_destination_image"].dataType,
        T.BooleanType,
    ):
        raise ValueError(
            "advert_image_flags advert_has_destination_image must be BOOLEAN"
        )
    prepared = advert_image_flags.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        F.col("advert_has_destination_image"),
    )
    invalid = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("advert_has_destination_image").isNull()
    )
    if prepared.where(invalid).limit(1).collect():
        raise ValueError("advert_image_flags contains a null key or flag")
    _raise_for_duplicate_keys(
        prepared,
        "advert_image_flags",
        ("advert_id", "feature_date"),
    )
    return prepared


def _prepare_item_text(advert_item_bridge, product_text):
    from pyspark.sql import functions as F

    _require_columns(
        advert_item_bridge,
        "advert_item_bridge",
        CANONICAL_ADVERT_ITEM_COLUMNS,
    )
    _require_columns(product_text, "product_text", PRODUCT_TEXT_COLUMNS)
    bridge = advert_item_bridge.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.col("item_rank").cast("int").alias("item_rank"),
        F.to_date("source_rundate").alias("source_rundate"),
    )
    invalid_bridge = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("item_id").isNull()
        | (F.col("item_id") == "")
        | F.col("item_rank").isNull()
        | (F.col("item_rank") <= 0)
        | F.col("source_rundate").isNull()
        | (F.col("source_rundate") > F.col("feature_date"))
    )
    if bridge.where(invalid_bridge).limit(1).collect():
        raise ValueError(
            "advert_item_bridge contains a blank key, invalid rank, or "
            "future source_rundate"
        )
    _raise_for_duplicate_keys(
        bridge,
        "advert_item_bridge",
        ("advert_id", "feature_date", "item_id"),
    )

    products_raw = product_text.select(
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.col("embedding_text").cast("string").alias("embedding_text"),
        F.trim(F.col("embedding_text_hash").cast("string")).alias(
            "embedding_text_hash"
        ),
    )
    invalid_product = (
        F.col("item_id").isNull()
        | (F.col("item_id") == "")
        | F.col("embedding_text").isNull()
        | (F.trim(F.col("embedding_text")) == "")
        | ~F.col("embedding_text_hash").rlike("^[0-9a-f]{64}$")
        | (
            F.sha2(F.col("embedding_text"), 256)
            != F.col("embedding_text_hash")
        )
    )
    if products_raw.where(invalid_product).limit(1).collect():
        raise ValueError(
            "product_text contains a blank key or text, or a hash that does "
            "not match the repository-owned embedding text"
        )
    products = products_raw.select(
        "item_id",
        _clean_text(F, F.col("embedding_text")).alias("embedding_text"),
    )
    _raise_for_duplicate_keys(products, "product_text", ("item_id",))

    joined = bridge.join(products, on="item_id", how="left")
    return joined.groupBy("advert_id", "feature_date").agg(
        F.concat_ws(
            " ",
            F.transform(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "item_rank",
                            "item_id",
                            "embedding_text",
                        )
                    )
                ),
                lambda item: F.coalesce(
                    item["embedding_text"],
                    F.lit(""),
                ),
            ),
        ).alias("advert_item_text_corpus")
    )


def build_advert_semantic_text_source(
    advert_core,
    advert_attributes,
    advert_item_bridge,
    product_text,
    advert_image_flags,
):
    """Build one repository-owned text corpus per advert and feature date."""
    from pyspark.sql import functions as F

    core = _prepare_core_text(advert_core)
    attributes = _prepare_attributes(advert_attributes)
    item_text = _prepare_item_text(advert_item_bridge, product_text)
    image_flags = _prepare_image_flags(advert_image_flags)

    rows = (
        core.join(attributes, ["advert_id", "feature_date"], "left")
        .join(item_text, ["advert_id", "feature_date"], "left")
        .join(image_flags, ["advert_id", "feature_date"], "left")
    )
    missing_image = (
        rows.where(F.col("advert_has_destination_image").isNull())
        .limit(1)
        .collect()
    )
    if missing_image:
        row = missing_image[0]
        raise ValueError(
            "advert_image_flags does not cover advert_id="
            f"{row['advert_id']} and feature_date={row['feature_date']}"
        )

    page_title = F.coalesce(
        F.nullif(F.col("headline"), F.lit("")),
        F.nullif(F.col("advert_title"), F.lit("")),
        F.nullif(F.col("top_use"), F.lit("")),
        F.nullif(F.col("top_brand"), F.lit("")),
        F.lit(""),
    )
    image_caption = F.when(
        F.col("advert_has_destination_image"),
        F.concat_ws(
            " ",
            F.lit("advert image"),
            F.col("top_colour"),
            F.col("top_brand"),
            F.col("top_use"),
            F.col("top_style"),
            F.col("top_category"),
            F.col("top_department"),
            F.col("top_gender"),
        ),
    ).otherwise(F.lit(""))
    corpus = _clean_text(
        F,
        F.concat_ws(
            " ",
            page_title,
            F.col("headline"),
            F.col("subtext"),
            F.col("cta"),
            F.col("top_brand"),
            F.col("top_use"),
            F.col("top_colour"),
            F.col("top_style"),
            F.col("top_category"),
            F.col("top_department"),
            F.col("top_gender"),
            F.col("advert_item_text_corpus"),
            image_caption,
        ),
    )
    output = rows.withColumn("advert_text_corpus", corpus).where(
        F.col("advert_text_corpus") != ""
    )
    source_count = core.count()
    output_count = output.count()
    if output_count != source_count:
        raise ValueError(
            "advert semantic text is empty or missing for one or more active "
            f"adverts: source={source_count}, output={output_count}"
        )
    return output.select(
        "advert_id",
        "feature_date",
        "advert_text_corpus",
        F.sha2("advert_text_corpus", 256).alias("advert_text_hash"),
        "advert_has_destination_image",
    ).select(*ADVERT_SEMANTIC_TEXT_COLUMNS)


def select_exact_product_text(product_embeddings, binding):
    """Select product text only when it uses the approved model artifact."""
    from pyspark.sql import functions as F

    _require_columns(
        product_embeddings,
        "product_embeddings",
        PRODUCT_EMBEDDING_SOURCE_COLUMNS,
    )
    rows = (
        product_embeddings.select(
            F.trim("embedding_model_name").alias("embedding_model_name"),
            F.trim("embedding_model_version").alias(
                "embedding_model_version"
            ),
            F.trim("embedding_model_uri").alias("embedding_model_uri"),
            F.trim("embedding_source_run_id").alias(
                "embedding_source_run_id"
            ),
            F.trim("embedding_artifact_sha256").alias(
                "embedding_artifact_sha256"
            ),
            "embedding_dimension",
        )
        .distinct()
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            "product_embeddings must contain exactly one model artifact"
        )
    lineage = AdvertEmbeddingLineage(**rows[0].asDict())
    expected = {
        "embedding_model_name": binding.model.registered_model_name,
        "embedding_model_version": str(
            binding.model.registered_model_version
        ),
        "embedding_model_uri": binding.model.model_uri,
        "embedding_source_run_id": binding.source_run_id,
        "embedding_artifact_sha256": binding.artifact_sha256,
    }
    actual = {
        "embedding_model_name": lineage.embedding_model_name,
        "embedding_model_version": lineage.embedding_model_version,
        "embedding_model_uri": lineage.embedding_model_uri,
        "embedding_source_run_id": lineage.embedding_source_run_id,
        "embedding_artifact_sha256": lineage.embedding_artifact_sha256,
    }
    mismatches = [
        f"{name}: expected {expected[name]!r}, found {actual[name]!r}"
        for name in expected
        if actual[name] != expected[name]
    ]
    if mismatches:
        raise ValueError(
            "product_embeddings does not use the approved model artifact: "
            + "; ".join(mismatches)
        )
    return product_embeddings.select(*PRODUCT_TEXT_COLUMNS)


def _semantic_cache_id(F):
    return F.sha2(
        F.concat_ws(
            "||",
            F.trim(F.col("advert_id").cast("string")),
            F.date_format(F.to_date("feature_date"), "yyyy-MM-dd"),
        ),
        256,
    )


def build_advert_semantic_vector_frame(
    text_source,
    existing_profiles,
    *,
    binding,
    model_path,
):
    """Reuse exact advert vectors or encode changed repository-owned text."""
    from pyspark.sql import functions as F

    from next_ads.features.product_embedding_inference import (
        build_product_embeddings_frame,
    )

    _require_columns(
        text_source,
        "advert_semantic_text_source",
        ADVERT_SEMANTIC_TEXT_COLUMNS,
    )
    existing_columns = (
        "advert_id",
        "feature_date",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "advert_text_hash",
        "advert_embedding",
        "advert_embedding_dimension",
        "created_at",
    )
    _require_columns(
        existing_profiles,
        "existing_advert_semantic_profiles",
        existing_columns,
    )
    source = text_source.select(
        _semantic_cache_id(F).alias("item_id"),
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        F.col("advert_text_corpus").cast("string").alias("embedding_text"),
        F.col("advert_text_hash").cast("string").alias(
            "embedding_text_hash"
        ),
    )
    invalid = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("embedding_text").isNull()
        | (F.trim("embedding_text") == "")
        | ~F.col("embedding_text_hash").rlike("^[0-9a-f]{64}$")
        | (
            F.sha2(F.col("embedding_text"), 256)
            != F.col("embedding_text_hash")
        )
    )
    if source.where(invalid).limit(1).collect():
        raise ValueError(
            "advert semantic text contains an invalid key, text, or hash"
        )
    _raise_for_duplicate_keys(
        source,
        "advert_semantic_text_source",
        ("advert_id", "feature_date"),
    )

    existing = existing_profiles.select(
        _semantic_cache_id(F).alias("item_id"),
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        F.col("advert_embedding").alias("embedding"),
        F.col("advert_embedding_dimension").alias("embedding_dimension"),
        F.col("advert_text_hash").alias("embedding_text_hash"),
        "created_at",
    )
    encoded, evidence = build_product_embeddings_frame(
        source.select("item_id", "embedding_text", "embedding_text_hash"),
        existing,
        binding=binding,
        model_path=model_path,
    )
    source_keys = source.select(
        "item_id",
        "advert_id",
        "feature_date",
        F.col("embedding_text_hash").alias("advert_text_hash"),
    ).alias("source")
    encoded_vectors = encoded.select(
        "item_id",
        "embedding_text_hash",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "embedding",
        "embedding_dimension",
    ).alias("encoded")
    vectors = source_keys.join(
        encoded_vectors,
        (
            F.col("source.item_id")
            == F.col("encoded.item_id")
        )
        & (
            F.col("source.advert_text_hash")
            == F.col("encoded.embedding_text_hash")
        ),
        "inner",
    ).select(
        F.col("source.advert_id").alias("advert_id"),
        F.col("source.feature_date").alias("feature_date"),
        F.col("source.advert_text_hash").alias("advert_text_hash"),
        *(
            F.col(f"encoded.{column_name}").alias(column_name)
            for column_name in (
                "embedding_model_name",
                "embedding_model_version",
                "embedding_model_uri",
                "embedding_source_run_id",
                "embedding_artifact_sha256",
                "embedding",
                "embedding_dimension",
            )
        ),
    )
    return (
        vectors.select(*ADVERT_SEMANTIC_VECTOR_COLUMNS),
        evidence,
    )


def _read_lineage(embeddings) -> AdvertEmbeddingLineage:
    from pyspark.sql import functions as F

    rows = (
        embeddings.select(
            F.trim(F.col("embedding_model_name").cast("string")).alias(
                "embedding_model_name"
            ),
            F.trim(F.col("embedding_model_version").cast("string")).alias(
                "embedding_model_version"
            ),
            F.trim(F.col("embedding_model_uri").cast("string")).alias(
                "embedding_model_uri"
            ),
            F.trim(F.col("embedding_source_run_id").cast("string")).alias(
                "embedding_source_run_id"
            ),
            F.trim(
                F.col("embedding_artifact_sha256").cast("string")
            ).alias("embedding_artifact_sha256"),
            F.col("embedding_dimension").alias("embedding_dimension"),
        )
        .distinct()
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            "advert_embeddings must contain exactly one model version"
        )
    return AdvertEmbeddingLineage(**rows[0].asDict())


def _raise_for_invalid_semantic_inputs(text_source, embeddings) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    _require_columns(
        text_source,
        "advert_semantic_text_source",
        ADVERT_SEMANTIC_TEXT_COLUMNS,
    )
    _require_columns(
        embeddings,
        "advert_embeddings",
        ADVERT_SEMANTIC_VECTOR_COLUMNS,
    )
    _raise_for_duplicate_keys(
        text_source,
        "advert_semantic_text_source",
        ("advert_id", "feature_date"),
    )
    _raise_for_duplicate_keys(
        embeddings,
        "advert_embeddings",
        (
            "advert_id",
            "feature_date",
            "embedding_model_name",
            "embedding_model_version",
        ),
    )
    embedding_type = embeddings.schema["embedding"].dataType
    if not isinstance(embedding_type, T.ArrayType) or not isinstance(
        embedding_type.elementType,
        T.DoubleType,
    ):
        raise ValueError("advert_embeddings embedding must be ARRAY<DOUBLE>")
    if not isinstance(
        text_source.schema["advert_has_destination_image"].dataType,
        T.BooleanType,
    ):
        raise ValueError(
            "advert semantic text advert_has_destination_image must be BOOLEAN"
        )

    text_invalid = (
        F.col("advert_id").isNull()
        | (F.trim("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("advert_text_corpus").isNull()
        | (F.trim("advert_text_corpus") == "")
        | ~F.col("advert_text_hash").rlike("^[0-9a-f]{64}$")
        | (
            F.sha2(F.col("advert_text_corpus"), 256)
            != F.col("advert_text_hash")
        )
        | F.col("advert_has_destination_image").isNull()
    )
    if text_source.where(text_invalid).limit(1).collect():
        raise ValueError(
            "advert semantic text source contains an invalid key, text, "
            "text hash, or image flag"
        )

    vector = F.col("embedding")
    non_finite = F.exists(
        vector,
        lambda value: value.isNull()
        | F.isnan(value)
        | (F.abs(value) == F.lit(float("inf"))),
    )
    squared_norm = F.aggregate(
        vector,
        F.lit(0.0).cast("double"),
        lambda total, value: total + (value * value),
    )
    embedding_invalid = (
        F.col("advert_id").isNull()
        | (F.trim("advert_id") == "")
        | F.col("feature_date").isNull()
        | ~F.col("advert_text_hash").rlike("^[0-9a-f]{64}$")
        | vector.isNull()
        | (F.size(vector) != F.lit(EXPECTED_EMBEDDING_DIMENSION))
        | non_finite
        | (F.abs(F.sqrt(squared_norm) - F.lit(1.0)) > F.lit(1e-5))
    )
    if embeddings.where(embedding_invalid).limit(1).collect():
        raise ValueError(
            "advert_embeddings contains an invalid key, text hash, or "
            f"{EXPECTED_EMBEDDING_DIMENSION}-value L2-normalised vector"
        )


def build_advert_semantic_profile_frame(text_source, advert_embeddings):
    """Join exact vectors to advert text and calculate neighbour evidence."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _raise_for_invalid_semantic_inputs(text_source, advert_embeddings)
    lineage = _read_lineage(advert_embeddings)
    embeddings = advert_embeddings.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        F.col("advert_text_hash").cast("string"),
        F.col("embedding").alias("advert_embedding"),
    )
    joined = text_source.join(
        embeddings,
        on=["advert_id", "feature_date", "advert_text_hash"],
        how="inner",
    )
    source_count = text_source.count()
    output_count = joined.count()
    if output_count != source_count:
        raise ValueError(
            "advert_embeddings do not cover the exact current advert text: "
            f"source={source_count}, matched={output_count}"
        )

    source = joined.alias("source")
    neighbour = joined.alias("neighbour")
    similarity = F.aggregate(
        F.zip_with(
            F.col("source.advert_embedding"),
            F.col("neighbour.advert_embedding"),
            lambda left, right: left * right,
        ),
        F.lit(0.0).cast("double"),
        lambda total, value: total + value,
    )
    neighbours = (
        source.join(
            neighbour,
            (F.col("source.feature_date") == F.col("neighbour.feature_date"))
            & (F.col("source.advert_id") != F.col("neighbour.advert_id")),
            "inner",
        )
        .select(
            F.col("source.feature_date").alias("feature_date"),
            F.col("source.advert_id").alias("advert_id"),
            F.col("neighbour.advert_id").alias("neighbour_advert_id"),
            F.greatest(
                F.lit(-1.0),
                F.least(F.lit(1.0), similarity),
            ).alias("cosine_similarity"),
        )
        .where(F.col("cosine_similarity") >= F.lit(NEIGHBOUR_COSINE_THRESHOLD))
        .withColumn(
            "_neighbour_rank",
            F.row_number().over(
                Window.partitionBy("feature_date", "advert_id").orderBy(
                    F.col("cosine_similarity").desc(),
                    F.col("neighbour_advert_id").asc(),
                )
            ),
        )
        .where(F.col("_neighbour_rank") <= F.lit(MAX_NEIGHBOURS_PER_ADVERT))
        .groupBy("feature_date", "advert_id")
        .agg(
            F.count(F.lit(1))
            .cast("long")
            .alias("advert_embedding_neighbour_count"),
            F.max("cosine_similarity")
            .cast("double")
            .alias("advert_embedding_top_similarity"),
            F.avg("cosine_similarity")
            .cast("double")
            .alias("advert_embedding_avg_similarity"),
        )
    )
    build_time = F.current_timestamp()
    tokens = F.split(F.col("advert_text_corpus"), r"\s+")
    return (
        joined.join(neighbours, ["feature_date", "advert_id"], "left")
        .withColumn(
            "embedding_model_name", F.lit(lineage.embedding_model_name)
        )
        .withColumn(
            "embedding_model_version",
            F.lit(lineage.embedding_model_version),
        )
        .withColumn(
            "embedding_model_uri",
            F.lit(lineage.embedding_model_uri),
        )
        .withColumn(
            "embedding_source_run_id",
            F.lit(lineage.embedding_source_run_id),
        )
        .withColumn(
            "embedding_artifact_sha256",
            F.lit(lineage.embedding_artifact_sha256),
        )
        .withColumn(
            "advert_embedding_dimension",
            F.lit(lineage.embedding_dimension).cast("int"),
        )
        .withColumn(
            "advert_semantic_token_count",
            F.size(tokens).cast("long"),
        )
        .withColumn(
            "advert_semantic_unique_token_count",
            F.size(F.array_distinct(tokens)).cast("long"),
        )
        .fillna(
            {
                "advert_embedding_neighbour_count": 0,
                "advert_embedding_top_similarity": 0.0,
                "advert_embedding_avg_similarity": 0.0,
            }
        )
        .withColumn("created_at", build_time)
        .withColumn("updated_at", build_time)
        .select(*ADVERT_SEMANTIC_PROFILE_COLUMNS)
    )


__all__ = [
    "ADVERT_ATTRIBUTE_TEXT_COLUMNS",
    "ADVERT_CORE_TEXT_COLUMNS",
    "ADVERT_IMAGE_FLAG_COLUMNS",
    "ADVERT_SEMANTIC_PROFILE_COLUMNS",
    "ADVERT_SEMANTIC_TEXT_COLUMNS",
    "ADVERT_SEMANTIC_VECTOR_COLUMNS",
    "CONTROL_SHEET_IMAGE_COLUMNS",
    "MAX_NEIGHBOURS_PER_ADVERT",
    "NEIGHBOUR_COSINE_THRESHOLD",
    "NEIGHBOUR_DISTANCE_THRESHOLD",
    "PRODUCT_TEXT_COLUMNS",
    "PRODUCT_EMBEDDING_SOURCE_COLUMNS",
    "AdvertEmbeddingLineage",
    "build_advert_semantic_profile_frame",
    "build_advert_semantic_text_source",
    "build_advert_semantic_vector_frame",
    "build_advert_image_flags",
    "normalise_advert_text",
    "select_exact_product_text",
]
