"""Spark transforms for product text and advert product profiles."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from math import isclose, isfinite, sqrt
from typing import Any

from next_ads.features.advert_items import CANONICAL_ADVERT_ITEM_COLUMNS
from next_ads.features.embedding_contract import (
    EXPECTED_EMBEDDING_DIMENSION,
)


PRODUCT_ID_PRECEDENCE = (
    "pid",
    "itemno",
    "item_number",
    "itemNumber",
    "productSku",
)
PRODUCT_DESCRIPTOR_PRECEDENCE = (
    ("brand", ("brand", "Brand")),
    ("title", ("title", "product_title", "item_title", "name")),
    ("gender", ("gender", "next_gender")),
    ("crumbs", ("crumbs", "breadcrumbs", "breadcrumb")),
    (
        "primary_colour",
        ("primary_colour", "next_colour", "colour", "color"),
    ),
    ("material", ("material",)),
    ("pattern", ("pattern",)),
    ("neckline", ("neckline",)),
    ("sleeve", ("sleeve",)),
    ("occasion", ("occasion",)),
    ("use", ("use",)),
    ("description", ("description", "product_description")),
)
PRODUCT_TEXT_OUTPUT_COLUMNS = (
    "item_id",
    "embedding_text",
    "embedding_text_hash",
)
ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS = (
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

_MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]*")
_ARTIFACT_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_WEIGHT_SUM_TOLERANCE = 1e-9
_VECTOR_NORM_TOLERANCE = 1e-5
_ZERO_VECTOR_TOLERANCE = 1e-12
_MISSING_TEXT_VALUES = ("nan", "na", "n/a", "null", "none")


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    resolved = value.strip()
    if not resolved:
        raise ValueError(f"{field_name} cannot be empty")
    return resolved


def _as_date(value: date | str, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be an ISO date (YYYY-MM-DD)"
            ) from exc
    raise ValueError(f"{field_name} must be a date or ISO date string")


def _positive_model_version(value: object) -> str:
    if isinstance(value, bool):
        resolved = ""
    elif isinstance(value, int):
        resolved = str(value)
    elif isinstance(value, str):
        resolved = value.strip()
    else:
        resolved = ""
    if _MODEL_VERSION_PATTERN.fullmatch(resolved) is None:
        raise ValueError(
            "embedding_model_version must be a positive numeric version"
        )
    return resolved


def _exact_dimension(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError(
            "embedding_dimension must be exactly "
            f"{EXPECTED_EMBEDDING_DIMENSION}"
        )
    try:
        dimension = int(value)
        numeric_value = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "embedding_dimension must be exactly "
            f"{EXPECTED_EMBEDDING_DIMENSION}"
        ) from exc
    if (
        not isfinite(numeric_value)
        or numeric_value != dimension
        or dimension != EXPECTED_EMBEDDING_DIMENSION
    ):
        raise ValueError(
            "embedding_dimension must be exactly "
            f"{EXPECTED_EMBEDDING_DIMENSION}"
        )
    return dimension


@dataclass(frozen=True)
class ProductCatalogColumnBinding:
    """Resolved product identifier and optional descriptor columns."""

    item_id: str
    descriptors: tuple[tuple[str, str | None], ...]
    start_date: str | None
    end_date: str | None

    def descriptor(self, logical_name: str) -> str | None:
        """Return the physical source for one logical descriptor."""
        values = dict(self.descriptors)
        if logical_name not in values:
            raise KeyError(logical_name)
        return values[logical_name]


@dataclass(frozen=True)
class ProductEmbeddingLineage:
    """One exact model artifact represented by an embedding frame."""

    embedding_model_name: str
    embedding_model_version: str | int
    embedding_artifact_sha256: str
    embedding_dimension: int

    def __post_init__(self) -> None:
        """Normalise and validate exact model artifact identity."""
        model_name = _required_text(
            self.embedding_model_name,
            "embedding_model_name",
        )
        model_version = _positive_model_version(self.embedding_model_version)
        artifact_sha256 = _required_text(
            self.embedding_artifact_sha256,
            "embedding_artifact_sha256",
        )
        if _ARTIFACT_SHA256_PATTERN.fullmatch(artifact_sha256) is None:
            raise ValueError(
                "embedding_artifact_sha256 must be a 64-character lowercase "
                "hexadecimal SHA-256 digest"
            )
        dimension = _exact_dimension(self.embedding_dimension)
        object.__setattr__(self, "embedding_model_name", model_name)
        object.__setattr__(
            self,
            "embedding_model_version",
            model_version,
        )
        object.__setattr__(
            self,
            "embedding_artifact_sha256",
            artifact_sha256,
        )
        object.__setattr__(self, "embedding_dimension", dimension)


def resolve_product_catalog_columns(
    columns: list[str] | tuple[str, ...],
) -> ProductCatalogColumnBinding:
    """Resolve catalogue columns using the established experiment order."""
    available = set(columns)
    item_id = next(
        (name for name in PRODUCT_ID_PRECEDENCE if name in available),
        None,
    )
    if item_id is None:
        raise ValueError(
            "Could not find a product identifier column. Available columns: "
            f"{list(columns)}"
        )

    descriptors = tuple(
        (
            logical_name,
            next(
                (name for name in candidates if name in available),
                None,
            ),
        )
        for logical_name, candidates in PRODUCT_DESCRIPTOR_PRECEDENCE
    )
    return ProductCatalogColumnBinding(
        item_id=item_id,
        descriptors=descriptors,
        start_date="start_date" if "start_date" in available else None,
        end_date="end_date" if "end_date" in available else None,
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
    cleaned = F.trim(F.coalesce(column.cast("string"), F.lit("")))
    missing = F.lower(cleaned).isin(*_MISSING_TEXT_VALUES)
    return F.when((cleaned == "") | missing, F.lit("")).otherwise(cleaned)


def _normalise_item(F, column):
    return F.regexp_replace(
        F.lower(_clean_text(F, column)),
        r"[^a-z0-9]",
        "",
    )


def _optional_text(F, binding: ProductCatalogColumnBinding, logical: str):
    source = binding.descriptor(logical)
    if source is None:
        return F.lit("").cast("string").alias(logical)
    return _clean_text(F, F.col(source)).alias(logical)


def build_current_product_text_source(
    product_catalog_history,
    *,
    reference_date: date | str,
):
    """Build one deterministic, effective product text row per item."""
    binding = resolve_product_catalog_columns(product_catalog_history.columns)
    resolved_reference_date = _as_date(reference_date, "reference_date")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    start_date = (
        F.expr(f"try_cast(`{binding.start_date}` AS DATE)")
        if binding.start_date is not None
        else F.lit(None).cast("date")
    )
    end_date = (
        F.expr(f"try_cast(`{binding.end_date}` AS DATE)")
        if binding.end_date is not None
        else F.lit(None).cast("date")
    )
    invalid_start_date = (
        F.col(binding.start_date).isNotNull() & start_date.isNull()
        if binding.start_date is not None
        else F.lit(False)
    )
    invalid_end_date = (
        F.col(binding.end_date).isNotNull() & end_date.isNull()
        if binding.end_date is not None
        else F.lit(False)
    )
    selected = product_catalog_history.select(
        _normalise_item(F, F.col(binding.item_id)).alias("item_id"),
        start_date.alias("_catalog_start_date"),
        end_date.alias("_catalog_end_date"),
        invalid_start_date.alias("_invalid_catalog_start_date"),
        invalid_end_date.alias("_invalid_catalog_end_date"),
        *(
            _optional_text(F, binding, logical_name)
            for logical_name, _ in PRODUCT_DESCRIPTOR_PRECEDENCE
        ),
    )
    invalid_catalog_date = selected.where(
        F.col("_invalid_catalog_start_date")
        | F.col("_invalid_catalog_end_date")
    ).limit(1).collect()
    if invalid_catalog_date:
        raise ValueError(
            "product_catalog_history contains a malformed non-null "
            "start_date or end_date"
        )
    embedding_text = F.trim(
        F.lower(
            F.regexp_replace(
                F.concat_ws(
                    " ",
                    F.col("brand"),
                    F.col("title"),
                    F.col("gender"),
                    F.regexp_replace(F.col("crumbs"), r"[|;]", " "),
                    F.col("primary_colour"),
                    F.col("material"),
                    F.col("pattern"),
                    F.col("neckline"),
                    F.col("sleeve"),
                    F.col("occasion"),
                    F.col("use"),
                    F.col("description"),
                ),
                r"\s+",
                " ",
            )
        )
    )
    effective = (
        selected.where(F.col("item_id") != "")
        .where(
            F.col("_catalog_start_date").isNull()
            | (F.col("_catalog_start_date") <= F.lit(resolved_reference_date))
        )
        .where(
            F.col("_catalog_end_date").isNull()
            | (F.col("_catalog_end_date") >= F.lit(resolved_reference_date))
        )
        .withColumn("embedding_text", embedding_text)
    )

    latest_order = (
        F.col("_catalog_start_date").desc_nulls_last(),
        F.col("_catalog_end_date").desc_nulls_last(),
    )
    latest = effective.withColumn(
        "_latest_rank",
        F.dense_rank().over(
            Window.partitionBy("item_id").orderBy(*latest_order)
        ),
    ).where(F.col("_latest_rank") == 1)
    conflict = (
        latest.groupBy("item_id")
        .agg(F.countDistinct("embedding_text").alias("_embedding_text_count"))
        .where(F.col("_embedding_text_count") > 1)
        .orderBy("item_id")
        .limit(1)
        .collect()
    )
    if conflict:
        raise ValueError(
            "product_catalog_history has conflicting equally-latest text "
            f"for item_id={conflict[0]['item_id']}"
        )

    chosen = (
        latest.withColumn(
            "_chosen_row",
            F.row_number().over(
                Window.partitionBy("item_id").orderBy(
                    *latest_order,
                    F.col("embedding_text").asc(),
                )
            ),
        )
        .where(F.col("_chosen_row") == 1)
        .where(F.col("embedding_text") != "")
    )
    result = chosen.select(
        "item_id",
        "embedding_text",
        F.sha2("embedding_text", 256).alias("embedding_text_hash"),
    ).select(*PRODUCT_TEXT_OUTPUT_COLUMNS)
    if not result.limit(1).collect():
        raise ValueError(
            "product_catalog_history produced no current product text rows"
        )
    return result


def _read_embedding_lineage(
    product_embeddings,
    approved_binding=None,
) -> ProductEmbeddingLineage:
    from pyspark.sql import functions as F

    selected_columns = [
        F.trim(F.col("embedding_model_name").cast("string")).alias(
            "embedding_model_name"
        ),
        F.trim(F.col("embedding_model_version").cast("string")).alias(
            "embedding_model_version"
        ),
        F.trim(F.col("embedding_artifact_sha256").cast("string")).alias(
            "embedding_artifact_sha256"
        ),
        F.col("embedding_dimension").alias("embedding_dimension"),
    ]
    if approved_binding is not None:
        selected_columns.extend(
            [
                F.trim(F.col("embedding_model_uri").cast("string")).alias(
                    "embedding_model_uri"
                ),
                F.trim(
                    F.col("embedding_source_run_id").cast("string")
                ).alias("embedding_source_run_id"),
            ]
        )
    rows = (
        product_embeddings.select(*selected_columns)
        .distinct()
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            "product_embeddings must contain exactly one model artifact "
            "lineage"
        )
    row = rows[0]
    lineage = ProductEmbeddingLineage(
        embedding_model_name=row["embedding_model_name"],
        embedding_model_version=row["embedding_model_version"],
        embedding_artifact_sha256=row["embedding_artifact_sha256"],
        embedding_dimension=row["embedding_dimension"],
    )
    if approved_binding is not None:
        expected = {
            "embedding_model_name": (
                approved_binding.model.registered_model_name
            ),
            "embedding_model_version": str(
                approved_binding.model.registered_model_version
            ),
            "embedding_model_uri": approved_binding.model.model_uri,
            "embedding_source_run_id": approved_binding.source_run_id,
            "embedding_artifact_sha256": approved_binding.artifact_sha256,
            "embedding_dimension": EXPECTED_EMBEDDING_DIMENSION,
        }
        actual = {
            "embedding_model_name": lineage.embedding_model_name,
            "embedding_model_version": lineage.embedding_model_version,
            "embedding_model_uri": row["embedding_model_uri"],
            "embedding_source_run_id": row["embedding_source_run_id"],
            "embedding_artifact_sha256": (
                lineage.embedding_artifact_sha256
            ),
            "embedding_dimension": lineage.embedding_dimension,
        }
        mismatches = [
            f"{field_name}: expected {expected_value!r}, "
            f"found {actual[field_name]!r}"
            for field_name, expected_value in expected.items()
            if actual[field_name] != expected_value
        ]
        if mismatches:
            raise ValueError(
                "product_embeddings model lineage does not match the "
                "approved materialization binding: " + "; ".join(mismatches)
            )
    return lineage


def _weighted_l2_embedding(rows: list[Any] | None) -> list[float] | None:
    if not rows:
        return None
    accumulator = [0.0] * EXPECTED_EMBEDDING_DIMENSION
    embedded_weight = 0.0
    for row in rows:
        embedding = row["embedding"]
        if embedding is None:
            continue
        values = [float(value) for value in embedding]
        input_norm = sqrt(sum(value * value for value in values))
        if not isfinite(input_norm) or not isclose(
            input_norm,
            1.0,
            rel_tol=_VECTOR_NORM_TOLERANCE,
            abs_tol=_VECTOR_NORM_TOLERANCE,
        ):
            raise ValueError(
                "product embedding inputs must be finite L2-normalised "
                "vectors"
            )
        weight = float(row["weight"])
        for index, value in enumerate(values):
            accumulator[index] += weight * value
        embedded_weight += weight
    if embedded_weight <= 0:
        return None
    averaged = [value / embedded_weight for value in accumulator]
    norm = sqrt(sum(value * value for value in averaged))
    if not isfinite(norm) or norm <= _ZERO_VECTOR_TOLERANCE:
        raise ValueError(
            "weighted product embeddings cancel to a zero-norm vector"
        )
    return [value / norm for value in averaged]


def _raise_for_invalid_bridge_rows(bridge) -> None:
    from pyspark.sql import functions as F

    invalid = (
        F.col("advert_id").isNull()
        | (F.col("advert_id") == "")
        | F.col("feature_date").isNull()
        | F.col("item_id").isNull()
        | (F.col("item_id") == "")
        | F.col("item_weight").isNull()
        | F.isnan("item_weight")
        | (F.abs(F.col("item_weight")) == F.lit(float("inf")))
        | (F.col("item_weight") <= 0)
    )
    if bridge.where(invalid).limit(1).collect():
        raise ValueError(
            "advert_item_bridge contains a null, blank, or invalid key or "
            "weight"
        )

    duplicate = (
        bridge.groupBy("advert_id", "feature_date", "item_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise ValueError(
            "advert_item_bridge contains duplicate "
            "(advert_id, feature_date, item_id) keys"
        )

    bad_weight_sum = (
        bridge.groupBy("advert_id", "feature_date")
        .agg(F.sum("item_weight").alias("_item_weight_sum"))
        .where(
            F.abs(F.col("_item_weight_sum") - F.lit(1.0))
            > F.lit(_WEIGHT_SUM_TOLERANCE)
        )
        .limit(1)
        .collect()
    )
    if bad_weight_sum:
        raise ValueError(
            "advert_item_bridge item_weight must sum to 1 for every "
            "advert_id and feature_date"
        )


def _raise_for_invalid_embedding_rows(product_embeddings) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    embedding_type = product_embeddings.schema["embedding"].dataType
    if not isinstance(embedding_type, T.ArrayType) or not isinstance(
        embedding_type.elementType,
        T.DoubleType,
    ):
        raise ValueError("product_embeddings embedding must be ARRAY<DOUBLE>")

    non_finite = F.exists(
        F.col("embedding"),
        lambda value: value.isNull()
        | F.isnan(value)
        | (F.abs(value) == F.lit(float("inf"))),
    )
    squared_norm = F.aggregate(
        F.col("embedding"),
        F.lit(0.0).cast("double"),
        lambda total, value: total + (value * value),
    )
    invalid = (
        F.col("item_id").isNull()
        | (F.col("item_id") == "")
        | F.col("embedding").isNull()
        | (F.size("embedding") != F.lit(EXPECTED_EMBEDDING_DIMENSION))
        | non_finite
        | (
            F.abs(F.sqrt(squared_norm) - F.lit(1.0))
            > F.lit(_VECTOR_NORM_TOLERANCE)
        )
    )
    if product_embeddings.where(invalid).limit(1).collect():
        raise ValueError(
            "product_embeddings contains an invalid key or 384-value "
            "finite embedding with L2 norm 1"
        )
    duplicate = (
        product_embeddings.groupBy("item_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise ValueError(
            "product_embeddings contains duplicate item_id keys for its "
            "exact model artifact"
        )


def build_advert_product_profile_frame(
    advert_item_bridge,
    product_embeddings,
    *,
    approved_binding=None,
):
    """Build daily advert profiles from the canonical bridge and one model."""
    _require_columns(
        advert_item_bridge,
        "advert_item_bridge",
        CANONICAL_ADVERT_ITEM_COLUMNS,
    )
    required_embedding_columns = (
        "item_id",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_artifact_sha256",
        "embedding",
        "embedding_dimension",
    )
    if approved_binding is not None:
        required_embedding_columns = (
            *required_embedding_columns,
            "embedding_model_uri",
            "embedding_source_run_id",
        )
    _require_columns(
        product_embeddings,
        "product_embeddings",
        required_embedding_columns,
    )

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    lineage = _read_embedding_lineage(
        product_embeddings,
        approved_binding=approved_binding,
    )
    bridge = advert_item_bridge.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        F.to_date("feature_date").alias("feature_date"),
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.col("item_weight").cast("double").alias("item_weight"),
    )
    embeddings = product_embeddings.select(
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.col("embedding").alias("embedding"),
    )
    _raise_for_invalid_bridge_rows(bridge)
    _raise_for_invalid_embedding_rows(embeddings)

    joined = bridge.join(embeddings, on="item_id", how="left")
    profiles = joined.groupBy("advert_id", "feature_date").agg(
        F.count(F.lit(1)).cast("long").alias("advert_product_item_count"),
        F.sum(
            F.when(F.col("embedding").isNotNull(), F.lit(1)).otherwise(
                F.lit(0)
            )
        )
        .cast("long")
        .alias("advert_product_embedded_item_count"),
        F.expr(
            "aggregate("
            "sort_array(collect_list(named_struct("
            "'item_id', item_id, "
            "'weight', item_weight, "
            "'embedding', embedding))), "
            f"array_repeat(CAST(0.0 AS DOUBLE), "
            f"{EXPECTED_EMBEDDING_DIMENSION}), "
            "(accumulator, product) -> "
            "IF(product.embedding IS NULL, accumulator, "
            "zip_with(accumulator, product.embedding, "
            "(total, value) -> total + (product.weight * value))))"
        ).alias("_weighted_embedding_sum"),
    )
    profiles = profiles.withColumn(
        "_weighted_embedding_norm",
        F.sqrt(
            F.expr(
                "aggregate(_weighted_embedding_sum, "
                "CAST(0.0 AS DOUBLE), "
                "(total, value) -> total + (value * value))"
            )
        ),
    )
    cancelled = (
        profiles.where(F.col("advert_product_embedded_item_count") > 0)
        .where(
            F.col("_weighted_embedding_norm").isNull()
            | F.isnan(F.col("_weighted_embedding_norm"))
            | (
                F.abs(F.col("_weighted_embedding_norm"))
                == F.lit(float("inf"))
            )
            | (
                F.col("_weighted_embedding_norm")
                <= F.lit(_ZERO_VECTOR_TOLERANCE)
            )
        )
        .limit(1)
        .collect()
    )
    if cancelled:
        raise ValueError(
            "weighted product embeddings cancel to a zero-norm vector"
        )
    embedding_array_type = T.ArrayType(
        T.DoubleType(),
        containsNull=False,
    )
    profiles = profiles.withColumn(
        "advert_product_embedding",
        F.when(
            F.col("advert_product_embedded_item_count") == 0,
            F.lit(None).cast(embedding_array_type),
        ).otherwise(
            F.expr(
                "transform(_weighted_embedding_sum, "
                "value -> value / _weighted_embedding_norm)"
            )
        ),
    )
    build_time = F.current_timestamp()
    return (
        profiles.withColumn(
            "embedding_model_name",
            F.lit(lineage.embedding_model_name),
        )
        .withColumn(
            "embedding_model_version",
            F.lit(lineage.embedding_model_version),
        )
        .withColumn(
            "embedding_artifact_sha256",
            F.lit(lineage.embedding_artifact_sha256),
        )
        .withColumn(
            "advert_product_embedding_coverage",
            F.col("advert_product_embedded_item_count")
            / F.col("advert_product_item_count"),
        )
        .withColumn(
            "advert_product_embedding_dimension",
            F.lit(EXPECTED_EMBEDDING_DIMENSION).cast("int"),
        )
        .withColumn("created_at", build_time)
        .withColumn("updated_at", build_time)
        .select(*ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS)
    )


__all__ = [
    "ADVERT_PRODUCT_PROFILE_OUTPUT_COLUMNS",
    "PRODUCT_DESCRIPTOR_PRECEDENCE",
    "PRODUCT_ID_PRECEDENCE",
    "PRODUCT_TEXT_OUTPUT_COLUMNS",
    "ProductCatalogColumnBinding",
    "ProductEmbeddingLineage",
    "build_advert_product_profile_frame",
    "build_current_product_text_source",
    "resolve_product_catalog_columns",
]
