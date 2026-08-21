"""Pure contracts for product and advert embedding feature builds."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from math import isclose, isfinite, sqrt

from next_ads.features.embedding_contract import (
    EXPECTED_EMBEDDING_DIMENSION,
)


_WEIGHT_SUM_TOLERANCE = 1e-9
_MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]*")
_ARTIFACT_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    resolved = value.strip()
    if not resolved:
        raise ValueError(f"{field_name} cannot be empty")
    return resolved


def _exact_model_version(value: object) -> str:
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


def _artifact_sha256(value: object) -> str:
    resolved = _required_text(value, "embedding_artifact_sha256")
    if _ARTIFACT_SHA256_PATTERN.fullmatch(resolved) is None:
        raise ValueError(
            "embedding_artifact_sha256 must be a 64-character lowercase "
            "hexadecimal SHA-256 digest"
        )
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


def _embedding_vector(values: Sequence[float]) -> tuple[float, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError("embedding must be a numeric sequence")
    try:
        resolved = tuple(float(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError("embedding must contain only numbers") from exc
    if len(resolved) != EXPECTED_EMBEDDING_DIMENSION:
        raise ValueError(
            "embedding must contain exactly "
            f"{EXPECTED_EMBEDDING_DIMENSION} values"
        )
    if not all(isfinite(value) for value in resolved):
        raise ValueError("embedding cannot contain non-finite values")
    return resolved


@dataclass(frozen=True, order=True)
class ProductEmbeddingKey:
    """Physical key for one exact product embedding model version."""

    item_id: str
    embedding_model_name: str
    embedding_model_version: str

    def __post_init__(self) -> None:
        """Normalise and validate all parts of the physical key."""
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "item_id"),
        )
        object.__setattr__(
            self,
            "embedding_model_name",
            _required_text(
                self.embedding_model_name,
                "embedding_model_name",
            ),
        )
        object.__setattr__(
            self,
            "embedding_model_version",
            _required_text(
                self.embedding_model_version,
                "embedding_model_version",
            ),
        )


@dataclass(frozen=True)
class ProductEmbeddingSource:
    """Current product text represented by its stable source hash."""

    item_id: str
    embedding_text_hash: str

    def __post_init__(self) -> None:
        """Normalise and validate the source identity."""
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "item_id"),
        )
        object.__setattr__(
            self,
            "embedding_text_hash",
            _required_text(
                self.embedding_text_hash,
                "embedding_text_hash",
            ),
        )


@dataclass(frozen=True)
class ExistingProductEmbedding:
    """Metadata needed to decide whether an existing vector can be reused."""

    key: ProductEmbeddingKey
    embedding_text_hash: str
    embedding_dimension: int
    has_embedding: bool = True

    def __post_init__(self) -> None:
        """Validate metadata while retaining stale rows for replacement."""
        if not isinstance(self.key, ProductEmbeddingKey):
            raise ValueError("key must be a ProductEmbeddingKey")
        object.__setattr__(
            self,
            "embedding_text_hash",
            _required_text(
                self.embedding_text_hash,
                "embedding_text_hash",
            ),
        )
        if isinstance(self.embedding_dimension, bool) or not isinstance(
            self.embedding_dimension,
            int,
        ):
            raise ValueError("embedding_dimension must be an integer")
        if not isinstance(self.has_embedding, bool):
            raise ValueError("has_embedding must be a boolean")

    def can_reuse(self, source_hash: str) -> bool:
        """Return whether this row exactly satisfies the current contract."""
        return (
            self.has_embedding
            and self.embedding_dimension == EXPECTED_EMBEDDING_DIMENSION
            and self.embedding_text_hash == source_hash
        )


@dataclass(frozen=True)
class ProductEmbeddingRefreshPlan:
    """Complete replacement plan; unmatched old keys are never carried on."""

    expected_output_keys: tuple[ProductEmbeddingKey, ...]
    reuse_keys: tuple[ProductEmbeddingKey, ...]
    generate_keys: tuple[ProductEmbeddingKey, ...]
    replace_keys: tuple[ProductEmbeddingKey, ...]
    obsolete_keys: tuple[ProductEmbeddingKey, ...]

    def __post_init__(self) -> None:
        """Enforce a complete, non-overlapping replacement plan."""
        expected = set(self.expected_output_keys)
        reused = set(self.reuse_keys)
        generated = set(self.generate_keys)
        replaced = set(self.replace_keys)
        obsolete = set(self.obsolete_keys)
        if reused.intersection(generated):
            raise ValueError("reuse_keys and generate_keys must be disjoint")
        if reused.union(generated) != expected:
            raise ValueError(
                "reuse_keys and generate_keys must cover expected_output_keys"
            )
        if not replaced.issubset(generated):
            raise ValueError("replace_keys must be included in generate_keys")
        if obsolete.intersection(expected):
            raise ValueError(
                "obsolete_keys cannot be carried into the replacement"
            )


def plan_product_embedding_snapshot(
    sources: Iterable[ProductEmbeddingSource],
    existing: Iterable[ExistingProductEmbedding],
    *,
    embedding_model_name: str,
    embedding_model_version: str,
) -> ProductEmbeddingRefreshPlan:
    """Plan a complete current-source replacement without retaining deletes."""
    model_name = _required_text(
        embedding_model_name,
        "embedding_model_name",
    )
    model_version = _required_text(
        embedding_model_version,
        "embedding_model_version",
    )

    source_by_item: dict[str, ProductEmbeddingSource] = {}
    for source in sources:
        if not isinstance(source, ProductEmbeddingSource):
            raise ValueError("sources must contain ProductEmbeddingSource")
        if source.item_id in source_by_item:
            raise ValueError(
                f"current product source contains duplicate item_id "
                f"{source.item_id}"
            )
        source_by_item[source.item_id] = source
    if not source_by_item:
        raise ValueError(
            "current product source is empty; refusing a full replacement"
        )

    existing_by_key: dict[
        ProductEmbeddingKey,
        ExistingProductEmbedding,
    ] = {}
    for row in existing:
        if not isinstance(row, ExistingProductEmbedding):
            raise ValueError("existing must contain ExistingProductEmbedding")
        if row.key in existing_by_key:
            raise ValueError(
                f"existing product embeddings contain duplicate key {row.key}"
            )
        existing_by_key[row.key] = row

    expected_keys = tuple(
        ProductEmbeddingKey(item_id, model_name, model_version)
        for item_id in sorted(source_by_item)
    )
    expected_set = set(expected_keys)
    reuse_keys = []
    generate_keys = []
    replace_keys = []
    for key in expected_keys:
        previous = existing_by_key.get(key)
        source = source_by_item[key.item_id]
        if previous is not None and previous.can_reuse(
            source.embedding_text_hash
        ):
            reuse_keys.append(key)
            continue
        generate_keys.append(key)
        if previous is not None:
            replace_keys.append(key)

    return ProductEmbeddingRefreshPlan(
        expected_output_keys=expected_keys,
        reuse_keys=tuple(reuse_keys),
        generate_keys=tuple(generate_keys),
        replace_keys=tuple(replace_keys),
        obsolete_keys=tuple(
            sorted(set(existing_by_key).difference(expected_set))
        ),
    )


@dataclass(frozen=True)
class AdvertProductEmbeddingInput:
    """One weighted advert-item relation and its optional product vector."""

    advert_id: str
    feature_date: date
    item_id: str
    item_weight: float
    embedding: tuple[float, ...] | None

    def __post_init__(self) -> None:
        """Normalise keys, date, weight, and optional vector."""
        object.__setattr__(
            self,
            "advert_id",
            _required_text(self.advert_id, "advert_id"),
        )
        object.__setattr__(
            self,
            "feature_date",
            _as_date(self.feature_date, "feature_date"),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "item_id"),
        )
        if isinstance(self.item_weight, bool):
            raise ValueError("item_weight must be a positive finite number")
        try:
            weight = float(self.item_weight)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "item_weight must be a positive finite number"
            ) from exc
        if not isfinite(weight) or weight <= 0:
            raise ValueError("item_weight must be a positive finite number")
        object.__setattr__(self, "item_weight", weight)
        if self.embedding is not None:
            object.__setattr__(
                self,
                "embedding",
                _embedding_vector(self.embedding),
            )


@dataclass(frozen=True)
class AdvertProductProfile:
    """Daily weighted advert vector and its embedding coverage evidence."""

    advert_id: str
    feature_date: date
    advert_product_item_count: int
    advert_product_embedded_item_count: int
    advert_product_embedding_coverage: float
    advert_product_embedding: tuple[float, ...] | None
    embedding_model_name: str
    embedding_model_version: str
    embedding_artifact_sha256: str
    advert_product_embedding_dimension: int


def _build_advert_product_profile(
    rows: Sequence[AdvertProductEmbeddingInput],
    *,
    embedding_model_name: str,
    embedding_model_version: str,
    embedding_artifact_sha256: str,
) -> AdvertProductProfile:
    ordered = sorted(rows, key=lambda row: row.item_id)
    weight_sum = sum(row.item_weight for row in ordered)
    if not isclose(
        weight_sum,
        1.0,
        rel_tol=0.0,
        abs_tol=_WEIGHT_SUM_TOLERANCE,
    ):
        example = ordered[0]
        raise ValueError(
            "item_weight must sum to 1 for "
            f"advert_id={example.advert_id}, "
            f"feature_date={example.feature_date.isoformat()}"
        )

    embedded = [row for row in ordered if row.embedding is not None]
    embedding = None
    if embedded:
        embedded_weight = sum(row.item_weight for row in embedded)
        accumulator = [0.0] * EXPECTED_EMBEDDING_DIMENSION
        for row in embedded:
            for index, value in enumerate(row.embedding or ()):
                accumulator[index] += row.item_weight * value
        averaged = [value / embedded_weight for value in accumulator]
        norm = sqrt(sum(value * value for value in averaged))
        embedding = tuple(
            value / norm if norm else value for value in averaged
        )

    item_count = len(ordered)
    embedded_item_count = len(embedded)
    first = ordered[0]
    return AdvertProductProfile(
        advert_id=first.advert_id,
        feature_date=first.feature_date,
        advert_product_item_count=item_count,
        advert_product_embedded_item_count=embedded_item_count,
        advert_product_embedding_coverage=(embedded_item_count / item_count),
        advert_product_embedding=embedding,
        embedding_model_name=embedding_model_name,
        embedding_model_version=embedding_model_version,
        embedding_artifact_sha256=embedding_artifact_sha256,
        advert_product_embedding_dimension=EXPECTED_EMBEDDING_DIMENSION,
    )


def build_advert_product_profiles(
    rows: Iterable[AdvertProductEmbeddingInput],
    *,
    embedding_model_name: str,
    embedding_model_version: str | int,
    embedding_artifact_sha256: str,
) -> tuple[AdvertProductProfile, ...]:
    """Aggregate daily profiles deterministically from canonical item weights."""
    model_name = _required_text(
        embedding_model_name,
        "embedding_model_name",
    )
    model_version = _exact_model_version(embedding_model_version)
    artifact_sha256 = _artifact_sha256(embedding_artifact_sha256)
    grouped: dict[
        tuple[str, date],
        list[AdvertProductEmbeddingInput],
    ] = defaultdict(list)
    seen_items: set[tuple[str, date, str]] = set()
    for row in rows:
        if not isinstance(row, AdvertProductEmbeddingInput):
            raise ValueError("rows must contain AdvertProductEmbeddingInput")
        item_key = (row.advert_id, row.feature_date, row.item_id)
        if item_key in seen_items:
            raise ValueError(
                f"advert product inputs contain duplicate key {item_key}"
            )
        seen_items.add(item_key)
        grouped[(row.advert_id, row.feature_date)].append(row)

    return tuple(
        _build_advert_product_profile(
            grouped[group_key],
            embedding_model_name=model_name,
            embedding_model_version=model_version,
            embedding_artifact_sha256=artifact_sha256,
        )
        for group_key in sorted(grouped)
    )


__all__ = [
    "AdvertProductEmbeddingInput",
    "AdvertProductProfile",
    "ExistingProductEmbedding",
    "ProductEmbeddingKey",
    "ProductEmbeddingRefreshPlan",
    "ProductEmbeddingSource",
    "build_advert_product_profiles",
    "plan_product_embedding_snapshot",
]
