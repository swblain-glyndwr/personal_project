"""Point-in-time seasonal product-demand feature transforms."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from next_ads.features.advert_items import CANONICAL_ADVERT_ITEM_COLUMNS
from next_ads.features.embedding_contract import (
    EXPECTED_EMBEDDING_DIMENSION,
)


SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS = (
    "entity_type",
    "entity_id",
    "item_id",
    "feature_date",
    "product_views_7d",
    "product_views_30d",
    "product_purchases_7d",
    "product_purchases_30d",
    "product_views_ly_same_month",
    "product_purchases_ly_same_month",
    "product_trending_7x30",
    "embedding_model_name",
    "embedding_model_version",
    "embedding_model_uri",
    "embedding_source_run_id",
    "embedding_artifact_sha256",
    "product_embedding_text_hash",
    "seasonal_product_embedding",
    "seasonal_product_embedding_dimension",
    "seasonal_product_embedding_coverage",
    "created_at",
    "updated_at",
)

_VIEW_SOURCE_COLUMNS = ("account_number", "productSku", "date")
_PURCHASE_SOURCE_COLUMNS = ("account_number", "itemno", "order_date")
_PRODUCT_EMBEDDING_COLUMNS = (
    "item_id",
    "embedding_model_name",
    "embedding_model_version",
    "embedding_model_uri",
    "embedding_source_run_id",
    "embedding_artifact_sha256",
    "embedding",
    "embedding_dimension",
    "embedding_text_hash",
)
_MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]*")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


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


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    resolved = value.strip()
    if not resolved:
        raise ValueError(f"{field_name} cannot be empty")
    return resolved


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


@dataclass(frozen=True)
class SeasonalDemandWindows:
    """Half-open event windows for one point-in-time feature date."""

    feature_date: date
    recent_7d_start: date
    recent_30d_start: date
    prior_year_month_start: date
    prior_year_month_end: date

    def in_recent_7d(self, event_date: date | str) -> bool:
        """Return whether an event belongs to ``[D-7, D)``."""
        resolved = _as_date(event_date, "event_date")
        return self.recent_7d_start <= resolved < self.feature_date

    def in_recent_30d(self, event_date: date | str) -> bool:
        """Return whether an event belongs to ``[D-30, D)``."""
        resolved = _as_date(event_date, "event_date")
        return self.recent_30d_start <= resolved < self.feature_date

    def in_prior_year_month(self, event_date: date | str) -> bool:
        """Return whether an event belongs to last year's matching month."""
        resolved = _as_date(event_date, "event_date")
        return (
            self.prior_year_month_start
            <= resolved
            < self.prior_year_month_end
        )

    def contributes_membership(self, event_date: date | str) -> bool:
        """Return whether an event makes an account-item membership row."""
        return self.in_recent_30d(event_date) or self.in_prior_year_month(
            event_date
        )


def resolve_seasonal_demand_windows(
    reference_date: date | str,
) -> SeasonalDemandWindows:
    """Resolve leakage-safe recent windows and the prior-year month."""
    feature_date = _as_date(reference_date, "reference_date")
    prior_year_month_start = date(
        feature_date.year - 1,
        feature_date.month,
        1,
    )
    return SeasonalDemandWindows(
        feature_date=feature_date,
        recent_7d_start=feature_date - timedelta(days=7),
        recent_30d_start=feature_date - timedelta(days=30),
        prior_year_month_start=prior_year_month_start,
        prior_year_month_end=_next_month(prior_year_month_start),
    )


@dataclass(frozen=True)
class SeasonalEmbeddingLineage:
    """Exact promoted model artifact used by every output item row."""

    model_name: str
    model_version: str
    model_uri: str
    source_run_id: str
    artifact_sha256: str
    dimension: int

    def __post_init__(self) -> None:
        """Reject aliases and incomplete artifact identities."""
        object.__setattr__(
            self,
            "model_name",
            _required_text(self.model_name, "embedding_model_name"),
        )
        version = _required_text(
            str(self.model_version),
            "embedding_model_version",
        )
        if _MODEL_VERSION_PATTERN.fullmatch(version) is None:
            raise ValueError(
                "embedding_model_version must be a positive numeric version"
            )
        object.__setattr__(self, "model_version", version)
        model_uri = _required_text(self.model_uri, "embedding_model_uri")
        if model_uri != f"models:/{self.model_name}/{version}":
            raise ValueError(
                "embedding_model_uri must identify the exact registered "
                "model name and numeric version"
            )
        object.__setattr__(self, "model_uri", model_uri)
        object.__setattr__(
            self,
            "source_run_id",
            _required_text(
                self.source_run_id,
                "embedding_source_run_id",
            ),
        )
        digest = _required_text(
            self.artifact_sha256,
            "embedding_artifact_sha256",
        )
        if _SHA256_PATTERN.fullmatch(digest) is None:
            raise ValueError(
                "embedding_artifact_sha256 must be a 64-character lowercase "
                "hexadecimal SHA-256 digest"
            )
        object.__setattr__(self, "artifact_sha256", digest)
        if (
            isinstance(self.dimension, bool)
            or not isinstance(self.dimension, int)
            or self.dimension != EXPECTED_EMBEDDING_DIMENSION
        ):
            raise ValueError(
                "embedding_dimension must be exactly "
                f"{EXPECTED_EMBEDDING_DIMENSION}"
            )


def _require_columns(frame, frame_name: str, required: tuple[str, ...]) -> None:
    if frame is None or not hasattr(frame, "columns"):
        raise ValueError(f"{frame_name} must be a DataFrame")
    missing = sorted(set(required).difference(frame.columns))
    if missing:
        raise ValueError(
            f"{frame_name} is missing required columns: {', '.join(missing)}"
        )


def _normalise_item(F, column):
    return F.regexp_replace(
        F.lower(F.trim(column.cast("string"))),
        r"[^a-z0-9]",
        "",
    )


def _normalise_account(F, column):
    return F.trim(column.cast("string"))


def _read_embedding_lineage(
    product_embeddings,
    approved_binding,
) -> SeasonalEmbeddingLineage:
    rows = (
        product_embeddings.select(
            "embedding_model_name",
            "embedding_model_version",
            "embedding_model_uri",
            "embedding_source_run_id",
            "embedding_artifact_sha256",
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
    row = rows[0]
    lineage = SeasonalEmbeddingLineage(
        model_name=row["embedding_model_name"],
        model_version=row["embedding_model_version"],
        model_uri=row["embedding_model_uri"],
        source_run_id=row["embedding_source_run_id"],
        artifact_sha256=row["embedding_artifact_sha256"],
        dimension=row["embedding_dimension"],
    )
    expected = {
        "model_name": approved_binding.model.registered_model_name,
        "model_version": str(
            approved_binding.model.registered_model_version
        ),
        "model_uri": approved_binding.model.model_uri,
        "source_run_id": approved_binding.source_run_id,
        "artifact_sha256": approved_binding.artifact_sha256,
        "dimension": EXPECTED_EMBEDDING_DIMENSION,
    }
    mismatches = [
        f"{name}: expected {expected[name]!r}, found "
        f"{getattr(lineage, name)!r}"
        for name in expected
        if getattr(lineage, name) != expected[name]
    ]
    if mismatches:
        raise ValueError(
            "product_embeddings do not match the approved materialization "
            "binding: " + "; ".join(mismatches)
        )
    return lineage


def _validate_embeddings(product_embeddings) -> None:
    from pyspark.sql import functions as F

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
        product_embeddings.where(
            F.col("item_id").isNull()
            | (_normalise_item(F, F.col("item_id")) == "")
            | F.col("embedding").isNull()
            | (
                F.size("embedding")
                != F.lit(EXPECTED_EMBEDDING_DIMENSION)
            )
            | non_finite
            | (F.abs(F.sqrt(squared_norm) - F.lit(1.0)) > F.lit(1e-5))
            | F.col("embedding_text_hash").isNull()
            | (~F.col("embedding_text_hash").rlike(r"^[0-9a-f]{64}$"))
        )
        .limit(1)
        .collect()
    )
    if invalid:
        raise ValueError(
            "product_embeddings contains an invalid item, vector, or text "
            "hash"
        )
    duplicate = (
        product_embeddings.select(
            _normalise_item(F, F.col("item_id")).alias("item_id")
        )
        .groupBy("item_id")
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


def _validate_advert_membership(advert_membership, feature_date: date) -> None:
    from pyspark.sql import functions as F

    invalid = (
        advert_membership.where(
            F.col("advert_id").isNull()
            | (F.col("advert_id") == "")
            | F.col("item_id").isNull()
            | (F.col("item_id") == "")
            | F.col("feature_date").isNull()
            | (F.col("feature_date") != F.lit(feature_date).cast("date"))
        )
        .limit(1)
        .collect()
    )
    if invalid:
        raise ValueError(
            "advert_item_bridge contains an invalid key or a feature date "
            "outside the requested build"
        )
    duplicate = (
        advert_membership.groupBy("advert_id", "item_id", "feature_date")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise ValueError(
            "advert_item_bridge contains duplicate advert-item membership"
        )


def _normalise_events(
    frame,
    *,
    item_column: str,
    date_column: str,
):
    from pyspark.sql import functions as F

    return frame.select(
        _normalise_account(F, F.col("account_number")).alias(
            "account_number"
        ),
        _normalise_item(F, F.col(item_column)).alias("item_id"),
        F.to_date(F.col(date_column)).alias("event_date"),
    ).where(F.col("item_id").isNotNull() & (F.col("item_id") != ""))


def _filter_relevant_events(events, windows: SeasonalDemandWindows):
    from pyspark.sql import functions as F

    recent = (
        (F.col("event_date") >= F.lit(windows.recent_30d_start))
        & (F.col("event_date") < F.lit(windows.feature_date))
    )
    prior_year = (
        (F.col("event_date") >= F.lit(windows.prior_year_month_start))
        & (F.col("event_date") < F.lit(windows.prior_year_month_end))
    )
    return events.where(F.col("event_date").isNotNull()).where(
        recent | prior_year
    )


def _aggregate_item_demand(views, purchases, windows: SeasonalDemandWindows):
    from pyspark.sql import functions as F

    recent_7d = (
        (F.col("event_date") >= F.lit(windows.recent_7d_start))
        & (F.col("event_date") < F.lit(windows.feature_date))
    )
    recent_30d = (
        (F.col("event_date") >= F.lit(windows.recent_30d_start))
        & (F.col("event_date") < F.lit(windows.feature_date))
    )
    prior_year = (
        (F.col("event_date") >= F.lit(windows.prior_year_month_start))
        & (F.col("event_date") < F.lit(windows.prior_year_month_end))
    )

    def aggregate(events, prefix: str):
        return events.groupBy("item_id").agg(
            F.sum(F.when(recent_7d, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias(f"product_{prefix}_7d"),
            F.sum(F.when(recent_30d, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias(f"product_{prefix}_30d"),
            F.sum(F.when(prior_year, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias(f"product_{prefix}_ly_same_month"),
        )

    return aggregate(views, "views").join(
        aggregate(purchases, "purchases"),
        on="item_id",
        how="full",
    )


def build_seasonal_product_demand_frame(
    *,
    account_views,
    account_purchases,
    advert_item_bridge,
    product_embeddings,
    approved_binding,
    reference_date: date | str,
):
    """Build account and advert item rows with shared global demand values."""
    _require_columns(
        account_views,
        "account_views",
        _VIEW_SOURCE_COLUMNS,
    )
    _require_columns(
        account_purchases,
        "account_purchases",
        _PURCHASE_SOURCE_COLUMNS,
    )
    _require_columns(
        advert_item_bridge,
        "advert_item_bridge",
        CANONICAL_ADVERT_ITEM_COLUMNS,
    )
    _require_columns(
        product_embeddings,
        "product_embeddings",
        _PRODUCT_EMBEDDING_COLUMNS,
    )

    from pyspark.sql import functions as F

    windows = resolve_seasonal_demand_windows(reference_date)
    lineage = _read_embedding_lineage(
        product_embeddings,
        approved_binding,
    )
    _validate_embeddings(product_embeddings)

    views = _filter_relevant_events(
        _normalise_events(
            account_views,
            item_column="productSku",
            date_column="date",
        ),
        windows,
    )
    purchases = _filter_relevant_events(
        _normalise_events(
            account_purchases,
            item_column="itemno",
            date_column="order_date",
        ),
        windows,
    )
    account_membership = (
        views.select("account_number", "item_id")
        .unionByName(purchases.select("account_number", "item_id"))
        .where(
            F.col("account_number").isNotNull()
            & (F.col("account_number") != "")
        )
        .dropDuplicates(["account_number", "item_id"])
        .select(
            F.lit("ACCOUNT").alias("entity_type"),
            F.col("account_number").alias("entity_id"),
            "item_id",
            F.lit(windows.feature_date).cast("date").alias("feature_date"),
        )
    )

    advert_membership = advert_item_bridge.select(
        F.trim(F.col("advert_id").cast("string")).alias("advert_id"),
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.to_date("feature_date").alias("feature_date"),
    )
    _validate_advert_membership(advert_membership, windows.feature_date)
    advert_membership = advert_membership.select(
        F.lit("ADVERT").alias("entity_type"),
        F.col("advert_id").alias("entity_id"),
        "item_id",
        "feature_date",
    )
    membership = account_membership.unionByName(advert_membership)

    demand = _aggregate_item_demand(views, purchases, windows)
    embeddings = product_embeddings.select(
        _normalise_item(F, F.col("item_id")).alias("item_id"),
        F.col("embedding_text_hash").alias("product_embedding_text_hash"),
        F.col("embedding").alias("seasonal_product_embedding"),
    )
    count_columns = (
        "product_views_7d",
        "product_views_30d",
        "product_purchases_7d",
        "product_purchases_30d",
        "product_views_ly_same_month",
        "product_purchases_ly_same_month",
    )
    result = membership.join(demand, on="item_id", how="left").fillna(
        0,
        subset=list(count_columns),
    )
    result = result.join(embeddings, on="item_id", how="left")
    build_time = F.current_timestamp()
    return (
        result.withColumn(
            "product_trending_7x30",
            F.when(
                F.col("product_views_30d") > 0,
                (F.col("product_views_7d") / F.lit(7.0))
                / (F.col("product_views_30d") / F.lit(30.0)),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("embedding_model_name", F.lit(lineage.model_name))
        .withColumn("embedding_model_version", F.lit(lineage.model_version))
        .withColumn("embedding_model_uri", F.lit(lineage.model_uri))
        .withColumn("embedding_source_run_id", F.lit(lineage.source_run_id))
        .withColumn(
            "embedding_artifact_sha256",
            F.lit(lineage.artifact_sha256),
        )
        .withColumn(
            "seasonal_product_embedding_dimension",
            F.lit(lineage.dimension).cast("int"),
        )
        .withColumn(
            "seasonal_product_embedding_coverage",
            F.when(
                F.col("seasonal_product_embedding").isNotNull(),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("created_at", build_time)
        .withColumn("updated_at", build_time)
        .select(*SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS)
    )


__all__ = [
    "SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS",
    "SeasonalDemandWindows",
    "SeasonalEmbeddingLineage",
    "build_seasonal_product_demand_frame",
    "resolve_seasonal_demand_windows",
]
