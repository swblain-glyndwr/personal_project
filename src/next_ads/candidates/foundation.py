from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.ranking.scoring_inputs import read_delta_version


CANDIDATE_FOUNDATION_CONTRACT_VERSION = "nextads_candidate_foundation/v1"
READY_FOR_NEXTADS = "READY_FOR_NEXTADS"
FALLBACK_PREVIOUS = "FALLBACK_PREVIOUS"
ACCEPTED_FOUNDATION_STATUSES = frozenset(
    {READY_FOR_NEXTADS, FALLBACK_PREVIOUS}
)


@dataclass(frozen=True)
class CandidateFoundationInputs:
    """Exact, manifest-bound frames consumed by one candidate adapter."""

    snapshot_id: str
    source_run_date: date
    customer_cells: DataFrame
    repeat_ad_exposure: DataFrame
    ad_feedback_metrics: DataFrame


def parse_run_date(value: date | str) -> date:
    if isinstance(value, datetime):
        raise ValueError("run_date must be a date, not a datetime")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("run_date must use ISO format YYYY-MM-DD") from exc
    raise ValueError("run_date must be a date or ISO date string")


def schema_checksum(frame: DataFrame) -> str:
    """Return a stable checksum for the ordered Spark schema."""
    signature = [
        (field.name, field.dataType.simpleString()) for field in frame.schema
    ]
    return hashlib.sha256(
        json.dumps(signature, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def source_binding(
    *,
    name: str,
    role: str,
    table: str,
    delta_version: int,
    frame: DataFrame,
    required: bool = True,
    schema_version: str = "source/v1",
) -> dict[str, Any]:
    if not name.strip() or not role.strip() or not table.strip():
        raise ValueError("Source binding names, roles and tables are required")
    if (
        isinstance(delta_version, bool)
        or not isinstance(delta_version, int)
        or delta_version < 0
    ):
        raise ValueError("Source Delta versions must be non-negative integers")
    return {
        "name": name.strip(),
        "role": role.strip(),
        "table": table.strip(),
        "delta_version": int(delta_version),
        "schema_version": schema_version,
        "schema_checksum": schema_checksum(frame),
        "required": bool(required),
    }


def build_repeat_ad_exposure(
    sessions: DataFrame,
    sessions_app: DataFrame,
    actions: DataFrame,
    actions_app: DataFrame,
    *,
    run_date: date | str,
) -> DataFrame:
    """Build the seven-day account/ad exposure once for all providers."""
    logical_date = parse_run_date(run_date)
    start_date = logical_date - timedelta(days=7)
    end_date = logical_date - timedelta(days=1)

    web_sessions = (
        sessions.where(
            (F.col("SiteCountry") == "UK")
            & F.col("Device").isin("Mobile", "Desktop")
            & F.col("Date").between(F.lit(start_date), F.lit(end_date))
            & F.col("AccountNumber_RPID").isNotNull()
        )
        .select(
            "UniqueVisitID",
            "Date",
            F.col("AccountNumber_RPID").alias("AccountNumber"),
        )
    )
    app_sessions = (
        sessions_app.where(
            (F.col("SiteCountry") == "UK")
            & F.col("Date").between(F.lit(start_date), F.lit(end_date))
            & F.col("AccountNumber_RPID").isNotNull()
        )
        .select(
            "UniqueVisitID",
            "Date",
            F.col("AccountNumber_RPID").alias("AccountNumber"),
        )
    )
    web_actions = actions.where(
        (F.col("Action") == "Banner Impression - Next Ads")
        & F.col("PagePath").isin(
            "/shoppingbag",
            "/secure/checkout/complete",
        )
        & F.col("Date").between(F.lit(start_date), F.lit(end_date))
        & F.col("Level2").isNotNull()
    ).select("UniqueVisitID", "Date", "Level2")
    app_actions = actions_app.where(
        (F.col("Action") == "Banner Impression - Next Ads")
        & (F.col("ScreenName") == "PLP")
        & F.col("Date").between(F.lit(start_date), F.lit(end_date))
        & F.col("Level2").isNotNull()
    ).select("UniqueVisitID", "Date", "Level2")

    observations = web_actions.join(
        web_sessions,
        on=["UniqueVisitID", "Date"],
        how="inner",
    ).unionByName(
        app_actions.join(
            app_sessions,
            on=["UniqueVisitID", "Date"],
            how="inner",
        )
    )
    return (
        observations.groupBy(
            "AccountNumber",
            F.col("Level2").alias("AdSeen"),
        )
        .agg(
            F.countDistinct("UniqueVisitID").alias(
                "sessions_seen_ad_in_last_7_days"
            )
        )
        .withColumn(
            "MultiSessionDownweightScore",
            F.when(
                F.col("sessions_seen_ad_in_last_7_days") == 3,
                F.lit(0.84),
            )
            .when(
                F.col("sessions_seen_ad_in_last_7_days") == 4,
                F.lit(0.8),
            )
            .when(
                F.col("sessions_seen_ad_in_last_7_days") == 5,
                F.lit(0.7),
            )
            .when(
                F.col("sessions_seen_ad_in_last_7_days") >= 6,
                F.lit(0.5),
            )
            .otherwise(F.lit(1.0)),
        )
    )


def build_ad_feedback_metrics(
    results: DataFrame,
    *,
    run_date: date | str,
    sessions_threshold: int = 10000,
    lookback_period_days: int = 7,
    lookback_offset_days: int = 2,
) -> DataFrame:
    """Aggregate reusable advert performance without route-specific scaling."""
    logical_date = parse_run_date(run_date)
    if sessions_threshold < 0:
        raise ValueError("sessions_threshold must not be negative")
    if lookback_period_days < 1 or lookback_offset_days < 0:
        raise ValueError("Feedback lookback values are invalid")
    date_start = logical_date - timedelta(
        days=(lookback_period_days - 1) + lookback_offset_days
    )
    date_end = logical_date - timedelta(days=lookback_offset_days)

    aggregated = (
        results.where(F.col("SessionDate").between(date_start, date_end))
        .groupBy("UniqueAdID")
        .agg(
            F.sum("Sessions").alias("Sessions"),
            F.sum("ApportionedRevenue").alias("ApportionedRevenue"),
            F.sum("C_Sessions").alias("C_Sessions"),
            F.sum("C_ApportionedRevenue").alias("C_ApportionedRevenue"),
            F.mean("SessionOverlapRatio").alias("SessionOverlapRatio"),
        )
        .where(
            (F.col("C_Sessions") >= sessions_threshold)
            & (F.col("Sessions") > 0)
            & (F.col("C_ApportionedRevenue") != 0)
            & (F.col("SessionOverlapRatio") > 0)
        )
        .withColumn("ARPS", F.col("ApportionedRevenue") / F.col("Sessions"))
        .withColumn(
            "C_ARPS",
            F.col("C_ApportionedRevenue") / F.col("C_Sessions"),
        )
        .withColumn("IncARPS", F.col("ARPS") - F.col("C_ARPS"))
        .withColumn(
            "IncARPSAdj",
            F.col("IncARPS") / F.col("SessionOverlapRatio"),
        )
        .withColumn(
            "IncARPSAdjPct",
            F.col("IncARPSAdj") / F.col("C_ARPS"),
        )
    )
    return aggregated.where(
        F.col("IncARPSAdjPct").isNotNull()
        & ~F.isnan("IncARPSAdjPct")
    ).select("UniqueAdID", "IncARPSAdjPct")


def score_ad_feedback_metrics(
    metrics: DataFrame,
    active_ads: DataFrame,
    *,
    ad_feedback_weight: float,
) -> DataFrame:
    """Apply the existing route-specific symmetric scaling to pinned metrics."""
    active_ad_ids = (
        active_ads.select("UniqueAdID")
        .groupBy("UniqueAdID")
        .count()
        .drop("count")
    )
    active_metrics = metrics.join(
        active_ad_ids,
        on="UniqueAdID",
        how="inner",
    )
    row = active_metrics.agg(
        F.max(F.abs("IncARPSAdjPct")).alias("scale_factor")
    ).first()
    scale_factor = row["scale_factor"] if row is not None else None
    if scale_factor is None or float(scale_factor) == 0.0:
        return active_metrics.withColumn("AdFeedbackScore", F.lit(1.0)).select(
            "UniqueAdID",
            "AdFeedbackScore",
        )
    return active_metrics.withColumn(
        "AdFeedbackScore",
        (
            F.col("IncARPSAdjPct") / F.lit(float(scale_factor))
        )
        * F.lit(float(ad_feedback_weight))
        + F.lit(1.0),
    ).select("UniqueAdID", "AdFeedbackScore")


def _required_binding(
    bindings: Mapping[str, Any],
    name: str,
) -> Mapping[str, Any]:
    value = bindings.get(name)
    if not isinstance(value, Mapping):
        raise ValueError(f"Candidate foundation is missing binding {name}")
    for field in ("table", "delta_version"):
        if field not in value:
            raise ValueError(f"Binding {name} is missing {field}")
    return value


def load_candidate_foundation_inputs(
    spark: Any,
    *,
    snapshot_id: str,
    source_run_date: date | str,
    customer_cells_table: str,
    customer_cells_delta_version: int,
    repeat_ad_exposure_table: str,
    repeat_ad_exposure_delta_version: int,
    ad_feedback_table: str,
    ad_feedback_delta_version: int,
) -> CandidateFoundationInputs:
    """Read only the exact Delta versions selected by the ready manifest."""
    if not snapshot_id.strip():
        raise ValueError("foundation snapshot ID is required")
    logical_date = parse_run_date(source_run_date)
    versions = (
        customer_cells_delta_version,
        repeat_ad_exposure_delta_version,
        ad_feedback_delta_version,
    )
    if any(
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < 0
        for version in versions
    ):
        raise ValueError("Foundation Delta versions must be non-negative integers")

    customer_cells = read_delta_version(
        spark,
        customer_cells_table,
        customer_cells_delta_version,
    )
    repeat_ad_exposure = read_delta_version(
        spark,
        repeat_ad_exposure_table,
        repeat_ad_exposure_delta_version,
    ).where(
        (F.col("CandidateFoundationSnapshotID") == snapshot_id)
        & (F.col("RunDate") == F.lit(logical_date))
    )
    ad_feedback = read_delta_version(
        spark,
        ad_feedback_table,
        ad_feedback_delta_version,
    ).where(
        (F.col("CandidateFoundationSnapshotID") == snapshot_id)
        & (F.col("RunDate") == F.lit(logical_date))
    )
    required_columns = {
        "customer_cells": (customer_cells, {"AccountNumber"}),
        "repeat_ad_exposure": (
            repeat_ad_exposure,
            {
                "AccountNumber",
                "AdSeen",
                "MultiSessionDownweightScore",
            },
        ),
        "ad_feedback": (
            ad_feedback,
            {"UniqueAdID", "IncARPSAdjPct"},
        ),
    }
    for name, (frame, columns) in required_columns.items():
        missing = sorted(columns.difference(frame.columns))
        if missing:
            raise ValueError(
                f"Foundation binding {name} is missing columns: "
                + ", ".join(missing)
            )
    return CandidateFoundationInputs(
        snapshot_id=snapshot_id,
        source_run_date=logical_date,
        customer_cells=customer_cells,
        repeat_ad_exposure=repeat_ad_exposure.drop(
            "CandidateFoundationSnapshotID",
            "RunDate",
        ),
        ad_feedback_metrics=ad_feedback.drop(
            "CandidateFoundationSnapshotID",
            "RunDate",
        ),
    )
