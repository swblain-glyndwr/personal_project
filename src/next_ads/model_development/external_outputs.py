"""Versioned adoption of model scores produced outside the generic trainer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import re
from typing import Any

from next_ads.common.delta_writes import schema_checksum
from next_ads.features.analytics_pctr_source import latest_delta_version
from next_ads.ranking.provider_signals import adapt_account_entity_scores


_EXACT_MODEL_URI = re.compile(r"models:/([^/]+)/([1-9][0-9]*)")


@dataclass(frozen=True)
class ExternalModelComponent:
    """One exact registered-model component used by an external scorer."""

    role: str
    model_uri: str
    expected_run_id: str

    def __post_init__(self) -> None:
        """Require a named component and immutable registered-model URI."""
        for field_name in ("role", "model_uri", "expected_run_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, value.strip())
        if _EXACT_MODEL_URI.fullmatch(self.model_uri) is None:
            raise ValueError(
                "External model components require an exact numeric model URI"
            )


@dataclass(frozen=True)
class ExternalScoreOutputReceipt:
    """Exact score output and model components accepted for adaptation."""

    receipt_id: str
    model_name: str
    provider_id: str
    source_table: str
    source_delta_version: int
    run_date: date
    row_count: int
    schema_checksum: str
    producing_run_id: str
    components: tuple[ExternalModelComponent, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        """Require complete evidence for an exact non-empty score output."""
        for field_name in (
            "receipt_id",
            "model_name",
            "provider_id",
            "source_table",
            "producing_run_id",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.source_delta_version < 0 or self.row_count < 1:
            raise ValueError("External score output needs a version and rows")
        if not isinstance(self.run_date, date) or isinstance(
            self.run_date, datetime
        ):
            raise ValueError("run_date must be a date")
        if re.fullmatch(r"[0-9a-f]{64}", self.schema_checksum) is None:
            raise ValueError("schema_checksum must be a SHA-256 digest")
        components = tuple(self.components)
        roles = [component.role for component in components]
        if not components or len(roles) != len(set(roles)):
            raise ValueError("External model component roles must be unique")
        object.__setattr__(self, "components", components)


def _receipt_id(
    *,
    model_name: str,
    provider_id: str,
    source_table: str,
    source_delta_version: int,
    run_date: date,
    producing_run_id: str,
    components: tuple[ExternalModelComponent, ...],
) -> str:
    payload = {
        "components": [
            {
                "expected_run_id": component.expected_run_id,
                "model_uri": component.model_uri,
                "role": component.role,
            }
            for component in components
        ],
        "model_name": model_name,
        "producing_run_id": producing_run_id,
        "provider_id": provider_id,
        "run_date": run_date.isoformat(),
        "source_delta_version": source_delta_version,
        "source_table": source_table,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def bind_external_score_output(
    spark: Any,
    *,
    model_name: str,
    provider_id: str,
    source_table: str,
    run_date: date,
    producing_run_id: str,
    components: tuple[ExternalModelComponent, ...],
    source_delta_version: int | None = None,
    run_date_column: str | None = "rundate",
) -> tuple[Any, ExternalScoreOutputReceipt]:
    """Read one external score output at an exact Delta version."""
    from pyspark.sql import functions as F

    version = (
        latest_delta_version(spark, source_table)
        if source_delta_version is None
        else source_delta_version
    )
    if version < 0:
        raise ValueError("source_delta_version cannot be negative")
    frame = spark.read.option("versionAsOf", version).table(source_table)
    if run_date_column is not None and run_date_column not in frame.columns:
        raise ValueError(
            f"External score output is missing {run_date_column}"
        )
    exact_output = (
        frame.where(F.to_date(F.col(run_date_column)) == F.lit(run_date))
        if run_date_column is not None
        else frame
    )
    row_count = exact_output.count()
    if row_count == 0:
        raise ValueError("External score output contains no rows for run_date")
    component_tuple = tuple(components)
    receipt = ExternalScoreOutputReceipt(
        receipt_id=_receipt_id(
            model_name=model_name,
            provider_id=provider_id,
            source_table=source_table,
            source_delta_version=version,
            run_date=run_date,
            producing_run_id=producing_run_id,
            components=component_tuple,
        ),
        model_name=model_name,
        provider_id=provider_id,
        source_table=source_table,
        source_delta_version=version,
        run_date=run_date,
        row_count=row_count,
        schema_checksum=schema_checksum(exact_output),
        producing_run_id=producing_run_id,
        components=component_tuple,
        created_at=datetime.now(timezone.utc),
    )
    return exact_output, receipt


def verify_external_model_components(
    client: Any,
    components: tuple[ExternalModelComponent, ...],
) -> None:
    """Prove every numeric model URI resolves to its recorded MLflow run."""
    for component in components:
        match = _EXACT_MODEL_URI.fullmatch(component.model_uri)
        if match is None:
            raise ValueError("External component model URI is not exact")
        model_name, version = match.groups()
        registered = client.get_model_version(model_name, version)
        if registered.run_id != component.expected_run_id:
            raise ValueError(
                f"External model component {component.role} resolves to "
                "a different MLflow run"
            )


def adapt_external_advert_scores(
    frame: Any,
    receipt: ExternalScoreOutputReceipt,
    *,
    provider_build_id: str,
    account_column: str,
    advert_column: str,
    raw_score_column: str,
    score_column: str,
) -> Any:
    """Map an accepted external output to account_entity_scores/v1."""
    return adapt_account_entity_scores(
        frame,
        provider_build_id=provider_build_id,
        provider_id=receipt.provider_id,
        entity_type="advert",
        run_date=receipt.run_date,
        account_column=account_column,
        entity_column=advert_column,
        raw_score_column=raw_score_column,
        score_column=score_column,
    )


__all__ = [
    "ExternalModelComponent",
    "ExternalScoreOutputReceipt",
    "adapt_external_advert_scores",
    "bind_external_score_output",
    "verify_external_model_components",
]
