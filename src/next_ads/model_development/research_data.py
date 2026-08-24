"""Leakage-safe, PII-reduced data frames for model research."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from functools import reduce
import hashlib
import json
from operator import or_
from typing import Any


TRAIN = "train"
VALIDATE = "validate"
TEST = "test"
RESEARCH_SPLITS = (TRAIN, VALIDATE, TEST)
RESEARCH_FRAME_COLUMNS = (
    "research_frame_id",
    "research_frame_attempt_id",
    "research_build_id",
    "research_attempt_id",
    "training_receipt_id",
    "row_id",
    "observation_date",
    "split",
    "label",
    "features_json",
    "slices_json",
    "created_at",
)

_IDENTITY_COLUMN_NAMES = frozenset(
    {
        "accountid",
        "accountnumber",
        "customerid",
        "customernumber",
        "email",
        "emailaddress",
        "exposureid",
        "rowid",
        "rowidhash",
        "rpid",
    }
)


def _normalized_column_name(value: str) -> str:
    return "".join(
        character for character in value.casefold() if character.isalnum()
    )


def validate_research_column_privacy(
    feature_columns: Iterable[str],
    slice_columns: Iterable[str],
) -> None:
    """Reject raw identity columns before values can enter persisted JSON."""
    unsafe = sorted(
        column
        for column in {*feature_columns, *slice_columns}
        if _normalized_column_name(column) in _IDENTITY_COLUMN_NAMES
    )
    if unsafe:
        raise ValueError(
            "Research features and slices cannot contain raw identity columns: "
            + ", ".join(unsafe)
        )


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _names(
    values: Iterable[str],
    field_name: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be a collection of names")
    result = tuple(_text(value, field_name) for value in values)
    if not result and not allow_empty:
        raise ValueError(f"{field_name} must contain at least one name")
    if len(result) != len(set(result)):
        raise ValueError(f"{field_name} must contain unique names")
    return result


def _dates(values: Iterable[str | date], field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be a collection of dates")
    result = []
    for value in values:
        try:
            parsed = (
                value
                if isinstance(value, date)
                else date.fromisoformat(_text(value, field_name))
            )
        except ValueError as exc:
            raise ValueError(
                f"{field_name} values must be YYYY-MM-DD"
            ) from exc
        result.append(parsed.isoformat())
    if not result:
        raise ValueError(f"{field_name} must contain at least one date")
    if len(result) != len(set(result)):
        raise ValueError(f"{field_name} must contain unique dates")
    return tuple(sorted(result))


@dataclass(frozen=True)
class ResearchFramePlan:
    """Exact columns and dates allowed in one persisted research frame."""

    observation_date_column: str
    label_column: str
    raw_key_columns: tuple[str, ...]
    feature_columns: tuple[str, ...]
    slice_columns: tuple[str, ...]
    train_dates: tuple[str | date, ...]
    validation_dates: tuple[str | date, ...]
    test_dates: tuple[str | date, ...]

    def __post_init__(self) -> None:
        """Reject ambiguous columns and overlapping temporal splits."""
        object.__setattr__(
            self,
            "observation_date_column",
            _text(self.observation_date_column, "observation_date_column"),
        )
        object.__setattr__(
            self,
            "label_column",
            _text(self.label_column, "label_column"),
        )
        for field_name in ("raw_key_columns", "feature_columns"):
            object.__setattr__(
                self,
                field_name,
                _names(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "slice_columns",
            _names(
                self.slice_columns,
                "slice_columns",
                allow_empty=True,
            ),
        )
        for field_name in ("train_dates", "validation_dates", "test_dates"):
            object.__setattr__(
                self,
                field_name,
                _dates(getattr(self, field_name), field_name),
            )
        model_values = set(self.feature_columns).union(self.slice_columns)
        raw_overlap = sorted(
            set(self.raw_key_columns).intersection(model_values)
        )
        if raw_overlap:
            raise ValueError(
                "raw keys overlap features or slices: "
                + ", ".join(raw_overlap)
            )
        validate_research_column_privacy(
            self.feature_columns,
            self.slice_columns,
        )
        reserved = {self.observation_date_column, self.label_column}
        collisions = sorted(
            reserved.intersection(
                {
                    *self.raw_key_columns,
                    *self.feature_columns,
                    *self.slice_columns,
                }
            )
        )
        if collisions:
            raise ValueError(
                "Observation date and label columns must have distinct roles: "
                + ", ".join(collisions)
            )
        split_dates = {
            TRAIN: set(self.train_dates),
            VALIDATE: set(self.validation_dates),
            TEST: set(self.test_dates),
        }
        for left, right in (
            (TRAIN, VALIDATE),
            (TRAIN, TEST),
            (VALIDATE, TEST),
        ):
            overlap = sorted(
                split_dates[left].intersection(split_dates[right])
            )
            if overlap:
                raise ValueError(
                    f"{left} and {right} dates overlap: " + ", ".join(overlap)
                )

    @property
    def checksum(self) -> str:
        """Return a deterministic identity for the packing contract."""
        payload = {
            "feature_columns": self.feature_columns,
            "label_column": self.label_column,
            "observation_date_column": self.observation_date_column,
            "raw_key_columns": self.raw_key_columns,
            "slice_columns": self.slice_columns,
            "test_dates": self.test_dates,
            "train_dates": self.train_dates,
            "validation_dates": self.validation_dates,
        }
        return hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class ResearchFrameSchemas:
    """Original Spark types needed to reconstruct packed values exactly."""

    feature_schema_json: str
    slice_schema_json: str

    def __post_init__(self) -> None:
        """Require valid Spark StructType JSON contracts."""
        from pyspark.sql.types import StructType

        for field_name in ("feature_schema_json", "slice_schema_json"):
            value = _text(getattr(self, field_name), field_name)
            try:
                parsed = StructType.fromJson(json.loads(value))
            except (TypeError, ValueError, KeyError) as exc:
                raise ValueError(
                    f"{field_name} must be valid Spark StructType JSON"
                ) from exc
            if field_name == "feature_schema_json" and not parsed.fields:
                raise ValueError("The research feature schema cannot be empty")
            object.__setattr__(self, field_name, value)


def declared_research_schemas(
    frame: Any,
    *,
    plan: ResearchFramePlan,
) -> ResearchFrameSchemas:
    """Capture the exact declared Spark types before keys are removed."""
    from pyspark.sql.types import StructType

    _require_columns(
        frame,
        (*plan.feature_columns, *plan.slice_columns),
    )
    features = StructType(
        [frame.schema[column] for column in plan.feature_columns]
    )
    slices = StructType(
        [frame.schema[column] for column in plan.slice_columns]
    )
    return ResearchFrameSchemas(
        feature_schema_json=features.json(),
        slice_schema_json=slices.json(),
    )


def _require_columns(frame: Any, columns: Iterable[str]) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        raise ValueError(
            "Research source is missing declared columns: "
            + ", ".join(missing)
        )


def _json_struct(columns: tuple[str, ...]) -> Any:
    from pyspark.sql import functions as F

    if not columns:
        return F.lit("{}")
    return F.to_json(
        F.struct(*[F.col(column).alias(column) for column in columns]),
        options={"ignoreNullFields": "false"},
    )


def pack_research_frame(
    frame: Any,
    *,
    plan: ResearchFramePlan,
    research_frame_id: str,
    research_frame_attempt_id: str,
    research_build_id: str,
    research_attempt_id: str,
    training_receipt_id: str,
) -> Any:
    """Pack declared values while dropping every raw observation key."""
    from pyspark.sql import functions as F

    identities = {
        "research_frame_id": _text(research_frame_id, "research_frame_id"),
        "research_frame_attempt_id": _text(
            research_frame_attempt_id,
            "research_frame_attempt_id",
        ),
        "research_build_id": _text(research_build_id, "research_build_id"),
        "research_attempt_id": _text(
            research_attempt_id,
            "research_attempt_id",
        ),
        "training_receipt_id": _text(
            training_receipt_id,
            "training_receipt_id",
        ),
    }
    source_columns = (
        plan.observation_date_column,
        plan.label_column,
        *plan.raw_key_columns,
        *plan.feature_columns,
        *plan.slice_columns,
    )
    _require_columns(frame, source_columns)
    observation_date = F.to_date(F.col(plan.observation_date_column))
    split = (
        F.when(observation_date.isin(*plan.train_dates), F.lit(TRAIN))
        .when(
            observation_date.isin(*plan.validation_dates),
            F.lit(VALIDATE),
        )
        .when(observation_date.isin(*plan.test_dates), F.lit(TEST))
    )
    row_identity = F.to_json(
        F.struct(
            F.lit(identities["research_frame_id"]).alias("namespace"),
            *[
                F.col(column).cast("string").alias(column)
                for column in plan.raw_key_columns
            ],
            observation_date.cast("string").alias("observation_date"),
        ),
        options={"ignoreNullFields": "false"},
    )
    packed = frame.select(
        *[
            F.lit(value).cast("string").alias(name)
            for name, value in identities.items()
        ],
        F.sha2(row_identity, 256).alias("row_id"),
        observation_date.alias("observation_date"),
        split.alias("split"),
        F.col(plan.label_column).cast("double").alias("label"),
        _json_struct(plan.feature_columns).alias("features_json"),
        _json_struct(plan.slice_columns).alias("slices_json"),
        F.current_timestamp().alias("created_at"),
    )
    missing_raw_identity = reduce(
        or_,
        (F.col(column).isNull() for column in plan.raw_key_columns),
    )
    invalid = (
        frame.withColumn("_packed_observation_date", observation_date)
        .withColumn("_packed_split", split)
        .where(
            missing_raw_identity
            | F.col("_packed_observation_date").isNull()
            | F.col("_packed_split").isNull()
            | F.col(plan.label_column).cast("double").isNull()
        )
        .limit(1)
        .count()
    )
    if invalid:
        raise ValueError(
            "Every research row needs a valid date, declared split, label and "
            "hashed row identity"
        )
    return packed.select(*RESEARCH_FRAME_COLUMNS)


def unpack_research_frame(
    frame: Any,
    *,
    schemas: ResearchFrameSchemas,
) -> Any:
    """Restore declared features and slices with their original Spark types."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    _require_columns(frame, RESEARCH_FRAME_COLUMNS)
    feature_schema = StructType.fromJson(
        json.loads(schemas.feature_schema_json)
    )
    slice_schema = StructType.fromJson(json.loads(schemas.slice_schema_json))
    decoded = frame.withColumn(
        "_features",
        F.from_json(F.col("features_json"), feature_schema),
    ).withColumn(
        "_slices",
        F.from_json(F.col("slices_json"), slice_schema),
    )
    metadata = tuple(
        column
        for column in RESEARCH_FRAME_COLUMNS
        if column not in {"features_json", "slices_json"}
    )
    feature_names = {field.name for field in feature_schema.fields}
    return decoded.select(
        *[F.col(column) for column in metadata],
        *[
            F.col("_features").getField(field.name).alias(field.name)
            for field in feature_schema.fields
        ],
        *[
            F.col("_slices").getField(field.name).alias(field.name)
            for field in slice_schema.fields
            if field.name not in feature_names
        ],
    )


def training_partition(frame: Any) -> Any:
    """Expose only train rows to candidate fitting."""
    from pyspark.sql import functions as F

    return frame.where(F.col("split") == F.lit(TRAIN))


def validation_partition(frame: Any) -> Any:
    """Expose only validation rows for candidate comparison."""
    from pyspark.sql import functions as F

    return frame.where(F.col("split") == F.lit(VALIDATE))


def automl_discovery_partition(frame: Any) -> Any:
    """Exclude the untouched test period from AutoML discovery."""
    from pyspark.sql import functions as F

    return frame.where(F.col("split").isin(TRAIN, VALIDATE))


def selected_test_partition(
    frame: Any,
    *,
    selection_decision_id: str,
) -> Any:
    """Release test rows only after an exact selection decision exists."""
    from pyspark.sql import functions as F

    _text(selection_decision_id, "selection_decision_id")
    return frame.where(F.col("split") == F.lit(TEST))


__all__ = [
    "RESEARCH_FRAME_COLUMNS",
    "RESEARCH_SPLITS",
    "ResearchFramePlan",
    "ResearchFrameSchemas",
    "TEST",
    "TRAIN",
    "VALIDATE",
    "automl_discovery_partition",
    "declared_research_schemas",
    "pack_research_frame",
    "selected_test_partition",
    "training_partition",
    "unpack_research_frame",
    "validate_research_column_privacy",
    "validation_partition",
]
