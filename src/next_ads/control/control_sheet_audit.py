from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from functools import reduce
from typing import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructType


WARNING = "WARNING"
REVIEW = "REVIEW"

DEFAULT_AUDIENCE_VALUES = ("TRUE", "FALSE")
DEFAULT_AD_VARIANTS = ("newborn", "toddler", "younger", "older", "teen")
V2_PAGE_TYPES = (
    "ProductListingPage",
    "ForYouPage",
    "CheckoutPage",
    "ShoppingBagPage",
    "HomePage",
)


_MESSAGES = {
    "BLANK_UNIQUE_AD_ID": "Rows have no usable UniqueAdID.",
    "DUPLICATE_UNIQUE_AD_ID": (
        "UniqueAdID occurs on more than one current raw row."
    ),
    "BLANK_CMS_PAGE_ID": (
        "An ad in the executable date window has no CMSPageID."
    ),
    "MALFORMED_START_DATE": "StartDate is blank or cannot be parsed.",
    "MALFORMED_END_DATE": "EndDate is blank or cannot be parsed.",
    "START_AFTER_END_DATE": "StartDate is later than EndDate.",
    "INVALID_STATUS": (
        "Status is not one of the values understood by the control sheet."
    ),
    "STATUS_INACTIVE_INSIDE_DATE_WINDOW": (
        "Status says inactive but the executable date window includes the ad."
    ),
    "INVALID_PLACEMENT_FLAG": (
        "A placement flag is not blank, TRUE or FALSE exactly."
    ),
    "ACTIVE_WITH_NO_SELECTED_PLACEMENT": (
        "An ad is in the executable date window but selects no placement."
    ),
    "INVALID_AUDIENCE_ONLY": (
        "AudienceOnly is not blank, TRUE or FALSE exactly."
    ),
    "UNKNOWN_AD_VARIANT": (
        "AdVariant is not blank or a currently executable age variant."
    ),
    "INVALID_PROCESSED_SCOPE": (
        "A processed row has a blank or unexpected assignment scope."
    ),
    "PROCESSED_OUT_OF_WINDOW": (
        "A processed row is outside the executable date window."
    ),
    "NULL_PROCESSED_KEY": (
        "A processed row has a blank UniqueAdID or assignment scope."
    ),
    "DUPLICATE_PROCESSED_KEY": (
        "A processed UniqueAdID and assignment scope occurs more than once."
    ),
    "SHARED_CMS_PAGE_ID": (
        "Different UniqueAdIDs share a CMSPageID; review only, because shared "
        "CMS content is not itself invalid."
    ),
    "AMBIGUOUS_CMS_DECISION_SIGNATURE": (
        "Different UniqueAdIDs use the same CMSPageID, scope and executable "
        "targeting signature."
    ),
    "CMS_NOT_IN_LATEST_PULL": (
        "An executable CMSPageID has no row in the latest CMS pull."
    ),
    "CMS_CONTENT_MISSING": (
        "The latest CMS pull has a row but no usable external page content."
    ),
    "CMS_EXTERNAL_ID_MISMATCH": (
        "CMS externalPageId does not match the control-sheet CMSPageID."
    ),
    "CMS_TARGET_URL_MISSING": (
        "CMS content exists but has no first-item target for the control URL."
    ),
    "CMS_TARGET_URL_MISMATCH": (
        "The first CMS item target does not match the control-sheet URL."
    ),
    "CONTROL_AD_ADDED": "UniqueAdID is new compared with the previous raw input.",
    "CONTROL_AD_REMOVED": (
        "UniqueAdID was present in the previous raw input but is now absent."
    ),
    "CONTROL_AD_CHANGED": (
        "At least one raw control value changed for an existing UniqueAdID."
    ),
    "PROCESSED_ROUTE_ADDED": (
        "UniqueAdID and scope is new compared with the previous processed input."
    ),
    "PROCESSED_ROUTE_REMOVED": (
        "UniqueAdID and scope was previously processed but is now absent."
    ),
    "PROCESSED_ROUTE_SET_CHANGED": (
        "The processed scope set changed for an existing UniqueAdID."
    ),
    "SCOPE_DROPPED_TO_ZERO": (
        "A previously populated processed scope now contains no rows."
    ),
}


@dataclass(frozen=True)
class ControlSheetAuditSpec:
    """Route-specific facts needed to audit a control-sheet build."""

    route: str
    run_date: date
    placement_columns: tuple[str, ...]
    expected_scopes: tuple[str, ...]
    scope_column: str | None = None
    date_format: str = "dd/MM/yyyy"
    active_day_offset: int = 1
    max_examples: int = 10
    allowed_audience_values: tuple[str, ...] = DEFAULT_AUDIENCE_VALUES
    allowed_ad_variants: tuple[str, ...] = DEFAULT_AD_VARIANTS

    def __post_init__(self) -> None:
        """Validate and normalise the immutable route contract."""
        route = self.route.strip().lower()
        if route not in {"v1", "v2"}:
            raise ValueError("route must be 'v1' or 'v2'")
        if self.max_examples <= 0:
            raise ValueError("max_examples must be greater than zero")
        if not self.placement_columns:
            raise ValueError("placement_columns must not be empty")
        if not self.expected_scopes:
            raise ValueError("expected_scopes must not be empty")

        object.__setattr__(self, "route", route)
        object.__setattr__(
            self,
            "placement_columns",
            tuple(dict.fromkeys(self.placement_columns)),
        )
        object.__setattr__(
            self,
            "expected_scopes",
            tuple(dict.fromkeys(self.expected_scopes)),
        )
        object.__setattr__(
            self,
            "allowed_audience_values",
            tuple(dict.fromkeys(self.allowed_audience_values)),
        )
        object.__setattr__(
            self,
            "allowed_ad_variants",
            tuple(dict.fromkeys(self.allowed_ad_variants)),
        )
        if self.scope_column is None:
            object.__setattr__(
                self,
                "scope_column",
                "Location" if route == "v1" else "PageType",
            )

    @property
    def effective_date(self) -> date:
        """Date used by the existing control-sheet executable window."""
        return self.run_date + timedelta(days=self.active_day_offset)


@dataclass(frozen=True)
class ControlSheetAuditFinding:
    """One deterministic, warning-only control-sheet finding."""

    severity: str
    code: str
    count: int
    examples: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class ControlSheetAuditReport:
    """Read-only audit result for one control-sheet route."""

    route: str
    effective_date: date
    findings: tuple[ControlSheetAuditFinding, ...]

    @property
    def has_warnings(self) -> bool:
        """Whether any review or warning facts were found."""
        return bool(self.findings)

    @property
    def warning_count(self) -> int:
        """Total number of affected facts across all finding types."""
        return sum(finding.count for finding in self.findings)

    def render(self) -> str:
        """Render the full deterministic report."""
        header = (
            f"Control-sheet audit route={self.route} "
            f"effective_date={self.effective_date.isoformat()} "
            f"finding_types={len(self.findings)} "
            f"affected_facts={self.warning_count}. "
            "Warning-only: no data was changed or blocked."
        )
        lines = [header]
        for finding in self.findings:
            examples = ", ".join(finding.examples) or "<none>"
            lines.append(
                f"[{finding.severity}] {finding.code} "
                f"count={finding.count}: {finding.message} "
                f"Examples: {examples}"
            )
        return "\n".join(lines)

    def compact_message(self, max_chars: int = 3500) -> str:
        """Render one bounded message suitable for an input-warning channel."""
        if max_chars <= 0:
            raise ValueError("max_chars must be greater than zero")

        prefix = (
            f"Control-sheet audit {self.route} "
            f"for {self.effective_date.isoformat()}: "
            f"{len(self.findings)} finding types, "
            f"{self.warning_count} affected facts. "
        )
        if not self.findings:
            message = prefix + "No warnings. No data was changed or blocked."
            return message[:max_chars]

        suffix = " Warning-only: no data was changed or blocked."
        parts: list[str] = []
        for index, finding in enumerate(self.findings):
            examples = ", ".join(finding.examples[:2])
            part = f"{finding.code}={finding.count}"
            if examples:
                part += f" [{examples}]"
            remaining = len(self.findings) - index - 1
            candidate_parts = [*parts, part]
            remainder = f"; +{remaining} more" if remaining else ""
            candidate = (
                prefix + "; ".join(candidate_parts) + remainder + suffix
            )
            if len(candidate) > max_chars:
                break
            parts.append(part)

        omitted = len(self.findings) - len(parts)
        body = "; ".join(parts)
        if omitted:
            body += ("; " if body else "") + f"+{omitted} more"
        message = prefix + body + suffix
        return message[:max_chars]


def _require_columns(
    df: DataFrame,
    frame_name: str,
    required_columns: Sequence[str],
) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(
            f"{frame_name} is missing required columns: {', '.join(missing)}"
        )


def _text(column_name: str):
    return F.trim(F.coalesce(F.col(column_name).cast("string"), F.lit("")))


def _labelled(label: str, column):
    return F.concat(F.lit(f"{label}="), F.coalesce(column.cast("string"), F.lit("")))


def _example(*parts):
    return F.concat_ws("|", *parts)


def _parsed_date(df: DataFrame, column_name: str, date_format: str):
    field = next(field for field in df.schema.fields if field.name == column_name)
    if isinstance(field.dataType, DateType):
        return F.col(column_name)
    return F.try_to_timestamp(
        F.col(column_name),
        F.lit(date_format),
    ).cast("date")


def _finding_frame(
    rows: DataFrame,
    *,
    severity: str,
    code: str,
    example,
    max_examples: int,
) -> DataFrame:
    example_counts = (
        rows.select(
            F.coalesce(example.cast("string"), F.lit("<null>")).alias("_example")
        )
        .groupBy("_example")
        .agg(F.count(F.lit(1)).alias("_occurrences"))
    )
    ranked = example_counts.withColumn(
        "_example_rank",
        F.row_number().over(Window.orderBy(F.col("_example").asc())),
    )
    return (
        ranked.agg(
            F.sum("_occurrences").cast("long").alias("count"),
            F.sort_array(
                F.collect_list(
                    F.when(
                        F.col("_example_rank") <= F.lit(max_examples),
                        F.col("_example"),
                    )
                )
            ).alias("examples"),
        )
        .where(F.col("count") > 0)
        .select(
            F.lit(severity).alias("severity"),
            F.lit(code).alias("code"),
            "count",
            "examples",
            F.lit(_MESSAGES[code]).alias("message"),
        )
    )


def _raw_with_audit_columns(
    raw_current: DataFrame,
    spec: ControlSheetAuditSpec,
) -> DataFrame:
    start_date = _parsed_date(raw_current, "StartDate", spec.date_format)
    end_date = _parsed_date(raw_current, "EndDate", spec.date_format)
    effective_date = F.lit(spec.effective_date)
    has_selected_placement = reduce(
        lambda left, right: left | right,
        [
            _text(column_name) == F.lit("TRUE")
            for column_name in spec.placement_columns
        ],
    )
    return (
        raw_current.withColumn("_UniqueAdID", _text("UniqueAdID"))
        .withColumn("_CMSPageID", _text("CMSPageID"))
        .withColumn("_ControlURL", _text("URL"))
        .withColumn("_StartDate", start_date)
        .withColumn("_EndDate", end_date)
        .withColumn(
            "_InWindow",
            start_date.isNotNull()
            & end_date.isNotNull()
            & (start_date <= effective_date)
            & (end_date >= effective_date),
        )
        .withColumn("_Status", F.lower(_text("Status")))
        .withColumn("_HasSelectedPlacement", has_selected_placement)
    )


def _raw_finding_frames(
    raw: DataFrame,
    spec: ControlSheetAuditSpec,
) -> list[DataFrame]:
    frames: list[DataFrame] = []
    ad_example = _labelled("UniqueAdID", F.col("_UniqueAdID"))

    frames.append(
        _finding_frame(
            raw.where(F.col("_UniqueAdID") == ""),
            severity=WARNING,
            code="BLANK_UNIQUE_AD_ID",
            example=F.lit("UniqueAdID=<blank>"),
            max_examples=spec.max_examples,
        )
    )

    duplicate_ids = (
        raw.where(F.col("_UniqueAdID") != "")
        .groupBy("_UniqueAdID")
        .agg(F.count(F.lit(1)).alias("_rows"))
        .where(F.col("_rows") > 1)
    )
    frames.append(
        _finding_frame(
            duplicate_ids,
            severity=WARNING,
            code="DUPLICATE_UNIQUE_AD_ID",
            example=_example(
                _labelled("UniqueAdID", F.col("_UniqueAdID")),
                _labelled("rows", F.col("_rows")),
            ),
            max_examples=spec.max_examples,
        )
    )

    frames.extend(
        [
            _finding_frame(
                raw.where(F.col("_StartDate").isNull()),
                severity=WARNING,
                code="MALFORMED_START_DATE",
                example=_example(
                    ad_example,
                    _labelled("StartDate", _text("StartDate")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(F.col("_EndDate").isNull()),
                severity=WARNING,
                code="MALFORMED_END_DATE",
                example=_example(
                    ad_example,
                    _labelled("EndDate", _text("EndDate")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(
                    F.col("_StartDate").isNotNull()
                    & F.col("_EndDate").isNotNull()
                    & (F.col("_StartDate") > F.col("_EndDate"))
                ),
                severity=WARNING,
                code="START_AFTER_END_DATE",
                example=_example(
                    ad_example,
                    _labelled("StartDate", _text("StartDate")),
                    _labelled("EndDate", _text("EndDate")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(
                    F.col("_InWindow")
                    & ~F.col("_Status").isin("", "active", "inactive")
                ),
                severity=WARNING,
                code="INVALID_STATUS",
                example=_example(
                    ad_example,
                    _labelled("Status", _text("Status")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(
                    (F.col("_Status") == "inactive") & F.col("_InWindow")
                ),
                severity=WARNING,
                code="STATUS_INACTIVE_INSIDE_DATE_WINDOW",
                example=ad_example,
                max_examples=spec.max_examples,
            ),
        ]
    )

    placement_values = raw.select(
        "_UniqueAdID",
        "_InWindow",
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(column_name).alias("_Placement"),
                        _text(column_name).alias("_Value"),
                    )
                    for column_name in spec.placement_columns
                ]
            )
        ).alias("_Flag"),
    ).select(
        "_UniqueAdID",
        "_InWindow",
        F.col("_Flag._Placement").alias("_Placement"),
        F.col("_Flag._Value").alias("_Value"),
    )
    frames.append(
        _finding_frame(
            placement_values.where(
                F.col("_InWindow")
                & ~F.col("_Value").isin("", "TRUE", "FALSE")
            ),
            severity=WARNING,
            code="INVALID_PLACEMENT_FLAG",
            example=_example(
                _labelled("UniqueAdID", F.col("_UniqueAdID")),
                _labelled("placement", F.col("_Placement")),
                _labelled("value", F.col("_Value")),
            ),
            max_examples=spec.max_examples,
        )
    )

    frames.append(
        _finding_frame(
            raw.where(
                F.col("_InWindow") & ~F.col("_HasSelectedPlacement")
            ),
            severity=WARNING,
            code="ACTIVE_WITH_NO_SELECTED_PLACEMENT",
            example=ad_example,
            max_examples=spec.max_examples,
        )
    )

    frames.extend(
        [
            _finding_frame(
                raw.where(
                    F.col("_InWindow")
                    &
                    (_text("AudienceOnly") != "")
                    & ~_text("AudienceOnly").isin(
                        *spec.allowed_audience_values
                    )
                ),
                severity=WARNING,
                code="INVALID_AUDIENCE_ONLY",
                example=_example(
                    ad_example,
                    _labelled("AudienceOnly", _text("AudienceOnly")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(
                    F.col("_InWindow")
                    &
                    (_text("AdVariant") != "")
                    & ~_text("AdVariant").isin(*spec.allowed_ad_variants)
                ),
                severity=WARNING,
                code="UNKNOWN_AD_VARIANT",
                example=_example(
                    ad_example,
                    _labelled("AdVariant", _text("AdVariant")),
                ),
                max_examples=spec.max_examples,
            ),
            _finding_frame(
                raw.where(
                    F.col("_InWindow")
                    & F.col("_HasSelectedPlacement")
                    & (F.col("_CMSPageID") == "")
                ),
                severity=WARNING,
                code="BLANK_CMS_PAGE_ID",
                example=ad_example,
                max_examples=spec.max_examples,
            ),
        ]
    )

    shared_cms = (
        raw.where(
            F.col("_InWindow")
            & F.col("_HasSelectedPlacement")
            & (F.col("_UniqueAdID") != "")
            & (F.col("_CMSPageID") != "")
        )
        .groupBy("_CMSPageID")
        .agg(F.countDistinct("_UniqueAdID").alias("_AdCount"))
        .where(F.col("_AdCount") > 1)
    )
    frames.append(
        _finding_frame(
            shared_cms,
            severity=REVIEW,
            code="SHARED_CMS_PAGE_ID",
            example=_example(
                _labelled("CMSPageID", F.col("_CMSPageID")),
                _labelled("UniqueAdIDs", F.col("_AdCount")),
            ),
            max_examples=spec.max_examples,
        )
    )
    return frames


def _processed_with_audit_columns(
    processed_current: DataFrame,
    spec: ControlSheetAuditSpec,
) -> DataFrame:
    scope_column = str(spec.scope_column)
    return (
        processed_current.withColumn("_UniqueAdID", _text("UniqueAdID"))
        .withColumn("_CMSPageID", _text("CMSPageID"))
        .withColumn("_Scope", _text(scope_column))
        .withColumn(
            "_StartDate",
            _parsed_date(processed_current, "StartDate", spec.date_format),
        )
        .withColumn(
            "_EndDate",
            _parsed_date(processed_current, "EndDate", spec.date_format),
        )
    )


def _processed_finding_frames(
    processed: DataFrame,
    spec: ControlSheetAuditSpec,
) -> list[DataFrame]:
    key_example = _example(
        _labelled("UniqueAdID", F.col("_UniqueAdID")),
        _labelled(str(spec.scope_column), F.col("_Scope")),
    )
    effective_date = F.lit(spec.effective_date)

    invalid_scope = processed.where(
        (F.col("_Scope") == "")
        | ~F.col("_Scope").isin(*spec.expected_scopes)
    )
    out_of_window = processed.where(
        F.col("_StartDate").isNull()
        | F.col("_EndDate").isNull()
        | (F.col("_StartDate") > effective_date)
        | (F.col("_EndDate") < effective_date)
    )
    null_key = processed.where(
        (F.col("_UniqueAdID") == "") | (F.col("_Scope") == "")
    )
    duplicate_key = (
        processed.groupBy("_UniqueAdID", "_Scope")
        .agg(F.count(F.lit(1)).alias("_rows"))
        .where(F.col("_rows") > 1)
    )

    decision_columns = [
        column_name
        for column_name in (
            "AudienceOnly",
            "AdVariant",
            "AlgoDivision",
            "TradeDivision",
            "Brand",
            "Segment",
            "AdDriver",
            "TargetingCriteria",
            "Items",
            "Tags",
            "Themes",
            "ClusterID",
        )
        if column_name in processed.columns
    ]
    ambiguous = (
        processed.where(
            (F.col("_UniqueAdID") != "")
            & (F.col("_CMSPageID") != "")
            & (F.col("_Scope") != "")
        )
        .withColumn(
            "_AudienceSignature",
            F.lower(_text("AudienceOnly")),
        )
        .withColumn(
            "_VariantSignature",
            F.lower(_text("AdVariant")),
        )
        .withColumn(
            "_DecisionSignature",
            F.sha2(
                F.to_json(
                    F.struct(
                        *[
                            F.lower(_text(column_name)).alias(column_name)
                            for column_name in decision_columns
                        ]
                    )
                ),
                256,
            ),
        )
        .groupBy(
            "_CMSPageID",
            "_Scope",
            "_AudienceSignature",
            "_VariantSignature",
            "_DecisionSignature",
        )
        .agg(F.countDistinct("_UniqueAdID").alias("_AdCount"))
        .where(F.col("_AdCount") > 1)
    )

    return [
        _finding_frame(
            invalid_scope,
            severity=WARNING,
            code="INVALID_PROCESSED_SCOPE",
            example=key_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            out_of_window,
            severity=WARNING,
            code="PROCESSED_OUT_OF_WINDOW",
            example=key_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            null_key,
            severity=WARNING,
            code="NULL_PROCESSED_KEY",
            example=key_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            duplicate_key,
            severity=WARNING,
            code="DUPLICATE_PROCESSED_KEY",
            example=_example(
                _labelled("UniqueAdID", F.col("_UniqueAdID")),
                _labelled(str(spec.scope_column), F.col("_Scope")),
                _labelled("rows", F.col("_rows")),
            ),
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            ambiguous,
            severity=WARNING,
            code="AMBIGUOUS_CMS_DECISION_SIGNATURE",
            example=_example(
                _labelled("CMSPageID", F.col("_CMSPageID")),
                _labelled(str(spec.scope_column), F.col("_Scope")),
                _labelled("audience", F.col("_AudienceSignature")),
                _labelled("variant", F.col("_VariantSignature")),
                _labelled(
                    "targeting",
                    F.substring(F.col("_DecisionSignature"), 1, 12),
                ),
                _labelled("UniqueAdIDs", F.col("_AdCount")),
            ),
            max_examples=spec.max_examples,
        ),
    ]


def _cms_external_page_id(cms_latest: DataFrame):
    if "externalPageId" in cms_latest.columns:
        return _text("externalPageId")
    if "cms_data" not in cms_latest.columns:
        raise ValueError(
            "cms_latest must contain cms_data or externalPageId"
        )

    cms_data_type = next(
        field.dataType
        for field in cms_latest.schema.fields
        if field.name == "cms_data"
    )
    if isinstance(cms_data_type, StringType):
        return F.trim(
            F.coalesce(
                F.get_json_object(F.col("cms_data"), "$.data.externalPageId"),
                F.lit(""),
            )
        )
    if isinstance(cms_data_type, StructType):
        return F.trim(
            F.coalesce(
                F.col("cms_data.data.externalPageId").cast("string"),
                F.lit(""),
            )
        )
    raise ValueError(
        "cms_latest.cms_data must be a JSON string or compatible struct"
    )


def _cms_content_title(cms_latest: DataFrame):
    if "title" in cms_latest.columns:
        return _text("title")
    if "cms_data" not in cms_latest.columns:
        return _cms_external_page_id(cms_latest)

    cms_data_type = next(
        field.dataType
        for field in cms_latest.schema.fields
        if field.name == "cms_data"
    )
    if isinstance(cms_data_type, StringType):
        return F.trim(
            F.coalesce(
                F.get_json_object(F.col("cms_data"), "$.data.title"),
                F.lit(""),
            )
        )
    if isinstance(cms_data_type, StructType):
        return F.trim(
            F.coalesce(
                F.col("cms_data.data.title").cast("string"),
                F.lit(""),
            )
        )
    raise ValueError(
        "cms_latest.cms_data must be a JSON string or compatible struct"
    )


def _cms_target_url(cms_latest: DataFrame):
    if "target" in cms_latest.columns:
        return _text("target")
    if "cms_data" not in cms_latest.columns:
        return F.lit("")

    cms_data_type = next(
        field.dataType
        for field in cms_latest.schema.fields
        if field.name == "cms_data"
    )
    if isinstance(cms_data_type, StringType):
        return F.trim(
            F.coalesce(
                F.get_json_object(
                    F.col("cms_data"),
                    "$.data.placements[0].content[0].items[0].target",
                ),
                F.lit(""),
            )
        )
    if isinstance(cms_data_type, StructType):
        return F.trim(
            F.coalesce(
                F.col(
                    "cms_data.data.placements"
                    "[0].content[0].items[0].target"
                ).cast("string"),
                F.lit(""),
            )
        )
    raise ValueError(
        "cms_latest.cms_data must be a JSON string or compatible struct"
    )


def _cms_finding_frames(
    raw: DataFrame,
    cms_latest: DataFrame,
    spec: ControlSheetAuditSpec,
) -> list[DataFrame]:
    cms = (
        cms_latest.withColumn("_CMSPageID", _text("CMSPageID"))
        .withColumn("_ExternalPageID", _cms_external_page_id(cms_latest))
        .withColumn("_TargetURL", _cms_target_url(cms_latest))
        .withColumn("_ContentTitle", _cms_content_title(cms_latest))
        .where(F.col("_CMSPageID") != "")
        .groupBy("_CMSPageID")
        .agg(
            F.count(F.lit(1)).alias("_CMSRows"),
            F.max(
                F.when(
                    (F.col("_ExternalPageID") != "")
                    & (F.col("_ContentTitle") != ""),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("_HasContent"),
            F.max(
                F.when(
                    (F.col("_ExternalPageID") != "")
                    & (F.col("_ExternalPageID") == F.col("_CMSPageID")),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("_HasMatchingExternalID"),
            F.sort_array(
                F.collect_set(
                    F.when(
                        F.col("_TargetURL") != "",
                        F.col("_TargetURL"),
                    )
                )
            ).alias("_TargetURLs"),
        )
    )

    executable_refs = (
        raw.where(
            F.col("_InWindow")
            & F.col("_HasSelectedPlacement")
            & (F.col("_CMSPageID") != "")
        )
        .groupBy("_UniqueAdID", "_CMSPageID")
        .agg(F.count(F.lit(1)).alias("_RawRows"))
        .join(cms, on="_CMSPageID", how="left")
    )
    example = _example(
        _labelled("UniqueAdID", F.col("_UniqueAdID")),
        _labelled("CMSPageID", F.col("_CMSPageID")),
    )
    target_example = _example(
        example,
        _labelled("control_url", F.col("_ControlURL")),
        _labelled(
            "cms_targets",
            F.concat_ws(",", F.slice(F.col("_TargetURLs"), 1, 2)),
        ),
    )
    return [
        _finding_frame(
            executable_refs.where(F.col("_CMSRows").isNull()),
            severity=WARNING,
            code="CMS_NOT_IN_LATEST_PULL",
            example=example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            executable_refs.where(
                F.col("_CMSRows").isNotNull()
                & (F.col("_HasContent") == F.lit(0))
            ),
            severity=WARNING,
            code="CMS_CONTENT_MISSING",
            example=example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            executable_refs.where(
                F.col("_CMSRows").isNotNull()
                & (F.col("_HasContent") == F.lit(1))
                & (F.col("_HasMatchingExternalID") == F.lit(0))
            ),
            severity=WARNING,
            code="CMS_EXTERNAL_ID_MISMATCH",
            example=example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            executable_refs.where(
                (F.col("_ControlURL") != "")
                & F.col("_CMSRows").isNotNull()
                & (F.col("_HasContent") == F.lit(1))
                & (
                    F.coalesce(
                        F.size(F.col("_TargetURLs")),
                        F.lit(0),
                    )
                    == 0
                )
            ),
            severity=WARNING,
            code="CMS_TARGET_URL_MISSING",
            example=target_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            executable_refs.where(
                (F.col("_ControlURL") != "")
                & F.col("_CMSRows").isNotNull()
                & (F.col("_HasContent") == F.lit(1))
                & (
                    F.coalesce(
                        F.size(F.col("_TargetURLs")),
                        F.lit(0),
                    )
                    > 0
                )
                & ~F.array_contains(
                    F.col("_TargetURLs"),
                    F.col("_ControlURL"),
                )
            ),
            severity=WARNING,
            code="CMS_TARGET_URL_MISMATCH",
            example=target_example,
            max_examples=spec.max_examples,
        ),
    ]


def _normalised_ids(df: DataFrame) -> DataFrame:
    return (
        df.select(_text("UniqueAdID").alias("_UniqueAdID"))
        .where(F.col("_UniqueAdID") != "")
        .groupBy("_UniqueAdID")
        .agg(F.count(F.lit(1)).alias("_SourceRows"))
    )


def _canonical_raw_by_id(
    df: DataFrame,
    shared_columns: Sequence[str],
) -> DataFrame:
    signature_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("")).alias(
            column_name
        )
        for column_name in shared_columns
    ]
    return (
        df.withColumn("_UniqueAdID", _text("UniqueAdID"))
        .where(F.col("_UniqueAdID") != "")
        .withColumn(
            "_Signature",
            F.sha2(F.to_json(F.struct(*signature_columns)), 256),
        )
        .groupBy("_UniqueAdID")
        .agg(
            F.sort_array(F.collect_set("_Signature")).alias("_Signatures")
        )
    )


def _raw_change_finding_frames(
    raw_current: DataFrame,
    previous_raw: DataFrame,
    spec: ControlSheetAuditSpec,
) -> list[DataFrame]:
    current_ids = _normalised_ids(raw_current)
    previous_ids = _normalised_ids(previous_raw)
    added = current_ids.join(previous_ids, on="_UniqueAdID", how="left_anti")
    removed = previous_ids.join(current_ids, on="_UniqueAdID", how="left_anti")

    excluded = {"UniqueAdID", "rundate"}
    shared_columns = sorted(
        (set(raw_current.columns) & set(previous_raw.columns)) - excluded
    )
    if shared_columns:
        current_signatures = _canonical_raw_by_id(
            raw_current,
            shared_columns,
        )
        previous_signatures = _canonical_raw_by_id(
            previous_raw,
            shared_columns,
        )
        changed = (
            current_signatures.alias("current")
            .join(
                previous_signatures.alias("previous"),
                on="_UniqueAdID",
                how="inner",
            )
            .where(
                F.col("current._Signatures")
                != F.col("previous._Signatures")
            )
            .select("_UniqueAdID")
        )
    else:
        changed = current_ids.limit(0).select("_UniqueAdID")

    example = _labelled("UniqueAdID", F.col("_UniqueAdID"))
    return [
        _finding_frame(
            added,
            severity=REVIEW,
            code="CONTROL_AD_ADDED",
            example=example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            removed,
            severity=REVIEW,
            code="CONTROL_AD_REMOVED",
            example=example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            changed,
            severity=REVIEW,
            code="CONTROL_AD_CHANGED",
            example=example,
            max_examples=spec.max_examples,
        ),
    ]


def _processed_routes(
    df: DataFrame,
    scope_column: str,
) -> DataFrame:
    return (
        df.select(
            _text("UniqueAdID").alias("_UniqueAdID"),
            _text(scope_column).alias("_Scope"),
        )
        .where(
            (F.col("_UniqueAdID") != "") & (F.col("_Scope") != "")
        )
        .groupBy("_UniqueAdID", "_Scope")
        .agg(F.count(F.lit(1)).alias("_SourceRows"))
    )


def _route_sets(routes: DataFrame) -> DataFrame:
    return routes.groupBy("_UniqueAdID").agg(
        F.sort_array(F.collect_set("_Scope")).alias("_Scopes")
    )


def _processed_change_finding_frames(
    processed_current: DataFrame,
    previous_processed: DataFrame,
    spec: ControlSheetAuditSpec,
) -> list[DataFrame]:
    scope_column = str(spec.scope_column)
    current_routes = _processed_routes(processed_current, scope_column)
    previous_routes = _processed_routes(previous_processed, scope_column)

    added = current_routes.join(
        previous_routes,
        on=["_UniqueAdID", "_Scope"],
        how="left_anti",
    )
    removed = previous_routes.join(
        current_routes,
        on=["_UniqueAdID", "_Scope"],
        how="left_anti",
    )
    changed_sets = (
        _route_sets(current_routes)
        .alias("current")
        .join(
            _route_sets(previous_routes).alias("previous"),
            on="_UniqueAdID",
            how="inner",
        )
        .where(F.col("current._Scopes") != F.col("previous._Scopes"))
        .select("_UniqueAdID")
    )
    dropped_scopes = (
        previous_routes.select("_Scope")
        .groupBy("_Scope")
        .agg(F.count(F.lit(1)).alias("_PreviousRows"))
        .join(
            current_routes.select("_Scope")
            .groupBy("_Scope")
            .agg(F.count(F.lit(1)).alias("_CurrentRows")),
            on="_Scope",
            how="left",
        )
        .where(F.coalesce(F.col("_CurrentRows"), F.lit(0)) == 0)
    )

    route_example = _example(
        _labelled("UniqueAdID", F.col("_UniqueAdID")),
        _labelled(scope_column, F.col("_Scope")),
    )
    return [
        _finding_frame(
            added,
            severity=REVIEW,
            code="PROCESSED_ROUTE_ADDED",
            example=route_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            removed,
            severity=REVIEW,
            code="PROCESSED_ROUTE_REMOVED",
            example=route_example,
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            changed_sets,
            severity=REVIEW,
            code="PROCESSED_ROUTE_SET_CHANGED",
            example=_labelled("UniqueAdID", F.col("_UniqueAdID")),
            max_examples=spec.max_examples,
        ),
        _finding_frame(
            dropped_scopes,
            severity=WARNING,
            code="SCOPE_DROPPED_TO_ZERO",
            example=_example(
                _labelled(scope_column, F.col("_Scope")),
                _labelled("previous_rows", F.col("_PreviousRows")),
            ),
            max_examples=spec.max_examples,
        ),
    ]


def audit_control_sheet(
    *,
    raw_current: DataFrame,
    processed_current: DataFrame,
    cms_latest: DataFrame,
    spec: ControlSheetAuditSpec,
    previous_raw: DataFrame | None = None,
    previous_processed: DataFrame | None = None,
) -> ControlSheetAuditReport:
    """Return warning-only facts without writing, filtering, or blocking data."""
    raw_required = [
        "UniqueAdID",
        "CMSPageID",
        "StartDate",
        "EndDate",
        "Status",
        "AudienceOnly",
        "AdVariant",
        "URL",
        *spec.placement_columns,
    ]
    processed_required = [
        "UniqueAdID",
        "CMSPageID",
        str(spec.scope_column),
        "StartDate",
        "EndDate",
        "AudienceOnly",
        "AdVariant",
    ]
    _require_columns(raw_current, "raw_current", raw_required)
    _require_columns(
        processed_current,
        "processed_current",
        processed_required,
    )
    _require_columns(cms_latest, "cms_latest", ["CMSPageID"])
    if previous_raw is not None:
        _require_columns(previous_raw, "previous_raw", ["UniqueAdID"])
    if previous_processed is not None:
        _require_columns(
            previous_processed,
            "previous_processed",
            ["UniqueAdID", str(spec.scope_column)],
        )

    raw = _raw_with_audit_columns(raw_current, spec).cache()
    processed = _processed_with_audit_columns(
        processed_current,
        spec,
    ).cache()

    finding_frames = [
        *_raw_finding_frames(raw, spec),
        *_processed_finding_frames(processed, spec),
        *_cms_finding_frames(raw, cms_latest, spec),
    ]
    if previous_raw is not None:
        finding_frames.extend(
            _raw_change_finding_frames(raw_current, previous_raw, spec)
        )
    if previous_processed is not None:
        finding_frames.extend(
            _processed_change_finding_frames(
                processed_current,
                previous_processed,
                spec,
            )
        )

    combined = reduce(
        lambda left, right: left.unionByName(right),
        finding_frames,
    )
    try:
        rows = combined.orderBy(
            F.when(F.col("severity") == WARNING, F.lit(0)).otherwise(
                F.lit(1)
            ),
            F.col("code"),
        ).collect()
    finally:
        raw.unpersist(blocking=False)
        processed.unpersist(blocking=False)

    findings = tuple(
        ControlSheetAuditFinding(
            severity=row["severity"],
            code=row["code"],
            count=int(row["count"]),
            examples=tuple(row["examples"]),
            message=row["message"],
        )
        for row in rows
    )
    return ControlSheetAuditReport(
        route=spec.route,
        effective_date=spec.effective_date,
        findings=findings,
    )
