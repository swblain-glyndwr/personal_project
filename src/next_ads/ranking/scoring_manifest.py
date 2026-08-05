from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, TypeVar


READY = "READY"
READY_WITH_WARNINGS = "READY_WITH_WARNINGS"
FAILED = "FAILED"
READY_FOR_NEXTADS = "READY_FOR_NEXTADS"
READY_FOR_PROVIDERS = "READY_FOR_PROVIDERS"
FALLBACK_PREVIOUS = "FALLBACK_PREVIOUS"
FAILED_BEFORE_PUBLISH = "FAILED_BEFORE_PUBLISH"
SERVING = "SERVING"
EVALUATE = "EVALUATE"
ALL = "*"

VALID_INPUT_STATUSES = frozenset({READY, READY_WITH_WARNINGS, FAILED})
VALID_PROVIDER_BUILD_STATUSES = frozenset(
    {READY_FOR_NEXTADS, FAILED_BEFORE_PUBLISH}
)
VALID_FOUNDATION_BUILD_STATUSES = frozenset(
    {READY_FOR_PROVIDERS, FAILED_BEFORE_PUBLISH}
)
VALID_PORTFOLIO_STATUSES = frozenset(
    {READY_FOR_NEXTADS, FALLBACK_PREVIOUS, FAILED_BEFORE_PUBLISH}
)
VALID_POLICY_ROLES = frozenset({"CHAMPION", "CHALLENGER", "SHADOW"})
VALID_EXECUTION_MODES = frozenset({SERVING, EVALUATE})
VALID_PROVIDER_ADAPTERS = frozenset(
    {"legacy_account_entity_table", "canonical_provider_job"}
)
VALID_COMPATIBILITY_PUBLISHERS = frozenset(
    {"none", "theme_affinity_legacy", "markov_legacy"}
)
SELECTOR_FIELDS = frozenset(
    {"locations", "page_types", "audiences", "customer_cells"}
)

ManifestItem = TypeVar(
    "ManifestItem",
    bound=(
        "ScoringInputSnapshot | ScoringFoundationBuild | "
        "ScoreProviderBuild | ScoringPortfolio"
    ),
)


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _count(value: Any, label: str, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
    ):
        raise ValueError(f"{label} must be an integer of at least {minimum}")
    return value


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite")
    return value


def _date(value: Any, label: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError(f"{label} must be a date")
    return value


def _timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{label} must be a datetime")
    return value


@dataclass(frozen=True)
class ScoringInputSource:
    input_snapshot_id: str
    input_snapshot_attempt_id: str
    run_date: date
    source_name: str
    source_role: str
    source_table: str
    delta_version: int
    schema_version: str
    is_required: bool
    row_count: int
    distinct_key_count: int
    null_key_count: int
    duplicate_key_count: int
    content_checksum: str
    task_run_id: int
    execution_count: int
    captured_at: datetime

    def __post_init__(self) -> None:
        """Validate one pinned scoring input source."""
        for name in (
            "input_snapshot_id",
            "input_snapshot_attempt_id",
            "source_name",
            "source_role",
            "source_table",
            "schema_version",
            "content_checksum",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _timestamp(self.captured_at, "captured_at")
        if not isinstance(self.is_required, bool):
            raise ValueError("is_required must be a boolean")
        for name in (
            "delta_version",
            "row_count",
            "distinct_key_count",
            "null_key_count",
            "duplicate_key_count",
            "execution_count",
        ):
            _count(getattr(self, name), name)
        _count(self.task_run_id, "task_run_id", 1)
        if self.distinct_key_count > self.row_count:
            raise ValueError("distinct_key_count must not exceed row_count")


@dataclass(frozen=True)
class ScoringInputSnapshot:
    input_snapshot_id: str
    input_snapshot_attempt_id: str
    run_date: date
    input_schema_version: str
    status: str
    warning_count: int
    task_run_id: int
    execution_count: int
    completed_at: datetime
    sources: tuple[ScoringInputSource, ...]

    def __post_init__(self) -> None:
        """Validate one scoring input snapshot attempt."""
        _text(self.input_snapshot_id, "input_snapshot_id")
        _text(self.input_snapshot_attempt_id, "input_snapshot_attempt_id")
        _text(self.input_schema_version, "input_schema_version")
        _date(self.run_date, "run_date")
        _timestamp(self.completed_at, "completed_at")
        if self.status not in VALID_INPUT_STATUSES:
            raise ValueError(f"Unsupported input status: {self.status}")
        _count(self.warning_count, "warning_count")
        _count(self.task_run_id, "task_run_id", 1)
        _count(self.execution_count, "execution_count")
        if self.status == READY and self.warning_count:
            raise ValueError("READY input snapshots cannot contain warnings")
        if self.status == READY_WITH_WARNINGS and not self.warning_count:
            raise ValueError(
                "READY_WITH_WARNINGS input snapshots need a warning"
            )

        sources = tuple(self.sources)
        object.__setattr__(self, "sources", sources)
        names = [source.source_name for source in sources]
        if len(names) != len(set(names)):
            raise ValueError("Input source names must be unique")
        for source in sources:
            identity = (
                source.input_snapshot_id,
                source.input_snapshot_attempt_id,
                source.run_date,
            )
            expected = (
                self.input_snapshot_id,
                self.input_snapshot_attempt_id,
                self.run_date,
            )
            if identity != expected:
                raise ValueError("Input sources must match snapshot attempt")

        if self.status in {READY, READY_WITH_WARNINGS}:
            if not sources:
                raise ValueError("A ready input snapshot must contain sources")
            invalid_required_sources = [
                source.source_name
                for source in sources
                if source.is_required
                and (
                    source.row_count == 0
                    or source.null_key_count
                    or source.duplicate_key_count
                )
            ]
            if invalid_required_sources:
                raise ValueError(
                    "Required input sources are structurally invalid: "
                    + ", ".join(invalid_required_sources)
                )


@dataclass(frozen=True)
class ScoringFoundationOutput:
    scoring_foundation_build_id: str
    scoring_foundation_build_attempt_id: str
    run_date: date
    output_name: str
    source_table: str
    source_delta_version: int | None
    source_schema_checksum: str
    output_table: str
    output_delta_version: int
    output_schema_version: str
    output_schema_checksum: str
    is_required: bool
    row_count: int
    account_count: int
    entity_count: int
    null_key_count: int
    duplicate_key_count: int
    invalid_value_count: int
    output_checksum: str
    published_at: datetime

    def __post_init__(self) -> None:
        """Validate one immutable scoring-foundation output binding."""
        for name in (
            "scoring_foundation_build_id",
            "scoring_foundation_build_attempt_id",
            "output_name",
            "source_table",
            "source_schema_checksum",
            "output_table",
            "output_schema_version",
            "output_schema_checksum",
            "output_checksum",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _timestamp(self.published_at, "published_at")
        if not isinstance(self.is_required, bool):
            raise ValueError("is_required must be a boolean")
        for name in (
            "output_delta_version",
            "row_count",
            "account_count",
            "entity_count",
            "null_key_count",
            "duplicate_key_count",
            "invalid_value_count",
        ):
            _count(getattr(self, name), name)
        if self.source_delta_version is not None:
            _count(self.source_delta_version, "source_delta_version")
        if self.account_count > self.row_count:
            raise ValueError("account_count must not exceed row_count")
        if self.entity_count > self.row_count:
            raise ValueError("entity_count must not exceed row_count")
        if self.is_required:
            if self.row_count == 0:
                raise ValueError("A required foundation output cannot be empty")
            if (
                self.null_key_count
                or self.duplicate_key_count
                or self.invalid_value_count
            ):
                raise ValueError(
                    "A required foundation output must have valid unique keys"
                )


@dataclass(frozen=True)
class ScoringFoundationBuild:
    scoring_foundation_build_id: str
    scoring_foundation_build_attempt_id: str
    input_snapshot_id: str
    input_snapshot_attempt_id: str
    run_date: date
    foundation_id: str
    foundation_version: str
    capability: str
    contract_version: str
    invocation_checksum: str
    required_output_names: tuple[str, ...]
    status: str
    warning_count: int
    task_run_id: int
    execution_count: int
    completed_at: datetime
    outputs: tuple[ScoringFoundationOutput, ...]
    input_bindings_json: str
    pipeline_id: str | None = None
    pipeline_update_id: str | None = None
    pipeline_task_run_id: int | None = None
    pipeline_update_type: str | None = None

    def __post_init__(self) -> None:
        """Validate one provider-neutral scoring-foundation attempt."""
        for name in (
            "scoring_foundation_build_id",
            "scoring_foundation_build_attempt_id",
            "input_snapshot_id",
            "input_snapshot_attempt_id",
            "foundation_id",
            "foundation_version",
            "capability",
            "contract_version",
            "invocation_checksum",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _timestamp(self.completed_at, "completed_at")
        _text(self.input_bindings_json, "input_bindings_json")
        try:
            input_bindings = json.loads(self.input_bindings_json)
        except json.JSONDecodeError as error:
            raise ValueError(
                "input_bindings_json must be valid JSON"
            ) from error
        if not isinstance(input_bindings, dict) or not input_bindings:
            raise ValueError("input_bindings_json must contain an object")
        for name in (
            "pipeline_id",
            "pipeline_update_id",
            "pipeline_update_type",
        ):
            _optional_text(getattr(self, name), name)
        if self.pipeline_task_run_id is not None:
            _count(self.pipeline_task_run_id, "pipeline_task_run_id", 1)
        if self.status not in VALID_FOUNDATION_BUILD_STATUSES:
            raise ValueError(
                f"Unsupported foundation build status: {self.status}"
            )
        _count(self.warning_count, "warning_count")
        _count(self.task_run_id, "task_run_id", 1)
        _count(self.execution_count, "execution_count")

        required_output_names = tuple(self.required_output_names)
        object.__setattr__(
            self,
            "required_output_names",
            required_output_names,
        )
        if not required_output_names:
            raise ValueError("A scoring foundation must define required outputs")
        for output_name in required_output_names:
            _text(output_name, "required_output_name")
        if len(required_output_names) != len(set(required_output_names)):
            raise ValueError("Required foundation output names must be unique")

        outputs = tuple(self.outputs)
        object.__setattr__(self, "outputs", outputs)
        output_names = [output.output_name for output in outputs]
        if len(output_names) != len(set(output_names)):
            raise ValueError("Foundation output names must be unique")
        for output in outputs:
            identity = (
                output.scoring_foundation_build_id,
                output.scoring_foundation_build_attempt_id,
                output.run_date,
            )
            expected = (
                self.scoring_foundation_build_id,
                self.scoring_foundation_build_attempt_id,
                self.run_date,
            )
            if identity != expected:
                raise ValueError("Foundation outputs must match the build attempt")

        if self.status == READY_FOR_PROVIDERS:
            _text(self.pipeline_id, "pipeline_id")
            _count(self.pipeline_task_run_id, "pipeline_task_run_id", 1)
            required_outputs = {
                output.output_name
                for output in outputs
                if output.is_required
            }
            if required_outputs != set(required_output_names):
                raise ValueError(
                    "Ready foundation outputs do not match the required contract"
                )


@dataclass(frozen=True)
class ScoreProviderBuild:
    provider_build_id: str
    provider_build_attempt_id: str
    input_snapshot_id: str
    run_date: date
    capability: str
    use_case: str
    provider_id: str
    provider_version: str
    contract_version: str
    status: str
    row_count: int
    account_count: int
    entity_count: int
    null_key_count: int
    duplicate_key_count: int
    invalid_score_count: int
    warning_count: int
    output_checksum: str | None
    task_run_id: int
    execution_count: int
    completed_at: datetime
    model_name: str | None = None
    model_version: str | None = None
    model_uri: str | None = None
    pipeline_update_id: str | None = None
    output_snapshot_id: str | None = None
    output_table: str | None = None
    output_delta_version: int | None = None
    scoring_foundation_build_id: str | None = None
    scoring_foundation_build_attempt_id: str | None = None

    def __post_init__(self) -> None:
        """Validate one role-neutral score provider attempt."""
        for name in (
            "provider_build_id",
            "provider_build_attempt_id",
            "input_snapshot_id",
            "capability",
            "use_case",
            "provider_id",
            "provider_version",
            "contract_version",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _timestamp(self.completed_at, "completed_at")
        if self.status not in VALID_PROVIDER_BUILD_STATUSES:
            raise ValueError(
                f"Unsupported provider build status: {self.status}"
            )
        for name in (
            "row_count",
            "account_count",
            "entity_count",
            "null_key_count",
            "duplicate_key_count",
            "invalid_score_count",
            "warning_count",
            "execution_count",
        ):
            _count(getattr(self, name), name)
        _count(self.task_run_id, "task_run_id", 1)
        for name in (
            "model_name",
            "model_version",
            "model_uri",
            "pipeline_update_id",
            "output_snapshot_id",
            "output_table",
        ):
            _optional_text(getattr(self, name), name)

        if self.status == READY_FOR_NEXTADS:
            for name in ("row_count", "account_count", "entity_count"):
                _count(getattr(self, name), name, 1)
            if self.account_count > self.row_count:
                raise ValueError("account_count must not exceed row_count")
            if self.entity_count > self.row_count:
                raise ValueError("entity_count must not exceed row_count")
            if any(
                (
                    self.null_key_count,
                    self.duplicate_key_count,
                    self.invalid_score_count,
                )
            ):
                raise ValueError(
                    "A ready provider build must have valid unique score keys"
                )
            _text(self.output_checksum, "output_checksum")
            outputs = (
                self.output_snapshot_id,
                self.output_table,
                self.output_delta_version,
            )
            if any(value is None for value in outputs):
                raise ValueError(
                    "A ready provider build must identify its output"
                )
        elif self.output_checksum is not None:
            _text(self.output_checksum, "output_checksum")

        if self.output_delta_version is not None:
            _count(self.output_delta_version, "output_delta_version")

        foundation_values = (
            self.scoring_foundation_build_id,
            self.scoring_foundation_build_attempt_id,
        )
        if any(value is None for value in foundation_values) and not all(
            value is None for value in foundation_values
        ):
            raise ValueError(
                "Provider foundation build and attempt IDs must be supplied together"
            )
        for name in (
            "scoring_foundation_build_id",
            "scoring_foundation_build_attempt_id",
        ):
            _optional_text(getattr(self, name), name)


@dataclass(frozen=True)
class ScoreProviderSignal:
    provider_build_id: str
    account_number: str
    entity_type: str
    entity_id: str
    provider_id: str
    run_date: date
    raw_score: float
    score: float
    provider_rank: int

    def __post_init__(self) -> None:
        """Validate one canonical provider signal."""
        for name in (
            "provider_build_id",
            "account_number",
            "entity_type",
            "entity_id",
            "provider_id",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _finite(self.raw_score, "raw_score")
        _finite(self.score, "score")
        _count(self.provider_rank, "provider_rank", 1)


@dataclass(frozen=True)
class ScoringPortfolioEntry:
    portfolio_entry_id: str
    provider_build_id: str
    policy_role: str
    execution_mode: str
    priority: int
    serving_slot: str | None = None

    def __post_init__(self) -> None:
        """Validate one provider selected by portfolio policy."""
        _text(self.portfolio_entry_id, "portfolio_entry_id")
        _text(self.provider_build_id, "provider_build_id")
        if self.policy_role not in VALID_POLICY_ROLES:
            raise ValueError(f"Unsupported policy role: {self.policy_role}")
        if self.execution_mode not in VALID_EXECUTION_MODES:
            raise ValueError(
                f"Unsupported execution mode: {self.execution_mode}"
            )
        _count(self.priority, "priority", 1)
        if self.serving_slot is not None:
            _text(self.serving_slot, "serving_slot")
        if self.policy_role == "CHAMPION" and self.execution_mode != SERVING:
            raise ValueError("The champion must be a serving entry")
        if self.policy_role == "SHADOW" and self.execution_mode != EVALUATE:
            raise ValueError("A shadow entry must be evaluation-only")
        if self.execution_mode == SERVING and self.serving_slot is None:
            raise ValueError("A serving entry must occupy a serving slot")
        if self.execution_mode == EVALUATE and self.serving_slot is not None:
            raise ValueError(
                "An evaluation-only entry cannot occupy a serving slot"
            )


@dataclass(frozen=True)
class ScoringPortfolio:
    portfolio_id: str
    portfolio_attempt_id: str
    run_date: date
    capability: str
    use_case: str
    route: str
    policy_id: str
    policy_priority: int
    location: str
    page_type: str
    audience: str
    customer_cell: str
    contract_version: str
    status: str
    warning_count: int
    task_run_id: int
    execution_count: int
    completed_at: datetime
    entries: tuple[ScoringPortfolioEntry, ...]
    fallback_source_portfolio_id: str | None = None
    fallback_source_run_date: date | None = None
    fallback_source_completed_at: datetime | None = None

    def __post_init__(self) -> None:
        """Validate one route/use-case/scope portfolio attempt."""
        for name in (
            "portfolio_id",
            "portfolio_attempt_id",
            "capability",
            "use_case",
            "route",
            "policy_id",
            "location",
            "page_type",
            "audience",
            "customer_cell",
            "contract_version",
        ):
            _text(getattr(self, name), name)
        _date(self.run_date, "run_date")
        _timestamp(self.completed_at, "completed_at")
        if self.status not in VALID_PORTFOLIO_STATUSES:
            raise ValueError(f"Unsupported portfolio status: {self.status}")
        _count(self.warning_count, "warning_count")
        _count(self.policy_priority, "policy_priority", 1)
        _count(self.task_run_id, "task_run_id", 1)
        _count(self.execution_count, "execution_count")

        entries = tuple(self.entries)
        object.__setattr__(self, "entries", entries)
        entry_ids = [entry.portfolio_entry_id for entry in entries]
        if len(entry_ids) != len(set(entry_ids)):
            raise ValueError("Portfolio entry IDs must be unique")
        serving_slots = [
            entry.serving_slot
            for entry in entries
            if entry.serving_slot is not None
        ]
        if len(serving_slots) != len(set(serving_slots)):
            raise ValueError("Portfolio serving slots must be unique")
        role_priorities = [
            (entry.policy_role, entry.priority) for entry in entries
        ]
        if len(role_priorities) != len(set(role_priorities)):
            raise ValueError("Portfolio role priorities must be unique")

        if self.status != FAILED_BEFORE_PUBLISH:
            if not entries:
                raise ValueError("A ready portfolio must contain entries")
            champions = [
                entry for entry in entries if entry.policy_role == "CHAMPION"
            ]
            if len(champions) != 1:
                raise ValueError("A ready portfolio must contain one champion")

        fallback_values = (
            self.fallback_source_portfolio_id,
            self.fallback_source_run_date,
            self.fallback_source_completed_at,
        )
        if self.status == FALLBACK_PREVIOUS:
            if any(value is None for value in fallback_values):
                raise ValueError(
                    "A fallback portfolio must identify its accepted source"
                )
            _text(
                self.fallback_source_portfolio_id,
                "fallback_source_portfolio_id",
            )
            _date(
                self.fallback_source_run_date,
                "fallback_source_run_date",
            )
            _timestamp(
                self.fallback_source_completed_at,
                "fallback_source_completed_at",
            )
            if self.fallback_source_run_date > self.run_date:
                raise ValueError("A fallback source cannot be from the future")
            if self.fallback_source_completed_at > self.completed_at:
                raise ValueError(
                    "A fallback source cannot complete after selection"
                )
        elif any(value is not None for value in fallback_values):
            raise ValueError(
                "Fallback provenance is only valid for fallback portfolios"
            )


def _select_latest_attempts(
    items: Iterable[ManifestItem],
    *,
    logical_id_field: str,
    attempt_id_field: str,
) -> tuple[ManifestItem, ...]:
    attempts = tuple(items)
    attempt_ids = [getattr(item, attempt_id_field) for item in attempts]
    if len(attempt_ids) != len(set(attempt_ids)):
        raise ValueError("Manifest attempt IDs must be unique")

    grouped: dict[str, list[ManifestItem]] = {}
    for item in attempts:
        grouped.setdefault(getattr(item, logical_id_field), []).append(item)

    selected = []
    for logical_id in sorted(grouped):
        candidates = grouped[logical_id]
        ordering = [
            (item.execution_count, item.completed_at, item.task_run_id)
            for item in candidates
        ]
        if len(ordering) != len(set(ordering)):
            raise ValueError(
                f"Contradictory manifest attempts for {logical_id}"
            )
        selected.append(
            max(
                candidates,
                key=lambda item: (
                    item.execution_count,
                    item.completed_at,
                    item.task_run_id,
                ),
            )
        )
    return tuple(selected)


def validate_scoring_input_snapshots(
    snapshots: Iterable[ScoringInputSnapshot],
) -> tuple[ScoringInputSnapshot, ...]:
    return _select_latest_attempts(
        snapshots,
        logical_id_field="input_snapshot_id",
        attempt_id_field="input_snapshot_attempt_id",
    )


def validate_scoring_foundation_builds(
    builds: Iterable[ScoringFoundationBuild],
) -> tuple[ScoringFoundationBuild, ...]:
    return _select_latest_attempts(
        builds,
        logical_id_field="scoring_foundation_build_id",
        attempt_id_field="scoring_foundation_build_attempt_id",
    )


def validate_score_provider_builds(
    builds: Iterable[ScoreProviderBuild],
) -> tuple[ScoreProviderBuild, ...]:
    return _select_latest_attempts(
        builds,
        logical_id_field="provider_build_id",
        attempt_id_field="provider_build_attempt_id",
    )


def validate_scoring_portfolios(
    portfolios: Iterable[ScoringPortfolio],
) -> tuple[ScoringPortfolio, ...]:
    result = _select_latest_attempts(
        portfolios,
        logical_id_field="portfolio_id",
        attempt_id_field="portfolio_attempt_id",
    )
    identities = [
        (
            item.run_date,
            item.capability,
            item.use_case,
            item.route,
            item.location,
            item.page_type,
            item.audience,
            item.customer_cell,
        )
        for item in result
        if item.status != FAILED_BEFORE_PUBLISH
    ]
    if len(identities) != len(set(identities)):
        raise ValueError(
            "Only one ready portfolio is allowed per route/use-case/scope"
        )
    return result


def _string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a non-empty list")
    result = [_text(item, label) for item in value]
    if len(result) != len(set(result)):
        raise ValueError(f"{label} must not contain duplicates")
    return result


def _validate_policy_entries(
    entries: Any,
    *,
    providers: Mapping[str, Any],
    capability: str,
    serving_slots: set[str],
) -> None:
    if not isinstance(entries, list) or not entries:
        raise ValueError("Portfolio policy must contain entries")
    entry_ids = [_text(entry.get("entry_id"), "entry_id") for entry in entries]
    if len(entry_ids) != len(set(entry_ids)):
        raise ValueError("Portfolio policy entry IDs must be unique")

    roles = []
    priorities = []
    occupied_slots = []
    for entry in entries:
        provider_id = _text(entry.get("provider_id"), "provider_id")
        if provider_id not in providers:
            raise ValueError("Portfolio references an unknown provider")
        if providers[provider_id].get("capability") != capability:
            raise ValueError("Portfolio provider capability does not match")

        role = entry.get("policy_role")
        if role not in VALID_POLICY_ROLES:
            raise ValueError("Portfolio references an unsupported policy role")
        mode = entry.get("execution_mode")
        if mode not in VALID_EXECUTION_MODES:
            raise ValueError(
                "Portfolio references an unsupported execution mode"
            )
        roles.append(role)
        priority = _count(entry.get("priority"), "priority", 1)
        priorities.append((role, priority))

        slot = entry.get("serving_slot")
        if slot is not None:
            slot = _text(slot, "serving_slot")
        if role == "CHAMPION" and mode != SERVING:
            raise ValueError("The champion policy must serve")
        if role == "SHADOW" and mode != EVALUATE:
            raise ValueError("A shadow policy must be evaluation-only")
        if mode == SERVING:
            if slot is None:
                raise ValueError("A serving policy needs a serving slot")
            if slot not in serving_slots:
                raise ValueError("Portfolio uses an unsupported serving slot")
            occupied_slots.append(slot)
        elif slot is not None:
            raise ValueError(
                "An evaluation policy cannot occupy a serving slot"
            )

    if roles.count("CHAMPION") != 1:
        raise ValueError("Portfolio policy must contain one champion")
    if len(priorities) != len(set(priorities)):
        raise ValueError("Portfolio policy role priorities must be unique")
    if len(occupied_slots) != len(set(occupied_slots)):
        raise ValueError("Portfolio policy serving slots must be unique")


def validate_scoring_config(scoring: Mapping[str, Any]) -> None:
    contract_version = _text(
        scoring.get("contract_version"), "scoring.contract_version"
    )
    canonical = scoring.get("canonical")
    capabilities = scoring.get("capabilities")
    foundations = scoring.get("foundations")
    providers = scoring.get("providers")
    client_portfolios = scoring.get("client_portfolios")
    if not isinstance(canonical, Mapping):
        raise ValueError("scoring.canonical must be a mapping")
    for field in (
        "foundation_builds_table",
        "foundation_outputs_table",
        "foundation_run_contexts_table",
        "provider_builds_table",
        "provider_signals_table",
        "portfolios_table",
        "portfolio_entries_table",
    ):
        _text(canonical.get(field), f"scoring.canonical.{field}")
    if not isinstance(capabilities, Mapping) or not capabilities:
        raise ValueError("scoring.capabilities must be a non-empty mapping")
    if not isinstance(foundations, Mapping) or not foundations:
        raise ValueError("scoring.foundations must be a non-empty mapping")
    if not isinstance(providers, Mapping) or not providers:
        raise ValueError("scoring.providers must be a non-empty mapping")
    if not isinstance(client_portfolios, Mapping) or not client_portfolios:
        raise ValueError(
            "scoring.client_portfolios must be a non-empty mapping"
        )

    capability_entities = {}
    for capability, definition in capabilities.items():
        if not isinstance(definition, Mapping):
            raise ValueError(f"Capability {capability} must be a mapping")
        capability_entities[capability] = _text(
            definition.get("entity_type"),
            f"{capability}.entity_type",
        )

    for foundation_key, foundation in foundations.items():
        if not isinstance(foundation, Mapping):
            raise ValueError(f"Foundation {foundation_key} must be a mapping")
        if (
            _text(foundation.get("foundation_id"), "foundation_id")
            != foundation_key
        ):
            raise ValueError("Foundation key must match foundation_id")
        _text(foundation.get("foundation_version"), "foundation_version")
        capability = _text(foundation.get("capability"), "capability")
        if capability not in capabilities:
            raise ValueError(f"Unknown foundation capability: {capability}")
        _text(foundation.get("contract_version"), "contract_version")
        required_outputs = foundation.get("required_outputs")
        if not isinstance(required_outputs, Mapping) or not required_outputs:
            raise ValueError("Foundation required_outputs must be a mapping")
        for output_name, schema_version in required_outputs.items():
            _text(output_name, "foundation output_name")
            _text(schema_version, "foundation output schema_version")
        input_bindings = foundation.get("input_bindings")
        if not isinstance(input_bindings, Mapping) or not input_bindings:
            raise ValueError("Foundation input_bindings must be a mapping")
        for binding_name, binding in input_bindings.items():
            _text(binding_name, "foundation input binding")
            if not isinstance(binding, Mapping):
                raise ValueError("Foundation input binding must be a mapping")
            _text(binding.get("table"), "foundation input table")
            _text(
                binding.get("schema_version"),
                "foundation input schema_version",
            )

    for provider_key, provider in providers.items():
        if not isinstance(provider, Mapping):
            raise ValueError(f"Provider {provider_key} must be a mapping")
        if _text(provider.get("provider_id"), "provider_id") != provider_key:
            raise ValueError("Provider key must match provider_id")
        _text(provider.get("implementation"), "implementation")
        capability = _text(provider.get("capability"), "capability")
        if capability not in capabilities:
            raise ValueError(f"Unknown provider capability: {capability}")
        _text(provider.get("provider_version"), "provider_version")
        adapter = _text(provider.get("adapter"), "adapter")
        if adapter not in VALID_PROVIDER_ADAPTERS:
            raise ValueError("Provider adapter is unsupported")
        compatibility_publisher = provider.get(
            "compatibility_publisher",
            "none",
        )
        if compatibility_publisher not in VALID_COMPATIBILITY_PUBLISHERS:
            raise ValueError("Provider compatibility publisher is unsupported")
        entity_type = _text(provider.get("entity_type"), "entity_type")
        if entity_type != capability_entities[capability]:
            raise ValueError("Provider entity_type must match its capability")
        if adapter == "legacy_account_entity_table":
            for field in (
                "legacy_source_table",
                "account_number_column",
                "entity_id_column",
                "raw_score_column",
                "score_column",
            ):
                _text(provider.get(field), field)
        if provider.get("score_direction") not in {
            "higher_is_better",
            "lower_is_better",
        }:
            raise ValueError("Provider score_direction is unsupported")
        _count(
            provider.get("max_entities_per_account"),
            "max_entities_per_account",
            1,
        )
        foundation_id = provider.get("foundation_id")
        if foundation_id is not None:
            foundation_id = _text(foundation_id, "foundation_id")
            if foundation_id not in foundations:
                raise ValueError("Provider references an unknown foundation")
            if foundations[foundation_id].get("capability") != capability:
                raise ValueError("Provider foundation capability does not match")

    for client, portfolios in client_portfolios.items():
        if not isinstance(portfolios, Mapping) or not portfolios:
            raise ValueError(f"Client {client} must define portfolios")
        for use_case, portfolio in portfolios.items():
            capability = _text(portfolio.get("capability"), "capability")
            if capability not in capabilities:
                raise ValueError(f"Unknown portfolio capability: {capability}")
            if portfolio.get("contract_version") != contract_version:
                raise ValueError(
                    f"Portfolio {use_case} contract version does not match"
                )
            serving_slots = set(
                _string_list(
                    portfolio.get("serving_slots"),
                    f"{use_case}.serving_slots",
                )
            )
            routes = portfolio.get("routes")
            if not isinstance(routes, Mapping) or not routes:
                raise ValueError(f"Portfolio {use_case} must define routes")
            for route, route_definition in routes.items():
                _text(route, "route")
                if not isinstance(route_definition, Mapping):
                    raise ValueError("Portfolio route must be a mapping")
                policies = route_definition.get("policies")
                if not isinstance(policies, list) or not policies:
                    raise ValueError("Portfolio route must contain policies")
                policy_ids = [
                    _text(policy.get("policy_id"), "policy_id")
                    for policy in policies
                ]
                if len(policy_ids) != len(set(policy_ids)):
                    raise ValueError(
                        "Portfolio route policy IDs must be unique"
                    )
                policy_priorities = [
                    _count(
                        policy.get("priority"),
                        "policy.priority",
                        1,
                    )
                    for policy in policies
                ]
                if len(policy_priorities) != len(set(policy_priorities)):
                    raise ValueError(
                        "Portfolio route policy priorities must be unique"
                    )
                for policy in policies:
                    selector = policy.get("selector")
                    if not isinstance(selector, Mapping):
                        raise ValueError(
                            "Portfolio policy selector must be a mapping"
                        )
                    if set(selector) != SELECTOR_FIELDS:
                        raise ValueError(
                            "Portfolio selectors must use the typed fields"
                        )
                    for field in SELECTOR_FIELDS:
                        _string_list(
                            selector.get(field),
                            f"selector.{field}",
                        )
                    _validate_policy_entries(
                        policy.get("entries"),
                        providers=providers,
                        capability=capability,
                        serving_slots=serving_slots,
                    )


__all__ = [
    "ALL",
    "EVALUATE",
    "FAILED",
    "FAILED_BEFORE_PUBLISH",
    "FALLBACK_PREVIOUS",
    "READY",
    "READY_FOR_NEXTADS",
    "READY_FOR_PROVIDERS",
    "READY_WITH_WARNINGS",
    "SERVING",
    "ScoreProviderBuild",
    "ScoreProviderSignal",
    "ScoringFoundationBuild",
    "ScoringFoundationOutput",
    "ScoringInputSnapshot",
    "ScoringInputSource",
    "ScoringPortfolio",
    "ScoringPortfolioEntry",
    "validate_score_provider_builds",
    "validate_scoring_foundation_builds",
    "validate_scoring_config",
    "validate_scoring_input_snapshots",
    "validate_scoring_portfolios",
]
