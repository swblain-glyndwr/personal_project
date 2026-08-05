from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from next_ads.common.delta_writes import replace_scope_by_name
from next_ads.ranking.provider_selection import ProviderBuildSelection
from next_ads.ranking.scoring_manifest import (
    ALL,
    READY_FOR_NEXTADS,
    SERVING,
    ScoringPortfolio,
    ScoringPortfolioEntry,
)


SCOPE_FIELDS = (
    "locations",
    "page_types",
    "audiences",
    "customer_cells",
)


@dataclass(frozen=True)
class PortfolioPolicy:
    client: str
    use_case: str
    route: str
    capability: str
    contract_version: str
    policy_id: str
    policy_version: str
    priority: int
    checksum: str
    scope: Mapping[str, str]
    serving_slots: tuple[str, ...]
    entries: tuple[Mapping[str, Any], ...]


def _value(config: Any, name: str) -> Any:
    if isinstance(config, Mapping):
        return config[name]
    return getattr(config, name)


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{label} must be an integer of at least {minimum}")
    return value


def _normalise_scope(scope: Mapping[str, str] | None) -> dict[str, str]:
    values = dict(scope or {})
    unexpected = sorted(set(values).difference(SCOPE_FIELDS))
    if unexpected:
        raise ValueError(
            "Portfolio scope contains unsupported fields: "
            + ", ".join(unexpected)
        )
    return {
        field: _text(values.get(field, ALL), f"scope.{field}")
        for field in SCOPE_FIELDS
    }


def _matches(selector: Mapping[str, Any], scope: Mapping[str, str]) -> bool:
    for field in SCOPE_FIELDS:
        declared = tuple(selector[field])
        if ALL not in declared and scope[field] not in declared:
            return False
    return True


def _policy_payload(
    *,
    capability: str,
    contract_version: str,
    route: str,
    serving_slots: tuple[str, ...],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    selector = {
        field: sorted(str(value) for value in policy["selector"][field])
        for field in SCOPE_FIELDS
    }
    entries = sorted(
        (_plain(entry) for entry in policy["entries"]),
        key=lambda entry: entry["entry_id"],
    )
    return {
        "capability": capability,
        "contract_version": contract_version,
        "route": route,
        "serving_slots": list(serving_slots),
        "policy": {
            "policy_id": policy["policy_id"],
            "policy_version": policy["policy_version"],
            "priority": policy["priority"],
            "selector": selector,
            "entries": entries,
        },
    }


def resolve_portfolio_policy(
    scoring_config: Any,
    *,
    client: str,
    use_case: str,
    route: str,
    requested_policy_id: str,
    scope: Mapping[str, str] | None = None,
) -> PortfolioPolicy:
    """Resolve one declared policy by priority then stable policy ID."""
    client = _text(client, "client")
    use_case = _text(use_case, "use_case")
    route = _text(route, "route")
    requested_policy_id = _text(requested_policy_id, "requested_policy_id")
    scope = _normalise_scope(scope)

    clients = _value(scoring_config, "client_portfolios")
    if client not in clients:
        raise ValueError(f"No scoring portfolio is declared for client {client}")
    use_cases = clients[client]
    if use_case not in use_cases:
        raise ValueError(f"No scoring portfolio is declared for {use_case}")
    portfolio = use_cases[use_case]
    routes = _value(portfolio, "routes")
    if route not in routes:
        raise ValueError(f"No scoring portfolio route is declared for {route}")

    policies = tuple(_plain(policy) for policy in routes[route]["policies"])
    declared_ids = {policy["policy_id"] for policy in policies}
    if requested_policy_id not in declared_ids:
        raise ValueError(
            f"Portfolio policy {requested_policy_id} is not declared for {route}"
        )
    matching = [
        policy for policy in policies if _matches(policy["selector"], scope)
    ]
    if not matching:
        raise ValueError(f"No portfolio policy matches the requested {route} scope")
    selected = min(
        matching,
        key=lambda policy: (int(policy["priority"]), policy["policy_id"]),
    )
    if selected["policy_id"] != requested_policy_id:
        raise ValueError(
            f"Portfolio policy {requested_policy_id} is not the declared "
            f"winner for the requested scope; {selected['policy_id']} is"
        )

    capability = _text(_value(portfolio, "capability"), "capability")
    contract_version = _text(
        _value(portfolio, "contract_version"),
        "contract_version",
    )
    serving_slots = tuple(
        _text(slot, "serving_slot")
        for slot in _value(portfolio, "serving_slots")
    )
    payload = _policy_payload(
        capability=capability,
        contract_version=contract_version,
        route=route,
        serving_slots=serving_slots,
        policy=selected,
    )
    checksum = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return PortfolioPolicy(
        client=client,
        use_case=use_case,
        route=route,
        capability=capability,
        contract_version=contract_version,
        policy_id=_text(selected["policy_id"], "policy_id"),
        policy_version=_text(selected["policy_version"], "policy_version"),
        priority=_integer(selected["priority"], "policy.priority", minimum=1),
        checksum=checksum,
        scope=scope,
        serving_slots=serving_slots,
        entries=tuple(selected["entries"]),
    )


def build_scoring_portfolio(
    policy: PortfolioPolicy,
    *,
    run_date: date,
    selections: Mapping[str, ProviderBuildSelection],
    selection_cutoff: datetime,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime | None = None,
) -> ScoringPortfolio:
    """Bind one policy to exact immutable provider output versions."""
    completed_at = completed_at or datetime.now(timezone.utc)
    resolved_entries = []
    missing_optional = []
    for definition in sorted(
        policy.entries,
        key=lambda entry: (
            int(entry["priority"]),
            entry["policy_role"],
            entry["entry_id"],
        ),
    ):
        provider_id = _text(definition["provider_id"], "provider_id")
        selection = selections.get(provider_id)
        if selection is None:
            if definition["execution_mode"] == SERVING:
                raise ValueError(
                    f"Required serving provider {provider_id} was not resolved"
                )
            missing_optional.append(definition["entry_id"])
            continue
        resolved_entries.append(
            ScoringPortfolioEntry(
                portfolio_entry_id=_text(
                    definition["entry_id"],
                    "entry_id",
                ),
                provider_build_id=selection.provider_build_id,
                provider_build_attempt_id=(
                    selection.provider_build_attempt_id
                ),
                provider_output_table=selection.provider_signals_table,
                provider_output_delta_version=(
                    selection.provider_signals_delta_version
                ),
                provider_source_run_date=selection.source_run_date,
                input_snapshot_id=selection.input_snapshot_id,
                provider_selection_status=selection.selection_status,
                experiment_id=_text(
                    definition["experiment_id"],
                    "experiment_id",
                ),
                variant_id=_text(
                    definition["variant_id"],
                    "variant_id",
                ),
                policy_role=definition["policy_role"],
                execution_mode=definition["execution_mode"],
                serving_slot=definition.get("serving_slot"),
                priority=int(definition["priority"]),
            )
        )

    occupied_slots = {
        entry.serving_slot
        for entry in resolved_entries
        if entry.execution_mode == SERVING
    }
    missing_slots = sorted(set(policy.serving_slots).difference(occupied_slots))
    if missing_slots:
        raise ValueError(
            "Portfolio is missing required serving slots: "
            + ", ".join(missing_slots)
        )

    portfolio_identity = {
        "client": policy.client,
        "use_case": policy.use_case,
        "route": policy.route,
        "run_date": run_date.isoformat(),
        "policy_checksum": policy.checksum,
        "scope": dict(policy.scope),
    }
    digest = hashlib.sha256(
        json.dumps(
            portfolio_identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:20]
    portfolio_id = (
        f"{policy.client}_{policy.use_case}_{policy.route}_"
        f"{run_date:%Y%m%d}_{digest}"
    )
    return ScoringPortfolio(
        portfolio_id=portfolio_id,
        portfolio_attempt_id=(
            f"{portfolio_id}:{int(task_run_id)}:{int(execution_count)}"
        ),
        run_date=run_date,
        capability=policy.capability,
        use_case=policy.use_case,
        route=policy.route,
        policy_id=policy.policy_id,
        policy_priority=policy.priority,
        policy_version=policy.policy_version,
        policy_checksum=policy.checksum,
        selection_cutoff=selection_cutoff,
        location=policy.scope["locations"],
        page_type=policy.scope["page_types"],
        audience=policy.scope["audiences"],
        customer_cell=policy.scope["customer_cells"],
        contract_version=policy.contract_version,
        status=READY_FOR_NEXTADS,
        warning_count=len(missing_optional),
        task_run_id=int(task_run_id),
        execution_count=int(execution_count),
        completed_at=completed_at,
        entries=tuple(resolved_entries),
    )


def _entry_row(portfolio: ScoringPortfolio, entry: ScoringPortfolioEntry):
    return {
        "PortfolioID": portfolio.portfolio_id,
        "PortfolioAttemptID": portfolio.portfolio_attempt_id,
        "PortfolioEntryID": entry.portfolio_entry_id,
        "RunDate": portfolio.run_date,
        "ProviderBuildID": entry.provider_build_id,
        "PolicyRole": entry.policy_role,
        "ExecutionMode": entry.execution_mode,
        "ServingSlot": entry.serving_slot,
        "Priority": entry.priority,
        "TaskRunID": portfolio.task_run_id,
        "ExecutionCount": portfolio.execution_count,
        "ProviderBuildAttemptID": entry.provider_build_attempt_id,
        "ProviderOutputTable": entry.provider_output_table,
        "ProviderOutputDeltaVersion": entry.provider_output_delta_version,
        "ProviderSourceRunDate": entry.provider_source_run_date,
        "InputSnapshotID": entry.input_snapshot_id,
        "ProviderSelectionStatus": entry.provider_selection_status,
        "ExperimentID": entry.experiment_id,
        "VariantID": entry.variant_id,
    }


def _portfolio_row(portfolio: ScoringPortfolio):
    return {
        "PortfolioID": portfolio.portfolio_id,
        "PortfolioAttemptID": portfolio.portfolio_attempt_id,
        "RunDate": portfolio.run_date,
        "Capability": portfolio.capability,
        "UseCase": portfolio.use_case,
        "Route": portfolio.route,
        "PolicyID": portfolio.policy_id,
        "PolicyPriority": portfolio.policy_priority,
        "Location": portfolio.location,
        "PageType": portfolio.page_type,
        "Audience": portfolio.audience,
        "CustomerCell": portfolio.customer_cell,
        "ContractVersion": portfolio.contract_version,
        "Status": portfolio.status,
        "EntryCount": len(portfolio.entries),
        "WarningCount": portfolio.warning_count,
        "TaskRunID": portfolio.task_run_id,
        "ExecutionCount": portfolio.execution_count,
        "CompletedAt": portfolio.completed_at,
        "FallbackSourcePortfolioID": portfolio.fallback_source_portfolio_id,
        "FallbackSourceRunDate": portfolio.fallback_source_run_date,
        "FallbackSourceCompletedAt": (
            portfolio.fallback_source_completed_at
        ),
        "PolicyVersion": portfolio.policy_version,
        "PolicyChecksum": portfolio.policy_checksum,
        "SelectionCutoff": portfolio.selection_cutoff,
    }


def publish_scoring_portfolio(
    spark: Any,
    portfolio: ScoringPortfolio,
    *,
    entries_table: str,
    portfolios_table: str,
) -> None:
    """Publish exact entries first and the READY portfolio header last."""
    entry_rows = [_entry_row(portfolio, entry) for entry in portfolio.entries]
    entries = spark.createDataFrame(
        entry_rows,
        schema=spark.table(entries_table).schema,
    )
    replace_scope_by_name(
        entries,
        entries_table,
        {"PortfolioAttemptID": portfolio.portfolio_attempt_id},
        entries.columns,
        spark=spark,
    )
    header = spark.createDataFrame(
        [_portfolio_row(portfolio)],
        schema=spark.table(portfolios_table).schema,
    )
    replace_scope_by_name(
        header,
        portfolios_table,
        {"PortfolioAttemptID": portfolio.portfolio_attempt_id},
        header.columns,
        spark=spark,
    )


def serving_entry(
    portfolio: ScoringPortfolio,
    serving_slot: str,
) -> ScoringPortfolioEntry:
    matches = [
        entry
        for entry in portfolio.entries
        if entry.serving_slot == serving_slot
        and entry.execution_mode == SERVING
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Portfolio must contain exactly one {serving_slot} serving entry"
        )
    return matches[0]


def select_current_input_snapshot_id(
    spark: Any,
    *,
    snapshots_table: str,
    run_date: date,
) -> str:
    """Select the latest accepted same-day input without reviving old attempts."""
    required = {
        "InputSnapshotID",
        "RunDate",
        "Status",
        "ExecutionCount",
        "CompletedAt",
        "TaskRunID",
    }
    source = spark.table(snapshots_table)
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(
            "Scoring input manifest is missing columns: " + ", ".join(missing)
        )
    rows = source.where(F.col("RunDate") == F.lit(run_date)).select(
        *sorted(required)
    ).collect()
    grouped = {}
    for row in rows:
        values = row.asDict(recursive=True)
        grouped.setdefault(values["InputSnapshotID"], []).append(values)

    latest = []
    for snapshot_id in sorted(grouped):
        attempts = grouped[snapshot_id]
        ordering = [
            (
                int(attempt["ExecutionCount"]),
                attempt["CompletedAt"],
                int(attempt["TaskRunID"]),
            )
            for attempt in attempts
        ]
        if len(ordering) != len(set(ordering)):
            raise ValueError(
                f"Contradictory scoring input attempts for {snapshot_id}"
            )
        winner = max(
            attempts,
            key=lambda attempt: (
                int(attempt["ExecutionCount"]),
                attempt["CompletedAt"],
                int(attempt["TaskRunID"]),
            ),
        )
        if winner["Status"] in {"READY", "READY_WITH_WARNINGS"}:
            latest.append(winner)
    if not latest:
        raise ValueError(f"No accepted scoring input snapshot for {run_date}")
    return max(
        latest,
        key=lambda attempt: (
            attempt["CompletedAt"],
            attempt["InputSnapshotID"],
        ),
    )["InputSnapshotID"]


def unchanged_provider_themes(
    spark: Any,
    *,
    item_themes_table: str,
    provider_input_snapshot_id: str,
    current_input_snapshot_id: str,
):
    """Return fallback themes whose accepted item definitions are unchanged."""
    source = spark.table(item_themes_table)
    required = {"InputSnapshotID", "pid", "theme", "theme_rank"}
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(
            "Item-theme snapshot is missing columns: " + ", ".join(missing)
        )
    keys = ["pid", "theme", "theme_rank"]
    provider = source.where(
        F.col("InputSnapshotID") == F.lit(provider_input_snapshot_id)
    ).select(*keys)
    current = source.where(
        F.col("InputSnapshotID") == F.lit(current_input_snapshot_id)
    ).select(*keys)
    if provider.limit(1).count() == 0:
        raise ValueError("Provider input snapshot has no accepted item themes")
    if current.limit(1).count() == 0:
        raise ValueError("Current input snapshot has no accepted item themes")

    changed = (
        provider.join(current, keys, "left_anti")
        .select("theme")
        .unionByName(current.join(provider, keys, "left_anti").select("theme"))
        .groupBy("theme")
        .count()
        .drop("count")
    )
    return (
        provider.select("theme")
        .groupBy("theme")
        .count()
        .drop("count")
        .join(changed, "theme", "left_anti")
        .select(F.col("theme").alias("NextTheme"))
    )


__all__ = [
    "PortfolioPolicy",
    "SCOPE_FIELDS",
    "build_scoring_portfolio",
    "publish_scoring_portfolio",
    "resolve_portfolio_policy",
    "select_current_input_snapshot_id",
    "serving_entry",
    "unchanged_provider_themes",
]
