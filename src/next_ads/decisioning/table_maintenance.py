"""Allowlisted retention and Delta maintenance for NextAds outputs."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any


VACUUM_RETENTION_HOURS = 168
WEEKLY_MAINTENANCE_WEEKDAY = 6  # Sunday
_IDENTIFIER_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class MaintenanceTableSpec:
    """One repository-owned table maintenance contract."""

    name: str
    config_key: str
    retention_days: int | None
    retention_column: str | None
    retention_comparison: str | None

    def __post_init__(self) -> None:
        """Require complete and positive retention settings."""
        configured = (
            self.retention_days,
            self.retention_column,
            self.retention_comparison,
        )
        if any(value is None for value in configured) and not all(
            value is None for value in configured
        ):
            raise ValueError(
                "retention settings must be either all set or all omitted"
            )
        if self.retention_days is not None and self.retention_days < 1:
            raise ValueError("retention_days must be at least one")
        if self.retention_comparison not in {None, "<", "<="}:
            raise ValueError("retention_comparison must be '<' or '<='")


@dataclass(frozen=True)
class ResolvedMaintenanceTable:
    """One allowlisted table resolved to a validated physical name."""

    spec: MaintenanceTableSpec
    table: str


@dataclass(frozen=True)
class MaintenanceStatement:
    """One deterministic maintenance operation."""

    table_name: str
    operation: str
    sql: str


def _legacy_history(name: str, config_key: str) -> MaintenanceTableSpec:
    """Preserve the legacy dsutils 731-day strict-less-than boundary."""
    return MaintenanceTableSpec(
        name=name,
        config_key=config_key,
        retention_days=731,
        retention_column="rundate",
        retention_comparison="<",
    )


def _retained_history(
    name: str,
    config_key: str,
    *,
    days: int,
    comparison: str,
) -> MaintenanceTableSpec:
    """Declare an explicit date-retention contract."""
    return MaintenanceTableSpec(
        name=name,
        config_key=config_key,
        retention_days=days,
        retention_column="rundate",
        retention_comparison=comparison,
    )


def _snapshot(name: str, config_key: str) -> MaintenanceTableSpec:
    """Declare a table with weekly maintenance but no row deletion."""
    return MaintenanceTableSpec(
        name=name,
        config_key=config_key,
        retention_days=None,
        retention_column=None,
        retention_comparison=None,
    )


CELL_TABLE_SPECS = (
    _snapshot("fixed_cell_latest", "customer_cells_fixed_latest"),
    _legacy_history("transient_cell_history", "customer_cells_transient"),
    _snapshot("transient_cell_latest", "customer_cells_transient_latest"),
    _snapshot("combined_cell_latest", "customer_cells_latest"),
)

V1_CONTROL_TABLE_SPECS = (
    _legacy_history("v1_control_raw_history", "control_sheet_raw"),
    _snapshot("v1_control_raw_latest", "control_sheet_raw_latest"),
    _legacy_history("v1_placement_raw_history", "control_sheet_plp_raw"),
    _snapshot("v1_placement_raw_latest", "control_sheet_plp_raw_latest"),
    _legacy_history("v1_control_history", "control_sheet"),
    _snapshot("v1_control_latest", "control_sheet_latest"),
    _legacy_history("multipage_location_history", "multipage_locations"),
    _snapshot("multipage_location_latest", "multipage_locations_latest"),
)

V2_CONTROL_TABLE_SPECS = (
    _legacy_history("v2_control_raw_history", "control_sheet_raw_v2"),
    _snapshot("v2_control_raw_latest", "control_sheet_raw_latest_v2"),
    _legacy_history("v2_exclusion_history", "exclusions"),
    _snapshot("v2_exclusion_latest", "exclusions_latest"),
    _legacy_history("v2_control_history", "control_sheet_v2"),
    _snapshot("v2_control_latest", "control_sheet_latest_v2"),
)

ATTRIBUTE_THEME_TABLE_SPECS = (
    _legacy_history("attribute_set_history", "attribute_set"),
    _snapshot("attribute_set_latest", "attribute_set_latest"),
    _snapshot("item_attribute_latest", "item_attributes_latest"),
    _legacy_history("theme_mapping_history", "theme_mapping"),
    _snapshot("theme_mapping_latest", "theme_mapping_latest"),
    _legacy_history("item_theme_history", "item_themes"),
    _snapshot("item_theme_latest", "item_themes_latest"),
)

SCORING_INPUT_SNAPSHOT_TABLE_SPECS = (
    _snapshot("scoring_input_snapshots", "scoring_input_snapshots"),
    _snapshot(
        "scoring_input_snapshot_sources",
        "scoring_input_snapshot_sources",
    ),
    MaintenanceTableSpec(
        name="scoring_input_item_themes",
        config_key="scoring_input_item_themes",
        retention_days=35,
        retention_column="RunDate",
        retention_comparison="<=",
    ),
    _snapshot(
        "scoring_input_theme_mapping_raw",
        "scoring_input_theme_mapping_raw",
    ),
)

LEGACY_SCORING_TABLE_SPECS = (
    _snapshot("theme_scoring_event_latest", "theme_scoring_events_latest"),
    _legacy_history("theme_transition_history", "theme_transitions"),
    _snapshot("theme_transition_latest", "theme_transitions_latest"),
    _legacy_history("next_theme_score_history", "next_theme_scores"),
    _snapshot("next_theme_score_latest", "next_theme_scores_latest"),
)

CANDIDATE_TABLE_SPECS = (
    _legacy_history("theme_score_component_history", "theme_score_components"),
    _snapshot("theme_score_component_latest", "theme_score_components_latest"),
    _snapshot(
        "v1_preranked_candidate_latest",
        "preranked_ads_from_themes_latest",
    ),
    _snapshot(
        "v2_preranked_candidate_latest",
        "preranked_ads_from_themes_v2_latest",
    ),
)

PAYLOAD_TABLE_SPECS = (
    _legacy_history("v2_payload_history", "nextads_payload"),
    _snapshot("v2_payload_latest", "nextads_payload_latest"),
)

DATA_DELIVERY_TABLE_SPECS = (
    _legacy_history("sort_order_history", "sort_order_v2"),
    _legacy_history("cms_content_history", "cms_content"),
    _snapshot("viewed_bought_latest", "viewed_bought_latest"),
)

ASSIGNMENT_TABLE_SPECS = (
    _retained_history(
        "v1_assignment_history",
        "assignments",
        days=731,
        comparison="<=",
    ),
    _snapshot("v1_assignment_latest", "assignments_latest"),
    MaintenanceTableSpec(
        name="v1_assignment_staging",
        config_key="assignments_build_staging",
        retention_days=2,
        retention_column="rundate",
        retention_comparison="<=",
    ),
    _retained_history(
        "v2_assignment_history",
        "assignments_v2",
        days=731,
        comparison="<=",
    ),
    _snapshot("v2_assignment_latest", "assignments_v2_latest"),
    MaintenanceTableSpec(
        name="v2_assignment_staging",
        config_key="assignments_v2_build_staging",
        retention_days=2,
        retention_column="rundate",
        retention_comparison="<=",
    ),
    MaintenanceTableSpec(
        name="assignment_build_events",
        config_key="assignment_build_events",
        retention_days=7,
        retention_column="BuildDate",
        retention_comparison="<=",
    ),
)

CONFIGURED_TABLE_SPECS = (
    *CELL_TABLE_SPECS,
    *V1_CONTROL_TABLE_SPECS,
    *V2_CONTROL_TABLE_SPECS,
    *ATTRIBUTE_THEME_TABLE_SPECS,
    *SCORING_INPUT_SNAPSHOT_TABLE_SPECS,
    *LEGACY_SCORING_TABLE_SPECS,
    *CANDIDATE_TABLE_SPECS,
    *PAYLOAD_TABLE_SPECS,
    *DATA_DELIVERY_TABLE_SPECS,
    *ASSIGNMENT_TABLE_SPECS,
)

PLP_HISTORY_SPEC = MaintenanceTableSpec(
    name="plp_gs_history",
    config_key="nextads_plp_gs_latest",
    retention_days=365,
    retention_column="rundate",
    retention_comparison="<=",
)


def _mapping_value(mapping: Any, key: str, *, label: str) -> Any:
    """Read one case-insensitive key from a mapping-like config object."""
    if isinstance(mapping, Mapping):
        items = mapping.items()
    elif hasattr(mapping, "items"):
        items = mapping.items()
    else:
        value = getattr(mapping, key, None)
        if value is None:
            raise ValueError(f"Configuration is missing {label}.{key}")
        return value

    target = key.casefold()
    for candidate, value in items:
        if str(candidate).casefold() == target:
            return value
    raise ValueError(f"Configuration is missing {label}.{key}")


def validate_qualified_table_name(table: Any) -> str:
    """Require a simple three-part Unity Catalog table identifier."""
    if not isinstance(table, str) or not table.strip():
        raise ValueError("Configured table names must be non-empty strings")
    value = table.strip()
    parts = value.split(".")
    if len(parts) != 3 or any(
        not _IDENTIFIER_PART.fullmatch(part) for part in parts
    ):
        raise ValueError(
            "Configured table names must use catalog.schema.table with "
            f"simple identifier parts: {table!r}"
        )
    return value


def quote_qualified_table_name(table: str) -> str:
    """Quote a table name after strict identifier validation."""
    return ".".join(
        f"`{part}`" for part in validate_qualified_table_name(table).split(".")
    )


def resolve_maintenance_tables(config: Any) -> tuple[ResolvedMaintenanceTable, ...]:
    """Resolve only the repository-owned active-output table allowlist."""
    tables_write = getattr(config, "tables_write", None)
    if tables_write is None:
        raise ValueError("Configuration is missing tables_write")

    resolved = [
        ResolvedMaintenanceTable(
            spec=spec,
            table=validate_qualified_table_name(
                _mapping_value(
                    tables_write,
                    spec.config_key,
                    label="tables_write",
                )
            ),
        )
        for spec in CONFIGURED_TABLE_SPECS
    ]

    plp_history = validate_qualified_table_name(
        _mapping_value(
            tables_write,
            PLP_HISTORY_SPEC.config_key,
            label="tables_write",
        )
    )
    resolved.append(
        ResolvedMaintenanceTable(
            spec=PLP_HISTORY_SPEC,
            table=plp_history,
        )
    )

    physical_tables = [table.table for table in resolved]
    if len(set(physical_tables)) != len(physical_tables):
        raise ValueError("Maintenance allowlist resolves duplicate table names")
    return tuple(resolved)


def is_weekly_maintenance_day(run_date: date) -> bool:
    """Return whether the logical run date is the weekly Sunday cycle."""
    return run_date.weekday() == WEEKLY_MAINTENANCE_WEEKDAY


def build_retention_statement(
    table: ResolvedMaintenanceTable,
    run_date: date,
) -> MaintenanceStatement | None:
    """Build an idempotent date-retention statement for one table."""
    days = table.spec.retention_days
    column = table.spec.retention_column
    comparison = table.spec.retention_comparison
    if days is None or column is None or comparison is None:
        return None

    quoted_table = quote_qualified_table_name(table.table)
    sql = (
        f"DELETE FROM {quoted_table}\n"
        f"WHERE `{column}` {comparison} "
        f"date_sub(DATE '{run_date.isoformat()}', {days})"
    )
    return MaintenanceStatement(
        table_name=table.spec.name,
        operation="retention",
        sql=sql,
    )


def build_weekly_statements(
    table: ResolvedMaintenanceTable,
) -> tuple[MaintenanceStatement, MaintenanceStatement]:
    """Build weekly compaction and safe seven-day vacuum operations."""
    quoted_table = quote_qualified_table_name(table.table)
    return (
        MaintenanceStatement(
            table_name=table.spec.name,
            operation="optimize",
            sql=f"OPTIMIZE {quoted_table}",
        ),
        MaintenanceStatement(
            table_name=table.spec.name,
            operation="vacuum",
            sql=(
                f"VACUUM {quoted_table} "
                f"RETAIN {VACUUM_RETENTION_HOURS} HOURS"
            ),
        ),
    )


def build_maintenance_plan(
    config: Any,
    run_date: date,
) -> tuple[MaintenanceStatement, ...]:
    """Build the same ordered SQL plan for every replay of a logical date."""
    resolved_tables = resolve_maintenance_tables(config)
    statements = [
        statement
        for table in resolved_tables
        if (statement := build_retention_statement(table, run_date)) is not None
    ]
    if is_weekly_maintenance_day(run_date):
        for table in resolved_tables:
            statements.extend(build_weekly_statements(table))
    return tuple(statements)


def execute_maintenance_plan(
    spark: Any,
    statements: Sequence[MaintenanceStatement],
    *,
    run_date: date,
    logger: Any,
) -> None:
    """Execute an already validated, deterministic maintenance plan."""
    current_row = spark.sql(
        "SELECT current_date() AS current_date"
    ).first()
    current_date = current_row["current_date"]
    if run_date > current_date:
        raise ValueError(
            "Logical run_date cannot be in the future: "
            f"{run_date.isoformat()} > {current_date.isoformat()}"
        )

    for statement in statements:
        logger.info(
            "Running %s maintenance for %s",
            statement.operation,
            statement.table_name,
        )
        spark.sql(statement.sql)
