"""Read-only planning for the Next Ads offline feature-store contract."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import yaml

from next_ads.features.feature_store_registry import (
    DEFAULT_REGISTRY_PATH,
    PROJECT_ROOT,
    FeatureStoreRegistry,
    OfflineFeatureState,
    OfflineStoreBinding,
    load_feature_store_registry,
    normalize_release_id,
)


DEFAULT_JOB_DEFINITION_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "databricks"
    / "jobs"
    / "mktg_next_uk_nextads_feature_store.yml"
)
JOB_CONFIG_KEY = "nextads_feature_store_config"
JOB_RESOURCE_KEY = "mktg_next_uk_nextads_feature_store"


def _load_builder_tasks(
    job_definition_path: str | Path,
) -> tuple[dict[str, dict[str, Any]], frozenset[str]]:
    """Load the repository task graph without invoking Databricks APIs."""
    job_path = Path(job_definition_path)
    raw_job = yaml.safe_load(job_path.read_text())
    try:
        tasks = raw_job[JOB_CONFIG_KEY][JOB_RESOURCE_KEY]["tasks"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            f"Offline feature-store job definition is missing its task graph: "
            f"{job_path}"
        ) from exc

    builder_tasks: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_key = str(task["task_key"])
        if task_key in builder_tasks:
            raise ValueError(
                f"Duplicate offline feature-store task key: {task_key}"
            )
        task_dependencies = tuple(
            str(dependency["task_key"])
            for dependency in task.get("depends_on", ())
        )
        python_file = task.get("spark_python_task", {}).get("python_file")
        entrypoint = (
            (job_path.parent / str(python_file)).resolve()
            if python_file
            else None
        )
        builder_tasks[task_key] = {
            "task_dependencies": task_dependencies,
            "entrypoint": entrypoint,
        }

    known_tasks = set(builder_tasks)
    for task_key, task in builder_tasks.items():
        unknown_dependencies = sorted(
            set(task["task_dependencies"]) - known_tasks
        )
        if unknown_dependencies:
            raise ValueError(
                f"Offline feature-store task {task_key} references unknown "
                "task dependencies: " + ", ".join(unknown_dependencies)
            )

    visit_state: dict[str, int] = {}
    visit_stack: list[str] = []

    def visit(task_key: str) -> None:
        state = visit_state.get(task_key, 0)
        if state == 2:
            return
        if state == 1:
            cycle_start = visit_stack.index(task_key)
            cycle = visit_stack[cycle_start:] + [task_key]
            raise ValueError(
                "Offline feature-store task graph contains a cycle: "
                + " -> ".join(cycle)
            )
        visit_state[task_key] = 1
        visit_stack.append(task_key)
        for dependency in builder_tasks[task_key]["task_dependencies"]:
            visit(dependency)
        visit_stack.pop()
        visit_state[task_key] = 2

    for task_key in builder_tasks:
        visit(task_key)

    declared_targets = frozenset(
        str(target_name)
        for target_name, target in raw_job.get("targets", {}).items()
        if JOB_RESOURCE_KEY in target.get("resources", {}).get("jobs", {})
    )
    return builder_tasks, declared_targets


def _validate_implemented_contracts(
    registry: FeatureStoreRegistry,
    builder_tasks: dict[str, dict[str, Any]],
) -> None:
    """Fail when an implemented definition has no executable contract."""
    for feature in registry.implemented_features:
        sql_contract = registry.sql_contract_path(feature.name)
        if not sql_contract.is_file():
            raise ValueError(
                f"Implemented feature {feature.name} is missing SQL contract: "
                f"{sql_contract}"
            )
        if feature.builder not in builder_tasks:
            raise ValueError(
                f"Implemented feature {feature.name} references unknown builder: "
                f"{feature.builder}"
            )
        entrypoint = builder_tasks[feature.builder]["entrypoint"]
        if entrypoint is None or not entrypoint.is_file():
            raise ValueError(
                f"Implemented feature {feature.name} builder has no executable "
                f"entrypoint: {feature.builder}"
            )


def _selected_environments(
    registry: FeatureStoreRegistry,
    environments: Iterable[str] | None,
) -> tuple[str, ...]:
    if environments is None:
        return tuple(
            binding.environment for binding in registry.store_bindings
        )

    raw_environments = (
        (environments,) if isinstance(environments, str) else environments
    )
    selected = tuple(
        str(environment).strip().upper() for environment in raw_environments
    )
    if not selected or any(not environment for environment in selected):
        raise ValueError("At least one offline store environment is required")
    duplicates = sorted(
        {
            environment
            for environment in selected
            if selected.count(environment) > 1
        }
    )
    if duplicates:
        raise ValueError(
            "Duplicate offline store environments: " + ", ".join(duplicates)
        )
    for environment in selected:
        registry.store_binding(environment)
    return selected


def _planned_location(
    binding: OfflineStoreBinding,
    object_name: str,
    raw_release_id: str | None,
) -> tuple[str, str]:
    if binding.requires_release_id and raw_release_id is None:
        return binding.table_path_template(object_name), "RELEASE_ID_REQUIRED"
    return (
        binding.resolved_table_path(object_name, release_id=raw_release_id),
        "RESOLVED",
    )


def build_offline_feature_store_plan(
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    environments: Iterable[str] | None = None,
    job_definition_path: str | Path = DEFAULT_JOB_DEFINITION_PATH,
    release_id: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic plan without changing files or platform state."""
    registry = load_feature_store_registry(registry_path)
    builder_tasks, declared_targets = _load_builder_tasks(job_definition_path)
    _validate_implemented_contracts(registry, builder_tasks)

    for binding in registry.store_bindings:
        target_is_declared = binding.bundle_target in declared_targets
        if binding.repository_declared != target_is_declared:
            raise ValueError(
                f"Offline store binding {binding.environment} says "
                f"repository_declared={binding.repository_declared}, but "
                f"bundle target {binding.bundle_target} presence is "
                f"{target_is_declared}"
            )

    if release_id is None:
        normalized_release_id = None
    else:
        release_id = release_id.strip()
        if not release_id:
            raise ValueError("release_id cannot be blank")
        normalized_release_id = normalize_release_id(release_id)

    state_counts = Counter(
        feature.state.value for feature in registry.offline_features
    )
    selected_environments = _selected_environments(registry, environments)
    environment_plans = []

    for environment in selected_environments:
        binding = registry.store_binding(environment)
        features = []
        for feature in registry.offline_features:
            builder = builder_tasks.get(feature.builder, {})
            table_location, location_state = _planned_location(
                binding, feature.name, release_id
            )
            features.append(
                {
                    "feature_id": feature.feature_id,
                    "state": feature.state.value,
                    "implemented": feature.implemented,
                    "builder": feature.builder,
                    "task_dependencies": list(
                        builder.get("task_dependencies", ())
                    ),
                    "table_location": table_location,
                    "location_state": location_state,
                    "entity": feature.entity,
                    "grain": feature.grain,
                    "primary_keys": list(feature.primary_keys),
                    "timestamp_key": feature.timestamp_key,
                    "write_mode": feature.write_mode,
                    "freshness": feature.freshness,
                    "owner": feature.owner,
                    "missing_contracts": list(feature.missing_contracts),
                }
            )

        views = []
        for view in registry.compatibility_views:
            source_feature = registry.table_spec(str(view["source_feature"]))
            view_location, location_state = _planned_location(
                binding, str(view["name"]), release_id
            )
            views.append(
                {
                    "name": str(view["name"]),
                    "source_feature": source_feature.feature_id,
                    "status": (
                        "CONTRACT_READY"
                        if source_feature.implemented
                        else "BLOCKED"
                    ),
                    "implemented": source_feature.implemented,
                    "view_location": view_location,
                    "location_state": location_state,
                    "missing_contracts": (
                        []
                        if source_feature.implemented
                        else list(source_feature.missing_contracts)
                    ),
                }
            )

        environment_plans.append(
            {
                "environment": binding.environment,
                "repository_state": binding.repository_state,
                "bundle_target": binding.bundle_target,
                "catalog": binding.catalog,
                "schema": binding.schema,
                "features": features,
                "compatibility_views": views,
            }
        )

    return {
        "registry": registry.name,
        "mode": "READ_ONLY",
        "release_id": normalized_release_id,
        "summary": {
            state.value: state_counts.get(state.value, 0)
            for state in OfflineFeatureState
        },
        "environments": environment_plans,
    }


def render_offline_feature_store_plan(plan: dict[str, Any]) -> str:
    """Render a concise deterministic text representation of a plan."""
    summary = plan["summary"]
    lines = [
        f"Offline Feature Store plan ({plan['mode']})",
        (
            "Summary: "
            f"ACTIVE={summary['ACTIVE']} "
            f"COMPATIBILITY={summary['COMPATIBILITY']} "
            f"SCAFFOLD={summary['SCAFFOLD']}"
        ),
    ]

    for environment in plan["environments"]:
        lines.append(
            ""
            f"{environment['environment']} [{environment['repository_state']}] "
            f"target={environment['bundle_target']} "
            f"namespace={environment['catalog']}.{environment['schema']}"
        )
        for feature in environment["features"]:
            task_dependencies = (
                ",".join(feature["task_dependencies"]) or "none"
            )
            missing_contracts = (
                ",".join(feature["missing_contracts"]) or "none"
            )
            lines.append(
                "  "
                f"{feature['state']} {feature['feature_id']} "
                f"implemented={str(feature['implemented']).lower()} "
                f"builder={feature['builder']} "
                f"task_dependencies={task_dependencies} "
                f"missing_contracts={missing_contracts} "
                f"location_state={feature['location_state']} "
                f"table={feature['table_location']}"
            )
        for view in environment["compatibility_views"]:
            missing_contracts = ",".join(view["missing_contracts"]) or "none"
            lines.append(
                "  "
                f"VIEW {view['status']} {view['name']} "
                f"source={view['source_feature']} "
                f"missing_contracts={missing_contracts} "
                f"location_state={view['location_state']} "
                f"view={view['view_location']}"
            )

    return "\n".join(lines)
