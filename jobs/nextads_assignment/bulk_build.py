"""Run every scope for one assignment phase on a shared job cluster."""

from __future__ import annotations

import runpy
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger

from jobs.nextads_assignment.publish_build import (
    ScopeManifestEntry,
    parse_scope_manifest_json,
)
from next_ads.decisioning.candidate_inputs import clear_candidate_input_cache


VALID_PHASES = {
    "v1": frozenset({"primary", "secondary"}),
    "v2": frozenset({"all"}),
}
COMMON_ARGUMENTS = (
    "--client",
    "--job_env",
    "--scope_manifest_json",
    "--run_date",
    "--build_run_id",
    "--candidate_build_attempt_id",
    "--task_run_id",
    "--execution_count",
    "--customer_cells_table",
    "--customer_cells_delta_version",
)


@dataclass(frozen=True)
class ScopeInvocation:
    scope: str
    script: Path
    arguments: tuple[str, ...]


def _required_arg(job_parser: Any, name: str) -> str:
    value = job_parser.get_arg(name)
    if value is None or not str(value).strip():
        raise ValueError(f"{name} must be provided")
    return str(value).strip()


def select_phase_entries(
    manifest: tuple[ScopeManifestEntry, ...],
    *,
    route: str,
    phase: str,
) -> tuple[ScopeManifestEntry, ...]:
    if route not in VALID_PHASES:
        raise ValueError("--route must be one of: v1, v2")
    if phase not in VALID_PHASES[route]:
        raise ValueError(
            f"--phase must be one of: {', '.join(sorted(VALID_PHASES[route]))}"
        )
    entries = (
        manifest
        if route == "v2"
        else tuple(entry for entry in manifest if entry.phase == phase)
    )
    if not entries:
        raise ValueError(f"No {route} assignment scopes found for phase {phase}")
    return entries


def build_scope_invocations(
    *,
    project_root: Path,
    route: str,
    phase: str,
    manifest: tuple[ScopeManifestEntry, ...],
    common_arguments: tuple[str, ...],
) -> tuple[ScopeInvocation, ...]:
    entries = select_phase_entries(manifest, route=route, phase=phase)
    script = (
        project_root / "jobs/nextads_assignment/build_page.py"
        if route == "v1"
        else project_root / "jobs/nextads_v2/build_page.py"
    )
    invocations = []
    for entry in entries:
        scope_arguments = (
            ("--location", entry.scope)
            if route == "v1"
            else ("--page_type", entry.scope)
        )
        inheritance_arguments = (
            ("--inherit_basic_from", entry.inherit_basic_from)
            if entry.inherit_basic_from
            else ()
        )
        invocations.append(
            ScopeInvocation(
                scope=entry.scope,
                script=script,
                arguments=(
                    *common_arguments,
                    *scope_arguments,
                    *inheritance_arguments,
                ),
            )
        )
    return tuple(invocations)


def main() -> None:
    parser = get_job_parser()
    parser._parse_args()
    route = _required_arg(parser, "--route").lower()
    phase = _required_arg(parser, "--phase").lower()
    log_level = parser.get_arg("--log_level")
    configure_logging(log_level=log_level) if log_level else configure_logging()
    logger = get_logger(__name__)
    raw_manifest = _required_arg(parser, "--scope_manifest_json")
    manifest = parse_scope_manifest_json(raw_manifest)
    common_arguments = tuple(
        value
        for name in COMMON_ARGUMENTS
        for value in (name, _required_arg(parser, name))
    )
    if log_level:
        common_arguments = (*common_arguments, "--log_level", str(log_level))
    invocations = build_scope_invocations(
        project_root=PROJECT_ROOT,
        route=route,
        phase=phase,
        manifest=manifest,
        common_arguments=common_arguments,
    )

    original_argv = sys.argv[:]
    try:
        for index, invocation in enumerate(invocations, start=1):
            logger.info(
                "Building %s assignment scope %s (%s/%s)",
                route,
                invocation.scope,
                index,
                len(invocations),
            )
            sys.argv = [str(invocation.script), *invocation.arguments]
            runpy.run_path(
                str(invocation.script),
                run_name=f"__nextads_{route}_{invocation.scope}__",
            )
    finally:
        sys.argv = original_argv
        clear_candidate_input_cache()


if __name__ == "__main__":
    main()


__all__ = [
    "ScopeInvocation",
    "build_scope_invocations",
    "main",
    "select_phase_entries",
]
