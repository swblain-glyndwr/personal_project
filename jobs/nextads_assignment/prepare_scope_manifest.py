# Databricks notebook source
"""Prepare v1 assignment loop inputs from the job-owned scope manifest."""

from __future__ import annotations

import sys
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


from next_ads.decisioning.assignment_manifest import (  # noqa: E402
    split_assignment_scope_manifest,
)


def set_assignment_scope_task_values(dbutils_obj: Any) -> None:
    """Validate the manifest and publish the two loop inputs."""
    manifest = split_assignment_scope_manifest(
        dbutils_obj.widgets.get("scope_manifest_json")
    )
    dbutils_obj.jobs.taskValues.set(
        key="primary_scope_manifest",
        value=list(manifest.primary),
    )
    dbutils_obj.jobs.taskValues.set(
        key="secondary_scope_manifest",
        value=list(manifest.secondary),
    )


if __name__ == "__main__":
    set_assignment_scope_task_values(dbutils)  # type: ignore[name-defined]
