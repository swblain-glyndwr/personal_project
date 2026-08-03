from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PipelineUpdateIdentity:
    update_id: str
    update_type: str


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def resolve_pipeline_update(
    spark: Any,
    *,
    pipeline_id: str,
    pipeline_task_run_id: int,
    timeout_seconds: int = 300,
    poll_seconds: int = 10,
) -> PipelineUpdateIdentity:
    """Resolve a Lakeflow update by its exact triggering pipeline task run."""
    if not isinstance(pipeline_id, str) or not pipeline_id.strip():
        raise ValueError("pipeline_id must not be empty")
    if (
        isinstance(pipeline_task_run_id, bool)
        or not isinstance(pipeline_task_run_id, int)
        or pipeline_task_run_id < 1
    ):
        raise ValueError("pipeline_task_run_id must be a positive integer")
    if timeout_seconds < 0 or poll_seconds < 1:
        raise ValueError("Pipeline metadata polling values are invalid")

    statement = f"""
SELECT DISTINCT update_id, update_type
FROM system.lakeflow.pipeline_update_timeline
WHERE pipeline_id = {_quoted(pipeline_id.strip())}
  AND trigger_type = 'JOB_TASK'
  AND CAST(trigger_details.job_task.job_task_run_id AS STRING)
      = {_quoted(str(pipeline_task_run_id))}
  AND result_state = 'COMPLETED'
"""
    deadline = time.monotonic() + timeout_seconds
    while True:
        rows = spark.sql(statement).collect()
        identities = {
            (str(row["update_id"]), str(row["update_type"])) for row in rows
        }
        if len(identities) == 1:
            update_id, update_type = identities.pop()
            if not update_id or not update_type:
                raise ValueError("Pipeline update identity is incomplete")
            return PipelineUpdateIdentity(update_id, update_type)
        if len(identities) > 1:
            raise ValueError(
                "Pipeline task maps to contradictory Lakeflow updates"
            )
        if time.monotonic() >= deadline:
            raise ValueError(
                "No completed Lakeflow update found for pipeline task "
                f"{pipeline_task_run_id}"
            )
        time.sleep(poll_seconds)


__all__ = ["PipelineUpdateIdentity", "resolve_pipeline_update"]
