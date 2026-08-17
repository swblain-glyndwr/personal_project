from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineTaskIdentity:
    pipeline_id: str
    pipeline_task_run_id: int


def pipeline_task_identity(
    *,
    pipeline_id: str,
    pipeline_task_run_id: int,
) -> PipelineTaskIdentity:
    """Validate the supported job metadata that identifies a pipeline task."""
    if not isinstance(pipeline_id, str) or not pipeline_id.strip():
        raise ValueError("pipeline_id must not be empty")
    if (
        isinstance(pipeline_task_run_id, bool)
        or not isinstance(pipeline_task_run_id, int)
        or pipeline_task_run_id < 1
    ):
        raise ValueError("pipeline_task_run_id must be a positive integer")
    return PipelineTaskIdentity(
        pipeline_id=pipeline_id.strip(),
        pipeline_task_run_id=pipeline_task_run_id,
    )


__all__ = ["PipelineTaskIdentity", "pipeline_task_identity"]
