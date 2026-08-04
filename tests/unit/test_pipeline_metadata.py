from pathlib import Path

import pytest

from next_ads.ranking.pipeline_metadata import pipeline_task_identity


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_pipeline_task_uses_supported_job_metadata_as_its_identity():
    identity = pipeline_task_identity(
        pipeline_id="pipeline-123",
        pipeline_task_run_id=456,
    )

    assert identity.pipeline_id == "pipeline-123"
    assert identity.pipeline_task_run_id == 456


@pytest.mark.parametrize(
    "pipeline_id, pipeline_task_run_id, message",
    [
        ("", 456, "pipeline_id"),
        ("pipeline-123", 0, "pipeline_task_run_id"),
        ("pipeline-123", True, "pipeline_task_run_id"),
    ],
)
def test_pipeline_task_identity_rejects_invalid_job_metadata(
    pipeline_id,
    pipeline_task_run_id,
    message,
):
    with pytest.raises(ValueError, match=message):
        pipeline_task_identity(
            pipeline_id=pipeline_id,
            pipeline_task_run_id=pipeline_task_run_id,
        )


def test_runtime_does_not_depend_on_preview_lakeflow_system_tables():
    runtime_roots = ("jobs", "src", "pipelines")
    offenders = []
    for root_name in runtime_roots:
        for path in (PROJECT_ROOT / root_name).rglob("*"):
            if path.suffix.lower() not in {".py", ".sql", ".yml", ".yaml"}:
                continue
            if "system.lakeflow" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(PROJECT_ROOT)))

    assert offenders == []
