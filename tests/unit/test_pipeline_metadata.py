from types import SimpleNamespace

import pytest

from next_ads.ranking.pipeline_metadata import resolve_pipeline_update


class _Spark:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(collect=lambda: self.rows)


def test_pipeline_task_resolves_its_exact_lakeflow_update_identity():
    spark = _Spark([{"update_id": "update-1", "update_type": "REFRESH"}])

    identity = resolve_pipeline_update(
        spark,
        pipeline_id="pipeline-123",
        pipeline_task_run_id=456,
        timeout_seconds=0,
    )

    assert identity.update_id == "update-1"
    assert identity.update_type == "REFRESH"
    statement = spark.statements[0]
    assert "pipeline_id = 'pipeline-123'" in statement
    assert "trigger_type = 'JOB_TASK'" in statement
    assert "trigger_details.job_task.job_task_run_id" in statement
    assert "= '456'" in statement
    assert "result_state = 'COMPLETED'" in statement


@pytest.mark.parametrize(
    "rows, message",
    [
        ([], "No completed Lakeflow update"),
        (
            [
                {"update_id": "one", "update_type": "REFRESH"},
                {"update_id": "two", "update_type": "REFRESH"},
            ],
            "contradictory",
        ),
    ],
)
def test_pipeline_update_resolution_rejects_missing_or_ambiguous_updates(
    rows,
    message,
):
    with pytest.raises(ValueError, match=message):
        resolve_pipeline_update(
            _Spark(rows),
            pipeline_id="pipeline-123",
            pipeline_task_run_id=456,
            timeout_seconds=0,
        )
