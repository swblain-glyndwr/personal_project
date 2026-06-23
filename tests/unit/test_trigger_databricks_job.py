from types import SimpleNamespace

from scripts import trigger_databricks_job


class FakeJobsClient:
    def __init__(self):
        self.submitted_job_id = None

    def run_now(self, *, job_id):
        self.submitted_job_id = job_id
        return SimpleNamespace(run_id=12345)


class FakeWorkspaceClient:
    def __init__(self):
        self.jobs = FakeJobsClient()


def test_trigger_job_submits_run_without_waiting_for_completion():
    client = FakeWorkspaceClient()

    run_id = trigger_databricks_job.trigger_job(
        job_id=67890,
        job_name="mktg_next_uk_nextads_qa",
        client=client,
    )

    assert client.jobs.submitted_job_id == 67890
    assert run_id == 12345
