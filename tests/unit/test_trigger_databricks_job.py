from types import SimpleNamespace

import pytest

from jobs.orchestration import trigger_databricks_job


class FakeJobsClient:
    def __init__(self):
        self.submitted_job_id = None

    def run_now(self, *, job_id):
        self.submitted_job_id = job_id
        return SimpleNamespace(run_id=12345)


class FakeWorkspaceClient:
    def __init__(self):
        self.jobs = FakeJobsClient()


class FakeParameterizedJobsClient:
    def __init__(self):
        self.submitted_job_id = None
        self.submitted_job_parameters = None

    def run_now(self, *, job_id, job_parameters):
        self.submitted_job_id = job_id
        self.submitted_job_parameters = job_parameters
        return SimpleNamespace(run_id=54321)


class FakeParameterizedWorkspaceClient:
    def __init__(self):
        self.jobs = FakeParameterizedJobsClient()


def test_trigger_job_submits_run_without_waiting_for_completion():
    client = FakeWorkspaceClient()

    run_id = trigger_databricks_job.trigger_job(
        job_id=67890,
        job_name="mktg_next_uk_nextads_assignment_validation",
        client=client,
    )

    assert client.jobs.submitted_job_id == 67890
    assert run_id == 12345


def test_trigger_job_forwards_job_parameters_to_run_now():
    client = FakeParameterizedWorkspaceClient()
    job_parameters = {
        "run_date": "2026-07-29",
        "refresh_theme_mapping": "false",
    }

    run_id = trigger_databricks_job.trigger_job(
        job_id=67890,
        job_name="mktg_next_uk_nextads_assignment_validation",
        client=client,
        job_parameters=job_parameters,
    )

    assert client.jobs.submitted_job_id == 67890
    assert client.jobs.submitted_job_parameters == job_parameters
    assert run_id == 54321


def test_parse_args_collects_repeatable_job_parameters():
    args = trigger_databricks_job.parse_args(
        [
            "--job-id",
            "67890",
            "--job-name",
            "mktg_next_uk_nextads_assignment_validation",
            "--job-parameter",
            "run_date=2026-07-29",
            "--job-parameter",
            "refresh_theme_mapping=false",
        ]
    )

    assert args.job_parameters == {
        "run_date": "2026-07-29",
        "refresh_theme_mapping": "false",
    }


def test_parse_args_keeps_existing_no_parameter_behavior():
    args = trigger_databricks_job.parse_args(
        [
            "--job-id",
            "67890",
            "--job-name",
            "mktg_next_uk_nextads_assignment_validation",
        ]
    )

    assert args.job_parameters == {}


@pytest.mark.parametrize(
    "job_parameter",
    [
        "run_date",
        "=2026-07-29",
        "run_date=",
        " =2026-07-29",
        "run_date= ",
        "run date=2026-07-29",
        "run/date=2026-07-29",
        "run:date=2026-07-29",
    ],
)
def test_parse_args_rejects_malformed_or_empty_job_parameters(job_parameter):
    with pytest.raises(SystemExit):
        trigger_databricks_job.parse_args(
            [
                "--job-id",
                "67890",
                "--job-name",
                "mktg_next_uk_nextads_assignment_validation",
                "--job-parameter",
                job_parameter,
            ]
        )


def test_parse_args_rejects_duplicate_job_parameter_keys():
    with pytest.raises(SystemExit):
        trigger_databricks_job.parse_args(
            [
                "--job-id",
                "67890",
                "--job-name",
                "mktg_next_uk_nextads_assignment_validation",
                "--job-parameter",
                "run_date=2026-07-29",
                "--job-parameter",
                "run_date=2026-07-30",
            ]
        )
