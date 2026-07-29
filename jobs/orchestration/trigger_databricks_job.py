import argparse
import re
from collections.abc import Mapping, Sequence
from typing import Optional

from databricks.sdk import WorkspaceClient


def trigger_job(
    job_id: int,
    job_name: str,
    client: Optional[object] = None,
    job_parameters: Optional[Mapping[str, str]] = None,
):
    """Submit a Databricks job run without waiting for it to finish."""
    workspace_client = client or WorkspaceClient()
    run_now_arguments = {"job_id": job_id}
    if job_parameters:
        run_now_arguments["job_parameters"] = dict(job_parameters)

    response = workspace_client.jobs.run_now(**run_now_arguments)
    run_id = getattr(response, "run_id", None)

    print(f"Triggered {job_name} with job_id={job_id}, run_id={run_id}")
    return run_id


def _parse_job_parameter(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            "job parameters must use the format KEY=VALUE"
        )

    key, parameter_value = value.split("=", 1)
    key = key.strip()
    parameter_value = parameter_value.strip()

    if not key:
        raise argparse.ArgumentTypeError(
            "job parameter keys must not be empty"
        )
    if re.fullmatch(r"[A-Za-z0-9_.-]+", key) is None:
        raise argparse.ArgumentTypeError(
            "job parameter keys may contain only letters, numbers, "
            "underscores, hyphens, and periods"
        )
    if not parameter_value:
        raise argparse.ArgumentTypeError(
            f"job parameter '{key}' must have a non-empty value"
        )

    return key, parameter_value


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(
        description="Trigger a Databricks job without waiting for completion."
    )
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--job-name", required=True)
    parser.add_argument(
        "--job-parameter",
        action="append",
        default=[],
        type=_parse_job_parameter,
        metavar="KEY=VALUE",
        help=(
            "Job parameter forwarded to the downstream run. "
            "Repeat for each parameter."
        ),
    )
    parser.add_argument(
        "--fail-on-submit-error",
        action="store_true",
        help="Fail this task if the downstream job cannot be submitted.",
    )
    args = parser.parse_args(argv)

    job_parameters = {}
    for key, value in args.job_parameter:
        if key in job_parameters:
            parser.error(f"duplicate job parameter key: '{key}'")
        job_parameters[key] = value
    args.job_parameters = job_parameters
    del args.job_parameter

    return args


def main():
    args = parse_args()

    try:
        trigger_job(
            job_id=args.job_id,
            job_name=args.job_name,
            job_parameters=args.job_parameters,
        )
    except Exception as exc:
        print(
            f"WARNING: Failed to submit {args.job_name} "
            f"with job_id={args.job_id}: {exc}"
        )
        if args.fail_on_submit_error:
            raise


if __name__ == "__main__":
    main()
