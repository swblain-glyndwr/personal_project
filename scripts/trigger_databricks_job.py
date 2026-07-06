import argparse
from typing import Optional

from databricks.sdk import WorkspaceClient


def trigger_job(job_id: int, job_name: str, client: Optional[object] = None):
    """Submit a Databricks job run without waiting for it to finish."""
    workspace_client = client or WorkspaceClient()
    response = workspace_client.jobs.run_now(job_id=job_id)
    run_id = getattr(response, "run_id", None)

    print(f"Triggered {job_name} with job_id={job_id}, run_id={run_id}")
    return run_id


def parse_args():
    parser = argparse.ArgumentParser(
        description="Trigger a Databricks job without waiting for completion."
    )
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--job-name", required=True)
    parser.add_argument(
        "--fail-on-submit-error",
        action="store_true",
        help="Fail this task if the downstream job cannot be submitted.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        trigger_job(job_id=args.job_id, job_name=args.job_name)
    except Exception as exc:
        print(
            f"WARNING: Failed to submit {args.job_name} "
            f"with job_id={args.job_id}: {exc}"
        )
        if args.fail_on_submit_error:
            raise


if __name__ == "__main__":
    main()
