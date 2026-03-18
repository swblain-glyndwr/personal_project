from databricks.sdk import WorkspaceClient
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils

APP_NAME = "startstop_job"

jobparser = get_job_parser()
jobparser._parse_args()
JOBID = jobparser.get_arg("--jobid")
WORKSPACE_URL = jobparser.get_arg("--workspace_url")
LOG_LEVEL = jobparser.get_arg("--log_level")
REFRESH_CONTROL_DATE = jobparser.get_arg("--refresh_control_date")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
dbutils = get_dbutils()


def cancel_and_restart_job(workspace_client: WorkspaceClient, job_id: int) -> int:
    """
    Cancel any active runs of a job, wait for cancellation, then start a new run.

    Args:
        workspace_client: A Databricks WorkspaceClient instance
        job_id: The job ID to cancel and restart

    Returns:
        The run ID of the newly started run
    """
    logger.info(f"Getting active runs for job {job_id}")

    # Get all active runs for this job
    active_runs = list(workspace_client.jobs.list_runs(job_id=job_id, active_only=True))

    # Cancel all active runs and wait for cancellation
    for run in active_runs:
        logger.info(f"Cancelling run {run.run_id}")
        workspace_client.jobs.cancel_run_and_wait(run_id=run.run_id)
        logger.info(f"Run {run.run_id} has been cancelled")

    # Start a new run
    logger.info(f"Starting new run for job {job_id}")
    run_response = workspace_client.jobs.run_now(job_id=job_id)
    new_run_id = run_response.run_id
    logger.info(f"Started new run with ID {new_run_id}")

    return new_run_id


try:
    job_id = int(JOBID)
except ValueError:
    logger.error(f"Invalid --jobid value: '{JOBID}'. Must be an integer.")
    raise SystemExit(f"Invalid --jobid value: '{JOBID}'. Must be an integer.")

# Validate workspace_url argument
if not WORKSPACE_URL:
    logger.error(
        "Missing required argument --workspace_url. Usage: --workspace_url <WORKSPACE_URL>"
    )
    raise SystemExit(
        "Missing required argument --workspace_url. Provide --workspace_url <WORKSPACE_URL>"
    )

# Required Service principal credentials
client_id = dbutils.secrets.get(
    scope="DataIng-KV-MKT-PROD-EUW", key="Databricks-Spn-MarketingData-ClientId"
)
client_secret = dbutils.secrets.get(
    scope="DataIng-KV-MKT-PROD-EUW", key="Databricks-Spn-MarketingData-ClientSecret"
)
tenant_id = dbutils.secrets.get(scope="realtime", key="DataPlatform-Prod-TenantId")

w = WorkspaceClient(
    host=WORKSPACE_URL,
    azure_client_id=client_id,
    azure_client_secret=client_secret,
    azure_tenant_id=tenant_id,
)

cancel_and_restart_job(w, job_id)
