import sys
from pathlib import Path

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from next_ads.delivery.masid_handoff import (  # noqa: E402
    check_masid_handoff_table,
    expected_rundate,
    resolve_assignments_latest_table,
)
from next_ads.utils import config_manager  # noqa: E402
from next_ads.common.paths import load_client_config


def main(
    job_env: str,
    client: str,
    log_level: str | None,
    expected_run_date: str | None = None,
) -> None:
    expected_run_date = expected_rundate(expected_run_date)
    configure_logging(log_level=log_level) if log_level else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info(f"Running in job environment: {job_env}")

    if not client:
        assert job_env.lower() == "dev", (
            f"Client must be specified when running in {job_env}"
        )
        client = "next_uk"
        logger.warning(f"Client not specified (defaulting to {client})")

    config = config_manager.load_config(job_env)
    logger.info(f"Configuring MASID handoff check for client: {client}")
    cfg = load_client_config(client)

    assignments_latest = resolve_assignments_latest_table(config, cfg, client)

    logger.info(f"Checking MASID handoff table: {assignments_latest}")
    check_masid_handoff_table(
        spark=spark,
        assignments_latest=assignments_latest,
        expected_run_date=expected_run_date,
        logger=logger,
    )


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    main(
        job_env=jobparser.get_arg("--job_env"),
        client=jobparser.get_arg("--client"),
        log_level=jobparser.get_arg("--log_level"),
        expected_run_date=jobparser.get_arg("--expected_rundate"),
    )
