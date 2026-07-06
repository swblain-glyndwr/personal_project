import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from typing import Optional
from dsutils.etl import insert_table_from_to
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import etl
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from dsutils.dbc import configure_spark


def _to_bool(value: Optional[object]) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def main(
    job_env: str,
    client: Optional[str] = None,
    log_level: Optional[str] = None,
    history_days: Optional[int] = None,
    input_tables_only: Optional[object] = None,
) -> None:
    configure_logging(log_level=log_level) if log_level else configure_logging()
    logger = get_logger(__name__)
    configure_spark()

    logger.info(f"Running in job environment: {job_env}")

    if not client:
        assert job_env.lower() == 'dev', \
            f'Client must be specified when running in {job_env}'
        client = 'next_uk'  # Client can be specified for interactive debugging
        logger.warning(f'Client not specified (defaulting to {client})')

    if not history_days:
        assert job_env.lower() == 'dev', \
            f'History Days must be specified when running in {job_env}'
        history_days = 1  # History Days can be specified for interactive debugging
        logger.warning(
            f'History Days not specified (defaulting to {history_days})')

    input_tables_only_bool = _to_bool(input_tables_only)

    runtime_config = config_manager.load_config("dev")
    logger.info(f"Configuring run for client: {client}")
    cfg = load_client_config(client)

    tbls = cfg["tables"]["read"]

    ignore_table_keys = [
        "theme_score_components_latest",
        "theme_scoring_events_latest",
        "preranked_ads_from_themes_latest",
        "next_theme_scores",
        "next_theme_scores_latest"
    ]

    for (k, v) in tbls.items():
        if k in ignore_table_keys and input_tables_only_bool:
            logger.info(f"Skipping {k} table as it is in the ignore list")
            continue

        logger.info(f"Mirroring {k} table (history days: {history_days})")

        tbl_prod = etl.map_tbl(v, catalog=runtime_config.catalog_read, schema=runtime_config.schema_read, client=client)
        tbl_dev = etl.map_tbl(v, catalog=runtime_config.catalog_write, schema=runtime_config.schema_write, client=client)

        logger.info(f"From {tbl_prod}")
        logger.info(f"To {tbl_dev}")

        try:
            insert_table_from_to(
                table_from=tbl_prod,
                table_to=tbl_dev,
                history_days=history_days,
                truncate_table_to=True
            )
        except Exception as e:
            logger.error(f"Failed to mirror {k} table: {str(e)}")

    logger.info("Run Complete")


if __name__ == '__main__':
    jobparser = get_job_parser()
    jobparser._parse_args()

    main(
        job_env=jobparser.get_arg('--job_env'),
        client=jobparser.get_arg('--client'),
        log_level=jobparser.get_arg('--log_level'),
        history_days=jobparser.get_typed_arg('--history_days', int),
        input_tables_only=jobparser.get_arg('--input_tables_only'),
    )
