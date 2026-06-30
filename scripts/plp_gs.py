import sys
from pathlib import Path

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    from dsutils.dbc import get_dbutils as _get_dbutils

    dbutils = _get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from next_ads.delivery.google_sheets import (  # noqa: E402
    process_control_sheet as _process_control_sheet,
    run_plp_gs_delivery,
)


spark = None
dbutils = None


def _get_spark():
    global spark
    if spark is None:
        spark = configure_spark()
    return spark


def _get_dbutils():
    global dbutils
    if dbutils is None:
        dbutils = get_dbutils()
    return dbutils


def process_control_sheet(config):
    return _process_control_sheet(config=config, spark_session=_get_spark())


def main(job_env: str, territory: str, client: str, log_level: str | None) -> None:
    run_plp_gs_delivery(
        job_env=job_env,
        territory=territory,
        client=client,
        log_level=log_level,
        spark_session=_get_spark(),
        dbutils_obj=_get_dbutils(),
    )


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    main(
        job_env=jobparser.get_arg("--job_env"),
        territory=jobparser.get_arg("--territory"),
        client=jobparser.get_arg("--client"),
        log_level=jobparser.get_arg("--log_level"),
    )
