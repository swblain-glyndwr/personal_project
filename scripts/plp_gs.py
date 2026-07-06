import sys
from pathlib import Path

from dsutils.argparser import get_job_parser

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from next_ads.delivery.google_sheets import run_plp_gs_delivery  # noqa: E402


def main(job_env: str, territory: str, client: str, log_level: str | None) -> None:
    run_plp_gs_delivery(
        job_env=job_env,
        territory=territory,
        client=client,
        log_level=log_level,
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
