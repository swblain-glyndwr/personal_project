import sys
from pathlib import Path

from dsutils.argparser import get_job_parser

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore # noqa
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.nextads_delivery.masid_handoff_check import main  # noqa: E402


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    main(
        job_env=jobparser.get_arg("--job_env"),
        client=jobparser.get_arg("--client"),
        log_level=jobparser.get_arg("--log_level"),
        expected_run_date=jobparser.get_arg("--expected_rundate"),
    )
