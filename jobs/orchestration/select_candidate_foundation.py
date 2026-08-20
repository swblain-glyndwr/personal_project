import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    notebook_path = (
        get_dbutils()
        .notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.candidates.foundation_manifest import (
    wait_for_candidate_foundation,
)
from next_ads.common import config_manager


READINESS_TIME = time(18, 30)
READINESS_TIMEZONE = ZoneInfo("Europe/London")


def _required(value, label):
    if value is None or not str(value).strip():
        raise ValueError(f"{label} is required")
    return str(value).strip()


def _integer(value, label, *, minimum=0):
    try:
        result = int(_required(value, label))
    except ValueError as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if result < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return result


def _parse_cutoff(value):
    if value is None or not str(value).strip():
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("selection_cutoff must include a timezone")
    return parsed.astimezone(timezone.utc)


def _default_cutoff(run_date):
    return datetime.combine(
        run_date,
        READINESS_TIME,
        tzinfo=READINESS_TIMEZONE,
    ).astimezone(timezone.utc)


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    FOUNDATION_SNAPSHOT_ID,
    READINESS_WAIT_SECONDS,
    READINESS_POLL_SECONDS,
    SELECTION_CUTOFF,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    ORCHESTRATION_RUN_ID,
):
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    job_env = _required(JOB_ENV, "job_env")
    client = _required(CLIENT, "client")
    run_date = date.fromisoformat(_required(RUN_DATE, "run_date"))
    requested_snapshot_id = _required(
        FOUNDATION_SNAPSHOT_ID,
        "foundation_snapshot_id",
    )
    wait_seconds = _integer(
        READINESS_WAIT_SECONDS,
        "readiness_wait_seconds",
    )
    poll_seconds = _integer(
        READINESS_POLL_SECONDS,
        "readiness_poll_seconds",
        minimum=1,
    )
    task_run_id = _integer(TASK_RUN_ID, "task_run_id", minimum=1)
    execution_count = _integer(EXECUTION_COUNT, "execution_count")
    orchestration_run_id = _integer(
        ORCHESTRATION_RUN_ID,
        "orchestration_run_id",
        minimum=1,
    )
    spark = configure_spark()
    config = config_manager.load_config(job_env, client=client)
    selection = wait_for_candidate_foundation(
        spark,
        builds_table=config.tables_write.candidate_foundation_builds,
        run_date=run_date,
        selection_cutoff=_parse_cutoff(SELECTION_CUTOFF)
        or _default_cutoff(run_date),
        requested_snapshot_id=requested_snapshot_id,
        wait_seconds=wait_seconds,
        poll_seconds=poll_seconds,
    )
    bindings = selection.output_bindings
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="foundation_snapshot_id", value=selection.snapshot_id)
    task_values.set(
        key="foundation_selection_status",
        value=selection.selection_status,
    )
    task_values.set(
        key="foundation_source_run_date",
        value=selection.source_run_date.isoformat(),
    )
    for binding_name, task_prefix in (
        ("customer_cells", "customer_cells"),
        ("repeat_ad_exposure", "repeat_ad_exposure"),
        ("ad_feedback", "ad_feedback"),
    ):
        binding = bindings[binding_name]
        task_values.set(key=f"{task_prefix}_table", value=binding["table"])
        task_values.set(
            key=f"{task_prefix}_delta_version",
            value=int(binding["delta_version"]),
        )
    logger.info(
        "Selected candidate foundation %s with status %s for task %s, "
        "execution %s and orchestration run %s",
        selection.snapshot_id,
        selection.selection_status,
        task_run_id,
        execution_count,
        orchestration_run_id,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    return {
        "JOB_ENV": parser.get_arg("--job_env"),
        "CLIENT": parser.get_arg("--client") or "next_uk",
        "LOG_LEVEL": parser.get_arg("--log_level"),
        "RUN_DATE": parser.get_arg("--run_date"),
        "FOUNDATION_SNAPSHOT_ID": parser.get_arg("--foundation_snapshot_id"),
        "READINESS_WAIT_SECONDS": (
            parser.get_arg("--readiness_wait_seconds") or "1800"
        ),
        "READINESS_POLL_SECONDS": (
            parser.get_arg("--readiness_poll_seconds") or "60"
        ),
        "SELECTION_CUTOFF": parser.get_arg("--selection_cutoff"),
        "TASK_RUN_ID": parser.get_arg("--task_run_id"),
        "EXECUTION_COUNT": parser.get_arg("--execution_count"),
        "ORCHESTRATION_RUN_ID": parser.get_arg("--orchestration_run_id"),
    }


if __name__ == "__main__":
    main(**parse_args())
