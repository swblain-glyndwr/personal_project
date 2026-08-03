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
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ranking.provider_selection import wait_for_score_provider_build


READINESS_TIME = time(18, 30)
READINESS_TIMEZONE = ZoneInfo("Europe/London")


def _required(value, label):
    if value is None or not str(value).strip():
        raise ValueError(f"{label} must not be empty")
    return str(value).strip()


def _positive_integer(value, label, *, allow_zero=False):
    try:
        parsed = int(_required(value, label))
    except ValueError as error:
        raise ValueError(f"{label} must be an integer") from error
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return parsed


def _parse_cutoff(value):
    if value is None or not str(value).strip():
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            "selection_cutoff must be an ISO-8601 timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise ValueError("selection_cutoff must include a timezone")
    return parsed.astimezone(timezone.utc)


def _default_selection_cutoff(run_date: date) -> datetime:
    """Return the fixed 18:30 Europe/London readiness deadline."""
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
    PROVIDER_ID,
    CAPABILITY,
    USE_CASE,
    ROUTE,
    READINESS_WAIT_SECONDS,
    READINESS_POLL_SECONDS,
    SELECTION_CUTOFF,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    ORCHESTRATION_RUN_ID,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)

    job_env = _required(JOB_ENV, "job_env")
    client = _required(CLIENT, "client")
    provider_id = _required(PROVIDER_ID, "provider_id")
    capability = _required(CAPABILITY, "capability")
    use_case = _required(USE_CASE, "use_case")
    route = _required(ROUTE, "route")
    task_run_id = _positive_integer(TASK_RUN_ID, "task_run_id")
    execution_count = _positive_integer(
        EXECUTION_COUNT,
        "execution_count",
        allow_zero=True,
    )
    orchestration_run_id = _positive_integer(
        ORCHESTRATION_RUN_ID,
        "orchestration_run_id",
    )
    try:
        run_date = date.fromisoformat(_required(RUN_DATE, "run_date"))
        wait_seconds = float(
            _required(READINESS_WAIT_SECONDS, "readiness_wait_seconds")
        )
        poll_seconds = float(
            _required(READINESS_POLL_SECONDS, "readiness_poll_seconds")
        )
    except ValueError as error:
        raise ValueError(
            "run_date and readiness timings must contain valid values"
        ) from error

    spark = configure_spark()
    config = config_manager.load_config(job_env, client=client)
    selection_cutoff = _parse_cutoff(SELECTION_CUTOFF)
    if selection_cutoff is None:
        selection_cutoff = _default_selection_cutoff(run_date)
    selection = wait_for_score_provider_build(
        spark,
        table=config.tables_write.score_provider_builds,
        run_date=run_date,
        provider_id=provider_id,
        capability=capability,
        use_case=use_case,
        wait_seconds=wait_seconds,
        poll_seconds=poll_seconds,
        selection_cutoff=selection_cutoff,
    )

    task_values = get_dbutils().jobs.taskValues
    task_values.set(
        key="provider_build_id",
        value=selection.provider_build_id,
    )
    task_values.set(
        key="provider_signals_table",
        value=selection.provider_signals_table,
    )
    task_values.set(
        key="provider_signals_delta_version",
        value=selection.provider_signals_delta_version,
    )
    task_values.set(
        key="input_snapshot_id",
        value=selection.input_snapshot_id,
    )
    task_values.set(
        key="scoring_foundation_build_id",
        value=selection.scoring_foundation_build_id or "",
    )
    task_values.set(
        key="provider_selection_status",
        value=selection.selection_status,
    )
    task_values.set(
        key="provider_source_run_date",
        value=selection.source_run_date.isoformat(),
    )
    logger.info(
        "Selected provider build %s for route %s (task %s, execution %s, "
        "orchestration run %s) with status %s",
        selection.provider_build_id,
        route,
        task_run_id,
        execution_count,
        orchestration_run_id,
        selection.selection_status,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    return {
        "JOB_ENV": parser.get_arg("--job_env"),
        "CLIENT": parser.get_arg("--client") or "next_uk",
        "LOG_LEVEL": parser.get_arg("--log_level"),
        "RUN_DATE": parser.get_arg("--run_date"),
        "PROVIDER_ID": parser.get_arg("--provider_id"),
        "CAPABILITY": parser.get_arg("--capability"),
        "USE_CASE": parser.get_arg("--use_case"),
        "ROUTE": parser.get_arg("--route"),
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
