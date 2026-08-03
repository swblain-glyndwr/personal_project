import json
import sys
from datetime import datetime, timezone
from pathlib import Path

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

from next_ads.ranking.provider_context import transition_provider_context
from next_ads.ranking.theme_affinity.clean_output import (
    publish_theme_affinity_provider_build,
)
from next_ads.ranking.theme_affinity.config import resolve_context_runtime


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    INPUT_SNAPSHOT_ID,
    PROVIDER_BUILD_ID,
    PROVIDER_BUILD_ATTEMPT_ID,
    PROVIDER_SIGNALS_DELTA_VERSION,
    CONTEXT_SLOT,
    ORCHESTRATION_RUN_ID,
    TASK_RUN_ID,
    EXECUTION_COUNT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    runtime, context = resolve_context_runtime(
        spark,
        job_env=JOB_ENV,
        client=CLIENT,
        context_slot=CONTEXT_SLOT,
        expected_run_date=RUN_DATE,
        expected_input_snapshot_id=INPUT_SNAPSHOT_ID,
        expected_provider_build_id=PROVIDER_BUILD_ID,
        expected_provider_build_attempt_id=PROVIDER_BUILD_ATTEMPT_ID,
    )
    if context.orchestration_run_id != int(ORCHESTRATION_RUN_ID):
        raise ValueError(
            "Provider context does not belong to this orchestration run"
        )

    result = publish_theme_affinity_provider_build(
        spark,
        runtime,
        provider_signals_delta_version=int(
            PROVIDER_SIGNALS_DELTA_VERSION
        ),
        task_run_id=int(TASK_RUN_ID),
        execution_count=int(EXECUTION_COUNT),
    )
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="provider_build_id", value=result.build.provider_build_id)
    task_values.set(
        key="provider_signals_delta_version",
        value=result.build.output_delta_version,
    )
    task_values.set(
        key="compatibility_output_versions",
        value=json.dumps(
            result.compatibility_output_versions,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    task_values.set(key="status", value=result.build.status)
    transition_provider_context(
        spark,
        context_table=(
            runtime.config.tables_write.score_provider_run_contexts
        ),
        context=context,
        status="CONSUMED",
        completed_at=datetime.now(timezone.utc),
    )
    logger.info(
        "Accepted provider build %s at Delta version %s",
        result.build.provider_build_id,
        result.build.output_delta_version,
    )


def parse_args():
    parser = get_job_parser()
    parser._parse_args()
    return {
        "JOB_ENV": parser.get_arg("--job_env"),
        "CLIENT": parser.get_arg("--client") or "next_uk",
        "LOG_LEVEL": parser.get_arg("--log_level"),
        "RUN_DATE": parser.get_arg("--run_date"),
        "INPUT_SNAPSHOT_ID": parser.get_arg("--input_snapshot_id"),
        "PROVIDER_BUILD_ID": parser.get_arg("--provider_build_id"),
        "PROVIDER_BUILD_ATTEMPT_ID": parser.get_arg(
            "--provider_build_attempt_id"
        ),
        "PROVIDER_SIGNALS_DELTA_VERSION": parser.get_arg(
            "--provider_signals_delta_version"
        ),
        "CONTEXT_SLOT": parser.get_arg("--context_slot"),
        "ORCHESTRATION_RUN_ID": parser.get_arg("--orchestration_run_id"),
        "TASK_RUN_ID": parser.get_arg("--task_run_id"),
        "EXECUTION_COUNT": parser.get_arg("--execution_count"),
    }


if __name__ == "__main__":
    main(**parse_args())
