import sys
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

from datetime import datetime, timezone

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.ranking.foundation_context import (
    load_active_foundation_context,
    transition_foundation_context,
)
from next_ads.ranking.foundation_publication import (
    FoundationOutputSpec,
    foundation_output_bindings_json,
    publish_required_foundation_outputs,
    register_ready_foundation,
    validate_foundation_build_marker,
)
from next_ads.ranking.pipeline_metadata import pipeline_task_identity


def _as_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else dict(value)


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    CONTEXT_SLOT,
    ORCHESTRATION_RUN_ID,
    SOURCE_NAMESPACE,
    SOURCE_TABLE_PREFIX,
    TARGET_NAMESPACE,
    TARGET_TABLE_PREFIX,
    PIPELINE_ID,
    PIPELINE_TASK_RUN_ID,
    TASK_RUN_ID,
    EXECUTION_COUNT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    context = load_active_foundation_context(
        spark,
        context_table=config.tables_write.scoring_foundation_run_contexts,
        context_slot=CONTEXT_SLOT,
    )
    if context.orchestration_run_id != int(ORCHESTRATION_RUN_ID):
        raise ValueError("Foundation context belongs to a different job run")
    pipeline_task_run_id = int(PIPELINE_TASK_RUN_ID)
    pipeline_task = pipeline_task_identity(
        pipeline_id=PIPELINE_ID,
        pipeline_task_run_id=pipeline_task_run_id,
    )

    foundation_config = config.scoring.foundations[context.foundation_id]
    required_outputs = _as_dict(foundation_config.required_outputs)
    source_namespace = SOURCE_NAMESPACE.strip().strip(".")
    target_namespace = TARGET_NAMESPACE.strip().strip(".")
    if source_namespace.count(".") != 1 or target_namespace.count(".") != 1:
        raise ValueError("Foundation namespaces must use catalog.schema")
    validate_foundation_build_marker(
        spark,
        context=context,
        marker_table=(
            f"{source_namespace}.{SOURCE_TABLE_PREFIX}_build_marker"
        ),
    )
    specs = tuple(
        FoundationOutputSpec(
            output_name=output_name,
            source_table=(
                f"{source_namespace}.{SOURCE_TABLE_PREFIX}_{output_name}"
            ),
            target_table=(
                f"{target_namespace}.{TARGET_TABLE_PREFIX}_{output_name}"
            ),
            output_schema_version=schema_version,
            key_columns=("reference_date", "account_number", "theme_clean"),
            account_column="account_number",
            entity_column="theme_clean",
            required_non_null_columns=(
                ("simple_rules_rank",) if output_name == "ranked" else ()
            ),
        )
        for output_name, schema_version in sorted(required_outputs.items())
    )
    outputs = publish_required_foundation_outputs(
        spark,
        context=context,
        output_specs=specs,
    )
    build = register_ready_foundation(
        spark,
        context=context,
        outputs=outputs,
        required_output_names=tuple(sorted(required_outputs)),
        pipeline_id=pipeline_task.pipeline_id,
        pipeline_update_id=None,
        pipeline_update_type=None,
        builds_table=config.tables_write.scoring_foundation_builds,
        outputs_table=config.tables_write.scoring_foundation_outputs,
        task_run_id=int(TASK_RUN_ID),
        execution_count=int(EXECUTION_COUNT),
        pipeline_task_run_id=pipeline_task.pipeline_task_run_id,
    )
    transition_foundation_context(
        spark,
        context_table=config.tables_write.scoring_foundation_run_contexts,
        context=context,
        status="CONSUMED",
        completed_at=datetime.now(timezone.utc),
    )
    task_values = get_dbutils().jobs.taskValues
    task_values.set(
        key="scoring_foundation_build_id",
        value=build.scoring_foundation_build_id,
    )
    task_values.set(
        key="scoring_foundation_build_attempt_id",
        value=build.scoring_foundation_build_attempt_id,
    )
    task_values.set(
        key="input_snapshot_id",
        value=build.input_snapshot_id,
    )
    task_values.set(
        key="foundation_outputs_json",
        value=foundation_output_bindings_json(build),
    )
    logger.info(
        "Published scoring foundation %s with outputs %s",
        build.scoring_foundation_build_id,
        ",".join(output.output_name for output in outputs),
    )


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--context_slot"),
        parser.get_arg("--orchestration_run_id"),
        parser.get_arg("--source_namespace"),
        parser.get_arg("--source_table_prefix"),
        parser.get_arg("--target_namespace"),
        parser.get_arg("--target_table_prefix"),
        parser.get_arg("--pipeline_id"),
        parser.get_arg("--pipeline_task_run_id"),
        parser.get_arg("--task_run_id"),
        parser.get_arg("--execution_count"),
    )
