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
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from jobs.orchestration.finalize_scoring_foundation_context import (
    main as fail_foundation_context,
)
from jobs.orchestration.prepare_score_provider_context import (
    main as prepare_provider_context,
)
from jobs.orchestration.publish_scoring_foundation import (
    main as publish_foundation,
)
from next_ads.common import config_manager
from next_ads.common.delta_writes import find_delta_write_receipt
from next_ads.common.spark_runtime import configure_lean_spark
from next_ads.ranking.provider_publication import (
    publish_provider_build,
    validate_provider_publication_contract,
)
from next_ads.ranking.provider_context import transition_provider_context
from next_ads.ranking.theme_affinity.clean_output import stage_model_output
from next_ads.ranking.theme_affinity.config import resolve_runtime
from next_ads.ranking.theme_affinity.predict import build_predictions


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    RUN_DATE,
    MODEL_URI,
    FOUNDATION_CONTEXT_SLOT,
    PROVIDER_CONTEXT_SLOT,
    ORCHESTRATION_RUN_ID,
    SOURCE_NAMESPACE,
    SOURCE_TABLE_PREFIX,
    TARGET_NAMESPACE,
    TARGET_TABLE_PREFIX,
    PIPELINE_ID,
    PIPELINE_TASK_RUN_ID,
    TASK_RUN_ID,
    EXECUTION_COUNT,
    GIT_COMMIT,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    configure_lean_spark(spark)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    validate_provider_publication_contract(
        spark,
        signals_table=config.tables_write.score_provider_signals,
        builds_table=config.tables_write.score_provider_builds,
    )
    context = None
    try:
        foundation = publish_foundation(
            JOB_ENV,
            CLIENT,
            LOG_LEVEL,
            FOUNDATION_CONTEXT_SLOT,
            ORCHESTRATION_RUN_ID,
            SOURCE_NAMESPACE,
            SOURCE_TABLE_PREFIX,
            TARGET_NAMESPACE,
            TARGET_TABLE_PREFIX,
            PIPELINE_ID,
            PIPELINE_TASK_RUN_ID,
            TASK_RUN_ID,
            EXECUTION_COUNT,
            GIT_COMMIT,
            False,
        )
        context = prepare_provider_context(
            JOB_ENV,
            CLIENT,
            LOG_LEVEL,
            RUN_DATE,
            foundation.input_snapshot_id,
            MODEL_URI,
            PROVIDER_CONTEXT_SLOT,
            TASK_RUN_ID,
            EXECUTION_COUNT,
            "0",
            "1",
            "theme_affinity",
            "theme_ranking",
            ORCHESTRATION_RUN_ID,
            foundation.scoring_foundation_build_id,
            foundation.scoring_foundation_build_attempt_id,
            ALLOW_SERIAL_RUN_TAKEOVER=True,
            ACTIVATE_CONTEXT=True,
            EMIT_TASK_VALUES=False,
            REUSE_INCOMPLETE_ATTEMPT=True,
        )
        receipt = find_delta_write_receipt(
            spark,
            target_table=config.tables_write.score_provider_signals,
            build_id=context.provider_build_id,
            attempt_id=context.provider_build_attempt_id,
        )
        if receipt is None:
            runtime = resolve_runtime(
                JOB_ENV,
                CLIENT,
                model_uri=MODEL_URI,
                run_date=context.run_date,
                input_snapshot_id=context.input_snapshot_id,
                provider_build_id=context.provider_build_id,
                provider_build_attempt_id=(context.provider_build_attempt_id),
                item_themes_table=(
                    config.tables_write.scoring_input_item_themes
                ),
                context_slot=PROVIDER_CONTEXT_SLOT,
                provider_context=context,
                git_commit=GIT_COMMIT,
            )
            receipt = stage_model_output(
                spark,
                runtime,
                build_predictions(spark, runtime),
            )
            config = runtime.config
        else:
            logger.info(
                "Reusing provider signals receipt %s at Delta version %s",
                receipt.receipt_id,
                receipt.delta_version,
            )
        provider = config.scoring.providers[context.provider_id]
        publish_provider_build(
            spark,
            context=context,
            signals_table=config.tables_write.score_provider_signals,
            signals_delta_version=receipt.delta_version,
            builds_table=config.tables_write.score_provider_builds,
            provider_config=provider,
            contract_version=config.scoring.contract_version,
            git_commit=GIT_COMMIT,
            task_run_id=int(TASK_RUN_ID),
            execution_count=int(EXECUTION_COUNT),
            completed_at=datetime.now(timezone.utc),
        )
    except Exception:
        if context is not None:
            try:
                transition_provider_context(
                    spark,
                    context_table=(
                        config.tables_write.score_provider_run_contexts
                    ),
                    context=context,
                    status="FAILED",
                    completed_at=datetime.now(timezone.utc),
                )
            except Exception:
                logger.exception("Unable to release failed provider context")
        try:
            fail_foundation_context(
                JOB_ENV,
                CLIENT,
                LOG_LEVEL,
                FOUNDATION_CONTEXT_SLOT,
                ORCHESTRATION_RUN_ID,
                "FAILED",
            )
        except Exception:
            logger.exception("Unable to release failed foundation context")
        raise
    try:
        transition_provider_context(
            spark,
            context_table=config.tables_write.score_provider_run_contexts,
            context=context,
            status="CONSUMED",
            completed_at=datetime.now(timezone.utc),
        )
    except Exception:
        logger.exception("Provider READY; context cleanup will expire safely")
    try:
        fail_foundation_context(
            JOB_ENV,
            CLIENT,
            LOG_LEVEL,
            FOUNDATION_CONTEXT_SLOT,
            ORCHESTRATION_RUN_ID,
            "CONSUMED",
        )
    except Exception:
        logger.exception(
            "Provider READY; foundation cleanup will expire safely"
        )


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--log_level"),
        parser.get_arg("--run_date"),
        parser.get_arg("--model_uri"),
        parser.get_arg("--foundation_context_slot"),
        parser.get_arg("--provider_context_slot"),
        parser.get_arg("--orchestration_run_id"),
        parser.get_arg("--source_namespace"),
        parser.get_arg("--source_table_prefix"),
        parser.get_arg("--target_namespace"),
        parser.get_arg("--target_table_prefix"),
        parser.get_arg("--pipeline_id"),
        parser.get_arg("--pipeline_task_run_id"),
        parser.get_arg("--task_run_id"),
        parser.get_arg("--execution_count"),
        parser.get_arg("--git_commit"),
    )
