"""Score exact accepted Shopping Bag candidates in isolated DEV EVALUATE."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
import sys


def resolve_project_root(
    script_file: str | None,
    notebook_path: str | None = None,
) -> Path:
    """Resolve the bundle root for Python and Databricks execution."""
    if script_file:
        return Path(script_file).resolve().parents[3]
    if notebook_path is None:
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
    return Path(notebook_path).parents[3]


PROJECT_ROOT = resolve_project_root(globals().get("__file__"))
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from dsutils.dbc import configure_spark
from next_ads.common.job_logging import configure_job_logging
from next_ads.decisioning.candidate_inputs import (
    load_accepted_candidate_inputs,
)
from next_ads.model_development.ongoing_evaluation import (
    BUILDING,
    CANDIDATE_KEYS,
    FAILED,
    READY,
    SHOPPING_BAG_SCOPES,
    OngoingEvaluationBuild,
    build_shopping_bag_candidate_frame,
    candidate_input_binding,
    create_ongoing_evaluation_tables,
    evaluation_scoring_build_id,
    persist_evaluation_build,
    persist_evaluation_scores,
    resolve_candidate_attempt_id,
    score_shopping_bag_candidates,
    scoring_build_attempt_id,
)
from next_ads.model_development.promotion import (
    validate_registered_model_build,
)
from next_ads.model_development.registry import load_model_definition
from next_ads.model_development.scoring_sets import (
    build_label_free_scoring_set,
)
from next_ads.model_development.store import load_ready_model_build


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_ONGOING_EVALUATION_EVIDENCE="


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--model_build_id", required=True)
    parser.add_argument("--run_date", required=True)
    parser.add_argument("--feature_catalog", required=True)
    parser.add_argument("--feature_schema", required=True)
    parser.add_argument("--feature_reference_dates", default="AUTO")
    parser.add_argument("--model_catalog", required=True)
    parser.add_argument("--model_schema", required=True)
    parser.add_argument("--candidate_builds_table", required=True)
    parser.add_argument("--candidate_scores_table", required=True)
    parser.add_argument("--candidate_ad_sets_table", required=True)
    parser.add_argument(
        "--v1_candidate_build_attempt_id",
        default="AUTO",
    )
    parser.add_argument("--candidate_serving_slot", default="best")
    parser.add_argument("--account_limit", type=int, required=True)
    parser.add_argument("--code_sha", required=True)
    parser.add_argument("--orchestration_run_id", type=int, required=True)
    parser.add_argument("--task_run_id", type=int, required=True)
    parser.add_argument("--execution_count", type=int, required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def _feature_reference_dates(
    value: str,
) -> tuple[str, ...] | None:
    """Keep AUTO separate from an explicit reproducible date list."""
    if value.strip().upper() == "AUTO":
        return None
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise ValueError("feature_reference_dates must not be empty")
    for item in values:
        date.fromisoformat(item)
    if len(values) != len(set(values)):
        raise ValueError("feature_reference_dates must be unique")
    return tuple(sorted(values))


def _scopes(route: str) -> tuple[str, ...]:
    return tuple(
        scope_value
        for scope_route, _scope_type, scope_value in SHOPPING_BAG_SCOPES
        if scope_route == route
    )


def _load_model_build(args, spark, definition):
    import mlflow
    from next_ads.ml.lifecycle import configure_mlflow

    configure_mlflow(mlflow)

    build = load_ready_model_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        model_build_id=args.model_build_id,
    )
    if build is None:
        raise ValueError(
            f"No READY model build exists for {args.model_build_id}"
        )
    if (
        build.model_name != definition.model_name
        or build.model_definition_checksum != definition.checksum
    ):
        raise ValueError(
            "The requested model build does not match the definition"
        )
    expected_uri = (
        f"models:/{build.registered_model_name}/"
        f"{build.registered_model_version}"
    )
    if build.model_uri != expected_uri:
        raise ValueError(
            "The requested model build does not use an exact version"
        )
    validate_registered_model_build(mlflow.tracking.MlflowClient(), build)
    return build


def run_evaluation(
    args: argparse.Namespace,
    *,
    spark=None,
) -> dict[str, object]:
    """Run the bounded evaluator declared by the model use case."""
    run_date = date.fromisoformat(args.run_date)
    feature_reference_dates = _feature_reference_dates(
        args.feature_reference_dates
    )
    definition = load_model_definition(args.model_name)
    if definition.evaluation_use_case != "shopping_bag_advert_ranking":
        raise ValueError(
            "No ongoing evaluator is registered for use case: "
            f"{definition.evaluation_use_case}"
        )
    if spark is None:
        spark = configure_spark()
    create_ongoing_evaluation_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    model_build = _load_model_build(args, spark, definition)

    v1_attempt_id = resolve_candidate_attempt_id(
        spark,
        builds_table=args.candidate_builds_table,
        run_date=run_date,
        route="v1",
        requested_attempt_id=args.v1_candidate_build_attempt_id,
    )
    accepted_v1 = load_accepted_candidate_inputs(
        spark,
        builds_table=args.candidate_builds_table,
        scores_table=args.candidate_scores_table,
        ad_sets_table=args.candidate_ad_sets_table,
        candidate_build_attempt_id=v1_attempt_id,
        route="v1",
    )
    candidates = build_shopping_bag_candidate_frame(
        accepted_v1,
        serving_slot=args.candidate_serving_slot,
        account_limit=args.account_limit,
    ).cache()
    input_account_count = (
        candidates.select("account_number").distinct().count()
    )
    scoring_set = build_label_free_scoring_set(
        spark,
        definition,
        candidates,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
        scoring_date=run_date,
        candidate_keys=CANDIDATE_KEYS,
        feature_reference_dates=feature_reference_dates,
    )
    candidate_bindings = (
        candidate_input_binding(
            accepted_v1,
            serving_slot=args.candidate_serving_slot,
            scopes=_scopes("v1"),
            builds_table=args.candidate_builds_table,
            scores_table=args.candidate_scores_table,
            ad_sets_table=args.candidate_ad_sets_table,
        ),
    )
    build_id = evaluation_scoring_build_id(
        definition=definition,
        model_build=model_build,
        run_date=run_date,
        serving_slot=args.candidate_serving_slot,
        account_limit=args.account_limit,
        input_account_count=input_account_count,
        candidate_bindings=candidate_bindings,
        feature_bindings=scoring_set.feature_bindings,
        input_row_count=scoring_set.row_count,
        input_schema_checksum=scoring_set.schema_checksum,
        input_value_checksum=scoring_set.value_checksum,
        git_commit=args.code_sha,
    )
    attempt_id = scoring_build_attempt_id(
        args.orchestration_run_id,
        args.task_run_id,
        args.execution_count,
    )
    created_at = datetime.now(timezone.utc)
    building = OngoingEvaluationBuild(
        scoring_build_id=build_id,
        scoring_build_attempt_id=attempt_id,
        model_build_id=model_build.model_build_id,
        model_name=model_build.model_name,
        model_definition_checksum=model_build.model_definition_checksum,
        registered_model_name=model_build.registered_model_name,
        registered_model_version=model_build.registered_model_version,
        model_uri=model_build.model_uri,
        artifact_digest=model_build.artifact_digest,
        run_date=run_date,
        serving_slot=args.candidate_serving_slot,
        account_limit=args.account_limit,
        input_account_count=input_account_count,
        candidate_bindings=candidate_bindings,
        feature_bindings=scoring_set.feature_bindings,
        input_row_count=scoring_set.row_count,
        input_schema_checksum=scoring_set.schema_checksum,
        input_value_checksum=scoring_set.value_checksum,
        git_commit=args.code_sha,
        orchestration_run_id=args.orchestration_run_id,
        task_run_id=args.task_run_id,
        execution_count=args.execution_count,
        status=BUILDING,
        created_at=created_at,
    )
    persist_evaluation_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        build=building,
    )

    try:
        scoring = score_shopping_bag_candidates(
            definition,
            model_build,
            scoring_set.frame,
            scoring_build_id=build_id,
            scoring_build_attempt_id=attempt_id,
            run_date=run_date,
        )
        output = persist_evaluation_scores(
            spark,
            scoring.frame,
            catalog=args.model_catalog,
            schema=args.model_schema,
            scoring_build_id=build_id,
            scoring_build_attempt_id=attempt_id,
            git_commit=args.code_sha,
        )
        if output.row_count != scoring_set.row_count:
            raise ValueError(
                "Declared score provider changed the candidate row count: "
                f"expected {scoring_set.row_count}, found {output.row_count}"
            )
        ready = replace(
            building,
            status=READY,
            output_table=output.target_table,
            output_delta_version=output.delta_version,
            output_row_count=output.row_count,
            output_schema_checksum=output.schema_checksum,
            output_value_checksum=output.value_checksum,
            completed_at=datetime.now(timezone.utc),
        )
        persist_evaluation_build(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            build=ready,
        )
    except Exception as exc:
        failed = replace(
            building,
            status=FAILED,
            completed_at=datetime.now(timezone.utc),
            failure_reason=str(exc)[:4000],
        )
        try:
            persist_evaluation_build(
                spark,
                catalog=args.model_catalog,
                schema=args.model_schema,
                build=failed,
            )
        except Exception:
            LOGGER.exception("Could not persist failed evaluation manifest")
        raise

    evidence = {
        "activation_mode": "EVALUATE",
        "account_limit": args.account_limit,
        "artifact_digest": model_build.artifact_digest,
        "candidate_bindings": [
            binding.candidate_build_attempt_id
            for binding in candidate_bindings
        ],
        "feature_bindings": [
            {
                "feature_id": binding.feature_id,
                "reference_date": binding.reference_date,
                "snapshot_attempt_id": (binding.feature_snapshot_attempt_id),
                "delta_version": binding.delta_version,
            }
            for binding in scoring_set.feature_bindings
        ],
        "model_build_id": model_build.model_build_id,
        "model_uri": model_build.model_uri,
        "input_account_count": input_account_count,
        "output_delta_version": output.delta_version,
        "output_rows": output.row_count,
        "provider_contract": scoring.provider_contract,
        "provider_signal_rows": scoring.provider_row_count,
        "provider_signal_schema_checksum": (scoring.provider_schema_checksum),
        "provider_signal_value_checksum": scoring.provider_value_checksum,
        "run_date": run_date,
        "scoring_build_attempt_id": attempt_id,
        "scoring_build_id": build_id,
        "status": READY,
    }
    return evidence


def main(argv: list[str] | None = None) -> None:
    """Run one declared ongoing evaluation and emit bounded evidence."""
    args = parse_args(argv)
    configure_job_logging(args.log_level)
    evidence = run_evaluation(args)
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(evidence, sort_keys=True, default=str),
    )


if __name__ == "__main__":
    main()
