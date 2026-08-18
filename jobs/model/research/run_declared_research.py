"""Compare declared model candidates from one exact Feature Store receipt."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, timedelta
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
from next_ads.model_development import (
    build_training_set_from_feature_store,
    create_model_development_tables,
    load_model_definition,
    load_model_research_plan,
    persist_training_set_receipt,
)
from next_ads.model_development.research_contracts import (
    AUTO,
    REVIEW_REQUIRED,
)
from next_ads.model_development.research_runtime import (
    plan_observation_dates,
    run_model_research,
)
from next_ads.model_development.research_store import create_research_tables


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_RESEARCH_EVIDENCE="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--feature_catalog", required=True)
    parser.add_argument("--feature_schema", required=True)
    parser.add_argument("--model_catalog", required=True)
    parser.add_argument("--model_schema", required=True)
    parser.add_argument("--train_reference_dates", required=True)
    parser.add_argument("--validation_reference_dates", required=True)
    parser.add_argument("--test_reference_dates", required=True)
    parser.add_argument("--feature_reference_dates", required=True)
    parser.add_argument("--label_end", required=True)
    parser.add_argument("--registered_model_name", required=True)
    parser.add_argument("--experiment_path", required=True)
    parser.add_argument(
        "--selection_mode",
        choices=(AUTO, REVIEW_REQUIRED),
        required=True,
    )
    parser.add_argument("--code_sha", required=True)
    parser.add_argument("--orchestration_run_id", type=int, required=True)
    parser.add_argument("--task_run_id", type=int, required=True)
    parser.add_argument("--execution_count", type=int, required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _dates(value: str, field_name: str) -> tuple[date, ...]:
    values = tuple(
        date.fromisoformat(item.strip())
        for item in value.split(",")
        if item.strip()
    )
    if not values or len(values) != len(set(values)):
        raise ValueError(f"{field_name} must contain unique ISO dates")
    if values != tuple(sorted(values)):
        raise ValueError(f"{field_name} must be in ascending order")
    return values


def _assert_declared_dates(plan, train, validation, test) -> None:
    expected = plan_observation_dates(plan)
    requested = (*train, *validation, *test)
    if requested != expected:
        raise ValueError(
            "Research dates must match the reviewed temporal split: "
            f"expected={[value.isoformat() for value in expected]}, "
            f"requested={[value.isoformat() for value in requested]}"
        )


def _assert_declared_feature_dates(plan, feature_dates) -> None:
    expected = tuple(
        value - timedelta(days=1) for value in plan_observation_dates(plan)
    )
    if feature_dates != expected:
        expected_values = {value.isoformat() for value in expected}
        requested_values = {value.isoformat() for value in feature_dates}
        raise ValueError(
            "Feature reference dates must be exactly one day before every "
            "declared observation date: "
            f"expected={[value.isoformat() for value in expected]}, "
            f"requested={[value.isoformat() for value in feature_dates]}, "
            f"missing={sorted(expected_values - requested_values)}, "
            f"unexpected={sorted(requested_values - expected_values)}"
        )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    train_dates = _dates(args.train_reference_dates, "train_reference_dates")
    validation_dates = _dates(
        args.validation_reference_dates,
        "validation_reference_dates",
    )
    test_dates = _dates(args.test_reference_dates, "test_reference_dates")
    feature_dates = _dates(
        args.feature_reference_dates,
        "feature_reference_dates",
    )
    definition = load_model_definition(args.model_name)
    declared_plan = load_model_research_plan(args.model_name)
    if declared_plan is None:
        raise ValueError(
            f"Model has no declared research plan: {args.model_name}"
        )
    plan = replace(declared_plan, selection_policy=args.selection_mode)
    _assert_declared_dates(plan, train_dates, validation_dates, test_dates)
    _assert_declared_feature_dates(plan, feature_dates)
    spark = configure_spark()
    create_model_development_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    create_research_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    training = build_training_set_from_feature_store(
        spark,
        definition,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
        observation_reference_dates=tuple(
            value.isoformat()
            for value in (*train_dates, *validation_dates, *test_dates)
        ),
        feature_reference_dates=tuple(
            value.isoformat() for value in feature_dates
        ),
        label_end=date.fromisoformat(args.label_end),
        code_sha=args.code_sha,
        include_feature_audit_columns=True,
    )
    persist_training_set_receipt(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        receipt=training.receipt,
    )
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    result = run_model_research(
        spark,
        definition=definition,
        plan=plan,
        training=training,
        catalog=args.model_catalog,
        schema=args.model_schema,
        registered_model_name=args.registered_model_name,
        experiment_path=args.experiment_path,
        code_sha=args.code_sha,
        invocation_id=(
            f"{args.orchestration_run_id}:{args.task_run_id}:"
            f"{args.execution_count}"
        ),
        mlflow_module=mlflow,
    )
    evidence = {
        "artifact_manifest_digest": (
            result.research_build.artifact_manifest_digest
        ),
        "automatic_recommendation": result.recommended_candidate_id,
        "candidate_runs": [
            {
                "candidate_id": item.candidate_id,
                "evaluation_id": item.candidate_evaluation_id,
                "mlflow_run_id": item.mlflow_run_id,
                "status": item.status,
                "validation_metrics": dict(item.metrics),
            }
            for item in result.candidate_evaluations
        ],
        "code_sha": args.code_sha,
        "feature_binding_count": len(training.receipt.feature_bindings),
        "model_build_id": (
            None
            if result.model_build is None
            else result.model_build.model_build_id
        ),
        "model_uri": (
            None
            if result.model_build is None
            else result.model_build.model_uri
        ),
        "parent_mlflow_run_id": result.research_build.mlflow_parent_run_id,
        "registered_model_version": (
            None
            if result.model_build is None
            else result.model_build.registered_model_version
        ),
        "research_attempt_id": result.research_build.research_attempt_id,
        "research_build_id": result.research_build.research_build_id,
        "research_frame_delta_version": (
            result.research_build.research_frame_delta_version
        ),
        "research_frame_rows": result.research_build.research_frame_row_count,
        "reused": result.reused,
        "selection_decision_id": (
            None
            if result.selection_decision is None
            else result.selection_decision.selection_decision_id
        ),
        "selection_mode": plan.selection_mode,
        "status": result.research_build.status,
        "training_receipt_id": training.receipt.receipt_id,
    }
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(
            evidence, default=str, sort_keys=True, separators=(",", ":")
        ),
    )


if __name__ == "__main__":
    main()
