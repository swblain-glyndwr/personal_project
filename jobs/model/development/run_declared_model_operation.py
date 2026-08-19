"""Dispatch one declared model lifecycle operation from the shared DEV job."""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
from pathlib import Path
import sys
from typing import Any, Callable


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


from jobs.model.development import run_declared_model
from jobs.model.development import run_shopping_bag_ongoing_evaluation
from jobs.model.research import run_declared_research
from jobs.model.research import select_research_candidate
from next_ads.model_development import ModelPluginRegistry
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_research_plan,
)
from next_ads.model_development.research_plugins import (
    resolve_candidate_plugin,
    resolve_evidence_producer,
)


BUILD = "BUILD"
RESEARCH = "RESEARCH"
REVIEW_SELECT = "REVIEW_SELECT"
EVALUATE = "EVALUATE"
OPERATIONS = (BUILD, RESEARCH, REVIEW_SELECT, EVALUATE)
EVIDENCE_PREFIX = "MODEL_LIFECYCLE_EVIDENCE="
LOGGER = logging.getLogger(__name__)

_EVALUATION_CONTRACTS = {
    "shopping_bag_advert_ranking": (
        run_shopping_bag_ongoing_evaluation.run_evaluation,
        "score_with_evaluation_scope",
    ),
}

_OPERATION_FIELDS = {
    "observation_reference_dates",
    "feature_reference_dates",
    "label_end",
    "research_build_id",
    "candidate_id",
    "written_reason",
    "reviewed_by",
    "model_build_id",
    "run_date",
    "evaluation_account_limit",
    "evaluation_serving_slot",
    "evaluation_candidate_build_attempt_id",
}
_REQUIRED_FIELDS = {
    BUILD: {
        "observation_reference_dates",
        "feature_reference_dates",
        "label_end",
    },
    RESEARCH: {"label_end"},
    REVIEW_SELECT: {
        "research_build_id",
        "candidate_id",
        "written_reason",
        "reviewed_by",
    },
    EVALUATE: {"model_build_id", "run_date"},
}
_OPTIONAL_FIELDS = {
    BUILD: set(),
    RESEARCH: set(),
    REVIEW_SELECT: set(),
    EVALUATE: {
        "feature_reference_dates",
        "evaluation_account_limit",
        "evaluation_serving_slot",
        "evaluation_candidate_build_attempt_id",
    },
}


def _operation(value: str) -> str:
    operation = str(value).strip().upper()
    if operation not in OPERATIONS:
        raise argparse.ArgumentTypeError(
            "operation must be one of " + ", ".join(OPERATIONS)
        )
    return operation


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the shared job contract without starting Spark."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", type=_operation, required=True)
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--feature_catalog", required=True)
    parser.add_argument("--feature_schema", required=True)
    parser.add_argument("--model_catalog", required=True)
    parser.add_argument("--model_schema", required=True)
    parser.add_argument("--experiment_root", required=True)
    for field_name in sorted(_OPERATION_FIELDS):
        parser.add_argument(f"--{field_name}", default="")
    parser.add_argument("--code_sha", required=True)
    parser.add_argument("--orchestration_run_id", type=int, required=True)
    parser.add_argument("--task_run_id", type=int, required=True)
    parser.add_argument("--execution_count", type=int, required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def _text(value: Any) -> str:
    return str(value).strip()


def _is_supplied(value: Any) -> bool:
    text = _text(value)
    return bool(text) and text.upper() != "REQUIRED"


def _required_text(args: argparse.Namespace, field_name: str) -> str:
    value = getattr(args, field_name, "")
    if not _is_supplied(value):
        raise ValueError(f"{field_name} is required for {args.operation}")
    return _text(value)


def _date_list(value: str, field_name: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise ValueError(f"{field_name} must contain ISO dates")
    for item in values:
        date.fromisoformat(item)
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must contain unique ISO dates")
    return values


def _registered_model_name(args: argparse.Namespace) -> str:
    return (
        f"{_text(args.model_catalog)}.{_text(args.model_schema)}."
        f"nextads_{_text(args.model_name)}"
    )


def _experiment_path(args: argparse.Namespace, operation: str) -> str:
    root = _text(args.experiment_root).rstrip("/")
    return f"{root}/{_text(args.model_name)}/{operation.lower()}"


def _table(args: argparse.Namespace, name: str) -> str:
    return f"{_text(args.model_catalog)}.{_text(args.model_schema)}.{name}"


def _validate_build_plugins(
    args: argparse.Namespace,
    definition: Any,
    observation_dates: tuple[str, ...],
) -> None:
    """Resolve every BUILD plug-in before Spark or durable writes start."""
    plugins = ModelPluginRegistry()
    trainer = plugins.trainer(
        definition,
        registered_model_name=_registered_model_name(args),
    )
    score_provider = plugins.score_provider(
        definition,
        run_date=max(date.fromisoformat(value) for value in observation_dates),
    )
    candidate_adapter = plugins.candidate_adapter(
        definition,
        account_column="account_number",
        advert_column="advert_id",
        scope_filters=definition.evaluation_scope,
    )
    required_methods = (
        (trainer, "train", "trainer"),
        (
            score_provider,
            (
                "score_with_evaluation_scope"
                if definition.evaluation_scope
                else "score"
            ),
            "score provider",
        ),
        (candidate_adapter, "apply", "candidate adapter"),
    )
    for plugin, method_name, kind in required_methods:
        if not callable(getattr(plugin, method_name, None)):
            raise ValueError(
                f"Resolved {kind} does not implement {method_name}"
            )


def _validate_research_plugins(plan: Any) -> None:
    """Resolve all declared research extensions before durable work."""
    for candidate in plan.candidates:
        resolve_candidate_plugin(candidate)
    for identifier in plan.evidence_producers:
        resolve_evidence_producer(identifier)


def _validate_evaluation_plugins(
    args: argparse.Namespace,
    definition: Any,
) -> None:
    """Resolve the declared score provider before evaluation writes."""
    score_provider = ModelPluginRegistry().score_provider(
        definition,
        run_date=date.fromisoformat(_required_text(args, "run_date")),
    )
    _handler, score_method = _EVALUATION_CONTRACTS[
        definition.evaluation_use_case
    ]
    if not callable(getattr(score_provider, score_method, None)):
        raise ValueError(
            f"Resolved score provider does not implement {score_method}"
        )


def validate_request(args: argparse.Namespace) -> Any:
    """Validate operation shape and declaration before any runtime writes."""
    args.operation = _operation(args.operation)
    for field_name in (
        "model_name",
        "feature_catalog",
        "feature_schema",
        "model_catalog",
        "model_schema",
        "experiment_root",
        "code_sha",
    ):
        _required_text(args, field_name)
    if args.execution_count < 0:
        raise ValueError("execution_count must be non-negative")

    required = _REQUIRED_FIELDS[args.operation]
    allowed = required | _OPTIONAL_FIELDS[args.operation]
    for field_name in sorted(required):
        _required_text(args, field_name)
    irrelevant = sorted(
        field_name
        for field_name in _OPERATION_FIELDS.difference(allowed)
        if _is_supplied(getattr(args, field_name, ""))
    )
    if irrelevant:
        raise ValueError(
            f"Fields are not used by {args.operation}: "
            + ", ".join(irrelevant)
        )

    definition = load_model_definition(_text(args.model_name))
    if args.operation == BUILD:
        observation_dates = _date_list(
            _required_text(args, "observation_reference_dates"),
            "observation_reference_dates",
        )
        _date_list(
            _required_text(args, "feature_reference_dates"),
            "feature_reference_dates",
        )
        date.fromisoformat(_required_text(args, "label_end"))
        _validate_build_plugins(args, definition, observation_dates)
    elif args.operation == RESEARCH:
        plan = load_model_research_plan(definition.model_name)
        if plan is None:
            raise ValueError(
                f"Model has no declared research plan: {definition.model_name}"
            )
        label_end = date.fromisoformat(_required_text(args, "label_end"))
        if label_end < plan.temporal_split.test_end:
            raise ValueError(
                "label_end cannot predate the declared test split"
            )
        run_declared_research._declared_dates(plan)
        _validate_research_plugins(plan)
    elif args.operation == EVALUATE:
        date.fromisoformat(_required_text(args, "run_date"))
        feature_dates = _text(args.feature_reference_dates)
        if feature_dates and feature_dates.upper() != "AUTO":
            _date_list(feature_dates, "feature_reference_dates")
        limit = _text(args.evaluation_account_limit)
        if limit:
            try:
                parsed_limit = int(limit)
            except ValueError as error:
                raise ValueError(
                    "evaluation_account_limit must be a positive integer"
                ) from error
            if parsed_limit <= 0:
                raise ValueError(
                    "evaluation_account_limit must be a positive integer"
                )
        if definition.evaluation_use_case not in _EVALUATION_CONTRACTS:
            raise ValueError(
                "No ongoing evaluator is registered for use case: "
                f"{definition.evaluation_use_case}"
            )
        _validate_evaluation_plugins(args, definition)
    return definition


def _build_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        model_name=_text(args.model_name),
        feature_catalog=_text(args.feature_catalog),
        feature_schema=_text(args.feature_schema),
        model_catalog=_text(args.model_catalog),
        model_schema=_text(args.model_schema),
        observation_reference_dates=_text(args.observation_reference_dates),
        feature_reference_dates=_text(args.feature_reference_dates),
        label_end=_text(args.label_end),
        code_sha=_text(args.code_sha),
        registered_model_name=_registered_model_name(args),
        experiment_path=_experiment_path(args, BUILD),
        provider_signals_table=_table(
            args, "next_uk_nextads_score_provider_signals"
        ),
        provider_builds_table=_table(
            args, "next_uk_nextads_score_provider_builds"
        ),
        orchestration_run_id=args.orchestration_run_id,
        task_run_id=args.task_run_id,
        execution_count=args.execution_count,
        log_level=args.log_level,
    )


def _research_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        model_name=_text(args.model_name),
        feature_catalog=_text(args.feature_catalog),
        feature_schema=_text(args.feature_schema),
        model_catalog=_text(args.model_catalog),
        model_schema=_text(args.model_schema),
        train_reference_dates=None,
        validation_reference_dates=None,
        test_reference_dates=None,
        feature_reference_dates=None,
        label_end=_text(args.label_end),
        registered_model_name=_registered_model_name(args),
        experiment_path=_experiment_path(args, RESEARCH),
        selection_mode=None,
        code_sha=_text(args.code_sha),
        orchestration_run_id=args.orchestration_run_id,
        task_run_id=args.task_run_id,
        execution_count=args.execution_count,
        log_level=args.log_level,
    )


def _review_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        model_name=_text(args.model_name),
        research_build_id=_text(args.research_build_id),
        candidate_id=_text(args.candidate_id),
        written_reason=_text(args.written_reason),
        reviewed_by=_text(args.reviewed_by),
        model_catalog=_text(args.model_catalog),
        model_schema=_text(args.model_schema),
        registered_model_name=_registered_model_name(args),
        code_sha=_text(args.code_sha),
        orchestration_run_id=str(args.orchestration_run_id),
        task_run_id=str(args.task_run_id),
        execution_count=args.execution_count,
        log_level=args.log_level,
    )


def _evaluation_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        model_name=_text(args.model_name),
        model_build_id=_text(args.model_build_id),
        run_date=_text(args.run_date),
        feature_catalog=_text(args.feature_catalog),
        feature_schema=_text(args.feature_schema),
        feature_reference_dates=(
            _text(args.feature_reference_dates) or "AUTO"
        ),
        model_catalog=_text(args.model_catalog),
        model_schema=_text(args.model_schema),
        candidate_builds_table=_table(
            args, "next_uk_nextads_candidate_builds"
        ),
        candidate_scores_table=_table(
            args, "next_uk_nextads_candidate_scores"
        ),
        candidate_ad_sets_table=_table(
            args, "next_uk_nextads_candidate_ad_sets"
        ),
        v1_candidate_build_attempt_id=(
            _text(args.evaluation_candidate_build_attempt_id) or "AUTO"
        ),
        candidate_serving_slot=(_text(args.evaluation_serving_slot) or "best"),
        account_limit=int(_text(args.evaluation_account_limit) or "10000"),
        code_sha=_text(args.code_sha),
        orchestration_run_id=args.orchestration_run_id,
        task_run_id=args.task_run_id,
        execution_count=args.execution_count,
        log_level=args.log_level,
    )


def run_operation(
    args: argparse.Namespace,
    *,
    spark: Any | None = None,
    handlers: dict[str, Callable[..., dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Validate then invoke one callable lifecycle handler."""
    definition = validate_request(args)
    operation_handlers = handlers or {
        BUILD: run_declared_model.run_model,
        RESEARCH: run_declared_research.run_research,
        REVIEW_SELECT: select_research_candidate.run_selection,
    }
    handler_args = {
        BUILD: _build_args,
        RESEARCH: _research_args,
        REVIEW_SELECT: _review_args,
        EVALUATE: _evaluation_args,
    }[args.operation](args)
    if handlers is None and args.operation == EVALUATE:
        handler = _EVALUATION_CONTRACTS[definition.evaluation_use_case][0]
    else:
        handler = operation_handlers[args.operation]
    if args.operation == REVIEW_SELECT:
        if spark is None:
            spark = select_research_candidate._spark_session()
        evidence = handler(handler_args, spark=spark)
    else:
        evidence = handler(handler_args, spark=spark)
    return {
        "model_name": definition.model_name,
        "operation": args.operation,
        "result": evidence,
    }


def main(argv: list[str] | None = None) -> None:
    """Run one shared lifecycle operation and emit bounded evidence."""
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    evidence = run_operation(args)
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(
            evidence, default=str, sort_keys=True, separators=(",", ":")
        ),
    )


if __name__ == "__main__":
    main()
