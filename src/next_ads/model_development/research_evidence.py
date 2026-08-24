"""Consistent MLflow-ready research artifacts with immutable manifests."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from next_ads.model_development.research_evaluation import (
    COMPLETE,
)
from next_ads.model_development.research_contracts import (
    MANDATORY_BINARY_METRICS,
)
from next_ads.model_development.research_explainability import (
    GlobalExplanation,
    validate_readable_explanation,
)


FAILED = "FAILED"
NOT_APPLICABLE = "NOT_APPLICABLE"
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SAFE_REASON = re.compile(r"^[A-Za-z0-9 _.,:;()/'-]{1,256}$")
_OPTIONAL_EVIDENCE_MAX_BYTES = 256 * 1024
_OPTIONAL_EVIDENCE_MAX_DEPTH = 8
_OPTIONAL_EVIDENCE_MAX_VALUES = 2000
_FORBIDDEN_ARTIFACT_KEYS = {
    "account_id",
    "accountid",
    "account_number",
    "accountnumber",
    "accounts",
    "customerid",
    "customernumber",
    "customer_number",
    "customer_id",
    "customers",
    "email",
    "email_address",
    "exposure_id",
    "observations",
    "observation_rows",
    "predictions",
    "prediction_rows",
    "raw_rows",
    "row_id",
    "row_id_hash",
}
_FORBIDDEN_ARTIFACT_NORMALIZED = frozenset(
    "".join(character for character in item.casefold() if character.isalnum())
    for item in _FORBIDDEN_ARTIFACT_KEYS
)
MANDATORY_CANDIDATE_ARTIFACTS = (
    "evaluation.json",
    "metrics.json",
    "feature_coverage.csv",
    "feature_coverage.png",
    "feature_importance.csv",
    "feature_importance.png",
    "precision_recall_curve.csv",
    "precision_recall_curve.png",
    "roc_curve.csv",
    "roc_curve.png",
    "calibration.csv",
    "calibration.png",
    "lift_gain.csv",
    "lift_gain.png",
    "score_distribution.csv",
    "score_distribution.png",
    "top_confusion.csv",
    "top_confusion.png",
    "slice_metrics.csv",
    "slice_metrics.png",
)


class MandatoryEvidenceError(ValueError):
    """Raised when a candidate is selected without its standard evidence."""


@dataclass(frozen=True)
class EvidenceArtifact:
    """One immutable artifact entry."""

    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class EvidenceBundle:
    """Artifact receipt used to gate candidate selection."""

    root: Path
    status: str
    artifacts: tuple[EvidenceArtifact, ...]
    manifest_sha256: str
    failures: tuple[str, ...] = ()

    @property
    def selectable(self) -> bool:
        """Return whether every standard evidence gate passed."""
        return self.status == COMPLETE and not self.failures

    def require_selectable(self) -> None:
        """Prevent registration when standard evidence is incomplete."""
        if not self.selectable:
            raise MandatoryEvidenceError(
                "Candidate mandatory evidence is incomplete: "
                + "; ".join(self.failures or (self.status,))
            )


def write_candidate_evidence(
    output_directory: str | Path,
    *,
    candidate_id: str,
    evaluation: Mapping[str, Any],
    feature_coverage: Sequence[Mapping[str, Any]],
    explanation: GlobalExplanation | Mapping[str, Any],
    confidence_intervals: Mapping[str, Any] | None = None,
    optional_evidence: Mapping[str, Mapping[str, Any]] | None = None,
) -> EvidenceBundle:
    """Write the same machine-readable data and graphs for one candidate."""
    if not _SAFE_ID.fullmatch(candidate_id):
        raise ValueError(
            "candidate_id contains unsafe artifact-path characters"
        )
    root = Path(output_directory)
    root.mkdir(parents=True, exist_ok=True)
    explanation_payload = (
        explanation.as_dict()
        if isinstance(explanation, GlobalExplanation)
        else dict(explanation)
    )
    payloads = {
        "evaluation": dict(evaluation),
        "feature_coverage": [dict(row) for row in feature_coverage],
        "explanation": explanation_payload,
        "confidence_intervals": (
            None
            if confidence_intervals is None
            else dict(confidence_intervals)
        ),
        "optional_evidence": {
            name: dict(value)
            for name, value in sorted((optional_evidence or {}).items())
        },
    }
    for name, result in payloads["optional_evidence"].items():
        if result.get("status") not in {COMPLETE, FAILED, NOT_APPLICABLE}:
            raise ValueError(
                "Optional evidence must record COMPLETE, FAILED or "
                f"NOT_APPLICABLE: {name}"
            )
        validate_optional_evidence_result(result, identifier=name)
    _validate_insufficient_slice_privacy(payloads["evaluation"])
    _validate_aggregate_payload(payloads)
    _write_json(root / "evaluation.json", payloads["evaluation"])
    _write_json(root / "metrics.json", evaluation.get("metrics", {}))
    _write_json(root / "explanation.json", explanation_payload)
    _write_json(root / "feature_coverage.json", payloads["feature_coverage"])
    if confidence_intervals is not None:
        _write_json(
            root / "confidence_intervals.json",
            payloads["confidence_intervals"],
        )
    _write_json(root / "optional_evidence.json", payloads["optional_evidence"])

    precision_recall = list(evaluation.get("precision_recall_curve", ()))
    roc = list(evaluation.get("roc_curve", ()))
    calibration = list(evaluation.get("calibration", ()))
    lift_gain = list(evaluation.get("lift_gain", ()))
    score_distribution = list(evaluation.get("score_distribution", ()))
    top_confusion = list(evaluation.get("top_confusion", ()))
    slices = _flatten_slice_metrics(evaluation.get("slices", ()))
    explanation_rows = list(explanation_payload.get("features", ()))
    validate_readable_explanation(explanation_rows)

    _write_csv(root / "precision_recall_curve.csv", precision_recall)
    _write_csv(root / "roc_curve.csv", roc)
    _write_csv(root / "calibration.csv", calibration)
    _write_csv(root / "lift_gain.csv", lift_gain)
    _write_csv(root / "score_distribution.csv", score_distribution)
    _write_csv(root / "top_confusion.csv", top_confusion)
    _write_csv(root / "slice_metrics.csv", slices)
    _write_csv(root / "feature_coverage.csv", feature_coverage)
    _write_csv(root / "feature_importance.csv", explanation_rows)

    _plot_precision_recall(
        root / "precision_recall_curve.png", precision_recall
    )
    _plot_roc(root / "roc_curve.png", roc)
    _plot_calibration(root / "calibration.png", calibration)
    _plot_lift_gain(root / "lift_gain.png", lift_gain)
    _plot_score_distribution(
        root / "score_distribution.png",
        score_distribution,
    )
    _plot_top_confusion(root / "top_confusion.png", top_confusion)
    _plot_slices(root / "slice_metrics.png", slices)
    _plot_feature_coverage(root / "feature_coverage.png", feature_coverage)
    _plot_feature_importance(
        root / "feature_importance.png",
        explanation_rows,
    )

    failures = validate_mandatory_evidence(
        root,
        evaluation=evaluation,
        feature_coverage=feature_coverage,
        explanation=explanation_payload,
    )
    manifest = build_artifact_manifest(root)
    _write_json(
        root / "artifact_manifest.json",
        {
            "candidate_id": candidate_id,
            "status": COMPLETE if not failures else FAILED,
            "failures": list(failures),
            "artifacts": [artifact.__dict__ for artifact in manifest],
        },
    )
    manifest_sha256 = _sha256(root / "artifact_manifest.json")
    return EvidenceBundle(
        root=root,
        status=COMPLETE if not failures else FAILED,
        artifacts=manifest,
        manifest_sha256=manifest_sha256,
        failures=failures,
    )


def write_candidate_comparison_evidence(
    output_directory: str | Path,
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[EvidenceArtifact, ...]:
    """Write a bounded candidate comparison table and graph for the parent run."""
    root = Path(output_directory)
    root.mkdir(parents=True, exist_ok=True)
    rows = []
    seen = set()
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id", ""))
        if not _SAFE_ID.fullmatch(candidate_id) or candidate_id in seen:
            raise ValueError(
                "Candidate comparison IDs must be unique and safe"
            )
        seen.add(candidate_id)
        metrics = candidate.get("metrics") or {}
        rows.append(
            {
                "candidate_id": candidate_id,
                "status": candidate.get("status"),
                "auc_pr": metrics.get("auc_pr"),
                "prevalence": metrics.get("prevalence"),
                "auc_roc": metrics.get("auc_roc"),
                "log_loss": metrics.get("log_loss"),
                "lift_at_5_percent": metrics.get("lift_at_5_percent"),
                "selectable": bool(candidate.get("selectable", False)),
            }
        )
    _validate_aggregate_payload(rows)
    rows.sort(key=lambda row: row["candidate_id"])
    _write_json(root / "candidate_comparison.json", rows)
    _write_csv(root / "candidate_comparison.csv", rows)
    _plot_candidate_comparison(root / "candidate_comparison.png", rows)
    return build_artifact_manifest(root)


def validate_mandatory_evidence(
    root: str | Path,
    *,
    evaluation: Mapping[str, Any],
    feature_coverage: Sequence[Mapping[str, Any]],
    explanation: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return every reason a candidate cannot be selected."""
    root = Path(root)
    failures = []
    if evaluation.get("status") != COMPLETE:
        failures.append(
            "evaluation status is " + str(evaluation.get("status"))
        )
    for key in (
        "precision_recall_curve",
        "roc_curve",
        "calibration",
        "lift_gain",
        "score_distribution",
        "top_confusion",
    ):
        if not evaluation.get(key):
            failures.append(f"{key} evidence is missing")
    metrics = evaluation.get("metrics") or {}
    required_metrics = set(MANDATORY_BINARY_METRICS)
    missing_metrics = sorted(required_metrics.difference(metrics))
    if missing_metrics:
        failures.append("metrics are missing: " + ", ".join(missing_metrics))
    if not feature_coverage:
        failures.append("feature coverage is missing")
    if explanation.get("status") != COMPLETE:
        failures.append(
            "explanation status is " + str(explanation.get("status"))
        )
    if not explanation.get("features"):
        failures.append("feature explanation is missing")
    for relative_path in MANDATORY_CANDIDATE_ARTIFACTS:
        path = root / relative_path
        if not path.is_file() or path.stat().st_size == 0:
            failures.append(f"artifact is missing or empty: {relative_path}")
    return tuple(failures)


def build_artifact_manifest(
    root: str | Path,
) -> tuple[EvidenceArtifact, ...]:
    """Hash artifact names and bytes in deterministic path order."""
    root = Path(root)
    if not root.is_dir():
        raise ValueError(f"Evidence directory does not exist: {root}")
    artifacts = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if relative == "artifact_manifest.json":
            continue
        artifacts.append(
            EvidenceArtifact(
                path=relative,
                sha256=_sha256(path),
                bytes=path.stat().st_size,
            )
        )
    if not artifacts:
        raise ValueError("Evidence artifact directory is empty")
    return tuple(artifacts)


def log_evidence_bundle(
    mlflow_module: Any,
    bundle: EvidenceBundle,
    *,
    artifact_path: str = "evidence",
    require_selectable: bool = False,
    parameter_prefix: str = "",
) -> None:
    """Log complete or failed evidence while optionally enforcing selection."""
    if require_selectable:
        bundle.require_selectable()
    mlflow_module.log_artifacts(str(bundle.root), artifact_path=artifact_path)
    mlflow_module.log_param(
        f"{parameter_prefix}evidence_manifest_sha256",
        bundle.manifest_sha256,
    )
    mlflow_module.log_param(
        f"{parameter_prefix}evidence_status",
        bundle.status,
    )


def _flatten_slice_metrics(
    slices: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for slice_result in slices:
        profile = slice_result.get("profile") or {}
        metrics = slice_result.get("metrics") or {}
        rows.append(
            {
                "slice_id": slice_result.get("slice_id"),
                "slice_column": slice_result.get("slice_column"),
                "slice_value": slice_result.get("slice_value"),
                "minimum_rows": slice_result.get("minimum_rows"),
                "status": slice_result.get("status"),
                "reason": slice_result.get("reason"),
                "rows": profile.get("rows"),
                "positives": profile.get("positives"),
                "negatives": profile.get("negatives"),
                **metrics,
            }
        )
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=str,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    materialised = [dict(row) for row in rows]
    fieldnames = sorted(
        {key for row in materialised for key in row},
        key=lambda value: (value not in {"candidate_id", "feature"}, value),
    )
    if not fieldnames:
        fieldnames = ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, lineterminator="\n"
        )
        writer.writeheader()
        for row in materialised:
            writer.writerow(
                {key: _csv_value(row.get(key)) for key in fieldnames}
            )


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return value


def _plot_precision_recall(
    path: Path, rows: Sequence[Mapping[str, Any]]
) -> None:
    _plot_line(
        path,
        rows,
        x="recall",
        y="precision",
        title="Precision-recall curve",
        x_label="Recall",
        y_label="Precision",
    )


def _plot_roc(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    _plot_line(
        path,
        rows,
        x="false_positive_rate",
        y="recall",
        title="ROC curve",
        x_label="False positive rate",
        y_label="True positive rate",
        reference=True,
    )


def _plot_calibration(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(7, 5))
    if rows:
        axis.plot(
            [float(row["mean_score"]) for row in rows],
            [float(row["observed_rate"]) for row in rows],
            marker="o",
            label="candidate",
        )
        axis.plot([0, 1], [0, 1], linestyle="--", label="ideal")
        axis.legend()
    else:
        _empty_axis(axis, "No calibration evidence")
    axis.set_title("Calibration")
    axis.set_xlabel("Mean predicted click rate")
    axis.set_ylabel("Observed click rate")
    _save_figure(plt, fig, path)


def _plot_lift_gain(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(8, 5))
    if rows:
        x_values = [float(row["population_fraction"]) for row in rows]
        axis.plot(
            x_values,
            [float(row["cumulative_gain"]) for row in rows],
            label="cumulative gain",
        )
        axis.plot([0, 1], [0, 1], linestyle="--", label="prevalence baseline")
        lift_axis = axis.twinx()
        lift_axis.plot(
            x_values,
            [float(row["cumulative_lift"]) for row in rows],
            color="tab:orange",
            label="cumulative lift",
        )
        lift_axis.set_ylabel("Lift")
        handles, labels = axis.get_legend_handles_labels()
        right_handles, right_labels = lift_axis.get_legend_handles_labels()
        axis.legend(handles + right_handles, labels + right_labels)
    else:
        _empty_axis(axis, "No lift or gain evidence")
    axis.set_title("Lift and cumulative gain")
    axis.set_xlabel("Population fraction")
    axis.set_ylabel("Cumulative gain")
    _save_figure(plt, fig, path)


def _plot_score_distribution(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(8, 5))
    if rows:
        labels = sorted({int(row["label"]) for row in rows})
        bins = sorted({int(row["score_bin"]) for row in rows})
        for label in labels:
            lookup = {
                int(row["score_bin"]): int(row["rows"])
                for row in rows
                if int(row["label"]) == label
            }
            axis.plot(
                bins,
                [lookup.get(score_bin, 0) for score_bin in bins],
                marker="o",
                label="clicked" if label == 1 else "not clicked",
            )
        axis.legend()
    else:
        _empty_axis(axis, "No score-distribution evidence")
    axis.set_title("Score distribution by observed label")
    axis.set_xlabel("Score bin")
    axis.set_ylabel("Rows")
    _save_figure(plt, fig, path)


def _plot_top_confusion(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(8, 5))
    if rows:
        labels = [f"top {row['percentage']}%" for row in rows]
        true_positive = [int(row["tp"]) for row in rows]
        false_positive = [int(row["fp"]) for row in rows]
        axis.bar(labels, true_positive, label="observed click")
        axis.bar(
            labels,
            false_positive,
            bottom=true_positive,
            label="no observed click",
        )
        axis.legend()
    else:
        _empty_axis(axis, "No top-fraction confusion evidence")
    axis.set_title("Confusion among highest scores")
    axis.set_ylabel("Selected rows")
    _save_figure(plt, fig, path)


def _plot_slices(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(10, 5))
    complete = [row for row in rows if row.get("status") == COMPLETE]
    if complete:
        labels = [
            f"{row['slice_column']}={row['slice_value']}" for row in complete
        ]
        axis.bar(
            labels,
            [float(row["auc_pr"]) for row in complete],
        )
        axis.tick_params(axis="x", rotation=30)
    else:
        _empty_axis(axis, "No sufficiently populated reporting slices")
    axis.set_title("Validation PR-AUC by reporting slice")
    axis.set_ylabel("PR-AUC")
    _save_figure(plt, fig, path)


def _plot_feature_coverage(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(10, 5))
    if rows:
        features = [str(row["feature"]) for row in rows]
        missing = [float(row["missing_rate"]) for row in rows]
        defaults = [float(row["default_rate"]) for row in rows]
        indexes = list(range(len(features)))
        axis.bar(
            [index - 0.2 for index in indexes],
            missing,
            0.4,
            label="missing",
        )
        axis.bar(
            [index + 0.2 for index in indexes],
            defaults,
            0.4,
            label="declared default",
        )
        axis.set_xticks(indexes, features, rotation=30, ha="right")
        axis.legend()
    else:
        _empty_axis(axis, "No feature-coverage evidence")
    axis.set_title("Feature missingness and default-value coverage")
    axis.set_ylabel("Fraction of rows")
    _save_figure(plt, fig, path)


def _plot_feature_importance(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(10, 6))
    top = sorted(
        rows,
        key=lambda row: float(row.get("absolute_importance") or 0.0),
        reverse=True,
    )[:30]
    if top:
        top.reverse()
        axis.barh(
            [str(row["feature"]) for row in top],
            [float(row.get("absolute_importance") or 0.0) for row in top],
        )
    else:
        _empty_axis(axis, "No feature-importance evidence")
    axis.set_title("Global feature importance")
    axis.set_xlabel("Absolute importance")
    _save_figure(plt, fig, path)


def _plot_candidate_comparison(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(9, 5))
    comparable = _candidate_comparison_plot_rows(rows)
    if comparable:
        labels = [str(row["candidate_id"]) for row in comparable]
        axis.bar(labels, [float(row["auc_pr"]) for row in comparable])
        axis.scatter(
            labels,
            [float(row["prevalence"]) for row in comparable],
            marker="_",
            color="black",
            label="prevalence reference",
        )
        axis.tick_params(axis="x", rotation=30)
        axis.legend()
    else:
        _empty_axis(axis, "No comparable candidates")
    axis.set_title("Candidate validation comparison")
    axis.set_ylabel("PR-AUC")
    _save_figure(plt, fig, path)


def _candidate_comparison_plot_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if row.get("auc_pr") is not None
        and (
            row.get("status") == COMPLETE
            or (row.get("status") == "READY" and row.get("selectable") is True)
        )
    ]


def _plot_line(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
    title: str,
    x_label: str,
    y_label: str,
    reference: bool = False,
) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(7, 5))
    if rows:
        axis.plot(
            [float(row[x]) for row in rows],
            [float(row[y]) for row in rows],
        )
        if reference:
            axis.plot([0, 1], [0, 1], linestyle="--")
    else:
        _empty_axis(axis, f"No {title.lower()} evidence")
    axis.set_title(title)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
    _save_figure(plt, fig, path)


def _empty_axis(axis: Any, message: str) -> None:
    axis.text(0.5, 0.5, message, ha="center", va="center")
    axis.set_axis_off()


def _save_figure(plt: Any, figure: Any, path: Path) -> None:
    figure.tight_layout()
    figure.savefig(
        path,
        dpi=120,
        metadata={"Software": "NextAds model research"},
    )
    plt.close(figure)


def _pyplot() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    return plt


def _validate_aggregate_payload(payload: Any, *, path: str = "root") -> None:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            normalized = str(key).casefold()
            if normalized in _FORBIDDEN_ARTIFACT_KEYS:
                raise ValueError(
                    f"Evidence artifacts cannot contain row identity: {path}.{key}"
                )
            _validate_aggregate_payload(value, path=f"{path}.{key}")
    elif isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            _validate_aggregate_payload(value, path=f"{path}[{index}]")


def _validate_insufficient_slice_privacy(
    evaluation: Mapping[str, Any],
) -> None:
    for item in evaluation.get("slices", ()):
        if (
            not isinstance(item, Mapping)
            or item.get("status") != "INSUFFICIENT"
        ):
            continue
        profile = item.get("profile")
        if not isinstance(profile, Mapping) or set(profile) - {"rows"}:
            raise ValueError(
                "Insufficient slice evidence may expose only aggregate row count"
            )
        reason = str(item.get("reason", "")).casefold()
        if any(
            token in reason
            for token in (
                "positive_rows=",
                "negative_rows=",
                "positives=",
                "negatives=",
                "click_rate=",
                "log_loss=",
            )
        ):
            raise ValueError(
                "Insufficient slice reason exposes suppressed outcomes"
            )


def _normalized_artifact_key(value: object) -> str:
    return "".join(
        character for character in str(value).casefold() if character.isalnum()
    )


def _optional_numeric_payload(
    payload: Any,
    *,
    path: str,
    depth: int,
    value_counter: list[int],
) -> None:
    """Validate one numeric aggregate payload without accepting row records."""
    if depth > _OPTIONAL_EVIDENCE_MAX_DEPTH:
        raise ValueError("Optional evidence exceeds the maximum nesting depth")
    value_counter[0] += 1
    if value_counter[0] > _OPTIONAL_EVIDENCE_MAX_VALUES:
        raise ValueError(
            "Optional evidence contains too many aggregate values"
        )
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            key_text = str(key)
            if not _SAFE_ID.fullmatch(key_text):
                raise ValueError(
                    f"Optional evidence contains an unsafe field name: {path}.{key}"
                )
            normalized = _normalized_artifact_key(key)
            if normalized in _FORBIDDEN_ARTIFACT_NORMALIZED:
                raise ValueError(
                    "Optional evidence cannot contain row identity: "
                    f"{path}.{key}"
                )
            _optional_numeric_payload(
                value,
                path=f"{path}.{key}",
                depth=depth + 1,
                value_counter=value_counter,
            )
        return
    if isinstance(payload, (list, tuple)):
        if any(isinstance(value, Mapping) for value in payload):
            raise ValueError(
                "Optional evidence cannot contain record-shaped row lists"
            )
        for index, value in enumerate(payload):
            _optional_numeric_payload(
                value,
                path=f"{path}[{index}]",
                depth=depth + 1,
                value_counter=value_counter,
            )
        return
    if payload is None or isinstance(payload, (bool, int)):
        return
    if isinstance(payload, float):
        if not math.isfinite(payload):
            raise ValueError("Optional evidence values must be finite")
        return
    raise ValueError(
        "Optional evidence must contain aggregate numeric JSON values only: "
        + path
    )


def validate_optional_evidence_result(
    result: Mapping[str, Any],
    *,
    identifier: str,
) -> None:
    """Enforce a bounded, aggregate-only extension artifact contract."""
    if not _SAFE_ID.fullmatch(identifier):
        raise ValueError("Optional evidence identifier is unsafe")
    status = result.get("status")
    if status == COMPLETE:
        if set(result) != {"status", "evidence"}:
            raise ValueError(
                "Completed optional evidence may contain only status and evidence"
            )
        evidence = result.get("evidence")
        if not isinstance(evidence, Mapping) or not evidence:
            raise ValueError(
                "Completed optional evidence needs a non-empty aggregate mapping"
            )
        _optional_numeric_payload(
            evidence,
            path=f"optional.{identifier}",
            depth=0,
            value_counter=[0],
        )
    elif status in {FAILED, NOT_APPLICABLE}:
        if set(result) != {"status", "reason"}:
            raise ValueError(
                "Non-complete optional evidence may contain only status and reason"
            )
        reason = str(result.get("reason", "")).strip()
        if status == NOT_APPLICABLE and not _SAFE_REASON.fullmatch(reason):
            raise ValueError(
                "NOT_APPLICABLE optional evidence needs a bounded safe reason"
            )
        if status == FAILED:
            try:
                envelope = json.loads(reason)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError(
                    "FAILED optional evidence needs a safe failure envelope"
                ) from exc
            if set(envelope) != {
                "error_type",
                "message",
                "message_sha256",
                "stage",
            } or not re.fullmatch(
                r"[0-9a-f]{64}", str(envelope.get("message_sha256", ""))
            ):
                raise ValueError(
                    "FAILED optional evidence needs a safe failure envelope"
                )
    else:
        raise ValueError(
            "Optional evidence must record COMPLETE, FAILED or NOT_APPLICABLE"
        )
    encoded = json.dumps(
        result,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > _OPTIONAL_EVIDENCE_MAX_BYTES:
        raise ValueError("Optional evidence exceeds the bounded byte limit")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "FAILED",
    "NOT_APPLICABLE",
    "MANDATORY_CANDIDATE_ARTIFACTS",
    "EvidenceArtifact",
    "EvidenceBundle",
    "MandatoryEvidenceError",
    "build_artifact_manifest",
    "log_evidence_bundle",
    "validate_mandatory_evidence",
    "validate_optional_evidence_result",
    "write_candidate_comparison_evidence",
    "write_candidate_evidence",
]
