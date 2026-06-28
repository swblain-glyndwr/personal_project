"""MLflow evidence artifacts for Theme Affinity training runs."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory


def log_training_evidence_artifacts(
    mlflow_module,
    sample_profile: dict,
    split_label_stats: dict,
    prediction_evidence: dict | None = None,
) -> None:
    """Log compact JSON and ranking-relevant diagnostic plots to MLflow."""
    prediction_evidence = prediction_evidence or {}
    with TemporaryDirectory() as tmp:
        artifact_dir = Path(tmp)
        _write_json(
            artifact_dir / "sample_profile.json",
            {
                "sample_profile": sample_profile,
                "split_label_stats": split_label_stats,
                "prediction_evidence": prediction_evidence,
            },
        )
        _write_sample_distribution_plot(
            artifact_dir / "sample_distribution_pre_post.png",
            sample_profile,
        )
        _write_label_distribution_plot(
            artifact_dir / "label_distribution_by_split.png",
            split_label_stats,
        )
        _write_rank_band_plot(
            artifact_dir / "rank_band_distribution.png",
            sample_profile,
        )
        _write_score_distribution_plot(
            artifact_dir / "score_distribution_pos_neg.png",
            prediction_evidence.get("score_distribution", []),
        )
        _write_lift_plot(
            artifact_dir / "lift_by_decile.png",
            prediction_evidence.get("lift_by_decile", []),
        )
        for k, matrix in prediction_evidence.get("top_k_confusion_matrices", {}).items():
            _write_confusion_matrix_plot(
                artifact_dir / f"top_k_confusion_matrix_k{k}.png",
                matrix,
                f"Top-{k} confusion matrix",
            )
        mlflow_module.log_artifacts(str(artifact_dir))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _write_sample_distribution_plot(path: Path, sample_profile: dict) -> None:
    population = sample_profile.get("population", {}).get("distributions", {})
    sampled = sample_profile.get("sampled", {}).get("distributions", {})
    columns = [
        column
        for column in [
            "label_bucket",
            "repurchase_stage",
            "GmaName",
            "user_total_views_bucket",
            "num_retrieval_methods_bucket",
        ]
        if column in population or column in sampled
    ]
    if not columns:
        _write_empty_plot(path, "No sample distributions available")
        return

    plt = _pyplot()
    fig, axes = plt.subplots(len(columns), 1, figsize=(10, 3 * len(columns)))
    if len(columns) == 1:
        axes = [axes]
    for axis, column in zip(axes, columns):
        _plot_distribution_pair(
            axis,
            population.get(column, []),
            sampled.get(column, []),
            column,
        )
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _write_label_distribution_plot(path: Path, split_label_stats: dict) -> None:
    plt = _pyplot()
    splits = ["train", "validation", "test"]
    positives = [
        split_label_stats.get(split, {}).get("positive_rows", 0) for split in splits
    ]
    negatives = [
        split_label_stats.get(split, {}).get("negative_rows", 0) for split in splits
    ]
    fig, axis = plt.subplots(figsize=(8, 5))
    axis.bar(splits, negatives, label="negative")
    axis.bar(splits, positives, bottom=negatives, label="positive")
    axis.set_title("Label distribution by split")
    axis.set_ylabel("Rows")
    axis.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _write_rank_band_plot(path: Path, sample_profile: dict) -> None:
    rank_dist = (
        sample_profile.get("sampled", {})
        .get("distributions", {})
        .get("simple_rules_rank_band", [])
    )
    if not rank_dist:
        _write_empty_plot(path, "No rank-band distribution available")
        return
    plt = _pyplot()
    labels = [row["value"] for row in rank_dist]
    counts = [row["count"] for row in rank_dist]
    fig, axis = plt.subplots(figsize=(10, 5))
    axis.bar(labels, counts)
    axis.set_title("Sampled simple-rules rank-band distribution")
    axis.set_ylabel("Rows")
    axis.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _write_score_distribution_plot(path: Path, rows: list[dict]) -> None:
    if not rows:
        _write_empty_plot(path, "No score distribution available")
        return
    plt = _pyplot()
    labels = sorted({str(row["label_bucket"]) for row in rows})
    bins = sorted({int(row["score_bin"]) for row in rows})
    fig, axis = plt.subplots(figsize=(10, 5))
    for label in labels:
        counts = [
            next(
                (
                    int(row["count"])
                    for row in rows
                    if str(row["label_bucket"]) == label
                    and int(row["score_bin"]) == score_bin
                ),
                0,
            )
            for score_bin in bins
        ]
        axis.plot(bins, counts, marker="o", label=label)
    axis.set_title("Score distribution by label")
    axis.set_xlabel("Prediction score bin")
    axis.set_ylabel("Rows")
    axis.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _write_lift_plot(path: Path, rows: list[dict]) -> None:
    if not rows:
        _write_empty_plot(path, "No lift data available")
        return
    plt = _pyplot()
    rows = sorted(rows, key=lambda row: int(row["score_decile"]))
    deciles = [int(row["score_decile"]) for row in rows]
    positive_rates = [float(row["positive_rate"]) for row in rows]
    fig, axis = plt.subplots(figsize=(10, 5))
    axis.plot(deciles, positive_rates, marker="o")
    axis.set_title("Positive capture rate by score decile")
    axis.set_xlabel("Score decile, 1 = highest score")
    axis.set_ylabel("Positive rate")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _write_confusion_matrix_plot(path: Path, matrix: dict, title: str) -> None:
    plt = _pyplot()
    values = [
        [matrix.get("tp", 0), matrix.get("fp", 0)],
        [matrix.get("fn", 0), matrix.get("tn", 0)],
    ]
    fig, axis = plt.subplots(figsize=(5, 4))
    image = axis.imshow(values, cmap="Blues")
    axis.set_xticks([0, 1], labels=["pred +", "pred -"])
    axis.set_yticks([0, 1], labels=["actual +", "actual -"])
    axis.set_title(title)
    for row_index, row in enumerate(values):
        for col_index, value in enumerate(row):
            axis.text(col_index, row_index, str(value), ha="center", va="center")
    fig.colorbar(image, ax=axis)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _plot_distribution_pair(axis, population: list[dict], sampled: list[dict], title: str):
    values = list(
        dict.fromkeys(
            [row["value"] for row in population] + [row["value"] for row in sampled]
        )
    )
    population_counts = _counts_for_values(population, values)
    sampled_counts = _counts_for_values(sampled, values)
    indexes = list(range(len(values)))
    width = 0.4
    axis.bar([index - width / 2 for index in indexes], population_counts, width, label="population")
    axis.bar([index + width / 2 for index in indexes], sampled_counts, width, label="sampled")
    axis.set_title(title)
    axis.set_xticks(indexes, values, rotation=30, ha="right")
    axis.set_ylabel("Rows")
    axis.legend()


def _counts_for_values(rows: list[dict], values: list[str]) -> list[int]:
    lookup = {row["value"]: int(row["count"]) for row in rows}
    return [lookup.get(value, 0) for value in values]


def _write_empty_plot(path: Path, title: str) -> None:
    plt = _pyplot()
    fig, axis = plt.subplots(figsize=(8, 4))
    axis.text(0.5, 0.5, title, ha="center", va="center")
    axis.set_axis_off()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _pyplot():
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    return plt
