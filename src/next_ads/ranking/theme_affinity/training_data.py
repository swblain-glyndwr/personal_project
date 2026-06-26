from dataclasses import dataclass

from pyspark.sql import Window
from pyspark.sql import functions as F


ACCOUNT_COL = "account_number"
LABEL_COL = "label"
RANK_COL = "simple_rules_rank"


@dataclass(frozen=True)
class TrainingFrameConfig:
    max_accounts: int
    max_candidates_per_account: int
    max_rows: int
    rank_filter_threshold: int | None = None
    random_seed: int = 42


def resolve_training_frame_config(model_config) -> TrainingFrameConfig:
    frame_config = getattr(model_config, "training_frame", None)
    if frame_config is None:
        raise ValueError("ranking_model.training_frame config is required")

    rank_filter_threshold = getattr(frame_config, "rank_filter_threshold", None)
    return TrainingFrameConfig(
        max_accounts=int(frame_config.max_accounts),
        max_candidates_per_account=int(frame_config.max_candidates_per_account),
        max_rows=int(frame_config.max_rows),
        rank_filter_threshold=(
            int(rank_filter_threshold)
            if rank_filter_threshold not in (None, "")
            else None
        ),
        random_seed=int(getattr(frame_config, "random_seed", 42)),
    )


def build_bounded_training_frame(base_df, frame_config: TrainingFrameConfig):
    _validate_training_frame_config(frame_config)
    _require_columns(base_df, [ACCOUNT_COL, LABEL_COL])

    filtered = _filter_ranked_candidates(base_df, frame_config.rank_filter_threshold)
    account_window = Window.orderBy(
        F.col("stratify_label").desc(),
        F.pmod(
            F.xxhash64(F.col(ACCOUNT_COL), F.lit(frame_config.random_seed)),
            F.lit(9223372036854775807),
        ).asc(),
    )
    selected_accounts = (
        filtered.groupBy(ACCOUNT_COL)
        .agg(F.max(LABEL_COL).alias("stratify_label"))
        .withColumn("account_sample_row", F.row_number().over(account_window))
        .filter(F.col("account_sample_row") <= F.lit(frame_config.max_accounts))
        .select(ACCOUNT_COL)
    )

    candidate_order = [F.col(LABEL_COL).desc()]
    if RANK_COL in filtered.columns:
        candidate_order.append(F.col(RANK_COL).asc_nulls_last())
    if "theme_clean" in filtered.columns:
        candidate_order.append(F.col("theme_clean").asc())

    candidate_window = Window.partitionBy(ACCOUNT_COL).orderBy(*candidate_order)
    training_frame = (
        filtered.join(selected_accounts, on=ACCOUNT_COL, how="inner")
        .withColumn("candidate_sample_row", F.row_number().over(candidate_window))
        .filter(
            F.col("candidate_sample_row")
            <= F.lit(frame_config.max_candidates_per_account)
        )
        .drop("candidate_sample_row")
    )

    row_count = training_frame.count()
    if row_count > frame_config.max_rows:
        raise ValueError(
            "Theme Affinity training frame exceeds configured max_rows: "
            f"{row_count:,} > {frame_config.max_rows:,}. "
            "Tighten ranking_model.training_frame before training."
        )

    stats = training_frame_stats(training_frame)
    stats["training_frame_rank_filter_threshold"] = (
        frame_config.rank_filter_threshold or 0
    )
    stats["training_frame_max_accounts"] = frame_config.max_accounts
    stats["training_frame_max_candidates_per_account"] = (
        frame_config.max_candidates_per_account
    )
    stats["training_frame_max_rows"] = frame_config.max_rows
    return training_frame, stats


def split_training_frame(base_df, train_ratio: float, test_ratio: float):
    train_end = train_ratio
    test_end = train_ratio + test_ratio
    account_metadata = (
        base_df.groupBy(ACCOUNT_COL)
        .agg(F.max(LABEL_COL).alias("stratify_label"))
        .withColumn("rand", F.rand(seed=42))
    )
    stratify_window = Window.partitionBy("stratify_label").orderBy("rand")
    total_window = Window.partitionBy("stratify_label")
    account_splits = (
        account_metadata.withColumn(
            "row_num",
            F.row_number().over(stratify_window),
        )
        .withColumn("total_in_class", F.count("*").over(total_window))
        .withColumn(
            "split",
            F.when(
                F.col("row_num") <= F.col("total_in_class") * train_end,
                "train",
            )
            .when(
                F.col("row_num") <= F.col("total_in_class") * test_end,
                "test",
            )
            .otherwise("validation"),
        )
    )
    return base_df.join(
        account_splits.select(ACCOUNT_COL, "split"),
        on=ACCOUNT_COL,
        how="inner",
    )


def split_counts(base_with_split):
    return {
        row["split"]: row["count"]
        for row in base_with_split.groupBy("split").count().collect()
    }


def validate_split_counts(counts: dict[str, int]):
    missing = [
        split_name
        for split_name in ["train", "validation", "test"]
        if counts.get(split_name, 0) <= 0
    ]
    if missing:
        raise ValueError(
            "Theme Affinity training frame produced empty split(s): "
            + ", ".join(missing)
        )


def training_frame_stats(training_frame):
    row = training_frame.agg(
        F.count("*").alias("training_frame_row_count"),
        F.countDistinct(ACCOUNT_COL).alias("training_frame_account_count"),
        F.sum(F.col(LABEL_COL).cast("double")).alias("training_frame_positive_rows"),
        F.avg(F.col(LABEL_COL).cast("double")).alias("training_frame_positive_rate"),
    ).first()
    if row is None:
        return {
            "training_frame_row_count": 0,
            "training_frame_account_count": 0,
            "training_frame_positive_rows": 0,
            "training_frame_positive_rate": 0.0,
        }
    return {
        "training_frame_row_count": int(row["training_frame_row_count"] or 0),
        "training_frame_account_count": int(
            row["training_frame_account_count"] or 0
        ),
        "training_frame_positive_rows": int(
            row["training_frame_positive_rows"] or 0
        ),
        "training_frame_positive_rate": float(
            row["training_frame_positive_rate"] or 0.0
        ),
    }


def _filter_ranked_candidates(base_df, rank_filter_threshold: int | None):
    if rank_filter_threshold is None:
        return base_df
    _require_columns(base_df, [RANK_COL])
    return base_df.filter(
        (F.col(RANK_COL) <= F.lit(rank_filter_threshold))
        | (F.col(LABEL_COL) > F.lit(0))
    )


def _require_columns(df, columns: list[str]):
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(
            "Theme Affinity training frame is missing required column(s): "
            + ", ".join(missing)
        )


def _validate_training_frame_config(frame_config: TrainingFrameConfig):
    if frame_config.max_accounts <= 0:
        raise ValueError("training_frame.max_accounts must be greater than 0")
    if frame_config.max_candidates_per_account <= 0:
        raise ValueError(
            "training_frame.max_candidates_per_account must be greater than 0"
        )
    if frame_config.max_rows <= 0:
        raise ValueError("training_frame.max_rows must be greater than 0")
