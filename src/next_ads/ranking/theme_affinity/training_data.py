from dataclasses import asdict, dataclass

from pyspark.sql import Window
from pyspark.sql import functions as F


ACCOUNT_COL = "account_number"
LABEL_COL = "label"
RANK_COL = "simple_rules_rank"
REFERENCE_DATE_COL = "reference_date"
THEME_COLS = ("theme", "theme_clean")

GROUP_STRATUM_COL = "__sample_group_stratum"
CANDIDATE_STRATUM_COL = "__candidate_stratum"
RANK_BAND_COL = "simple_rules_rank_band"
LABEL_BUCKET_COL = "label_bucket"
ACTIVITY_BUCKET_COL = "user_total_views_bucket"
RETRIEVAL_BUCKET_COL = "num_retrieval_methods_bucket"

DEFAULT_RANK_BAND_EDGES = (20, 100, 256)
DEFAULT_CANDIDATE_RANK_BAND_FRACTIONS = {
    "rank_001_020": 1.0,
    "rank_021_100": 0.75,
    "rank_101_256": 0.5,
    "rank_gt_256": 0.25,
    "rank_missing": 0.5,
}
DEFAULT_SAMPLE_STRATA_COLUMNS = (
    LABEL_BUCKET_COL,
    "repurchase_stage",
    "GmaName",
    ACTIVITY_BUCKET_COL,
    RETRIEVAL_BUCKET_COL,
)


@dataclass(frozen=True)
class TrainingFrameConfig:
    max_accounts: int
    max_candidates_per_account: int
    max_rows: int
    rank_filter_threshold: int | None = None
    random_seed: int = 42
    min_positive_group_fraction: float = 1.0
    activity_bucket_count: int = 4
    retrieval_bucket_count: int = 4
    rank_band_edges: tuple[int, ...] = DEFAULT_RANK_BAND_EDGES
    candidate_rank_band_fractions: dict[str, float] | None = None
    sample_strata_columns: tuple[str, ...] = DEFAULT_SAMPLE_STRATA_COLUMNS
    profile_top_n: int = 25


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
        min_positive_group_fraction=float(
            getattr(frame_config, "min_positive_group_fraction", 1.0)
        ),
        activity_bucket_count=int(getattr(frame_config, "activity_bucket_count", 4)),
        retrieval_bucket_count=int(
            getattr(frame_config, "retrieval_bucket_count", 4)
        ),
        rank_band_edges=tuple(
            int(edge)
            for edge in getattr(frame_config, "rank_band_edges", DEFAULT_RANK_BAND_EDGES)
        ),
        candidate_rank_band_fractions=dict(
            getattr(
                frame_config,
                "candidate_rank_band_fractions",
                DEFAULT_CANDIDATE_RANK_BAND_FRACTIONS,
            )
        ),
        sample_strata_columns=tuple(
            getattr(frame_config, "sample_strata_columns", DEFAULT_SAMPLE_STRATA_COLUMNS)
        ),
        profile_top_n=int(getattr(frame_config, "profile_top_n", 25)),
    )


def build_bounded_training_frame(base_df, frame_config: TrainingFrameConfig):
    _validate_training_frame_config(frame_config)
    _require_columns(base_df, [ACCOUNT_COL, LABEL_COL])

    filtered = _filter_ranked_candidates(base_df, frame_config.rank_filter_threshold)
    filtered = _with_rank_band(filtered, frame_config)
    group_keys = _group_key_columns(filtered)
    population_profile = frame_profile(
        filtered,
        frame_config,
        group_keys,
        "population",
    )

    group_metadata = _build_group_metadata(filtered, frame_config, group_keys)
    selected_groups, group_sampling_profile = _sample_groups(
        group_metadata,
        frame_config,
        group_keys,
    )
    selected = filtered.join(selected_groups.select(*group_keys), on=group_keys)
    training_frame = _sample_candidates(selected, frame_config, group_keys)

    sampled_profile = frame_profile(
        training_frame,
        frame_config,
        group_keys,
        "sampled",
    )

    row_count = training_frame.count()
    if row_count > frame_config.max_rows:
        raise ValueError(
            "Theme Affinity training frame exceeds configured max_rows: "
            f"{row_count:,} > {frame_config.max_rows:,}. "
            "Tighten ranking_model.training_frame before training."
        )

    stats = training_frame_stats(training_frame)
    validate_training_frame_stats(stats)
    stats["training_frame_rank_filter_threshold"] = (
        frame_config.rank_filter_threshold or 0
    )
    stats["training_frame_max_accounts"] = frame_config.max_accounts
    stats["training_frame_max_candidates_per_account"] = (
        frame_config.max_candidates_per_account
    )
    stats["training_frame_max_rows"] = frame_config.max_rows
    stats["training_frame_sample_profile"] = {
        "config": asdict(frame_config),
        "group_keys": group_keys,
        "population": population_profile,
        "group_sampling": group_sampling_profile,
        "sampled": sampled_profile,
    }
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


def split_label_stats(base_with_split):
    return {
        row["split"]: {
            "row_count": int(row["row_count"] or 0),
            "positive_rows": int(row["positive_rows"] or 0),
            "negative_rows": int(row["negative_rows"] or 0),
            "positive_rate": float(row["positive_rate"] or 0.0),
        }
        for row in (
            base_with_split.groupBy("split")
            .agg(
                F.count("*").alias("row_count"),
                F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
                F.sum(
                    F.when(F.col(LABEL_COL) <= F.lit(0), F.lit(1)).otherwise(
                        F.lit(0)
                    )
                ).alias("negative_rows"),
                F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
            )
            .collect()
        )
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


def validate_split_label_stats(stats: dict[str, dict[str, int]]):
    invalid = []
    for split_name in ["train", "validation", "test"]:
        split_stats = stats.get(split_name, {})
        row_count = int(split_stats.get("row_count", 0))
        positive_rows = int(split_stats.get("positive_rows", 0))
        negative_rows = row_count - positive_rows
        if positive_rows <= 0:
            invalid.append(f"{split_name} has no positive labels")
        if negative_rows <= 0:
            invalid.append(f"{split_name} has no negative labels")

    if invalid:
        raise ValueError(
            "Theme Affinity training split label quality failed: "
            + "; ".join(invalid)
        )


def validate_training_frame_stats(stats: dict[str, int | float]):
    row_count = int(stats.get("training_frame_row_count", 0) or 0)
    positive_rows = int(stats.get("training_frame_positive_rows", 0) or 0)
    negative_rows = row_count - positive_rows

    if row_count <= 0:
        raise ValueError("Theme Affinity training frame has no rows")
    if positive_rows <= 0:
        raise ValueError(
            "Theme Affinity training frame has no positive labels. "
            "Check that the training input table is a labelled historical "
            "training set, not an unlabeled prediction/scoring table."
        )
    if negative_rows <= 0:
        raise ValueError(
            "Theme Affinity training frame has no negative labels. "
            "Training requires both positive and negative candidates."
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


def frame_profile(df, frame_config: TrainingFrameConfig, group_keys: list[str], name: str):
    row = df.agg(
        F.count("*").alias("row_count"),
        F.countDistinct(*group_keys).alias("group_count"),
        F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
        F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
    ).first()
    profile = {
        "name": name,
        "row_count": int(row["row_count"] or 0) if row else 0,
        "group_count": int(row["group_count"] or 0) if row else 0,
        "positive_rows": int(row["positive_rows"] or 0) if row else 0,
        "positive_rate": float(row["positive_rate"] or 0.0) if row else 0.0,
        "distributions": {},
    }
    distribution_cols = [
        LABEL_BUCKET_COL,
        RANK_BAND_COL,
        "repurchase_stage",
        "GmaName",
        ACTIVITY_BUCKET_COL,
        RETRIEVAL_BUCKET_COL,
    ]
    for column in distribution_cols:
        if column not in df.columns:
            continue
        profile["distributions"][column] = _top_distribution(
            df,
            column,
            frame_config.profile_top_n,
        )
    return profile


def _build_group_metadata(df, frame_config: TrainingFrameConfig, group_keys: list[str]):
    aggregations = [
        F.max(F.col(LABEL_COL).cast("double")).alias("group_has_positive"),
    ]
    if "repurchase_stage" in df.columns:
        aggregations.append(
            F.first("repurchase_stage", ignorenulls=True).alias("repurchase_stage")
        )
    if "GmaName" in df.columns:
        aggregations.append(F.first("GmaName", ignorenulls=True).alias("GmaName"))
    if "user_total_views" in df.columns:
        aggregations.append(
            F.avg(F.col("user_total_views").cast("double")).alias("user_total_views")
        )
    if "num_retrieval_methods" in df.columns:
        aggregations.append(
            F.avg(F.col("num_retrieval_methods").cast("double")).alias(
                "num_retrieval_methods"
            )
        )

    metadata = df.groupBy(*group_keys).agg(*aggregations)
    metadata = metadata.withColumn(
        LABEL_BUCKET_COL,
        F.when(F.col("group_has_positive") > F.lit(0), F.lit("positive")).otherwise(
            F.lit("normal")
        ),
    )
    metadata = _with_ntile_bucket(
        metadata,
        "user_total_views",
        ACTIVITY_BUCKET_COL,
        frame_config.activity_bucket_count,
    )
    metadata = _with_ntile_bucket(
        metadata,
        "num_retrieval_methods",
        RETRIEVAL_BUCKET_COL,
        frame_config.retrieval_bucket_count,
    )
    return _with_group_stratum(metadata, frame_config)


def _sample_groups(group_metadata, frame_config: TrainingFrameConfig, group_keys):
    total_groups = group_metadata.count()
    if total_groups <= 0:
        return group_metadata, {
            "total_groups": 0,
            "base_fraction": 0.0,
            "fractions": {},
        }

    base_fraction = min(1.0, frame_config.max_accounts / total_groups)
    stratum_counts = {
        row[GROUP_STRATUM_COL]: int(row["count"] or 0)
        for row in group_metadata.groupBy(GROUP_STRATUM_COL).count().collect()
    }
    fractions = {}
    for stratum in stratum_counts:
        fraction = base_fraction
        if stratum.startswith("positive|"):
            fraction = max(fraction, frame_config.min_positive_group_fraction)
        fractions[stratum] = min(1.0, fraction)

    sampled = group_metadata.sampleBy(
        GROUP_STRATUM_COL,
        fractions=fractions,
        seed=frame_config.random_seed,
    )
    sampled_count = sampled.count()
    if sampled_count > frame_config.max_accounts:
        sampled = _cap_groups(sampled, frame_config, group_keys)
        sampled_count = sampled.count()

    return sampled, {
        "total_groups": total_groups,
        "sampled_groups": sampled_count,
        "base_fraction": base_fraction,
        "fractions": fractions,
        "stratum_counts": stratum_counts,
    }


def _sample_candidates(df, frame_config: TrainingFrameConfig, group_keys: list[str]):
    df = _with_candidate_stratum(df, frame_config)
    positives = df.filter(F.col(LABEL_COL) > F.lit(0))
    negatives = df.filter(F.col(LABEL_COL) <= F.lit(0))
    candidate_fractions = _candidate_sample_fractions(
        negatives,
        frame_config.candidate_rank_band_fractions
        or DEFAULT_CANDIDATE_RANK_BAND_FRACTIONS,
    )
    sampled_negatives = negatives.sampleBy(
        CANDIDATE_STRATUM_COL,
        fractions=candidate_fractions,
        seed=frame_config.random_seed,
    )
    sampled = positives.unionByName(sampled_negatives)
    candidate_window = Window.partitionBy(*group_keys).orderBy(
        F.when(F.col(LABEL_COL) > F.lit(0), F.lit(0)).otherwise(F.lit(1)).asc(),
        _stable_hash_expr(_candidate_hash_columns(sampled), frame_config.random_seed),
    )
    return (
        sampled.withColumn("candidate_sample_row", F.row_number().over(candidate_window))
        .filter(
            F.col("candidate_sample_row")
            <= F.lit(frame_config.max_candidates_per_account)
        )
        .drop("candidate_sample_row", CANDIDATE_STRATUM_COL)
    )


def _candidate_sample_fractions(negative_df, rank_band_fractions: dict[str, float]):
    rows = (
        negative_df.select(CANDIDATE_STRATUM_COL, RANK_BAND_COL)
        .dropDuplicates()
        .collect()
    )
    return {
        row[CANDIDATE_STRATUM_COL]: float(
            rank_band_fractions.get(row[RANK_BAND_COL], 0.5)
        )
        for row in rows
    }


def _with_group_stratum(df, frame_config: TrainingFrameConfig):
    parts = []
    for column in frame_config.sample_strata_columns:
        if column in df.columns:
            parts.append(F.coalesce(F.col(column).cast("string"), F.lit("missing")))
    if not parts:
        parts.append(F.lit("all"))
    return df.withColumn(GROUP_STRATUM_COL, F.concat_ws("|", *parts))


def _with_candidate_stratum(df, frame_config: TrainingFrameConfig):
    df = _with_group_stratum(df, frame_config)
    return df.withColumn(
        CANDIDATE_STRATUM_COL,
        F.concat_ws(
            "|",
            F.col(GROUP_STRATUM_COL),
            F.coalesce(F.col(RANK_BAND_COL).cast("string"), F.lit("rank_missing")),
        ),
    )


def _with_rank_band(df, frame_config: TrainingFrameConfig):
    if RANK_COL not in df.columns:
        return df.withColumn(RANK_BAND_COL, F.lit("rank_missing"))

    edges = sorted(frame_config.rank_band_edges or DEFAULT_RANK_BAND_EDGES)
    expr = F.when(F.col(RANK_COL).isNull(), F.lit("rank_missing"))
    lower = 1
    for edge in edges:
        expr = expr.when(
            F.col(RANK_COL) <= F.lit(edge),
            F.lit(f"rank_{lower:03d}_{edge:03d}"),
        )
        lower = edge + 1
    return df.withColumn(RANK_BAND_COL, expr.otherwise(F.lit(f"rank_gt_{edges[-1]}")))


def _with_ntile_bucket(df, source_col: str, bucket_col: str, bucket_count: int):
    if source_col not in df.columns:
        return df.withColumn(bucket_col, F.lit("missing"))
    bucket_window = Window.orderBy(F.col(source_col).asc_nulls_first())
    return df.withColumn(
        bucket_col,
        F.when(F.col(source_col).isNull(), F.lit("missing")).otherwise(
            F.concat(
                F.lit("q"),
                F.ntile(max(1, bucket_count)).over(bucket_window).cast("string"),
            )
        ),
    )


def _cap_groups(sampled_groups, frame_config: TrainingFrameConfig, group_keys: list[str]):
    cap_window = Window.orderBy(
        _stable_hash_expr(group_keys, frame_config.random_seed),
    )
    return (
        sampled_groups.withColumn("group_sample_row", F.row_number().over(cap_window))
        .filter(F.col("group_sample_row") <= F.lit(frame_config.max_accounts))
        .drop("group_sample_row")
    )


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


def _group_key_columns(df) -> list[str]:
    columns = [ACCOUNT_COL]
    if REFERENCE_DATE_COL in df.columns:
        columns.append(REFERENCE_DATE_COL)
    return columns


def _candidate_hash_columns(df) -> list[str]:
    columns = [ACCOUNT_COL]
    if REFERENCE_DATE_COL in df.columns:
        columns.append(REFERENCE_DATE_COL)
    for theme_col in THEME_COLS:
        if theme_col in df.columns:
            columns.append(theme_col)
            break
    if RANK_COL in df.columns:
        columns.append(RANK_COL)
    return columns


def _stable_hash_expr(columns: list[str], seed: int):
    return F.pmod(
        F.xxhash64(*[F.col(column) for column in columns], F.lit(seed)),
        F.lit(9223372036854775807),
    ).asc()


def _top_distribution(df, column: str, limit: int):
    return [
        {
            "value": str(row[column]) if row[column] is not None else "null",
            "count": int(row["count"] or 0),
        }
        for row in (
            df.groupBy(column)
            .count()
            .orderBy(F.col("count").desc(), F.col(column).asc_nulls_last())
            .limit(limit)
            .collect()
        )
    ]


def _validate_training_frame_config(frame_config: TrainingFrameConfig):
    if frame_config.max_accounts <= 0:
        raise ValueError("training_frame.max_accounts must be greater than 0")
    if frame_config.max_candidates_per_account <= 0:
        raise ValueError(
            "training_frame.max_candidates_per_account must be greater than 0"
        )
    if frame_config.max_rows <= 0:
        raise ValueError("training_frame.max_rows must be greater than 0")
    if not 0 < frame_config.min_positive_group_fraction <= 1:
        raise ValueError(
            "training_frame.min_positive_group_fraction must be in the range (0, 1]"
        )
    for band, fraction in (
        frame_config.candidate_rank_band_fractions
        or DEFAULT_CANDIDATE_RANK_BAND_FRACTIONS
    ).items():
        if not 0 <= float(fraction) <= 1:
            raise ValueError(
                "training_frame.candidate_rank_band_fractions values must be "
                f"in the range [0, 1]; {band}={fraction}"
            )
