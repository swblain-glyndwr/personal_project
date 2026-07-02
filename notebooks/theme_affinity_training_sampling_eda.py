# Databricks notebook source
# MAGIC %md
# MAGIC # Theme Affinity Training Sampling EDA
# MAGIC
# MAGIC Use this notebook before changing Theme Affinity training sampling
# MAGIC fractions. Edit the constants in the first code cell, run all, then send
# MAGIC back the final JSON report.
# MAGIC
# MAGIC This notebook is intentionally standalone. It does not import repo package
# MAGIC code and it does not write production tables.

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F


INPUT_TABLE = (
    "marketingdata_dev.nextads_feature_store."
    "next_uk_nextads_fs_theme_affinity_model_input"
)
OUTPUT_PATH = ""

RANK_BAND_EDGES = [20, 100, 256]
MAX_ACCOUNTS = 50000
MAX_CANDIDATES_PER_GROUP = 256
SAMPLE_SEED = 42
MIN_POSITIVE_GROUP_FRACTION = 1.0
ACTIVITY_BUCKET_COUNT = 4
RETRIEVAL_BUCKET_COUNT = 4
MAX_DISTRIBUTION_VALUES = 50
MAX_REFERENCE_DATES = 40
CANDIDATE_RANK_BAND_FRACTIONS = {
    "rank_001_020": 1.0,
    "rank_021_100": 0.75,
    "rank_101_256": 0.5,
    "rank_gt_256": 0.25,
    "rank_missing": 0.5,
}

print(f"Profiling {INPUT_TABLE}")

# COMMAND ----------

ACCOUNT_COL = "account_number"
REFERENCE_DATE_COL = "reference_date"
LABEL_COL = "label"
RANK_COL = "simple_rules_rank"
RANK_BAND_COL = "simple_rules_rank_band"
LABEL_BUCKET_COL = "label_bucket"
ACTIVITY_BUCKET_COL = "user_total_views_bucket"
RETRIEVAL_BUCKET_COL = "num_retrieval_methods_bucket"
GROUP_STRATUM_COL = "sample_group_stratum"
CANDIDATE_STRATUM_COL = "candidate_stratum"


df = spark.table(INPUT_TABLE)
display(df.limit(20))

# COMMAND ----------

def group_key_columns(frame) -> list[str]:
    columns = [ACCOUNT_COL]
    if REFERENCE_DATE_COL in frame.columns:
        columns.append(REFERENCE_DATE_COL)
    return columns


def with_rank_band(frame):
    if RANK_COL not in frame.columns:
        return frame.withColumn(RANK_BAND_COL, F.lit("rank_missing"))

    edges = sorted(RANK_BAND_EDGES)
    expr = F.when(F.col(RANK_COL).isNull(), F.lit("rank_missing"))
    lower = 1
    for edge in edges:
        expr = expr.when(
            F.col(RANK_COL) <= F.lit(edge),
            F.lit(f"rank_{lower:03d}_{edge:03d}"),
        )
        lower = edge + 1
    return frame.withColumn(
        RANK_BAND_COL,
        expr.otherwise(F.lit(f"rank_gt_{edges[-1]}")),
    )


def with_ntile_bucket(frame, source_col: str, bucket_col: str, bucket_count: int):
    if source_col not in frame.columns:
        return frame.withColumn(bucket_col, F.lit("missing"))
    window = Window.orderBy(F.col(source_col).asc_nulls_first())
    return frame.withColumn(
        bucket_col,
        F.when(F.col(source_col).isNull(), F.lit("missing")).otherwise(
            F.concat(F.lit("q"), F.ntile(bucket_count).over(window).cast("string"))
        ),
    )


def with_eda_buckets(frame):
    frame = with_rank_band(frame)
    frame = frame.withColumn(
        LABEL_BUCKET_COL,
        F.when(F.col(LABEL_COL) > F.lit(0), F.lit("positive")).otherwise(
            F.lit("normal")
        ),
    )
    frame = with_ntile_bucket(
        frame,
        "user_total_views",
        ACTIVITY_BUCKET_COL,
        ACTIVITY_BUCKET_COUNT,
    )
    return with_ntile_bucket(
        frame,
        "num_retrieval_methods",
        RETRIEVAL_BUCKET_COL,
        RETRIEVAL_BUCKET_COUNT,
    )


def distribution_with_positive_rate(frame, column: str, limit: int | None = None):
    if column not in frame.columns:
        return []
    profiled = (
        frame.groupBy(column)
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct(ACCOUNT_COL).alias("account_count"),
            F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
            F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
        )
        .orderBy(F.col("row_count").desc(), F.col(column).asc_nulls_last())
    )
    if limit:
        profiled = profiled.limit(limit)
    return profiled


def collect_rows(frame):
    return [row.asDict(recursive=True) for row in frame.collect()]


profile_df = with_eda_buckets(df)
GROUP_KEYS = group_key_columns(profile_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic trainability checks

# COMMAND ----------

summary_exprs = [
    F.count("*").alias("row_count"),
    F.countDistinct(ACCOUNT_COL).alias("account_count"),
    F.countDistinct(*GROUP_KEYS).alias("ranking_group_count"),
    F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
    F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
]
if REFERENCE_DATE_COL in profile_df.columns:
    summary_exprs.extend(
        [
            F.countDistinct(REFERENCE_DATE_COL).alias("reference_date_count"),
            F.min(REFERENCE_DATE_COL).alias("min_reference_date"),
            F.max(REFERENCE_DATE_COL).alias("max_reference_date"),
        ]
    )

summary_df = profile_df.agg(*summary_exprs)
display(summary_df)

# COMMAND ----------

if REFERENCE_DATE_COL in profile_df.columns:
    reference_date_df = (
        profile_df.groupBy(REFERENCE_DATE_COL)
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct(ACCOUNT_COL).alias("account_count"),
            F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
            F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
        )
        .orderBy(F.col(REFERENCE_DATE_COL).desc())
        .limit(MAX_REFERENCE_DATES)
    )
    display(reference_date_df)
else:
    reference_date_df = None
    print("No reference_date column found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Population distributions

# COMMAND ----------

rank_band_df = distribution_with_positive_rate(profile_df, RANK_BAND_COL)
display(rank_band_df)

positive_rank_band_df = (
    profile_df.where(F.col(LABEL_COL) > F.lit(0))
    .groupBy(RANK_BAND_COL)
    .count()
    .orderBy(RANK_BAND_COL)
)
display(positive_rank_band_df)

# COMMAND ----------

for column in [
    "repurchase_stage",
    "GmaName",
    ACTIVITY_BUCKET_COL,
    RETRIEVAL_BUCKET_COL,
]:
    if column in profile_df.columns:
        print(f"Distribution for {column}")
        display(distribution_with_positive_rate(profile_df, column, MAX_DISTRIBUTION_VALUES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Numeric shape

# COMMAND ----------

quantile_columns = [
    "user_total_views",
    "num_retrieval_methods",
    "simple_rules_rank",
    "model_score",
]
quantiles = {}
for column in quantile_columns:
    if column in profile_df.columns:
        values = profile_df.approxQuantile(
            column,
            [0.0, 0.25, 0.5, 0.75, 0.9, 0.99],
            0.01,
        )
        quantiles[column] = {
            "min": values[0] if len(values) > 0 else None,
            "p25": values[1] if len(values) > 1 else None,
            "p50": values[2] if len(values) > 2 else None,
            "p75": values[3] if len(values) > 3 else None,
            "p90": values[4] if len(values) > 4 else None,
            "p99": values[5] if len(values) > 5 else None,
        }

display(spark.createDataFrame([(key, json.dumps(value)) for key, value in quantiles.items()], ["feature", "quantiles"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simulate representative sampling
# MAGIC
# MAGIC This samples account/reference-date ranking groups first, then keeps all
# MAGIC positives for selected groups and samples normal negative candidates across
# MAGIC configured rank bands. This mirrors the intended modelling logic without
# MAGIC fitting a model.

# COMMAND ----------

def with_group_stratum(frame):
    parts = [
        F.coalesce(F.col(LABEL_BUCKET_COL).cast("string"), F.lit("missing")),
    ]
    for column in [
        "repurchase_stage",
        "GmaName",
        ACTIVITY_BUCKET_COL,
        RETRIEVAL_BUCKET_COL,
    ]:
        if column in frame.columns:
            parts.append(F.coalesce(F.col(column).cast("string"), F.lit("missing")))
    return frame.withColumn(GROUP_STRATUM_COL, F.concat_ws("|", *parts))


def stable_hash_expr(columns: list[str]):
    return F.pmod(
        F.xxhash64(*[F.col(column) for column in columns], F.lit(SAMPLE_SEED)),
        F.lit(9223372036854775807),
    ).asc()


group_aggs = [
    F.max(F.col(LABEL_COL).cast("double")).alias("group_has_positive"),
]
for column in ["repurchase_stage", "GmaName"]:
    if column in profile_df.columns:
        group_aggs.append(F.first(column, ignorenulls=True).alias(column))
for column in ["user_total_views", "num_retrieval_methods"]:
    if column in profile_df.columns:
        group_aggs.append(F.avg(F.col(column).cast("double")).alias(column))

group_metadata = profile_df.groupBy(*GROUP_KEYS).agg(*group_aggs)
group_metadata = group_metadata.withColumn(
    LABEL_BUCKET_COL,
    F.when(F.col("group_has_positive") > F.lit(0), F.lit("positive")).otherwise(
        F.lit("normal")
    ),
)
group_metadata = with_ntile_bucket(
    group_metadata,
    "user_total_views",
    ACTIVITY_BUCKET_COL,
    ACTIVITY_BUCKET_COUNT,
)
group_metadata = with_ntile_bucket(
    group_metadata,
    "num_retrieval_methods",
    RETRIEVAL_BUCKET_COL,
    RETRIEVAL_BUCKET_COUNT,
)
group_metadata = with_group_stratum(group_metadata)

total_groups = group_metadata.count()
base_fraction = min(1.0, MAX_ACCOUNTS / total_groups) if total_groups else 0.0
stratum_counts = {
    row[GROUP_STRATUM_COL]: int(row["count"] or 0)
    for row in group_metadata.groupBy(GROUP_STRATUM_COL).count().collect()
}
group_fractions = {}
for stratum in stratum_counts:
    fraction = base_fraction
    if stratum.startswith("positive|"):
        fraction = max(fraction, MIN_POSITIVE_GROUP_FRACTION)
    group_fractions[stratum] = min(1.0, fraction)

sampled_groups = group_metadata.sampleBy(
    GROUP_STRATUM_COL,
    fractions=group_fractions,
    seed=SAMPLE_SEED,
)
if sampled_groups.count() > MAX_ACCOUNTS:
    cap_window = Window.orderBy(stable_hash_expr(GROUP_KEYS))
    sampled_groups = (
        sampled_groups.withColumn("group_sample_row", F.row_number().over(cap_window))
        .where(F.col("group_sample_row") <= F.lit(MAX_ACCOUNTS))
        .drop("group_sample_row")
    )

display(sampled_groups.groupBy(GROUP_STRATUM_COL).count().orderBy(F.col("count").desc()))

# COMMAND ----------

selected = profile_df.join(sampled_groups.select(*GROUP_KEYS), GROUP_KEYS)
selected = with_group_stratum(selected).withColumn(
    CANDIDATE_STRATUM_COL,
    F.concat_ws(
        "|",
        F.col(GROUP_STRATUM_COL),
        F.coalesce(F.col(RANK_BAND_COL), F.lit("rank_missing")),
    ),
)

positives = selected.where(F.col(LABEL_COL) > F.lit(0))
negatives = selected.where(F.col(LABEL_COL) <= F.lit(0))
candidate_fractions = {
    row[CANDIDATE_STRATUM_COL]: CANDIDATE_RANK_BAND_FRACTIONS.get(
        row[RANK_BAND_COL],
        0.5,
    )
    for row in negatives.select(CANDIDATE_STRATUM_COL, RANK_BAND_COL)
    .dropDuplicates()
    .collect()
}
sampled_negatives = negatives.sampleBy(
    CANDIDATE_STRATUM_COL,
    candidate_fractions,
    SAMPLE_SEED,
)
sampled_candidates = positives.unionByName(sampled_negatives)

theme_col = "theme" if "theme" in sampled_candidates.columns else "theme_clean"
candidate_hash_cols = [*GROUP_KEYS]
if theme_col in sampled_candidates.columns:
    candidate_hash_cols.append(theme_col)
if RANK_COL in sampled_candidates.columns:
    candidate_hash_cols.append(RANK_COL)

candidate_window = Window.partitionBy(*GROUP_KEYS).orderBy(
    F.when(F.col(LABEL_COL) > F.lit(0), F.lit(0)).otherwise(F.lit(1)).asc(),
    stable_hash_expr(candidate_hash_cols),
)
sampled_candidates = (
    sampled_candidates.withColumn(
        "candidate_sample_row",
        F.row_number().over(candidate_window),
    )
    .where(F.col("candidate_sample_row") <= F.lit(MAX_CANDIDATES_PER_GROUP))
    .drop("candidate_sample_row")
)

sample_summary_df = sampled_candidates.agg(
    F.count("*").alias("sampled_row_count"),
    F.countDistinct(ACCOUNT_COL).alias("sampled_account_count"),
    F.countDistinct(*GROUP_KEYS).alias("sampled_ranking_group_count"),
    F.sum(F.col(LABEL_COL).cast("double")).alias("sampled_positive_rows"),
    F.avg(F.col(LABEL_COL).cast("double")).alias("sampled_positive_rate"),
)
display(sample_summary_df)
display(distribution_with_positive_rate(sampled_candidates, RANK_BAND_COL))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Train/validation/test split label coverage

# COMMAND ----------

split_metadata = (
    sampled_candidates.groupBy(*GROUP_KEYS)
    .agg(F.max(F.col(LABEL_COL).cast("double")).alias("stratify_label"))
    .withColumn("rand", F.rand(seed=SAMPLE_SEED))
)
stratify_window = Window.partitionBy("stratify_label").orderBy("rand")
total_window = Window.partitionBy("stratify_label")
split_metadata = (
    split_metadata.withColumn("row_num", F.row_number().over(stratify_window))
    .withColumn("total_in_class", F.count("*").over(total_window))
    .withColumn(
        "split",
        F.when(F.col("row_num") <= F.col("total_in_class") * F.lit(0.75), "train")
        .when(F.col("row_num") <= F.col("total_in_class") * F.lit(0.90), "test")
        .otherwise("validation"),
    )
)
sampled_with_split = sampled_candidates.join(
    split_metadata.select(*GROUP_KEYS, "split"),
    GROUP_KEYS,
)
split_label_df = (
    sampled_with_split.groupBy("split")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.col(LABEL_COL).cast("double")).alias("positive_rows"),
        F.sum(
            F.when(F.col(LABEL_COL) <= F.lit(0), F.lit(1)).otherwise(F.lit(0))
        ).alias("negative_rows"),
        F.avg(F.col(LABEL_COL).cast("double")).alias("positive_rate"),
    )
    .orderBy("split")
)
display(split_label_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. JSON report to send back

# COMMAND ----------

summary = summary_df.first().asDict(recursive=True)
sample_summary = sample_summary_df.first().asDict(recursive=True)
report = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "input_table": INPUT_TABLE,
    "widgets": {
        "rank_band_edges": RANK_BAND_EDGES,
        "max_accounts": MAX_ACCOUNTS,
        "max_candidates_per_group": MAX_CANDIDATES_PER_GROUP,
        "sample_seed": SAMPLE_SEED,
        "min_positive_group_fraction": MIN_POSITIVE_GROUP_FRACTION,
        "activity_bucket_count": ACTIVITY_BUCKET_COUNT,
        "retrieval_bucket_count": RETRIEVAL_BUCKET_COUNT,
        "candidate_rank_band_fractions": CANDIDATE_RANK_BAND_FRACTIONS,
    },
    "population_summary": summary,
    "reference_dates": collect_rows(reference_date_df) if reference_date_df else [],
    "rank_band_distribution": collect_rows(rank_band_df),
    "positive_rank_band_distribution": collect_rows(positive_rank_band_df),
    "quantiles": quantiles,
    "group_sampling": {
        "total_groups": total_groups,
        "base_fraction": base_fraction,
        "stratum_counts": stratum_counts,
        "group_fractions": group_fractions,
    },
    "candidate_sampling": {
        "candidate_rank_band_fractions": CANDIDATE_RANK_BAND_FRACTIONS,
        "resolved_candidate_stratum_count": len(candidate_fractions),
    },
    "sample_summary": sample_summary,
    "split_label_stats": collect_rows(split_label_df),
    "review_notes": [
        "Do not train if population_positive_rows or any split positive_rows is zero.",
        "Use positive_rank_band_distribution to decide whether rank bands need widening.",
        "Use sample_summary versus population_summary to tune max_accounts and candidate fractions.",
        "Use strata displays above to check whether normal low/high activity behaviour is retained.",
    ],
}
report_json = json.dumps(report, indent=2, sort_keys=True, default=str)

if OUTPUT_PATH:
    dbutils.fs.put(OUTPUT_PATH, report_json, True)
    print(f"Wrote report to {OUTPUT_PATH}")
else:
    print("THEME_AFFINITY_TRAINING_EDA_JSON_START")
    print(report_json)
    print("THEME_AFFINITY_TRAINING_EDA_JSON_END")
