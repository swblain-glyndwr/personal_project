from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from next_ads.ranking.theme_affinity.config import ThemeAffinityRuntime


INTERMEDIATE_TABLE_SUFFIXES = [
    "product_catalog",
    "atbs",
    "baskets",
    "baskets_ly",
    "views",
    "views_ly",
    "atbs_themes",
    "baskets_themes",
    "views_themes",
    "vatb",
    "advanced_features",
    "atbs_bytheme",
    "baskets_bytheme",
    "views_bytheme",
    "repurchase",
    "baskets_target",
    "algo_atbs1",
    "algo_atbs5",
    "algo_baskets1",
    "algo_baskets5",
    "algo_views1",
    "algo_views5",
    "customer_features",
    "customer_segments",
    "popularity_metrics",
    "master",
    "complete",
    "ranked",
    "half",
]
DATA_TABLE_SUFFIXES = [
    suffix for suffix in INTERMEDIATE_TABLE_SUFFIXES if suffix != "half"
]
MODEL_OUTPUT_TABLE_SUFFIXES = ["half"]
VALID_CHECK_SCOPES = {"all", "data", "model_outputs"}
ROW_RATIO_WARN_MIN = 0.95
ROW_RATIO_WARN_MAX = 1.05

FINAL_JOIN_KEYS = ["AccountNumber", "NextTheme"]
FINAL_SCORE_COL = "ProbAggRebased"
SUMMARY_SCHEMA = (
    "checked_at STRING, "
    "check_name STRING, "
    "candidate_table STRING, "
    "baseline_table STRING, "
    "candidate_filter STRING, "
    "baseline_filter STRING, "
    "candidate_rows LONG, "
    "baseline_rows LONG, "
    "candidate_distinct_accounts LONG, "
    "baseline_distinct_accounts LONG, "
    "joined_rows LONG, "
    "row_ratio DOUBLE, "
    "match_rate DOUBLE, "
    "avg_abs_score_delta DOUBLE, "
    "max_abs_score_delta DOUBLE, "
    "missing_columns STRING, "
    "extra_columns STRING, "
    "status STRING, "
    "notes STRING"
)


@dataclass(frozen=True)
class SenseCheckConfig:
    baseline_intermediate_namespace: str
    baseline_intermediate_prefix: str
    baseline_final_table: str
    summary_table: str
    check_scope: str = "all"


def default_summary_table(runtime: ThemeAffinityRuntime) -> str:
    return (
        f"{runtime.namespace}."
        f"{runtime.client}_nextads_theme_affinity_sense_check_summary"
    )


def run_sense_checks(spark, runtime: ThemeAffinityRuntime, config):
    rows = []
    checked_at = datetime.utcnow().isoformat(timespec="seconds")
    candidate_prefix = runtime.config.ranking_model_table_prefix
    check_scope = _normalise_check_scope(config.check_scope)

    if check_scope in ("all", "model_outputs"):
        rows.extend(
            _final_output_rows(
                spark=spark,
                checked_at=checked_at,
                candidate_table=runtime.config.ranking_model_tables.model_full,
                baseline_table=config.baseline_final_table,
            )
        )

    table_suffixes = []
    if check_scope in ("all", "data"):
        table_suffixes.extend(DATA_TABLE_SUFFIXES)
    if check_scope in ("all", "model_outputs"):
        table_suffixes.extend(MODEL_OUTPUT_TABLE_SUFFIXES)

    for suffix in table_suffixes:
        candidate_table = f"{runtime.namespace}.{candidate_prefix}_{suffix}"
        baseline_table = (
            f"{config.baseline_intermediate_namespace}."
            f"{config.baseline_intermediate_prefix}_{suffix}"
        )
        if suffix == "product_catalog":
            baseline_table = (
                "marketingdata_prod.warehouse.product_catalog_history"
            )

        rows.append(
            _table_sense_row(
                spark=spark,
                checked_at=checked_at,
                check_name=f"intermediate_{suffix}",
                candidate_table=candidate_table,
                baseline_table=baseline_table,
            )
        )

    summary_df = spark.createDataFrame(rows, schema=SUMMARY_SCHEMA)
    (
        summary_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config.summary_table)
    )
    return summary_df


def _normalise_check_scope(check_scope: str) -> str:
    normalised = (check_scope or "all").strip().lower()
    if normalised not in VALID_CHECK_SCOPES:
        raise ValueError(
            "check_scope must be one of: " + ", ".join(sorted(VALID_CHECK_SCOPES))
        )
    return normalised


def _final_output_rows(spark, checked_at, candidate_table, baseline_table):
    rows = [
        _table_sense_row(
            spark=spark,
            checked_at=checked_at,
            check_name="final_output",
            candidate_table=candidate_table,
            baseline_table=baseline_table,
        )
    ]
    if not (
        _table_exists(spark, candidate_table)
        and _table_exists(spark, baseline_table)
    ):
        return rows

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    candidate_df, candidate_filter = _latest_comparable_df(
        spark.table(candidate_table)
    )
    baseline_df, baseline_filter = _latest_comparable_df(
        spark.table(baseline_table),
        preferred_filter=candidate_filter,
    )

    joined = candidate_df.alias("candidate").join(
        baseline_df.alias("baseline"),
        FINAL_JOIN_KEYS,
        "inner",
    )
    joined_count = joined.count()
    candidate_count = candidate_df.count()
    baseline_count = baseline_df.count()
    candidate_accounts = _distinct_count(candidate_df, "AccountNumber")
    baseline_accounts = _distinct_count(baseline_df, "AccountNumber")

    score_delta = joined.select(
        F.abs(
            F.col(f"candidate.{FINAL_SCORE_COL}")
            - F.col(f"baseline.{FINAL_SCORE_COL}")
        ).alias("score_delta")
    )
    score_stats = _score_stats(score_delta)
    rows.append(
        {
            "checked_at": checked_at,
            "check_name": "final_joined_pair_overlap",
            "candidate_table": candidate_table,
            "baseline_table": baseline_table,
            "candidate_filter": candidate_filter,
            "baseline_filter": baseline_filter,
            "candidate_rows": candidate_count,
            "baseline_rows": baseline_count,
            "candidate_distinct_accounts": candidate_accounts,
            "baseline_distinct_accounts": baseline_accounts,
            "joined_rows": joined_count,
            "row_ratio": _ratio(candidate_count, baseline_count),
            "match_rate": _ratio(joined_count, candidate_count),
            "avg_abs_score_delta": score_stats["avg_abs_score_delta"],
            "max_abs_score_delta": score_stats["max_abs_score_delta"],
            "missing_columns": "",
            "extra_columns": "",
            "status": "OK",
            "notes": "Joined on AccountNumber and NextTheme.",
        }
    )

    top_window = Window.partitionBy("AccountNumber").orderBy(
        F.col(FINAL_SCORE_COL).desc(),
        F.col("NextTheme"),
    )
    candidate_top = (
        candidate_df.withColumn("top_rank", F.row_number().over(top_window))
        .filter(F.col("top_rank") == 1)
        .select(
            "AccountNumber",
            F.col("NextTheme").alias("candidate_top_theme"),
        )
    )
    baseline_top = (
        baseline_df.withColumn("top_rank", F.row_number().over(top_window))
        .filter(F.col("top_rank") == 1)
        .select(
            "AccountNumber",
            F.col("NextTheme").alias("baseline_top_theme"),
        )
    )
    top_joined = candidate_top.join(baseline_top, "AccountNumber", "inner")
    top_joined_count = top_joined.count()
    top_matches = top_joined.filter(
        F.col("candidate_top_theme") == F.col("baseline_top_theme")
    ).count()
    rows.append(
        {
            "checked_at": checked_at,
            "check_name": "final_top_theme_match",
            "candidate_table": candidate_table,
            "baseline_table": baseline_table,
            "candidate_filter": candidate_filter,
            "baseline_filter": baseline_filter,
            "candidate_rows": candidate_top.count(),
            "baseline_rows": baseline_top.count(),
            "candidate_distinct_accounts": candidate_accounts,
            "baseline_distinct_accounts": baseline_accounts,
            "joined_rows": top_joined_count,
            "row_ratio": _ratio(candidate_count, baseline_count),
            "match_rate": _ratio(top_matches, top_joined_count),
            "avg_abs_score_delta": None,
            "max_abs_score_delta": None,
            "missing_columns": "",
            "extra_columns": "",
            "status": "OK",
            "notes": "Compares top ProbAggRebased theme per account.",
        }
    )
    return rows


def _table_sense_row(
    spark,
    checked_at: str,
    check_name: str,
    candidate_table: str,
    baseline_table: str,
):
    candidate_exists = _table_exists(spark, candidate_table)
    baseline_exists = _table_exists(spark, baseline_table)
    if not candidate_exists or not baseline_exists:
        return _empty_row(
            checked_at,
            check_name,
            candidate_table,
            baseline_table,
            "MISSING_TABLE",
            _missing_table_note(candidate_exists, baseline_exists),
        )

    candidate_df, candidate_filter = _latest_comparable_df(
        spark.table(candidate_table)
    )
    baseline_df, baseline_filter = _latest_comparable_df(
        spark.table(baseline_table),
        preferred_filter=candidate_filter,
    )
    candidate_columns = set(candidate_df.columns)
    baseline_columns = set(baseline_df.columns)
    missing_columns = sorted(baseline_columns - candidate_columns)
    extra_columns = sorted(candidate_columns - baseline_columns)
    candidate_rows = candidate_df.count()
    baseline_rows = baseline_df.count()
    row_ratio = _ratio(candidate_rows, baseline_rows)
    status, notes = _table_status(
        candidate_rows=candidate_rows,
        baseline_rows=baseline_rows,
        row_ratio=row_ratio,
        missing_columns=missing_columns,
        extra_columns=extra_columns,
    )

    return {
        "checked_at": checked_at,
        "check_name": check_name,
        "candidate_table": candidate_table,
        "baseline_table": baseline_table,
        "candidate_filter": candidate_filter,
        "baseline_filter": baseline_filter,
        "candidate_rows": candidate_rows,
        "baseline_rows": baseline_rows,
        "candidate_distinct_accounts": _distinct_count(
            candidate_df,
            _first_present(candidate_df, ["account_number", "AccountNumber"]),
        ),
        "baseline_distinct_accounts": _distinct_count(
            baseline_df,
            _first_present(baseline_df, ["account_number", "AccountNumber"]),
        ),
        "joined_rows": None,
        "row_ratio": row_ratio,
        "match_rate": None,
        "avg_abs_score_delta": None,
        "max_abs_score_delta": None,
        "missing_columns": ",".join(missing_columns),
        "extra_columns": ",".join(extra_columns),
        "status": status,
        "notes": notes,
    }


def _table_status(
    candidate_rows,
    baseline_rows,
    row_ratio,
    missing_columns,
    extra_columns,
):
    if candidate_rows == 0 and baseline_rows and baseline_rows > 0:
        return (
            "FAIL",
            "Candidate has no rows for the comparable slice while baseline has rows.",
        )
    if baseline_rows == 0 and candidate_rows and candidate_rows > 0:
        return (
            "WARN",
            "Baseline has no rows for the comparable slice while candidate has rows.",
        )
    if row_ratio is not None and (
        row_ratio < ROW_RATIO_WARN_MIN or row_ratio > ROW_RATIO_WARN_MAX
    ):
        return (
            "WARN",
            (
                "Candidate row count differs from baseline by more than "
                f"{int((1 - ROW_RATIO_WARN_MIN) * 100)}%."
            ),
        )
    if missing_columns:
        return "WARN", "Candidate is missing baseline columns."
    if extra_columns:
        return "INFO", "Candidate has additional columns."
    return "OK", ""


def _latest_comparable_df(df, preferred_filter=None):
    from pyspark.sql import functions as F

    if preferred_filter:
        column, value = preferred_filter.split("=", 1)
        column = column.strip()
        value = value.strip()
        if column in df.columns:
            filtered = df.filter(F.col(column).cast("string") == value)
            if filtered.limit(1).count() > 0:
                return filtered, preferred_filter

    for column in ["reference_date", "rundate"]:
        if column in df.columns:
            value = df.select(F.max(column).cast("string")).collect()[0][0]
            if value is not None:
                return df.filter(F.col(column).cast("string") == value), (
                    f"{column}={value}"
                )

    return df, ""


def _table_exists(spark, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(0).collect()
    except Exception:
        return False
    return True


def _distinct_count(df, column):
    if not column:
        return None
    from pyspark.sql import functions as F

    return df.select(F.countDistinct(column)).collect()[0][0]


def _score_stats(score_delta):
    from pyspark.sql import functions as F

    stats = score_delta.agg(
        F.avg("score_delta").alias("avg_abs_score_delta"),
        F.max("score_delta").alias("max_abs_score_delta"),
    ).collect()[0]
    return {
        "avg_abs_score_delta": stats["avg_abs_score_delta"],
        "max_abs_score_delta": stats["max_abs_score_delta"],
    }


def _first_present(df, columns):
    for column in columns:
        if column in df.columns:
            return column
    return None


def _ratio(numerator, denominator):
    if denominator in (None, 0):
        return None
    if numerator is None:
        return None
    return float(numerator) / float(denominator)


def _missing_table_note(candidate_exists, baseline_exists):
    missing = []
    if not candidate_exists:
        missing.append("candidate")
    if not baseline_exists:
        missing.append("baseline")
    return "Missing table(s): " + ", ".join(missing)


def _empty_row(
    checked_at,
    check_name,
    candidate_table,
    baseline_table,
    status,
    notes,
):
    return {
        "checked_at": checked_at,
        "check_name": check_name,
        "candidate_table": candidate_table,
        "baseline_table": baseline_table,
        "candidate_filter": "",
        "baseline_filter": "",
        "candidate_rows": None,
        "baseline_rows": None,
        "candidate_distinct_accounts": None,
        "baseline_distinct_accounts": None,
        "joined_rows": None,
        "row_ratio": None,
        "match_rate": None,
        "avg_abs_score_delta": None,
        "max_abs_score_delta": None,
        "missing_columns": "",
        "extra_columns": "",
        "status": status,
        "notes": notes,
    }
