"""Point-in-time advert-to-item bridge for reusable offline features."""

from __future__ import annotations

from datetime import date, datetime


CANONICAL_ADVERT_ITEM_COLUMNS = (
    "advert_id",
    "feature_date",
    "item_id",
    "item_rank",
    "item_weight",
    "item_source",
    "source_rundate",
)

MAX_ITEMS_PER_ADVERT = 10


def _as_date(value, *, name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be an ISO date (YYYY-MM-DD)") from exc
    raise TypeError(f"{name} must be a date or ISO date string")


def _resolve_dates(feature_date, cutoff_date) -> tuple[date, date]:
    resolved_feature_date = _as_date(feature_date, name="feature_date")
    resolved_cutoff_date = _as_date(cutoff_date, name="cutoff_date")
    if resolved_cutoff_date > resolved_feature_date:
        raise ValueError("cutoff_date cannot be after feature_date")
    return resolved_feature_date, resolved_cutoff_date


def _require_columns(frame, source_name: str, required: tuple[str, ...]) -> None:
    missing = sorted(set(required).difference(frame.columns))
    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {', '.join(missing)}"
        )


def _latest_rows_per_advert(frame, cutoff_date: date):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    latest_window = Window.partitionBy("advert_id")
    return (
        frame.where(F.col("advert_id").isNotNull())
        .where(F.col("advert_id") != "")
        .where(F.col("source_rundate").isNotNull())
        .where(F.col("source_rundate") <= F.lit(cutoff_date))
        .withColumn(
            "_latest_source_rundate",
            F.max("source_rundate").over(latest_window),
        )
        .where(F.col("source_rundate") == F.col("_latest_source_rundate"))
        .drop("_latest_source_rundate")
    )


def _sort_history_candidates(
    frame,
    *,
    source_name: str,
    item_column: str,
    priority: int,
    cutoff_date: date,
):
    from pyspark.sql import functions as F

    _require_columns(
        frame,
        source_name,
        ("UniqueAdID", item_column, "item_pos", "rundate"),
    )
    latest = _latest_rows_per_advert(
        frame.select(
            F.trim(F.col("UniqueAdID").cast("string")).alias("advert_id"),
            F.trim(F.col(item_column).cast("string")).alias("item_id"),
            F.col("item_pos").cast("long").alias("source_rank"),
            F.col("rundate").cast("date").alias("source_rundate"),
        ),
        cutoff_date,
    )
    valid_item = F.col("item_id").isNotNull() & (F.col("item_id") != "")
    invalid_position = (
        latest.where(valid_item)
        .where(F.col("source_rank").isNull() | (F.col("source_rank") <= 0))
        .orderBy("advert_id", "source_rundate", "item_id")
        .limit(1)
        .collect()
    )
    if invalid_position:
        example = invalid_position[0]
        raise ValueError(
            f"{source_name} has an invalid item_pos for "
            f"advert_id={example['advert_id']}, "
            f"source_rundate={example['source_rundate']}, and "
            f"item_id={example['item_id']}"
        )
    conflicting_position = (
        latest.where(valid_item)
        .groupBy("advert_id", "source_rundate", "source_rank")
        .agg(F.countDistinct("item_id").alias("_item_count"))
        .where(F.col("_item_count") > 1)
        .orderBy("advert_id", "source_rundate", "source_rank")
        .limit(1)
        .collect()
    )
    if conflicting_position:
        example = conflicting_position[0]
        raise ValueError(
            f"{source_name} has conflicting items at item_pos="
            f"{example['source_rank']} for advert_id="
            f"{example['advert_id']} and source_rundate="
            f"{example['source_rundate']}"
        )
    return (
        latest.where(F.col("advert_id") != "")
        .where(valid_item)
        .dropDuplicates(
            ["advert_id", "source_rundate", "source_rank", "item_id"]
        )
        .select(
            "advert_id",
            "item_id",
            F.coalesce(F.col("source_rank"), F.lit(9223372036854775807)).alias(
                "source_rank"
            ),
            F.lit(source_name).alias("item_source"),
            "source_rundate",
            F.lit(priority).cast("int").alias("source_priority"),
        )
    )


def _representative_item_candidates(frame, *, priority: int, cutoff_date: date):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source_name = "representative_items"
    _require_columns(
        frame,
        source_name,
        ("UniqueAdID", "RepresentativeItems", "rundate"),
    )
    latest = _latest_rows_per_advert(
        frame.select(
            F.trim(F.col("UniqueAdID").cast("string")).alias("advert_id"),
            F.col("RepresentativeItems").alias("source_items"),
            F.col("rundate").cast("date").alias("source_rundate"),
        ),
        cutoff_date,
    ).withColumn(
        "_items_signature",
        F.coalesce(
            F.to_json("source_items"),
            F.lit("__NULL_REPRESENTATIVE_ITEMS__"),
        ),
    )
    conflict = (
        latest.groupBy("advert_id", "source_rundate")
        .agg(F.countDistinct("_items_signature").alias("_items_value_count"))
        .where(F.col("_items_value_count") > 1)
        .orderBy("advert_id", "source_rundate")
        .limit(1)
        .collect()
    )
    if conflict:
        example = conflict[0]
        raise ValueError(
            "representative_items has conflicting arrays for "
            f"advert_id={example['advert_id']} and "
            f"source_rundate={example['source_rundate']}"
        )
    row_window = Window.partitionBy("advert_id", "source_rundate").orderBy(
        F.col("_items_signature").asc_nulls_last()
    )
    latest = (
        latest.withColumn("_source_row_number", F.row_number().over(row_window))
        .where(F.col("_source_row_number") == 1)
        .drop("_source_row_number", "_items_signature")
    )
    return (
        latest.select(
            "advert_id",
            "source_rundate",
            F.posexplode_outer("source_items").alias("_item_position", "item_id"),
        )
        .select(
            "advert_id",
            F.trim(F.col("item_id").cast("string")).alias("item_id"),
            (F.col("_item_position") + F.lit(1)).cast("long").alias(
                "source_rank"
            ),
            F.lit(source_name).alias("item_source"),
            "source_rundate",
            F.lit(priority).cast("int").alias("source_priority"),
        )
        .where(F.col("advert_id") != "")
        .where(F.col("item_id").isNotNull() & (F.col("item_id") != ""))
    )


def _normalised_items(column):
    from pyspark.sql import functions as F

    split_items = F.split(
        F.trim(F.coalesce(column.cast("string"), F.lit(""))),
        r"[,|;\s]+",
    )
    return F.filter(split_items, lambda item: F.length(item) > 0)


def _control_item_candidates(
    frame,
    *,
    source_name: str,
    scope_column: str,
    priority: int,
    cutoff_date: date,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _require_columns(
        frame,
        source_name,
        ("UniqueAdID", scope_column, "Items", "rundate"),
    )
    prepared = frame.select(
        F.trim(F.col("UniqueAdID").cast("string")).alias("advert_id"),
        F.trim(F.col(scope_column).cast("string")).alias("source_scope"),
        _normalised_items(F.col("Items")).alias("source_items"),
        F.col("rundate").cast("date").alias("source_rundate"),
    )
    latest = _latest_rows_per_advert(prepared, cutoff_date).withColumn(
        "_items_signature",
        F.concat(
            F.size("source_items").cast("string"),
            F.lit(":"),
            F.concat_ws("|", "source_items"),
        ),
    )
    conflict = (
        latest.groupBy("advert_id", "source_rundate")
        .agg(F.countDistinct("_items_signature").alias("_items_value_count"))
        .where(F.col("_items_value_count") > 1)
        .orderBy("advert_id", "source_rundate")
        .limit(1)
        .collect()
    )
    if conflict:
        example = conflict[0]
        raise ValueError(
            f"{source_name} has conflicting Items values for "
            f"advert_id={example['advert_id']} and "
            f"source_rundate={example['source_rundate']} across "
            f"{scope_column} rows"
        )

    row_window = Window.partitionBy("advert_id", "source_rundate").orderBy(
        F.col("source_scope").asc_nulls_last(),
        F.col("_items_signature").asc(),
    )
    one_row_per_advert = (
        latest.withColumn("_source_row_number", F.row_number().over(row_window))
        .where(F.col("_source_row_number") == 1)
        .drop("_source_row_number")
    )
    return (
        one_row_per_advert.select(
            "advert_id",
            "source_rundate",
            F.posexplode_outer("source_items").alias("_item_position", "item_id"),
        )
        .select(
            "advert_id",
            F.trim(F.col("item_id").cast("string")).alias("item_id"),
            (F.col("_item_position") + F.lit(1)).cast("long").alias(
                "source_rank"
            ),
            F.lit(source_name).alias("item_source"),
            "source_rundate",
            F.lit(priority).cast("int").alias("source_priority"),
        )
        .where(F.col("advert_id") != "")
        .where(F.col("item_id").isNotNull() & (F.col("item_id") != ""))
    )


def _select_canonical_items(candidates, *, feature_date: date):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    advert_window = Window.partitionBy("advert_id")
    chosen_source = (
        candidates.withColumn(
            "_chosen_source_priority",
            F.min("source_priority").over(advert_window),
        )
        .where(F.col("source_priority") == F.col("_chosen_source_priority"))
        .drop("_chosen_source_priority")
    )

    item_window = Window.partitionBy("advert_id", "item_id").orderBy(
        F.col("source_rank").asc(),
        F.col("source_rundate").desc(),
    )
    deduplicated = (
        chosen_source.withColumn(
            "_item_occurrence", F.row_number().over(item_window)
        )
        .where(F.col("_item_occurrence") == 1)
        .drop("_item_occurrence")
    )

    rank_window = Window.partitionBy("advert_id").orderBy(
        F.col("source_rank").asc(),
        F.col("item_id").asc(),
    )
    ranked = (
        deduplicated.withColumn("item_rank", F.row_number().over(rank_window))
        .where(F.col("item_rank") <= MAX_ITEMS_PER_ADVERT)
        .withColumn("_reciprocal_rank", F.lit(1.0) / F.col("item_rank"))
    )
    weight_window = Window.partitionBy("advert_id")
    return ranked.select(
        F.col("advert_id").cast("string"),
        F.lit(feature_date).cast("date").alias("feature_date"),
        F.col("item_id").cast("string"),
        F.col("item_rank").cast("int"),
        (
            F.col("_reciprocal_rank")
            / F.sum("_reciprocal_rank").over(weight_window)
        )
        .cast("double")
        .alias("item_weight"),
        F.col("item_source").cast("string"),
        F.col("source_rundate").cast("date"),
    )


def build_advert_item_bridge(
    *,
    v2_sort_history,
    legacy_sort_history,
    representative_items,
    v2_control,
    v1_control,
    feature_date,
    cutoff_date,
):
    """Build the canonical point-in-time advert-to-item relationship.

    The five inputs are supplied DataFrames matching the repository's current
    table contracts. No table is read by this helper. For each source and
    advert, only the latest source date at or before ``cutoff_date`` is used.
    Source precedence is v2 sort history, legacy sort history,
    representative items, v2 control, then v1 control. Every supplied source
    is validated before precedence is applied, so a corrupt fallback fails the
    build even when a higher-priority source covers the same advert.
    """
    resolved_feature_date, resolved_cutoff_date = _resolve_dates(
        feature_date, cutoff_date
    )
    sources = (
        _sort_history_candidates(
            v2_sort_history,
            source_name="v2_sort_history",
            item_column="item",
            priority=1,
            cutoff_date=resolved_cutoff_date,
        ),
        _sort_history_candidates(
            legacy_sort_history,
            source_name="legacy_sort_history",
            item_column="items",
            priority=2,
            cutoff_date=resolved_cutoff_date,
        ),
        _representative_item_candidates(
            representative_items,
            priority=3,
            cutoff_date=resolved_cutoff_date,
        ),
        _control_item_candidates(
            v2_control,
            source_name="v2_control",
            scope_column="PageType",
            priority=4,
            cutoff_date=resolved_cutoff_date,
        ),
        _control_item_candidates(
            v1_control,
            source_name="v1_control",
            scope_column="Location",
            priority=5,
            cutoff_date=resolved_cutoff_date,
        ),
    )
    candidates = sources[0]
    for source in sources[1:]:
        candidates = candidates.unionByName(source)
    return _select_canonical_items(
        candidates,
        feature_date=resolved_feature_date,
    ).select(*CANONICAL_ADVERT_ITEM_COLUMNS)
