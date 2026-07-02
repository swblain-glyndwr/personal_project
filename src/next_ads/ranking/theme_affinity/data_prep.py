from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import string
from typing import Any


def calculate_target_month(reference_date: str) -> dict[str, str]:
    ref_date = _resolve_reference_date(reference_date)
    target_start = ref_date + timedelta(days=1)
    target_end = ref_date + timedelta(days=31)
    return {
        "target_month_start": target_start.strftime("%Y-%m-%d"),
        "target_month_end": target_end.strftime("%Y-%m-%d"),
    }


def calculate_custom_date_range(
    reference_date: str,
    lookback: int,
    days_lag: int = 0,
) -> dict[str, str]:
    ref_date = _resolve_reference_date(reference_date)
    start_date = (ref_date - timedelta(days=lookback)).strftime("%Y-%m-%d")
    today = datetime.today().date()
    if ref_date.date() == today:
        end_date = ref_date.strftime("%Y-%m-%d")
    else:
        end_date = (ref_date - timedelta(days=days_lag)).strftime("%Y-%m-%d")
    return {"start_date": start_date, "end_date": end_date}


def build_common_params(reference_date: str, namespace: str, table_prefix: str):
    target_dates = calculate_target_month(reference_date)
    start_date_views, end_date_views = _range_values(reference_date, 30)
    start_date_atbs, end_date_atbs = _range_values(reference_date, 30)
    start_date_baskets, end_date_baskets = _range_values(reference_date, 365)
    end_date_views_ly = _offset_date(end_date_views, 365)
    start_date_views_ly = _offset_date(end_date_views_ly, 30)
    end_date_baskets_ly = _offset_date(end_date_baskets, 365)
    start_date_baskets_ly = _offset_date(end_date_baskets_ly, 30)
    return {
        "catalog": namespace,
        "schema": namespace,
        "reference_date": _resolve_reference_date(reference_date).strftime(
            "%Y-%m-%d"
        ),
        "table_prefix": table_prefix,
        "start_date_views": start_date_views,
        "end_date_views": end_date_views,
        "start_date_views_ly": start_date_views_ly,
        "end_date_views_ly": end_date_views_ly,
        "start_date_atbs": start_date_atbs,
        "end_date_atbs": end_date_atbs,
        "start_date_baskets": start_date_baskets,
        "end_date_baskets": end_date_baskets,
        "start_date_baskets_ly": start_date_baskets_ly,
        "end_date_baskets_ly": end_date_baskets_ly,
        **target_dates,
    }


def build_sql_entries(reference_date: str, table_prefix: str):
    target_dates = calculate_target_month(reference_date)
    start_date_views, end_date_views = _range_values(reference_date, 30)
    start_date_atbs, end_date_atbs = _range_values(reference_date, 30)
    start_date_baskets, end_date_baskets = _range_values(reference_date, 365)
    end_date_views_ly = _offset_date(end_date_views, 365)
    start_date_views_ly = _offset_date(end_date_views_ly, 30)
    end_date_baskets_ly = _offset_date(end_date_baskets, 365)
    start_date_baskets_ly = _offset_date(end_date_baskets_ly, 30)

    return {
        0: [
            _entry(
                "0_product_catalog.sql",
                f"{table_prefix}_product_catalog",
                "overwrite",
                None,
            ),
            _entry("0_atbs.sql", f"{table_prefix}_atbs", "overwrite", {
                "start_date_atbs": start_date_atbs,
                "end_date_atbs": end_date_atbs,
            }),
            _entry("0_baskets.sql", f"{table_prefix}_baskets", "overwrite", {
                "start_date_baskets": start_date_baskets,
                "target_month_end": target_dates["target_month_end"],
            }),
            _entry(
                "0_baskets_ly.sql",
                f"{table_prefix}_baskets_ly",
                "overwrite",
                {
                    "start_date_baskets_ly": start_date_baskets_ly,
                    "end_date_baskets_ly": end_date_baskets_ly,
                },
            ),
            _entry("0_views.sql", f"{table_prefix}_views", "overwrite", {
                "start_date_views": start_date_views,
                "end_date_views": end_date_views,
            }),
            _entry("0_views_ly.sql", f"{table_prefix}_views_ly", "overwrite", {
                "start_date_views_ly": start_date_views_ly,
                "end_date_views_ly": end_date_views_ly,
            }),
        ],
        1: [
            _entry(
                "1_atbs_themes.sql",
                f"{table_prefix}_atbs_themes",
                "overwrite",
                {"start_date_atbs": start_date_atbs, "end_date_atbs": end_date_atbs},
                partition_by=["reference_date"],
            ),
            _entry(
                "1_baskets_themes.sql",
                f"{table_prefix}_baskets_themes",
                "overwrite",
                {
                    "start_date_baskets": start_date_baskets,
                    "end_date_baskets": end_date_baskets,
                },
                partition_by=["reference_date"],
            ),
            _entry(
                "1_views_themes.sql",
                f"{table_prefix}_views_themes",
                "overwrite",
                {"start_date_views": start_date_views, "end_date_views": end_date_views},
                partition_by=["reference_date"],
            ),
            _entry(
                "1a_vatb.sql",
                f"{table_prefix}_vatb",
                "overwrite",
                None,
                partition_by=["reference_date"],
            ),
        ],
        2: [
            _entry(
                "2_advanced_features.sql",
                f"{table_prefix}_advanced_features",
                "overwrite",
                {
                    "start_date_views": start_date_views,
                    "end_date_views": end_date_views,
                    "start_date_atbs": start_date_atbs,
                    "end_date_atbs": end_date_atbs,
                },
                partition_by=["reference_date"],
            ),
            _entry(
                "2_atbs_bythemes.sql",
                f"{table_prefix}_atbs_bytheme",
                "overwrite",
                {"end_date_atbs": end_date_atbs},
                partition_by=["reference_date"],
                post_process=None,
            ),
            _entry(
                "2_baskets_bythemes.sql",
                f"{table_prefix}_baskets_bytheme",
                "overwrite",
                {"end_date_baskets": end_date_baskets},
                partition_by=["reference_date"],
            ),
            _entry(
                "2_views_bythemes.sql",
                f"{table_prefix}_views_bytheme",
                "overwrite",
                {"end_date_views": end_date_views},
                partition_by=["reference_date"],
            ),
            _entry(
                "2_repurchased.sql",
                f"{table_prefix}_repurchase",
                "overwrite",
                None,
                partition_by=["reference_date"],
            ),
            _entry(
                "2_target.sql",
                f"{table_prefix}_baskets_target",
                "overwrite",
                {
                    "start_date": target_dates["target_month_start"],
                    "end_date": target_dates["target_month_end"],
                },
                partition_by=["reference_date"],
            ),
        ],
        3: [
            _entry(
                "3_atbs_1_algo.sql",
                f"{table_prefix}_algo_atbs1",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_atbs",
            ),
            _entry(
                "3_atbs_5_algo.sql",
                f"{table_prefix}_algo_atbs5",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_atbs",
            ),
            _entry(
                "3_baskets_1_algo.sql",
                f"{table_prefix}_algo_baskets1",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_baskets",
            ),
            _entry(
                "3_baskets_5_algo.sql",
                f"{table_prefix}_algo_baskets5",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_baskets",
            ),
            _entry(
                "3_views_1_algo.sql",
                f"{table_prefix}_algo_views1",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_views",
            ),
            _entry(
                "3_views_5_algo.sql",
                f"{table_prefix}_algo_views5",
                "overwrite",
                None,
                partition_by=["reference_date"],
                post_process="freq12_norm_views",
            ),
        ],
        4: [
            _entry(
                "4_customer_features.sql",
                f"{table_prefix}_customer_features",
                "overwrite",
                {
                    "start_date_baskets": start_date_baskets,
                    "end_date_baskets": end_date_baskets,
                },
            ),
            _entry(
                "4_customer_segments.sql",
                f"{table_prefix}_customer_segments",
                "overwrite",
                {
                    "start_date_baskets": start_date_baskets,
                    "end_date_baskets": end_date_baskets,
                },
            ),
            _entry(
                "4_popularity_metrics.sql",
                f"{table_prefix}_popularity_metrics",
                "overwrite",
                {
                    "end_date_views_ly": end_date_views_ly,
                    "end_date_baskets_ly": end_date_baskets_ly,
                    "end_date_views": end_date_views,
                },
            ),
        ],
        5: [
            _entry(
                "6_master_assoc.sql",
                f"{table_prefix}_master",
                "append",
                {
                    "start_date_views": start_date_views,
                    "end_date_views": end_date_views,
                    "start_date_atbs": start_date_atbs,
                    "end_date_atbs": end_date_atbs,
                    "start_date_baskets": start_date_baskets,
                    "end_date_baskets": end_date_baskets,
                    "target_month_start": target_dates["target_month_start"],
                    "target_month_end": target_dates["target_month_end"],
                },
                partition_by=["reference_date"],
            ),
        ],
    }


def run_layers(spark, runtime, layer: str, reference_date: str, dry_run=False):
    spark.sql(f"USE CATALOG {runtime.config.catalog_read}")
    common_params = build_common_params(
        reference_date, runtime.namespace, runtime.table_prefix
    )
    sql_entries = build_sql_entries(reference_date, runtime.table_prefix)
    layers = _selected_layers(layer)
    create_theme_mapping_view(spark)

    for layer_number in layers:
        if layer_number == 5:
            create_spine_view(spark, common_params)
        entries = sql_entries[layer_number]
        _run_layer(spark, entries, common_params, runtime.namespace, dry_run)

    if 5 in layers:
        write_complete_table(spark, runtime)


def _runtime_table(runtime, suffix: str) -> str:
    return f"{runtime.namespace}.{runtime.table_prefix}_{suffix}"


def write_complete_table(spark, runtime):
    from pyspark.sql import functions as F

    predict_df = (
        spark.read.table(_runtime_table(runtime, "master"))
        .filter(F.col("rundate") == F.current_date())
        .distinct()
    )
    month_value = F.month(F.date_add(F.col("reference_date"), 1))
    predict_df = predict_df.withColumn("month", month_value)

    decimal_cols = [
        "repurchase_ratio",
        "Familyconfidence_score",
        "Coupleconfidence_score",
        "Womenswearconfidence_score",
        "Menswearconfidence_score",
        "Beautyconfidence_score",
        "Homeconfidence_score",
    ]
    base = predict_df.select(
        [
            F.col(col).cast("double") if col in decimal_cols else F.col(col)
            for col in predict_df.columns
        ]
    )
    (
        base.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(_runtime_table(runtime, "complete"))
    )


def rank_complete_table(spark, runtime):
    predict_complete = _runtime_table(runtime, "complete")
    predict_input_table = _runtime_table(runtime, "ranked")
    sql = f"""
CREATE OR REPLACE TABLE {predict_input_table} AS
WITH t0 AS (
  SELECT *,
  CASE WHEN theme_clean LIKE '%women%'
      AND GREATEST(Womenswearconfidence_score,Menswearconfidence_score,Beautyconfidence_score,Homeconfidence_score) == Womenswearconfidence_score
      THEN 1
  WHEN (theme_clean LIKE '%girls%' OR theme_clean LIKE '%boys%')
      AND GREATEST(Womenswearconfidence_score,Menswearconfidence_score,Beautyconfidence_score,Homeconfidence_score,Familyconfidence_score) == Familyconfidence_score
      THEN 1
  WHEN theme_clean LIKE '%men%'
      AND GREATEST(Womenswearconfidence_score,Menswearconfidence_score,Beautyconfidence_score,Homeconfidence_score) == Menswearconfidence_score
      THEN 1
  WHEN theme_clean LIKE '%beauty%'
      AND GREATEST(Womenswearconfidence_score,Menswearconfidence_score,Beautyconfidence_score,Homeconfidence_score) == Beautyconfidence_score
      THEN 1
  WHEN theme_clean LIKE '%home%'
      AND GREATEST(Womenswearconfidence_score,Menswearconfidence_score,Beautyconfidence_score,Homeconfidence_score) == Homeconfidence_score
      THEN 1
  ELSE 0
  END AS same_dept
  FROM {predict_complete}
),
t1 AS (
  SELECT *,
  ROW_NUMBER() OVER (
      PARTITION BY account_number, reference_date
      ORDER BY
      CASE WHEN views_behavior__recency = 0 THEN 999999 ELSE views_behavior__recency END ASC,
      CASE WHEN atbs_behavior__recency = 0 THEN 999999 ELSE atbs_behavior__recency END ASC,
      repurchase_ratio DESC,
      num_retrieval_methods DESC,
      baskets_behavior__frequency DESC,
      atbs_behavior__frequency DESC,
      views_behavior__frequency DESC,
      COALESCE(algo_baskets5__lift_top10, 0) DESC,
      COALESCE(algo_atbs5__lift_top10, 0) DESC,
      COALESCE(algo_views5__lift_top10, 0) DESC,
      same_dept DESC,
      theme_clean
  ) AS simple_rules_rank
  FROM t0
),
final AS (
  SELECT *,
  CASE
  WHEN views_behavior__recency > 0 AND views_behavior__recency < 9999 THEN 'views_recency'
  WHEN atbs_behavior__recency > 0 AND atbs_behavior__recency < 9999 THEN 'atbs_recency'
  WHEN repurchase_ratio > 0 THEN 'repurchase_ratio'
  WHEN num_retrieval_methods > 0 AND baskets_behavior__frequency > 0 THEN 'ret_met_baskets_frequency'
  WHEN num_retrieval_methods > 0 AND atbs_behavior__frequency > 0 THEN 'ret_met_atbs_frequency'
  WHEN num_retrieval_methods > 0 AND views_behavior__frequency > 0 THEN 'ret_met_views_frequency'
  WHEN num_retrieval_methods > 0 AND algo_baskets5__lift_top10 > 0 THEN 'ret_met_algo_baskets5_lift_top10'
  WHEN num_retrieval_methods > 0 AND algo_atbs5__lift_top10 > 0 THEN 'ret_met_algo_atbs5_lift_top10'
  WHEN num_retrieval_methods > 0 AND algo_views5__lift_top10 > 0 THEN 'ret_met_algo_views5_lift_top10'
  WHEN num_retrieval_methods > 0 AND same_dept = 1 THEN 'ret_met_same_dept'
  WHEN baskets_behavior__frequency > 0 THEN 'baskets_frequency'
  WHEN atbs_behavior__frequency > 0 THEN 'atbs_frequency'
  WHEN views_behavior__frequency > 0 THEN 'views_frequency'
  WHEN algo_baskets5__lift_top10 > 0 THEN 'algo_baskets5_lift_top10'
  WHEN algo_atbs5__lift_top10 > 0 THEN 'algo_atbs5_lift_top10'
  WHEN algo_views5__lift_top10 > 0 THEN 'algo_views5_lift_top10'
  WHEN same_dept = 1 THEN 'same_dept'
  ELSE 'theme'
  END AS rules_rank_source
  FROM t1
  GROUP BY ALL
)
SELECT DISTINCT *
FROM final
ORDER BY account_number, simple_rules_rank
"""
    spark.sql(sql)


def _entry(
    file_name: str,
    table_name: str,
    write_mode: str,
    params: dict[str, Any] | None,
    partition_by: list[str] | None = None,
    post_process: str | None = None,
) -> dict[str, Any]:
    return {
        "file": file_name,
        "table_name": table_name,
        "write_mode": write_mode,
        "partition_by": partition_by,
        "post_process": post_process,
        "params": params,
    }


def _selected_layers(layer: str) -> list[int]:
    if layer == "all":
        return [0, 1, 2, 3, 4, 5]
    if layer == "0-3":
        return [0, 1, 2, 3]
    try:
        return [int(layer)]
    except ValueError as exc:
        raise ValueError(f"Unsupported Theme Affinity layer: {layer}") from exc


def _run_layer(spark, entries, common_params, namespace, dry_run):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                execute_sql_entry,
                spark,
                entry,
                common_params,
                namespace,
                dry_run,
            )
            for entry in entries
        ]
        for future in as_completed(futures):
            future.result()


def execute_sql_entry(spark, entry, common_params, namespace, dry_run=False):
    sql = render_sql_file(entry, common_params)
    table_name = entry.get("table_name")

    if dry_run:
        print(sql)
        return

    if not table_name:
        spark.sql(sql)
        return

    df = spark.sql(sql)
    df = apply_post_process(df, entry.get("post_process"))
    writer = df.write.mode(entry["write_mode"])
    if entry["write_mode"] == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if entry.get("partition_by"):
        writer = writer.partitionBy(*entry["partition_by"])
    writer.saveAsTable(f"{namespace}.{table_name}")


def render_sql_file(entry, common_params):
    sql = _sql_path(common_params, entry["file"]).read_text()
    params = dict(common_params)
    params.update(entry.get("params") or {})
    return sql.format(**params)


def _sql_path(common_params, file_name) -> Path:
    sql_path = common_params.get("sql_path")
    if sql_path:
        return Path(sql_path) / file_name
    for parent in Path(__file__).resolve().parents:
        candidate = (
            parent
            / "src"
            / "next_ads"
            / "ranking"
            / "theme_affinity"
            / "sql"
            / file_name
        )
        if candidate.exists():
            return candidate
        candidate = parent / "next_ads" / "ranking" / "theme_affinity" / "sql" / file_name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(file_name)


def apply_post_process(df, post_process_name):
    if not post_process_name:
        return df
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    freq_cols = {
        "freq12_norm_atbs": "theme_clean2_atbs_freq12",
        "freq12_norm_baskets": "theme_clean2_baskets_freq12",
        "freq12_norm_views": "theme_clean2_views_freq12",
    }
    freq_col = freq_cols[post_process_name]
    window = Window.partitionBy("account_number", "reference_date")
    min_val = F.min(freq_col).over(window)
    max_val = F.max(freq_col).over(window)
    return df.withColumn(
        "freq12_norm",
        F.when(max_val == min_val, F.lit(1.0)).otherwise(
            (F.col(freq_col) - min_val) / (max_val - min_val)
        ),
    )


def _resolve_reference_date(reference_date: str | None) -> datetime:
    reference_date_value = (reference_date or "").strip().lower()
    if reference_date_value == "current":
        return datetime.today()
    if not reference_date_value:
        raise ValueError("reference_date must be current or YYYY-MM-DD")
    resolved = datetime.strptime(reference_date_value, "%Y-%m-%d")
    if (datetime.today() - resolved).days < 28:
        raise ValueError(
            "reference_date must be at least 28 days ago unless set to current"
        )
    return resolved


def _range_values(reference_date: str, lookback: int) -> tuple[str, str]:
    result = calculate_custom_date_range(reference_date, lookback)
    return result["start_date"], result["end_date"]


def _offset_date(date_value: str, days: int) -> str:
    return (datetime.strptime(date_value, "%Y-%m-%d") - timedelta(days=days)).strftime(
        "%Y-%m-%d"
    )


def sql_placeholders(sql: str) -> set[str]:
    formatter = string.Formatter()
    return {
        field_name
        for _, field_name, _, _ in formatter.parse(sql)
        if field_name
    }


def create_theme_mapping_view(spark):
    return spark.sql(
        """
CREATE OR REPLACE TEMPORARY VIEW 0_theme_mapping AS
SELECT DISTINCT *, regexp_replace(theme, '[^a-zA-Z0-9]', '') AS theme_clean
FROM marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest
WHERE theme_rank = 1
"""
    )


def create_spine_view(spark, common_params):
    sql = """
CREATE OR REPLACE TEMPORARY VIEW spine AS
WITH base AS (
  SELECT DISTINCT account_number, itemno, theme
  FROM marketingdata_prod.warehouse.baskets_uk_3y
  INNER JOIN (
    SELECT DISTINCT pid, theme
    FROM marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest
  )
  ON pid = itemno
  WHERE order_date >= date_add(date"{reference_date}", -365)
  AND theme IS NOT NULL
),
base_filtered AS (
  SELECT DISTINCT account_number FROM base
),
0_theme_mapping AS (
  SELECT DISTINCT *, regexp_replace(theme, '[^a-zA-Z0-9]', '') AS theme_clean
  FROM marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest
  WHERE theme_rank = 1
),
themes AS (
  SELECT DISTINCT *, date"{reference_date}" AS reference_date
  FROM base_filtered
  CROSS JOIN (SELECT DISTINCT theme_clean FROM 0_theme_mapping)
),
spine AS (
  SELECT reference_date, account_number, theme_clean
  FROM (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_atbs1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_atbs5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_baskets1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_baskets5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_views1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {schema}.{table_prefix}_algo_views5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {schema}.{table_prefix}_atbs_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {schema}.{table_prefix}_baskets_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {schema}.{table_prefix}_views_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {schema}.{table_prefix}_repurchase
    WHERE reference_date = date"{reference_date}"
  )
)
SELECT a.*, spine.* EXCEPT(account_number, theme_clean, reference_date)
FROM (SELECT * FROM themes GROUP BY ALL) a
LEFT JOIN spine
USING(account_number, theme_clean, reference_date)
WHERE a.account_number IS NOT NULL
"""
    return spark.sql(sql.format(**common_params))
