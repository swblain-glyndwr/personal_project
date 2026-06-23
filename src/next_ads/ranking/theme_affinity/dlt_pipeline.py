from pathlib import Path
import re
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


def _candidate_bootstrap_paths():
    module_file = globals().get("__file__")
    if module_file:
        yield from Path(module_file).resolve().parents

    sql_path = spark.conf.get("pipeline.sql_path", None)
    if sql_path:
        yield from Path(sql_path).parents


def _bootstrap_repo_paths():
    for parent in _candidate_bootstrap_paths():
        src_path = parent / "src"
        if (src_path / "next_ads").exists():
            sys.path.insert(0, str(src_path))
            sys.path.insert(1, str(parent))
            return
        if (parent / "next_ads").exists():
            sys.path.insert(0, str(parent))
            return


_bootstrap_repo_paths()

from next_ads.ranking.theme_affinity.data_prep import (
    apply_post_process,
    build_common_params,
    build_sql_entries,
    render_sql_file,
)


SCHEMA = spark.conf.get("pipeline.schema")
TABLE_PREFIX = spark.conf.get("pipeline.table_prefix")
REFERENCE_DATE = spark.conf.get("pipeline.reference_date")
SQL_PATH = spark.conf.get("pipeline.sql_path")
JOB_ENV = spark.conf.get("pipeline.job_env", "dev")

COMMON_PARAMS = build_common_params(REFERENCE_DATE, SCHEMA, TABLE_PREFIX)
COMMON_PARAMS["sql_path"] = SQL_PATH
COMMON_PARAMS["job_env"] = JOB_ENV
SQL_ENTRIES = build_sql_entries(REFERENCE_DATE, TABLE_PREFIX)


def _pipeline_sql(entry):
    sql = render_sql_file(entry, COMMON_PARAMS)
    return _qualify_prod_sources(sql)


def _qualify_prod_sources(sql):
    sql = re.sub(
        r"(?<![\w.])warehouse\.",
        "marketingdata_prod.warehouse.",
        sql,
    )
    return re.sub(
        r"(?<![\w.])digital_marketing\.",
        "marketingdata_prod.digital_marketing.",
        sql,
    )


def _define_sql_table(entry):
    table_name = entry["table_name"]

    @dp.table(name=table_name)
    def _table_fn():
        df = spark.sql(_pipeline_sql(entry))
        return apply_post_process(df, entry.get("post_process"))

    return _table_fn


@dp.table(name="0_theme_mapping", private=True)
def theme_mapping():
    return spark.sql(
        """
SELECT DISTINCT *, regexp_replace(theme, '[^a-zA-Z0-9]', '') AS theme_clean
FROM marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest
WHERE theme_rank = 1
"""
    )


@dp.table(name="spine", private=True)
def spine():
    sql = """
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
    return spark.sql(sql.format(**COMMON_PARAMS))


for _layer in [0, 1, 2, 3, 4, 5]:
    for _entry in SQL_ENTRIES[_layer]:
        if "table_name" in _entry:
            _define_sql_table(_entry)


@dp.table(name=f"{TABLE_PREFIX}_complete")
def complete():
    predict_df = (
        spark.read.table(f"{TABLE_PREFIX}_master")
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
    return predict_df.select(
        [
            F.col(col).cast("double") if col in decimal_cols else F.col(col)
            for col in predict_df.columns
        ]
    )


@dp.table(name=f"{TABLE_PREFIX}_ranked")
def ranked():
    sql = f"""
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
  FROM {TABLE_PREFIX}_complete
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
    return spark.sql(sql)
