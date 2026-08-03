import re

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

from next_ads.ranking.theme_affinity.data_prep import (
    apply_post_process,
    build_account_theme_spine,
    build_common_params,
    build_ranked_sql,
    build_sql_entries,
    render_sql_file,
)
from next_ads.ranking.foundation_context import load_active_foundation_context
from next_ads.ranking.provider_context import pinned_item_themes


SCHEMA = spark.conf.get("pipeline.schema")
TABLE_PREFIX = spark.conf.get("pipeline.table_prefix")
SQL_PATH = spark.conf.get("pipeline.sql_path")
JOB_ENV = spark.conf.get("pipeline.job_env", "dev")
CONTEXT_TABLE = spark.conf.get("pipeline.context_table")
CONTEXT_SLOT = spark.conf.get("pipeline.context_slot")
ITEM_THEMES_TABLE = spark.conf.get("pipeline.item_themes_table")
FOUNDATION_CONTEXT = load_active_foundation_context(
    spark,
    context_table=CONTEXT_TABLE,
    context_slot=CONTEXT_SLOT,
)
REFERENCE_DATE = FOUNDATION_CONTEXT.run_date.isoformat()
PINNED_ITEM_THEMES = pinned_item_themes(
    spark,
    FOUNDATION_CONTEXT,
    input_table=ITEM_THEMES_TABLE,
).select(
    "pid",
    "theme",
    "theme_rank",
)

COMMON_PARAMS = build_common_params(
    REFERENCE_DATE,
    SCHEMA,
    TABLE_PREFIX,
    operational=True,
)
COMMON_PARAMS["sql_path"] = SQL_PATH
COMMON_PARAMS["job_env"] = JOB_ENV
SQL_ENTRIES = build_sql_entries(
    REFERENCE_DATE,
    TABLE_PREFIX,
    operational=True,
)


def _pipeline_sql(entry):
    sql = render_sql_file(entry, COMMON_PARAMS)
    sql = _qualify_prod_sources(sql)
    sql = sql.replace(
        "marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest",
        "pinned_item_themes",
    )
    return re.sub(
        r"\bcurrent_date\s*\(\s*\)",
        f'date"{REFERENCE_DATE}"',
        sql,
        flags=re.IGNORECASE,
    )


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
FROM pinned_item_themes
WHERE theme_rank = 1
"""
    )


@dp.table(name="pinned_item_themes", private=True)
def pinned_theme_mapping_input():
    return PINNED_ITEM_THEMES


@dp.table(name="spine", private=True)
def spine():
    return build_account_theme_spine(
        spark.table("marketingdata_prod.warehouse.baskets_uk_3y"),
        PINNED_ITEM_THEMES,
        REFERENCE_DATE,
    )


for _layer in [0, 1, 2, 3, 4, 5]:
    for _entry in SQL_ENTRIES[_layer]:
        if "table_name" in _entry:
            _define_sql_table(_entry)


@dp.table(name=f"{TABLE_PREFIX}_complete")
def complete():
    predict_df = (
        spark.read.table(f"{TABLE_PREFIX}_master")
        .filter(F.col("rundate") == F.lit(FOUNDATION_CONTEXT.run_date))
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


@dp.table(name=f"{TABLE_PREFIX}_build_marker")
def build_marker():
    return spark.createDataFrame(
        [
            {
                "ContextSlot": FOUNDATION_CONTEXT.context_slot,
                "OrchestrationRunID": (
                    FOUNDATION_CONTEXT.orchestration_run_id
                ),
                "FoundationID": FOUNDATION_CONTEXT.foundation_id,
                "FoundationVersion": FOUNDATION_CONTEXT.foundation_version,
                "ScoringFoundationBuildID": (
                    FOUNDATION_CONTEXT.scoring_foundation_build_id
                ),
                "ScoringFoundationBuildAttemptID": (
                    FOUNDATION_CONTEXT.scoring_foundation_build_attempt_id
                ),
                "InputSnapshotID": FOUNDATION_CONTEXT.input_snapshot_id,
                "InputSnapshotAttemptID": (
                    FOUNDATION_CONTEXT.input_snapshot_attempt_id
                ),
                "RunDate": FOUNDATION_CONTEXT.run_date,
                "InvocationChecksum": (
                    FOUNDATION_CONTEXT.invocation_checksum
                ),
            }
        ]
    )


@dp.table(name=f"{TABLE_PREFIX}_ranked")
def ranked():
    return spark.sql(build_ranked_sql(f"{TABLE_PREFIX}_complete"))
