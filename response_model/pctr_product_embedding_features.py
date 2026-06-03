# Databricks notebook source
# MAGIC %md
# MAGIC # pCTR Product Embedding Features
# MAGIC
# MAGIC Builds product embedding feature layers for pCTR.
# MAGIC
# MAGIC This workbook adapts the `next-gen-ads` topic-generation approach into the
# MAGIC pCTR build: product text is embedded and then rolled up into advert-side
# MAGIC and customer-side product features.
# MAGIC
# MAGIC Outputs:
# MAGIC
# MAGIC - `next_uk_pctr_product_embeddings_latest`
# MAGIC - `next_uk_pctr_advert_product_features_90d`
# MAGIC - `next_uk_pctr_customer_product_features`


# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install "protobuf==4.24.1" "sentence-transformers>=2.2.2,<=2.4.0" "transformers" "torch<2.12"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import importlib.util
import google.protobuf

print(google.protobuf.__version__)
print(google.protobuf.__file__)
print(importlib.util.find_spec("google.protobuf.service"))

# COMMAND ----------

from datetime import datetime
import json
import os
import shutil
from typing import Iterator

import mlflow
import mlflow.sentence_transformers
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
try:
    from sentence_transformers import SentenceTransformer
except ImportError as exc:
    raise ImportError(
        "Install sentence-transformers on the Databricks cluster before running "
        "this notebook, for example as a cluster library or with "
        "`%pip install -U sentence-transformers`."
    ) from exc

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.shuffle.partitions", "auto")
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = os.environ.get("MLFLOW_HTTP_REQUEST_TIMEOUT", "900")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the point-in-time anchor for product features. Product
# MAGIC text embeddings are reusable, but the customer product-interest vector,
# MAGIC recent views/purchases, and advert product rollups are built as if this date
# MAGIC is the day of the run.
# MAGIC
# MAGIC `advert_feature_horizon_days` extends advert product feature dates forward so
# MAGIC the tagged-click training rows can join product-backed advert features on
# MAGIC their future exposure `session_date`.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest tables. Use
# MAGIC `write_mode=append_snapshot` to write a partition into suffixed snapshot
# MAGIC tables such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed product embedding and product-match tables used by current
# MAGIC   debugging or latest scoring flows.
# MAGIC - `append_snapshot` is for repeatable point-in-time training data. It writes
# MAGIC   product features into a history-style table and replaces only the current
# MAGIC   `reference_date` partition when rerun.
# MAGIC - `snapshot_table_suffix` chooses which history-style table is used.
# MAGIC   `snapshots` is the proper backfill/training suffix. `smoke` is a safe test
# MAGIC   suffix so a one-month dry run does not overwrite the latest table or the
# MAGIC   real training snapshots.

# COMMAND ----------

def get_widget_value(name, default):
    try:
        dbutils.widgets.text(name, str(default))
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except NameError:
        return default


def validate_widget_choice(name, value, valid_values):
    if value not in valid_values:
        raise ValueError(f"Invalid widget value for {name}: {value}. Valid values are: {valid_values}")
    return value


def snapshot_table_name(table_name):
    return f"{table_name}_{snapshot_table_suffix}"


def write_output_table(df, table_name, partition_col="reference_date"):
    if write_mode == "append_snapshot":
        target_table = snapshot_table_name(table_name)
        df_to_write = df.withColumn(partition_col, F.lit(reference_date).cast("date"))
        if spark.catalog.tableExists(target_table):
            spark.sql(f"DELETE FROM {target_table} WHERE {partition_col} = DATE '{reference_date}'")
            df_to_write.write.mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        else:
            df_to_write.write.mode("overwrite").option("overwriteSchema", "true").partitionBy(partition_col).saveAsTable(target_table)
        print(f"Wrote snapshot for {reference_date} to {target_table}")
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        print(f"Wrote latest output to {table_name}")


def read_output_table(table_name, partition_col="reference_date"):
    if write_mode == "append_snapshot":
        return (
            spark.table(snapshot_table_name(table_name))
            .where(F.col(partition_col) == F.lit(reference_date).cast("date"))
        )
    return spark.table(table_name)


def materialise_embedding_input(df, table_name, input_cols, partition_col="reference_date"):
    df_to_write = (
        df
        .select(*input_cols)
        .withColumn(partition_col, F.lit(reference_date).cast("date"))
    )

    if spark.catalog.tableExists(table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE {partition_col} = DATE '{reference_date}'")
        df_to_write.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    else:
        (
            df_to_write.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(partition_col)
            .saveAsTable(table_name)
        )

    stable_df = (
        spark.table(table_name)
        .where(F.col(partition_col) == F.lit(reference_date).cast("date"))
        .select(*input_cols)
    )
    print(f"Materialised embedding input for {reference_date} to {table_name}: {stable_df.count()} rows")
    return stable_df


def checkpoint_with_count(df, label):
    try:
        checkpointed_df = df.localCheckpoint(eager=True)
        print(f"Checkpointed {label}: {checkpointed_df.count()} rows")
        return checkpointed_df
    except Exception as exc:
        print(f"Could not checkpoint {label}; falling back to cache. Error: {exc}")
        cached_df = df.cache()
        print(f"Materialised {label}: {cached_df.count()} rows")
        return cached_df


write_mode_options = ["overwrite_latest", "append_snapshot"]

reference_date = get_widget_value("reference_date", "2026-04-15")
write_mode = validate_widget_choice("write_mode", get_widget_value("write_mode", "overwrite_latest"), write_mode_options)
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
lookback_days = 90
advert_feature_horizon_days = int(get_widget_value("advert_feature_horizon_days", "14"))
baskets_lookback_days = 365
views_lookback_days = 28

purchase_weight = 10.0
view_weight = 1.0
time_decay_factor = -0.1
max_recent_views_per_customer = 5
embedding_inference_partitions = int(get_widget_value("embedding_inference_partitions", "8"))

print(
    "pCTR product embedding run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"advert_feature_horizon_days={advert_feature_horizon_days}, "
    f"embedding_inference_partitions={embedding_inference_partitions}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "advert_feature_horizon_days is an integer number of days; "
    "embedding_inference_partitions controls distributed product embedding inference fan-out. "
    "Meaning: customer product interest, recent views, recent purchases, and "
    "time decay are calculated relative to reference_date."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")

hf_embedding_model_name = "sentence-transformers/all-MiniLM-L12-v2"
model_alias = "batch_candidate"
register_embedding_model_if_missing = True
embedding_model_artifact_path = "sentence_transformer"
embedding_model_registered_name = f"marketingdata_dev.{dev_schema}.nextads_pctr_advert_sentence_transformer"
experiment_path = "/Shared/mlflow/nextads/dev/experiments/pctr_product_embedding_features"
staged_embedding_model_root = get_widget_value(
    "staged_embedding_model_root",
    "/Volumes/marketingdata_dev/ds_sandbox/ds_volume/next_ads/embedding_models",
)
embedding_batch_size = 256
embedding_feature_dims_for_model = 32

product_catalog_tbl = "marketingdata_prod.warehouse.product_catalog_history"
control_sheet_tbl = "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
ad_items_tbl = "marketingdata_prod.warehouse.next_uk_nextads_ad_items"
baskets_tbl = "marketingdata_prod.warehouse.baskets_uk_3y"
bq_sessions_with_accounts_tbl = "marketingdata_prod.warehouse.bq_views_sessions_next_uk_with_accounts"

product_embeddings_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_product_embeddings_latest"
advert_product_features_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_product_features_90d"
customer_product_features_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_product_features"
embedding_input_table_suffix = "latest" if write_mode == "overwrite_latest" else snapshot_table_suffix
product_embedding_input_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_product_embedding_input_{embedding_input_table_suffix}"
customer_product_interaction_input_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_product_interaction_input_{embedding_input_table_suffix}"
text_embedding_cache_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_text_embedding_cache"

feature_end_date = F.lit(reference_date).cast("date")
feature_start_date = F.date_sub(feature_end_date, lookback_days)
advert_feature_end_date = F.date_add(feature_end_date, advert_feature_horizon_days)
assignment_rundate_start = F.date_sub(feature_start_date, 1)
assignment_rundate_end = F.date_sub(advert_feature_end_date, 1)
run_date = F.lit(reference_date).cast("date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Embedding Model
# MAGIC
# MAGIC Reuse the same MLflow / Unity Catalog Sentence Transformer model as the
# MAGIC advert semantic workbook. If the alias is not available yet, register the
# MAGIC same Hugging Face model and assign the shared batch alias.

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_path)

embedding_model_rationale = {
    "selected_model": hf_embedding_model_name,
    "registered_model_name": embedding_model_registered_name,
    "model_alias": model_alias,
    "why_selected": (
        "Use the same MiniLM-L12 Sentence Transformer that the advert semantic "
        "workbook uses, so advert semantic features and product-match features "
        "live in the same 384-dimensional semantic space."
    ),
    "why_not_bge_from_next_gen_ads": (
        "The next-gen-ads repo used BGE for product topic generation. For this "
        "pCTR build, sharing the advert workbook's registered MiniLM model keeps "
        "customer, product, and advert features comparable and governed through "
        "the same MLflow model alias."
    ),
    "why_no_kmeans_topics": (
        "KMeans is intentionally not used here. Product topic clustering is useful "
        "for coarse explainability, but the pCTR signal should come from direct "
        "customer-to-ad product embedding similarity rather than hard product buckets."
    ),
}

display(
    spark.createDataFrame(
        [(key, value) for key, value in embedding_model_rationale.items()],
        ["field", "value"],
    )
)


def register_sentence_transformer_model():
    model = SentenceTransformer(hf_embedding_model_name)
    embedding_dimension = int(model.get_sentence_embedding_dimension())
    input_example = "black running trainers nike sporty womens footwear"
    output_example = model.encode(
        [input_example],
        batch_size=1,
        normalize_embeddings=True,
        show_progress_bar=False,
    )[0].tolist()
    signature = infer_signature([input_example], [output_example])
    run_name = f"register_{hf_embedding_model_name.split('/')[-1]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_param("hf_embedding_model_name", hf_embedding_model_name)
        mlflow.log_param("embedding_dimension", embedding_dimension)
        mlflow.log_param("normalize_embeddings", True)
        mlflow.log_text(json.dumps(embedding_model_rationale, indent=2), "model_choice_rationale.json")
        mlflow.sentence_transformers.log_model(
            model=model,
            artifact_path=embedding_model_artifact_path,
            registered_model_name=embedding_model_registered_name,
            signature=signature,
            input_example=input_example,
            pip_requirements=[
                "sentence-transformers",
                "transformers",
                "torch",
            ],
            metadata={
                "source_model_name": hf_embedding_model_name,
                "normalised_embeddings": "true",
                "batch_only": "true",
                "shared_with_advert_semantic_workbook": "true",
            },
        )
        run_id = run.info.run_id

    client = MlflowClient()
    matching_versions = [
        model_version
        for model_version in client.search_model_versions(f"name='{embedding_model_registered_name}'")
        if model_version.run_id == run_id
    ]
    if not matching_versions:
        raise ValueError(
            f"Registered model version was not found for run {run_id} "
            f"and model {embedding_model_registered_name}."
        )

    registered_version = sorted(matching_versions, key=lambda version: int(version.version))[-1]
    client.set_registered_model_alias(
        name=embedding_model_registered_name,
        alias=model_alias,
        version=registered_version.version,
    )

    return f"models:/{embedding_model_registered_name}/{registered_version.version}", embedding_dimension, registered_version.version


def get_or_register_sentence_transformer_model():
    client = MlflowClient()

    try:
        registered_version = client.get_model_version_by_alias(
            name=embedding_model_registered_name,
            alias=model_alias,
        )
        embedding_model_uri = f"models:/{embedding_model_registered_name}/{registered_version.version}"
        model = mlflow.sentence_transformers.load_model(embedding_model_uri)
        embedding_dimension = int(model.get_sentence_embedding_dimension())
        return embedding_model_uri, embedding_dimension, registered_version.version, False
    except Exception as exc:
        if not register_embedding_model_if_missing:
            raise ValueError(
                f"Could not load {embedding_model_registered_name}@{model_alias}. "
                "Run the advert semantic workbook first or enable registration."
            ) from exc

        embedding_model_uri, embedding_dimension, registered_model_version = register_sentence_transformer_model()
        return embedding_model_uri, embedding_dimension, registered_model_version, True


embedding_model_uri, embedding_dimension, registered_model_version, registered_model_this_run = get_or_register_sentence_transformer_model()


def stage_sentence_transformer_for_executors(model_uri, model_version):
    local_staging_root = staged_embedding_model_root
    if local_staging_root.startswith("dbfs:/"):
        local_staging_root = f"/dbfs/{local_staging_root[len('dbfs:/'):].lstrip('/')}"

    staged_model_path = os.path.join(
        local_staging_root,
        f"{embedding_model_registered_name.replace('.', '_')}_v{model_version or 'run'}",
    )
    marker_path = os.path.join(staged_model_path, "config_sentence_transformers.json")

    if not os.path.exists(marker_path):
        print(f"Staging embedding model for Spark workers at {staged_model_path}")
        if os.path.exists(staged_model_path):
            shutil.rmtree(staged_model_path)
        os.makedirs(staged_model_path, exist_ok=True)

        model = mlflow.sentence_transformers.load_model(model_uri)
        model.save(staged_model_path)
    else:
        print(f"Using staged embedding model at {staged_model_path}")

    return staged_model_path


executor_embedding_model_path = stage_sentence_transformer_for_executors(
    embedding_model_uri,
    registered_model_version,
)

print(f"Embedding model URI for batch inference: {embedding_model_uri}")
print(f"Executor embedding model path: {executor_embedding_model_path}")
print(f"Embedding dimension: {embedding_dimension}")
print(f"Registered model version: {registered_model_version}")
print(f"Registered model in this run: {registered_model_this_run}")

# COMMAND ----------


def clean_text_col(col):
    cleaned = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    bad_text = F.lower(cleaned).isin("nan", "na", "n/a", "null", "none")
    return F.when((cleaned == "") | bad_text, F.lit("")).otherwise(cleaned)


def normalise_item_col(col):
    return F.regexp_replace(F.lower(clean_text_col(col)), r"[^a-z0-9]", "")


def existing_col(df, candidates, alias):
    for candidate in candidates:
        if candidate in df.columns:
            return clean_text_col(F.col(candidate)).alias(alias)
    return F.lit("").cast("string").alias(alias)


def parse_items(col_name):
    return F.expr(f"filter(split(coalesce({col_name}, ''), '[^A-Za-z0-9]+'), x -> x <> '')")


def _prepare_executor_env():
    """Prepare executor environment for torch/sentence_transformers import."""
    import os, sys
    os.environ["USER"] = os.environ.get("USER") or "spark"
    os.environ["TORCHINDUCTOR_CACHE_DIR"] = os.environ.get("TORCHINDUCTOR_CACHE_DIR") or "/tmp/torchinductor_cache"
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = os.environ.get("MLFLOW_HTTP_REQUEST_TIMEOUT", "900")
    stale_keys = [k for k in sys.modules if "torch._dynamo" in k]
    for key in stale_keys:
        del sys.modules[key]


@F.udf(T.ArrayType(T.DoubleType()))
def l2_normalise(values):
    if values is None:
        return None
    dense_values = [float(value) for value in values]
    norm = sum(value * value for value in dense_values) ** 0.5
    if norm == 0:
        return dense_values
    return [value / norm for value in dense_values]


@F.udf(T.ArrayType(T.DoubleType()))
def weighted_mean_embedding(rows):
    if not rows:
        return None

    accumulator = None
    total_weight = 0.0

    for row in rows:
        embedding = row["embedding"]
        weight = row["weight"]
        if embedding is None or weight is None:
            continue

        weight = float(weight)
        if weight <= 0:
            continue

        values = [float(value) for value in embedding]
        if accumulator is None:
            accumulator = [0.0] * len(values)

        for index, value in enumerate(values):
            accumulator[index] += weight * value
        total_weight += weight

    if accumulator is None or total_weight <= 0:
        return None

    averaged = [value / total_weight for value in accumulator]
    norm = sum(value * value for value in averaged) ** 0.5
    if norm == 0:
        return averaged
    return [value / norm for value in averaged]


def embedding_cache_schema():
    return T.StructType(
        [
            T.StructField("embedding_cache_key", T.StringType(), False),
            T.StructField("embedding_model_name", T.StringType(), False),
            T.StructField("embedding_model_uri", T.StringType(), False),
            T.StructField("embedding_model_version", T.StringType(), True),
            T.StructField("embedding_text", T.StringType(), False),
            T.StructField("text_embedding", T.ArrayType(T.DoubleType()), False),
            T.StructField("created_at", T.TimestampType(), False),
        ]
    )


def read_text_embedding_cache(table_name):
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)
    return spark.createDataFrame([], embedding_cache_schema())


def with_embedding_cache_key(df, text_col):
    embedding_text = F.coalesce(F.col(text_col).cast("string"), F.lit(""))
    return (
        df
        .withColumn(text_col, embedding_text)
        .withColumn(
            "embedding_cache_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(hf_embedding_model_name),
                    F.lit(str(registered_model_version or "")),
                    F.col(text_col),
                ),
                256,
            ),
        )
    )


def build_cached_product_embeddings(df, id_cols, text_col, embedding_col, cache_table):
    input_df = (
        with_embedding_cache_key(df.select(*(id_cols + [text_col])), text_col)
        .cache()
    )

    cache_df = read_text_embedding_cache(cache_table).select(
        "embedding_cache_key",
        "text_embedding",
    ).dropDuplicates(["embedding_cache_key"])

    missing_input_df = (
        input_df
        .select(
            "embedding_cache_key",
            F.col(text_col).alias("embedding_text"),
        )
        .dropDuplicates(["embedding_cache_key"])
        .join(cache_df.select("embedding_cache_key"), on="embedding_cache_key", how="left_anti")
        .cache()
    )

    missing_count = missing_input_df.count()
    total_count = input_df.count()
    unique_text_count = input_df.select("embedding_cache_key").dropDuplicates().count()
    cached_unique_count = unique_text_count - missing_count
    print(
        f"{embedding_col} cache lookup: "
        f"{total_count} rows, {unique_text_count} unique texts, "
        f"{cached_unique_count} cached texts, {missing_count} new texts."
    )

    if missing_count > 0:
        cache_embedding_schema = T.StructType(
            [
                T.StructField("embedding_cache_key", T.StringType(), False),
                T.StructField("embedding_text", T.StringType(), False),
                T.StructField("text_embedding", T.ArrayType(T.DoubleType()), False),
            ]
        )

        def generate_text_embeddings(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            _prepare_executor_env()
            from sentence_transformers import SentenceTransformer

            if not hasattr(generate_text_embeddings, "_model"):
                generate_text_embeddings._model = SentenceTransformer(executor_embedding_model_path)

            model = generate_text_embeddings._model

            for batch_df in iterator:
                embeddings = model.encode(
                    batch_df["embedding_text"].fillna("").astype(str).tolist(),
                    batch_size=embedding_batch_size,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )

                yield pd.DataFrame(
                    {
                        "embedding_cache_key": batch_df["embedding_cache_key"],
                        "embedding_text": batch_df["embedding_text"],
                        "text_embedding": embeddings.astype(float).tolist(),
                    }
                )

        missing_vectors = (
            missing_input_df
            .repartition(embedding_inference_partitions)
            .mapInPandas(generate_text_embeddings, schema=cache_embedding_schema)
            .withColumn("text_embedding", l2_normalise(F.col("text_embedding")))
            .withColumn("embedding_model_name", F.lit(hf_embedding_model_name))
            .withColumn("embedding_model_uri", F.lit(embedding_model_uri))
            .withColumn("embedding_model_version", F.lit(str(registered_model_version or "")))
            .withColumn("created_at", F.current_timestamp())
            .select(
                "embedding_cache_key",
                "embedding_model_name",
                "embedding_model_uri",
                "embedding_model_version",
                "embedding_text",
                "text_embedding",
                "created_at",
            )
        )

        if spark.catalog.tableExists(cache_table):
            missing_vectors.write.mode("append").option("mergeSchema", "true").saveAsTable(cache_table)
        else:
            missing_vectors.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cache_table)
        print(f"Appended {missing_count} new text embeddings to {cache_table}")

    refreshed_cache_df = read_text_embedding_cache(cache_table).select(
        "embedding_cache_key",
        F.col("text_embedding").alias(embedding_col),
    ).dropDuplicates(["embedding_cache_key"])

    return (
        input_df
        .join(refreshed_cache_df, on="embedding_cache_key", how="inner")
        .select(
            *id_cols,
            text_col,
            embedding_col,
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Text
# MAGIC
# MAGIC Match the `next-gen-ads` text soup idea: each product gets one compact text
# MAGIC field built from catalogue descriptors that explain what the product is.

# COMMAND ----------

df_product_catalog_history_raw = spark.table(product_catalog_tbl)

product_id_col = next(
    (
        col_name
        for col_name in ["pid", "itemno", "item_number", "itemNumber", "productSku"]
        if col_name in df_product_catalog_history_raw.columns
    ),
    None,
)

if product_id_col is None:
    raise ValueError(
        f"Could not find a product identifier column in {product_catalog_tbl}. "
        f"Available columns: {df_product_catalog_history_raw.columns}"
    )

df_product_catalog_history = df_product_catalog_history_raw.withColumn(
    "itemno",
    normalise_item_col(F.col(product_id_col)),
)

if "start_date" in df_product_catalog_history.columns:
    df_product_catalog_history = df_product_catalog_history.withColumn(
        "_catalog_start_date",
        F.to_date(F.col("start_date")),
    )
else:
    df_product_catalog_history = df_product_catalog_history.withColumn(
        "_catalog_start_date",
        F.lit(None).cast("date"),
    )

if "end_date" in df_product_catalog_history.columns:
    df_product_catalog_history = df_product_catalog_history.withColumn(
        "_catalog_end_date",
        F.to_date(F.col("end_date")),
    )
else:
    df_product_catalog_history = df_product_catalog_history.withColumn(
        "_catalog_end_date",
        F.lit(None).cast("date"),
    )

df_product_catalog_history = (
    df_product_catalog_history
    .where(F.col("itemno").isNotNull())
    .where(F.col("_catalog_start_date").isNull() | (F.col("_catalog_start_date") <= feature_end_date))
    .where(F.col("_catalog_end_date").isNull() | (F.col("_catalog_end_date") >= feature_start_date))
)

catalog_latest_window = Window.partitionBy("itemno").orderBy(
    F.col("_catalog_end_date").desc_nulls_last(),
    F.col("_catalog_start_date").desc_nulls_last(),
)

df_product_catalog_raw = (
    df_product_catalog_history
    .withColumn("catalog_row_num", F.row_number().over(catalog_latest_window))
    .where(F.col("catalog_row_num") == 1)
)

df_product_text = (
    df_product_catalog_raw
    .select(
        "itemno",
        existing_col(df_product_catalog_raw, ["brand", "Brand"], "brand"),
        existing_col(df_product_catalog_raw, ["title", "product_title", "item_title", "name"], "title"),
        existing_col(df_product_catalog_raw, ["gender", "next_gender"], "gender"),
        existing_col(df_product_catalog_raw, ["crumbs", "breadcrumbs", "breadcrumb"], "crumbs"),
        existing_col(df_product_catalog_raw, ["primary_colour", "next_colour", "colour", "color"], "primary_colour"),
        existing_col(df_product_catalog_raw, ["material"], "material"),
        existing_col(df_product_catalog_raw, ["pattern"], "pattern"),
        existing_col(df_product_catalog_raw, ["neckline"], "neckline"),
        existing_col(df_product_catalog_raw, ["sleeve"], "sleeve"),
        existing_col(df_product_catalog_raw, ["occasion"], "occasion"),
        existing_col(df_product_catalog_raw, ["use"], "use"),
        existing_col(df_product_catalog_raw, ["description", "product_description"], "description"),
    )
    .where(F.col("itemno").isNotNull())
    .where(F.col("itemno") != "")
    .withColumn(
        "product_text",
        F.lower(
            F.regexp_replace(
                F.concat_ws(
                    " ",
                    "brand",
                    "title",
                    "gender",
                    F.regexp_replace(F.col("crumbs"), r"[|;]", " "),
                    "primary_colour",
                    "material",
                    "pattern",
                    "neckline",
                    "sleeve",
                    "occasion",
                    "use",
                    "description",
                ),
                r"\s+",
                " ",
            )
        ),
    )
    .select("itemno", "product_text")
    .where(F.col("product_text") != "")
    .dropDuplicates(["itemno"])
)

df_product_text = materialise_embedding_input(
    df_product_text,
    product_embedding_input_tbl,
    ["itemno", "product_text"],
)

display(df_product_text.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Embeddings

# COMMAND ----------

df_product_embeddings = (
    build_cached_product_embeddings(
        df_product_text,
        ["itemno"],
        "product_text",
        "product_embedding",
        text_embedding_cache_tbl,
    )
    .withColumn("embedding_model_name", F.lit(hf_embedding_model_name))
    .withColumn("embedding_model_uri", F.lit(embedding_model_uri))
    .withColumn("rundate", run_date)
    .cache()
)

print(f"Materialised product embeddings: {df_product_embeddings.count()} rows")

display(df_product_embeddings.limit(20))

# COMMAND ----------

# Persist the expensive embedding output before downstream rollups. This breaks
# the repartition/mapInPandas lineage so later advert/customer feature writes can
# retry safely if Spark has a shuffle task failure.
write_output_table(df_product_embeddings, product_embeddings_output_tbl)
df_product_embeddings = read_output_table(product_embeddings_output_tbl).cache()
print(f"Read stable product embeddings for {reference_date}: {df_product_embeddings.count()} rows")

# COMMAND ----------

df_product_embedding_checks = spark.createDataFrame(
    [
        ("product_text_rows", df_product_text.count()),
        ("product_embedding_rows", df_product_embeddings.count()),
        (
            "product_embedding_non_empty_rows",
            df_product_embeddings.where(F.size("product_embedding") > 0).count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_product_embedding_checks)

# COMMAND ----------

df_product_embedding_norm_check = (
    df_product_embeddings
    .select(
        F.expr(
            "sqrt(aggregate(product_embedding, cast(0.0 as double), "
            "(acc, value) -> acc + value * value))"
        ).alias("product_embedding_l2_norm")
    )
    .agg(
        F.min("product_embedding_l2_norm").alias("min_product_embedding_l2_norm"),
        F.avg("product_embedding_l2_norm").alias("avg_product_embedding_l2_norm"),
        F.max("product_embedding_l2_norm").alias("max_product_embedding_l2_norm"),
    )
)

display(df_product_embedding_norm_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Product Features
# MAGIC
# MAGIC Roll product embeddings up to advert grain using the same ordered item
# MAGIC weighting convention as the advert metadata profile workbook.

# COMMAND ----------

df_advert_daily_core = (
    spark.table(control_sheet_tbl)
    .where((F.col("rundate") >= assignment_rundate_start) & (F.col("rundate") <= assignment_rundate_end))
    .withColumn("feature_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        "feature_date",
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("Items").cast("string").alias("control_sheet_items"),
    )
    .where(F.col("advert_id").rlike("^P"))
    .dropDuplicates(["feature_date", "placement_id", "advert_id"])
)

df_control_sheet_items = (
    df_advert_daily_core
    .select(
        "feature_date",
        "advert_id",
        F.posexplode_outer(parse_items("control_sheet_items")).alias("item_position", "itemno_raw"),
    )
    .withColumn("itemno", normalise_item_col(F.col("itemno_raw")))
    .where(F.col("itemno").isNotNull())
    .where(F.col("itemno") != "")
    .withColumn("item_source", F.lit("control_sheet_items"))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source")
)

df_ad_items_raw = spark.table(ad_items_tbl)
if "rundate" in df_ad_items_raw.columns:
    latest_ad_items_rundate = (
        df_ad_items_raw
        .where(F.col("rundate") <= feature_end_date)
        .agg(F.max("rundate").alias("latest_rundate"))
        .first()["latest_rundate"]
    )
    df_ad_items_raw = df_ad_items_raw.where(F.col("rundate") == F.lit(latest_ad_items_rundate))

df_representative_items = (
    df_advert_daily_core
    .select("feature_date", "advert_id")
    .dropDuplicates()
    .join(
        df_ad_items_raw.select(
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            "RepresentativeItems",
        ),
        on="advert_id",
        how="inner",
    )
    .select(
        "feature_date",
        "advert_id",
        F.posexplode_outer("RepresentativeItems").alias("item_position", "itemno_raw"),
    )
    .withColumn("itemno", normalise_item_col(F.col("itemno_raw")))
    .where(F.col("itemno").isNotNull())
    .where(F.col("itemno") != "")
    .withColumn("item_source", F.lit("representative_items"))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source")
)

source_priority = (
    F.when(F.col("item_source") == "control_sheet_items", F.lit(1))
    .when(F.col("item_source") == "representative_items", F.lit(2))
    .otherwise(F.lit(9))
)

dedupe_advert_item_window = Window.partitionBy("feature_date", "advert_id", "itemno").orderBy(
    source_priority.asc(),
    F.col("item_position").asc_nulls_last(),
)

advert_weight_window = Window.partitionBy("feature_date", "advert_id")

df_advert_items_weighted = (
    df_control_sheet_items
    .unionByName(df_representative_items)
    .withColumn("item_row_num", F.row_number().over(dedupe_advert_item_window))
    .where(F.col("item_row_num") == 1)
    .withColumn("raw_item_weight", F.lit(1.0) / (F.col("item_position").cast("double") + F.lit(1.0)))
    .withColumn("item_weight", F.col("raw_item_weight") / F.sum("raw_item_weight").over(advert_weight_window))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source", "item_weight")
)

display(df_advert_items_weighted.limit(20))

# COMMAND ----------

df_advert_items_with_product_features = (
    df_advert_items_weighted
    .join(df_product_embeddings.select("itemno", "product_embedding"), on="itemno", how="left")
)

df_advert_product_features = (
    df_advert_items_with_product_features
    .groupBy("feature_date", "advert_id")
    .agg(
        F.countDistinct("itemno").alias("advert_product_item_count"),
        F.countDistinct(F.when(F.col("product_embedding").isNotNull(), F.col("itemno"))).alias("advert_product_embedded_item_count"),
        weighted_mean_embedding(
            F.collect_list(
                F.struct(
                    F.col("item_weight").alias("weight"),
                    F.col("product_embedding").alias("embedding"),
                )
            )
        ).alias("advert_product_embedding"),
    )
    .withColumn(
        "advert_product_embedding_coverage",
        F.when(
            F.col("advert_product_item_count") > 0,
            F.col("advert_product_embedded_item_count") / F.col("advert_product_item_count"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("rundate", run_date)
)

for dim_index in range(embedding_feature_dims_for_model):
    df_advert_product_features = df_advert_product_features.withColumn(
        f"advert_product_dim_{dim_index:03d}",
        F.col("advert_product_embedding").getItem(dim_index).cast("double"),
    )

df_advert_product_features = checkpoint_with_count(df_advert_product_features, "advert product features")

display(df_advert_product_features.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Product Features
# MAGIC
# MAGIC Create a customer product-interest vector from purchases and recent product
# MAGIC views, using the same weighting assumptions as `conditional_probability_recs`.

# COMMAND ----------

df_purchases = (
    spark.table(baskets_tbl)
    .where((F.col("order_date") >= F.date_sub(feature_end_date, baskets_lookback_days)) & (F.col("order_date") <= feature_end_date))
    .select(
        F.col("account_number").cast("string").alias("account_number"),
        normalise_item_col(F.col("itemno")).alias("itemno"),
        F.col("order_date").alias("date"),
    )
    .where(F.col("itemno") != "")
    .dropDuplicates(["account_number", "itemno", "date"])
    .withColumn("interaction_type", F.lit("purchase"))
)

df_views = (
    spark.table(bq_sessions_with_accounts_tbl)
    .where((F.col("date") >= F.date_sub(feature_end_date, views_lookback_days)) & (F.col("date") <= feature_end_date))
    .select(
        F.col("account_number").cast("string").alias("account_number"),
        normalise_item_col(F.col("productSku")).alias("itemno"),
        F.col("date").alias("date"),
    )
    .where(F.col("itemno") != "")
    .dropDuplicates(["account_number", "itemno", "date"])
    .withColumn("interaction_type", F.lit("view"))
)

df_customer_product_interactions_weighted = (
    df_purchases
    .unionByName(df_views)
    .withColumn("days_ago", F.datediff(feature_end_date, F.col("date")))
    .withColumn(
        "interaction_weight",
        F.when(F.col("interaction_type") == "purchase", F.lit(purchase_weight)).otherwise(F.lit(view_weight))
        * F.exp(F.lit(time_decay_factor) * F.col("days_ago")),
    )
)

recent_view_window = Window.partitionBy("account_number", "interaction_type").orderBy(
    F.desc("date"),
    F.desc("interaction_weight"),
    F.asc("itemno"),
)

df_customer_product_interactions = (
    df_customer_product_interactions_weighted
    .withColumn(
        "view_rank",
        F.when(F.col("interaction_type") == "view", F.row_number().over(recent_view_window)).otherwise(F.lit(1)),
    )
    .where((F.col("interaction_type") != "view") | (F.col("view_rank") <= max_recent_views_per_customer))
    .drop("view_rank")
)

display(df_customer_product_interactions.limit(20))

# COMMAND ----------

df_customer_interactions_with_product_features = (
    df_customer_product_interactions
    .join(df_product_embeddings.select("itemno", "product_embedding"), on="itemno", how="left")
)

df_customer_interactions_with_product_features = materialise_embedding_input(
    df_customer_interactions_with_product_features,
    customer_product_interaction_input_tbl,
    [
        "account_number",
        "itemno",
        "date",
        "interaction_type",
        "days_ago",
        "interaction_weight",
        "product_embedding",
    ],
)

df_customer_product_features = (
    df_customer_interactions_with_product_features
    .groupBy("account_number")
    .agg(
        F.count("*").alias("customer_product_interaction_count"),
        F.sum(F.when(F.col("interaction_type") == "purchase", 1).otherwise(0)).alias("customer_product_purchase_interaction_count"),
        F.sum(F.when(F.col("interaction_type") == "view", 1).otherwise(0)).alias("customer_product_view_interaction_count"),
        F.countDistinct("itemno").alias("customer_product_distinct_item_count"),
        F.countDistinct(F.when(F.col("product_embedding").isNotNull(), F.col("itemno"))).alias("customer_product_embedded_item_count"),
        weighted_mean_embedding(
            F.collect_list(
                F.struct(
                    F.col("interaction_weight").alias("weight"),
                    F.col("product_embedding").alias("embedding"),
                )
            )
        ).alias("customer_product_embedding"),
    )
    .withColumn(
        "customer_product_embedding_coverage",
        F.when(
            F.col("customer_product_distinct_item_count") > 0,
            F.col("customer_product_embedded_item_count") / F.col("customer_product_distinct_item_count"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("rundate", run_date)
)

for dim_index in range(embedding_feature_dims_for_model):
    df_customer_product_features = df_customer_product_features.withColumn(
        f"customer_product_dim_{dim_index:03d}",
        F.col("customer_product_embedding").getItem(dim_index).cast("double"),
    )

df_customer_product_features = checkpoint_with_count(df_customer_product_features, "customer product features")

display(df_customer_product_features.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Outputs

# COMMAND ----------

write_output_table(df_advert_product_features, advert_product_features_output_tbl)
write_output_table(df_customer_product_features, customer_product_features_output_tbl)

# COMMAND ----------

df_output_summary = spark.createDataFrame(
    [
        ("product_embedding_rows", df_product_embeddings.count()),
        ("advert_product_feature_rows", df_advert_product_features.count()),
        (
            "advert_rows_with_product_embeddings",
            df_advert_product_features.where(F.col("advert_product_embedding").isNotNull()).count(),
        ),
        ("customer_product_feature_rows", df_customer_product_features.count()),
        (
            "customer_rows_with_product_embeddings",
            df_customer_product_features.where(F.col("customer_product_embedding").isNotNull()).count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_output_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Run Summary

# COMMAND ----------

output_summary_metrics = {
    row["check_name"]: int(row["row_count"])
    for row in df_output_summary.collect()
}

with mlflow.start_run(
    run_name=f"product_embedding_features_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
):
    mlflow.log_param("reference_date", reference_date)
    mlflow.log_param("lookback_days", lookback_days)
    mlflow.log_param("baskets_lookback_days", baskets_lookback_days)
    mlflow.log_param("views_lookback_days", views_lookback_days)
    mlflow.log_param("purchase_weight", purchase_weight)
    mlflow.log_param("view_weight", view_weight)
    mlflow.log_param("time_decay_factor", time_decay_factor)
    mlflow.log_param("max_recent_views_per_customer", max_recent_views_per_customer)
    mlflow.log_param("hf_embedding_model_name", hf_embedding_model_name)
    mlflow.log_param("embedding_model_uri", embedding_model_uri)
    mlflow.log_param("embedding_dimension", embedding_dimension)
    mlflow.log_param("registered_model_name", embedding_model_registered_name)
    mlflow.log_param("registered_model_alias", model_alias)
    mlflow.log_param("registered_model_version", registered_model_version)
    mlflow.log_param("registered_model_this_run", registered_model_this_run)
    mlflow.log_metrics(output_summary_metrics)
    mlflow.log_text(json.dumps(embedding_model_rationale, indent=2), "embedding_model_rationale.json")
    mlflow.log_dict(
        {
            "product_embeddings_output_tbl": product_embeddings_output_tbl,
            "advert_product_features_output_tbl": advert_product_features_output_tbl,
            "customer_product_features_output_tbl": customer_product_features_output_tbl,
        },
        "output_tables.json",
    )
