# Databricks notebook source
# MAGIC %md
# MAGIC # pCTR Advert Semantic Embeddings
# MAGIC
# MAGIC Builds a semantic advert layer from destination metadata, creative text,
# MAGIC image fields, and linked item text.
# MAGIC
# MAGIC This version uses a Hugging Face Sentence Transformers model, registered in
# MAGIC MLflow / Unity Catalog and loaded back for distributed batch inference. It
# MAGIC is not served, because this feature layer is produced offline.
# MAGIC
# MAGIC Outputs:
# MAGIC
# MAGIC - `next_uk_pctr_advert_destination_content_90d`
# MAGIC - `next_uk_pctr_advert_semantic_embeddings_90d`
# MAGIC - `next_uk_pctr_item_semantic_embeddings_latest`
# MAGIC - `next_uk_pctr_advert_embedding_neighbours_90d`

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

import mlflow
import mlflow.sentence_transformers
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

from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.shuffle.partitions", "auto")
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = os.environ.get("MLFLOW_HTTP_REQUEST_TIMEOUT", "900")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the point-in-time anchor for the semantic advert build.
# MAGIC The notebook reads the advert/product descriptor tables for the same
# MAGIC snapshot date when `write_mode=append_snapshot`, then embeds the advert text
# MAGIC available for that point in time.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest tables. Use
# MAGIC `write_mode=append_snapshot` to write a partition into suffixed snapshot
# MAGIC tables such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed semantic advert tables used by current debugging or latest
# MAGIC   scoring flows.
# MAGIC - `append_snapshot` is for repeatable point-in-time training data. It writes
# MAGIC   the semantic features into a history-style table and replaces only the
# MAGIC   current `reference_date` partition when rerun.
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


def read_feature_table(table_name):
    if write_mode == "append_snapshot":
        return spark.table(snapshot_table_name(table_name)).where(F.col("reference_date") == F.lit(reference_date).cast("date"))
    return spark.table(table_name)


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


write_mode_options = ["overwrite_latest", "append_snapshot"]

reference_date = get_widget_value("reference_date", "2026-04-15")
write_mode = validate_widget_choice("write_mode", get_widget_value("write_mode", "overwrite_latest"), write_mode_options)
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
lookback_days = 90
embedding_feature_dims_for_model = 32
embedding_batch_size = 256
embedding_inference_partitions = int(get_widget_value("embedding_inference_partitions", "8"))
neighbour_distance_threshold = 1.25
top_neighbours_per_advert = 20

print(
    "pCTR advert semantic embedding run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    f"embedding_inference_partitions={embedding_inference_partitions}. "
    "Meaning: reference_date selects the as-of advert descriptor inputs and "
    "partitions the semantic output for that point in time."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")

hf_embedding_model_name = "sentence-transformers/all-MiniLM-L12-v2"
model_alias = "batch_candidate"
register_embedding_model = True
embedding_model_artifact_path = "sentence_transformer"
embedding_model_registered_name = f"marketingdata_dev.{dev_schema}.nextads_pctr_advert_sentence_transformer"
experiment_path = "/Shared/mlflow/nextads/dev/experiments/pctr_advert_semantic_embeddings"
staged_embedding_model_root = get_widget_value(
    "staged_embedding_model_root",
    "/Volumes/marketingdata_dev/ds_sandbox/ds_volume/next_ads/embedding_models",
)

advert_daily_core_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_daily_core_90d"
advert_attribute_profile_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_attribute_profile_90d"
item_attribute_lookup_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_item_attribute_lookup_latest"

advert_destination_content_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_destination_content_90d"
advert_semantic_embeddings_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_semantic_embeddings_90d"
item_semantic_embeddings_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_item_semantic_embeddings_latest"
advert_embedding_neighbours_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_embedding_neighbours_90d"
embedding_input_table_suffix = "latest" if write_mode == "overwrite_latest" else snapshot_table_suffix
advert_semantic_embedding_input_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_semantic_embedding_input_{embedding_input_table_suffix}"
item_semantic_embedding_input_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_item_semantic_embedding_input_{embedding_input_table_suffix}"
text_embedding_cache_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_text_embedding_cache"

feature_end_date = F.lit(reference_date).cast("date")
feature_start_date = F.date_sub(feature_end_date, lookback_days)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedding Model Choice
# MAGIC
# MAGIC Default model: `sentence-transformers/all-MiniLM-L12-v2`.
# MAGIC
# MAGIC This keeps the model small enough for cheap batch feature generation while
# MAGIC using a deeper MiniLM encoder than `all-MiniLM-L6-v2`. Both produce compact
# MAGIC 384-dimensional embeddings, but L12 is the better default for this workbook
# MAGIC because the job is offline batch rather than low-latency serving. If runtime
# MAGIC becomes the bottleneck, switch `hf_embedding_model_name` to
# MAGIC `sentence-transformers/all-MiniLM-L6-v2` without changing the output schema.

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_path)

embedding_model_rationale = {
    "selected_model": hf_embedding_model_name,
    "why_selected": (
        "MiniLM-L12 keeps the same compact 384-dimensional embedding shape as "
        "MiniLM-L6, but gives the offline batch job a little more semantic "
        "capacity for short advert, product, and catalogue text."
    ),
    "why_not_l6_default": (
        "MiniLM-L6 is faster and smaller, so it is the fallback if the batch "
        "runtime is too high. It is less attractive as the default because this "
        "job is not a real-time serving path."
    ),
    "why_not_larger_bge_e5_gte_default": (
        "BGE/E5/GTE base or large models may improve retrieval quality, but they "
        "increase dependency size, memory use, and runtime. MiniLM is a pragmatic "
        "first production-shaped advert feature layer."
    ),
}

display(
    spark.createDataFrame(
        [(key, value) for key, value in embedding_model_rationale.items()],
        ["field", "value"],
    )
)

# COMMAND ----------

def clean_text_col(col):
    return F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), r"https?://\S+", " "),
                r"[^A-Za-z0-9]+",
                " ",
            )
        )
    )


def with_text_quality_features(df, text_col, prefix):
    return (
        df
        .withColumn(f"{prefix}_char_count", F.length(F.col(text_col)))
        .withColumn(
            f"{prefix}_tokens",
            F.when(
                F.col(text_col) == "",
                F.expr("array()").cast("array<string>"),
            ).otherwise(F.split(F.col(text_col), r"\s+")),
        )
        .withColumn(f"{prefix}_token_count", F.size(F.col(f"{prefix}_tokens")))
        .withColumn(f"{prefix}_unique_token_count", F.size(F.array_distinct(F.col(f"{prefix}_tokens"))))
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
            registered_model_name=embedding_model_registered_name if register_embedding_model else None,
            signature=signature,
            input_example=input_example,
            pip_requirements=[
                "sentence-transformers>=2.2.2,<=2.4.0",
                "transformers",
                "torch<2.12",
            ],
            metadata={
                "source_model_name": hf_embedding_model_name,
                "normalised_embeddings": "true",
                "batch_only": "true",
            },
        )
        run_id = run.info.run_id

    if not register_embedding_model:
        return f"runs:/{run_id}/{embedding_model_artifact_path}", embedding_dimension, None

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
    if not register_embedding_model:
        embedding_model_uri, embedding_dimension, registered_model_version = register_sentence_transformer_model()
        return embedding_model_uri, embedding_dimension, registered_model_version, True

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
    except Exception:
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


@F.udf(VectorUDT())
def dense_vector(values):
    if values is None:
        return Vectors.sparse(embedding_dimension, [], [])
    return Vectors.dense([float(value) for value in values])


def cache_with_count(df, label):
    cached_df = df.cache()
    print(f"Materialised {label}: {cached_df.count()} rows")
    return cached_df


def _prepare_executor_env():
    """Prepare executor environment for torch/sentence_transformers import."""
    import os
    import sys
    os.environ["USER"] = os.environ.get("USER") or "spark"
    os.environ["TORCHINDUCTOR_CACHE_DIR"] = os.environ.get("TORCHINDUCTOR_CACHE_DIR") or "/tmp/torchinductor_cache"
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = os.environ.get("MLFLOW_HTTP_REQUEST_TIMEOUT", "900")
    # Evict any cached failed torch._dynamo imports from stale worker processes
    stale_keys = [k for k in sys.modules if "torch._dynamo" in k]
    for key in stale_keys:
        del sys.modules[key]


def build_sentence_transformer_embeddings(df, id_cols, text_col, prefix):
    input_cols = id_cols + ([] if text_col in id_cols else [text_col])
    output_schema = T.StructType(
        [df.schema[col_name] for col_name in id_cols]
        + [T.StructField(f"{prefix}_embedding", T.ArrayType(T.DoubleType()), False)]
    )

    _model_path = executor_embedding_model_path
    _batch_size = embedding_batch_size
    _inference_partitions = embedding_inference_partitions

    def encode_batches(iterator):
        _prepare_executor_env()
        from sentence_transformers import SentenceTransformer

        cache_key = f"_model_{prefix}"
        if not hasattr(encode_batches, cache_key):
            setattr(encode_batches, cache_key, SentenceTransformer(_model_path))

        model = getattr(encode_batches, cache_key)
        for pdf in iterator:
            texts = pdf[text_col].fillna("").astype(str).tolist()
            embeddings = model.encode(
                texts,
                batch_size=_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )

            output_pdf = pdf[id_cols].copy()
            output_pdf[f"{prefix}_embedding"] = [
                embedding.astype(float).tolist()
                for embedding in embeddings
            ]
            yield output_pdf

    return (
        df
        .select(*input_cols)
        .repartition(_inference_partitions)
        .mapInPandas(encode_batches, schema=output_schema)
        .withColumn(f"{prefix}_embedding_norm", dense_vector(F.col(f"{prefix}_embedding")))
    )


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


def build_cached_text_embeddings(df, id_cols, text_col, prefix, cache_table):
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
        f"{prefix} embedding cache lookup: "
        f"{total_count} rows, {unique_text_count} unique texts, "
        f"{cached_unique_count} cached texts, {missing_count} new texts."
    )

    if missing_count > 0:
        missing_vectors = (
            build_sentence_transformer_embeddings(
                missing_input_df,
                ["embedding_cache_key", "embedding_text"],
                "embedding_text",
                "cache_text",
            )
            .select(
                "embedding_cache_key",
                "embedding_text",
                F.col("cache_text_embedding").alias("text_embedding"),
            )
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
        F.col("text_embedding").alias(f"{prefix}_embedding"),
    ).dropDuplicates(["embedding_cache_key"])

    return (
        input_df
        .join(refreshed_cache_df, on="embedding_cache_key", how="inner")
        .select(
            *id_cols,
            F.col(f"{prefix}_embedding"),
        )
        .withColumn(f"{prefix}_embedding_norm", dense_vector(F.col(f"{prefix}_embedding")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Destination Content Scaffold
# MAGIC
# MAGIC This layer uses fields already available in the advert core and profile
# MAGIC outputs. It deliberately avoids `advert_theme` and `advert_category` as
# MAGIC semantic inputs because those fields are low-information for this use case.
# MAGIC A later version can replace the scaffold with fetched page body text and
# MAGIC real image captions while preserving the same output schema.

# COMMAND ----------

df_advert_daily_core = (
    read_feature_table(advert_daily_core_tbl)
    .where((F.col("feature_date") >= feature_start_date) & (F.col("feature_date") <= feature_end_date))
)

df_advert_attribute_profile = (
    read_feature_table(advert_attribute_profile_tbl)
    .where((F.col("feature_date") >= feature_start_date) & (F.col("feature_date") <= feature_end_date))
)

df_destination_content = (
    df_advert_daily_core.alias("core")
    .join(
        df_advert_attribute_profile.select(
            "feature_date",
            "advert_id",
            "top_brand",
            "top_use",
            "top_colour",
            "top_style",
            "top_category",
            "top_department",
            "top_gender",
            "advert_item_text_corpus",
            "advert_item_count",
        ).alias("profile"),
        on=["feature_date", "advert_id"],
        how="left",
    )
    .select(
        "feature_date",
        "advert_id",
        F.col("core.advert_url").alias("advert_url"),
        F.coalesce("core.headline", "core.advert_title", "profile.top_use", "profile.top_brand").alias("page_title"),
        F.concat_ws(" ", "core.headline", "core.subtext", "core.cta").alias("page_body"),
        F.coalesce("core.flat_jpg", "core.mobile_image", "core.background_image").alias("canonical_image_url"),
        "profile.top_brand",
        "profile.top_use",
        "profile.top_colour",
        "profile.top_style",
        "profile.top_category",
        "profile.top_department",
        "profile.top_gender",
        "profile.advert_item_text_corpus",
        F.coalesce("profile.advert_item_count", F.lit(0)).alias("advert_item_count"),
    )
    .withColumn(
        "image_caption",
        clean_text_col(
            F.concat_ws(
                " ",
                F.lit("advert image"),
                "top_colour",
                "top_brand",
                "top_use",
                "top_style",
                "top_category",
                "top_department",
                "top_gender",
            )
        ),
    )
    .withColumn(
        "advert_text_corpus",
        clean_text_col(
            F.concat_ws(
                " ",
                "page_title",
                "page_body",
                "top_brand",
                "top_use",
                "top_colour",
                "top_style",
                "top_category",
                "top_department",
                "top_gender",
                "advert_item_text_corpus",
                "image_caption",
            )
        ),
    )
    .withColumn("has_destination_image", F.col("canonical_image_url").isNotNull())
    .dropDuplicates(["feature_date", "advert_id"])
)

df_destination_content = with_text_quality_features(
    df_destination_content,
    "advert_text_corpus",
    "advert_semantic",
)

display(df_destination_content.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Semantic Embeddings

# COMMAND ----------

df_advert_semantic_input = materialise_embedding_input(
    df_destination_content,
    advert_semantic_embedding_input_tbl,
    ["feature_date", "advert_id", "advert_text_corpus"],
)

df_advert_semantic_vectors = build_cached_text_embeddings(
    df_advert_semantic_input,
    ["feature_date", "advert_id"],
    "advert_text_corpus",
    "advert_semantic",
    text_embedding_cache_tbl,
)
df_advert_semantic_vectors = cache_with_count(df_advert_semantic_vectors, "advert semantic vectors")

df_advert_semantic_embeddings = (
    df_destination_content
    .join(
        df_advert_semantic_vectors.select(
            "feature_date",
            "advert_id",
            "advert_semantic_embedding_norm",
            "advert_semantic_embedding",
        ),
        on=["feature_date", "advert_id"],
        how="inner",
    )
    .withColumn("advert_has_destination_image", F.col("has_destination_image").cast("int"))
    .withColumn("embedding_model_name", F.lit(hf_embedding_model_name))
    .withColumn("embedding_model_uri", F.lit(embedding_model_uri))
)

for dim_index in range(embedding_feature_dims_for_model):
    df_advert_semantic_embeddings = df_advert_semantic_embeddings.withColumn(
        f"advert_semantic_dim_{dim_index:03d}",
        F.col("advert_semantic_embedding").getItem(dim_index).cast("double"),
    )

display(df_advert_semantic_embeddings.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Item Semantic Embeddings

# COMMAND ----------

df_item_lookup = (
    read_feature_table(item_attribute_lookup_tbl)
    .select(
        "itemno",
        clean_text_col(F.col("item_text_corpus")).alias("item_text_corpus"),
    )
    .where(F.col("item_text_corpus") != "")
    .dropDuplicates(["itemno"])
)

df_item_lookup = with_text_quality_features(
    df_item_lookup,
    "item_text_corpus",
    "item_semantic",
)

df_item_semantic_input = materialise_embedding_input(
    df_item_lookup,
    item_semantic_embedding_input_tbl,
    ["itemno", "item_text_corpus"],
)

df_item_semantic_vectors = build_cached_text_embeddings(
    df_item_semantic_input,
    ["itemno"],
    "item_text_corpus",
    "item_semantic",
    text_embedding_cache_tbl,
)
df_item_semantic_vectors = cache_with_count(df_item_semantic_vectors, "item semantic vectors")

df_item_semantic_embeddings = (
    df_item_lookup
    .join(
        df_item_semantic_vectors.select(
            "itemno",
            "item_semantic_embedding",
            "item_semantic_embedding_norm",
        ),
        on="itemno",
        how="inner",
    )
    .withColumn("embedding_model_name", F.lit(hf_embedding_model_name))
    .withColumn("embedding_model_uri", F.lit(embedding_model_uri))
)

display(df_item_semantic_embeddings.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Embedding Neighbours
# MAGIC
# MAGIC Neighbours are calculated with a Spark SQL self-join over adverts on the
# MAGIC same feature date. The embeddings are already unit-normalised, so cosine
# MAGIC similarity is the dot product. Euclidean distance can be recovered as
# MAGIC `sqrt(2 - 2 * cosine_similarity)`.


# COMMAND ----------

cosine_similarity_threshold = 1.0 - (neighbour_distance_threshold ** 2 / 2.0)

df_similarity_input = df_advert_semantic_embeddings.select(
    "feature_date",
    "advert_id",
    "advert_semantic_embedding",
).where(
    F.col("advert_semantic_embedding").isNotNull()
)

df_neighbour_candidates = (
    df_similarity_input.alias("source")
    .join(
        df_similarity_input.alias("neighbour"),
        (F.col("source.feature_date") == F.col("neighbour.feature_date"))
        & (F.col("source.advert_id") != F.col("neighbour.advert_id")),
        how="inner",
    )
    .select(
        F.col("source.feature_date").alias("feature_date"),
        F.col("source.advert_id").alias("source_advert_id"),
        F.col("neighbour.advert_id").alias("neighbour_advert_id"),
        F.greatest(
            F.lit(-1.0),
            F.least(
                F.lit(1.0),
                F.aggregate(
                    F.zip_with(
                        F.col("source.advert_semantic_embedding"),
                        F.col("neighbour.advert_semantic_embedding"),
                        lambda left_value, right_value: left_value * right_value,
                    ),
                    F.lit(0.0).cast("double"),
                    lambda accumulator, value: accumulator + value,
                ),
            ),
        ).alias("cosine_similarity"),
    )
    .where(F.col("cosine_similarity") >= F.lit(cosine_similarity_threshold))
    .withColumn(
        "euclidean_distance",
        F.sqrt(F.greatest(F.lit(0.0), F.lit(2.0) - (F.lit(2.0) * F.col("cosine_similarity")))),
    )
)

neighbour_rank_window = Window.partitionBy("feature_date", "source_advert_id").orderBy(
    F.col("cosine_similarity").desc(),
    F.col("euclidean_distance").asc(),
    F.col("neighbour_advert_id").asc(),
)

df_advert_embedding_neighbours = (
    df_neighbour_candidates
    .withColumn("neighbour_rank", F.row_number().over(neighbour_rank_window))
    .where(F.col("neighbour_rank") <= top_neighbours_per_advert)
)

df_neighbour_summary = (
    df_advert_embedding_neighbours
    .groupBy(
        F.col("feature_date"),
        F.col("source_advert_id").alias("advert_id"),
    )
    .agg(
        F.count("*").alias("advert_embedding_neighbour_count"),
        F.max("cosine_similarity").alias("advert_embedding_top_similarity"),
        F.avg("cosine_similarity").alias("advert_embedding_avg_similarity"),
    )
)

df_advert_semantic_embeddings = (
    df_advert_semantic_embeddings
    .join(df_neighbour_summary, on=["feature_date", "advert_id"], how="left")
    .withColumn("advert_embedding_neighbour_count", F.coalesce(F.col("advert_embedding_neighbour_count"), F.lit(0)))
    .withColumn("advert_embedding_top_similarity", F.coalesce(F.col("advert_embedding_top_similarity"), F.lit(0.0)))
    .withColumn("advert_embedding_avg_similarity", F.coalesce(F.col("advert_embedding_avg_similarity"), F.lit(0.0)))
)

display(df_advert_embedding_neighbours.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Outputs

# COMMAND ----------

write_output_table(df_destination_content, advert_destination_content_output_tbl)
write_output_table(df_advert_semantic_embeddings, advert_semantic_embeddings_output_tbl)
write_output_table(df_item_semantic_embeddings, item_semantic_embeddings_output_tbl)
write_output_table(df_advert_embedding_neighbours, advert_embedding_neighbours_output_tbl)

# COMMAND ----------

df_output_summary = spark.createDataFrame(
    [
        ("advert_destination_content_rows", df_destination_content.count()),
        ("advert_semantic_embedding_rows", df_advert_semantic_embeddings.count()),
        ("item_semantic_embedding_rows", df_item_semantic_embeddings.count()),
        ("advert_embedding_neighbour_rows", df_advert_embedding_neighbours.count()),
        (
            "advert_rows_with_image",
            df_advert_semantic_embeddings.where(F.col("advert_has_destination_image") == 1).count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_output_summary)
