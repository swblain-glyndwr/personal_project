"""Distributed inference for immutable product embedding snapshots."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import sys
from uuid import uuid4

from next_ads.features.embedding_contract import (
    EXPECTED_EMBEDDING_DIMENSION,
)
from next_ads.features.embedding_runtime import (
    installed_package_versions,
    resolve_runtime_version,
    resolve_validated_sentence_transformer_artifact,
    validate_cpu_torch,
    validate_runtime_environment,
    validate_safe_model_artifacts,
    validate_threadpool_runtime,
)


PRODUCT_EMBEDDING_OUTPUT_COLUMNS = (
    "item_id",
    "embedding_model_name",
    "embedding_model_version",
    "embedding_model_uri",
    "embedding_source_run_id",
    "embedding_artifact_sha256",
    "embedding",
    "embedding_dimension",
    "embedding_text_hash",
    "embedding_text",
    "created_at",
    "updated_at",
)


@dataclass(frozen=True)
class ProductEmbeddingBuildEvidence:
    """Counts proving one complete product snapshot was assembled."""

    source_row_count: int
    reused_row_count: int
    generated_row_count: int
    output_row_count: int


def validate_materialization_runtime(
    spark,
    definition,
    *,
    torch_module=None,
    threadpoolctl_module=None,
) -> dict[str, object]:
    """Run the exact DBR 15.4 dependency checks before table work."""
    if torch_module is None:
        import torch as torch_module
    if threadpoolctl_module is None:
        import threadpoolctl as threadpoolctl_module

    runtime_version = resolve_runtime_version(spark)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    expected_packages = dict(definition.runtime_profile.package_versions)
    package_versions = installed_package_versions(expected_packages)
    validate_runtime_environment(
        definition,
        runtime_version=runtime_version,
        python_version=python_version,
        package_versions=package_versions,
    )
    return {
        "runtime_version": runtime_version,
        "python_version": python_version,
        "package_versions": package_versions,
        **validate_cpu_torch(torch_module),
        **validate_threadpool_runtime(threadpoolctl_module),
    }


def validate_promoted_model_provenance(version_info, binding) -> None:
    """Require the shared version to point to the recorded source version."""
    tags = dict(getattr(version_info, "tags", {}) or {})
    expected_tags = {
        "source_registered_model_name": (
            binding.source_registered_model_name
        ),
        "source_model_version": str(
            binding.source_registered_model_version
        ),
    }
    mismatches = [
        f"{key}: expected {expected!r}, found {tags.get(key)!r}"
        for key, expected in expected_tags.items()
        if tags.get(key) != expected
    ]
    if mismatches:
        raise ValueError(
            "Shared product embedding model provenance does not match: "
            + "; ".join(mismatches)
        )


def _staging_directory(binding) -> Path:
    model_token = re.sub(
        r"[^a-zA-Z0-9]+",
        "_",
        binding.model.registered_model_name,
    ).strip("_")
    return (
        Path(binding.model_staging_root)
        / "approved"
        / model_token
        / (
            f"v{binding.model.registered_model_version}_"
            f"{binding.artifact_sha256}"
        )
    )


def _validate_staged_artifact(path: Path, expected_digest: str) -> None:
    evidence = validate_safe_model_artifacts(path)
    if evidence["artifact_sha256"] != expected_digest:
        raise ValueError(
            "Staged product embedding model digest does not match the "
            f"approved artifact: {path}"
        )


def stage_validated_model_artifact(
    source_model_path: Path,
    binding,
) -> Path:
    """Copy one validated artifact to a digest-scoped executor path."""
    target = _staging_directory(binding)
    if target.exists():
        _validate_staged_artifact(target, binding.artifact_sha256)
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid4().hex}.staging"
    shutil.copytree(source_model_path, temporary)
    _validate_staged_artifact(temporary, binding.artifact_sha256)
    try:
        temporary.rename(target)
    except FileExistsError:
        _validate_staged_artifact(target, binding.artifact_sha256)
    return target


def prepare_validated_model_for_executors(
    mlflow_module,
    binding,
    definition,
) -> tuple[Path, dict[str, object]]:
    """Validate the exact shared version and stage its safe model files."""
    mlflow_module.set_tracking_uri("databricks")
    mlflow_module.set_registry_uri("databricks-uc")
    client = mlflow_module.MlflowClient()
    version_info = client.get_model_version(
        name=binding.model.registered_model_name,
        version=str(binding.model.registered_model_version),
    )
    validate_promoted_model_provenance(version_info, binding)
    model_path, evidence = resolve_validated_sentence_transformer_artifact(
        mlflow_module,
        binding,
        definition,
    )
    staged_path = stage_validated_model_artifact(model_path, binding)
    return staged_path, {
        **evidence,
        "registered_model_name": binding.model.registered_model_name,
        "registered_model_version": binding.model.registered_model_version,
        "model_uri": binding.model.model_uri,
        "staged_model_path": str(staged_path),
    }


def _valid_embedding_expression(F, column_name: str):
    vector = F.col(column_name)
    non_finite = F.exists(
        vector,
        lambda value: value.isNull()
        | F.isnan(value)
        | (F.abs(value) == F.lit(float("inf"))),
    )
    squared_norm = F.aggregate(
        vector,
        F.lit(0.0).cast("double"),
        lambda total, value: total + (value * value),
    )
    return (
        vector.isNotNull()
        & (F.size(vector) == F.lit(EXPECTED_EMBEDDING_DIMENSION))
        & ~non_finite
        & (F.abs(F.sqrt(squared_norm) - F.lit(1.0)) <= F.lit(1e-5))
    )


def _raise_for_invalid_product_source(source) -> int:
    from pyspark.sql import functions as F

    required = {"item_id", "embedding_text", "embedding_text_hash"}
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(
            "Product embedding source is missing columns: "
            + ", ".join(missing)
        )
    invalid = (
        F.col("item_id").isNull()
        | (F.trim("item_id") == "")
        | F.col("embedding_text").isNull()
        | (F.trim("embedding_text") == "")
        | ~F.col("embedding_text_hash").rlike("^[0-9a-f]{64}$")
    )
    if source.where(invalid).limit(1).collect():
        raise ValueError(
            "Product embedding source contains a blank key, text, or invalid "
            "text hash"
        )
    row_count = source.count()
    if row_count == 0:
        raise ValueError(
            "Product embedding source is empty; refusing a full replacement"
        )
    distinct_count = source.select("item_id").distinct().count()
    if distinct_count != row_count:
        raise ValueError("Product embedding source contains duplicate item_id")
    return row_count


def _exact_model_cache(existing, *, binding, F):
    required = {
        "item_id",
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "embedding",
        "embedding_dimension",
        "embedding_text_hash",
        "created_at",
    }
    missing = sorted(required.difference(existing.columns))
    if missing:
        raise ValueError(
            "Existing product embedding cache is missing columns: "
            + ", ".join(missing)
        )

    current_model = existing.where(
        (F.col("embedding_model_name") == F.lit(
            binding.model.registered_model_name
        ))
        & (F.col("embedding_model_version") == F.lit(
            str(binding.model.registered_model_version)
        ))
    )
    duplicate = (
        current_model.groupBy(
            "item_id",
            "embedding_model_name",
            "embedding_model_version",
        )
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise ValueError(
            "Existing product embedding cache contains a duplicate exact "
            "model key"
        )
    return current_model


def _encode_missing_products(
    missing,
    *,
    model_path: Path,
    inference_partitions: int,
    inference_batch_size: int,
):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("item_id", T.StringType(), False),
            T.StructField("embedding_text", T.StringType(), False),
            T.StructField("embedding_text_hash", T.StringType(), False),
            T.StructField(
                "embedding",
                T.ArrayType(T.DoubleType(), containsNull=False),
                False,
            ),
        ]
    )
    executor_model_path = str(model_path)

    def encode_batches(iterator):
        os.environ["USER"] = os.environ.get("USER") or "spark"
        os.environ["TORCHINDUCTOR_CACHE_DIR"] = os.environ.get(
            "TORCHINDUCTOR_CACHE_DIR",
            "/tmp/torchinductor_cache",
        )
        from sentence_transformers import SentenceTransformer

        if not hasattr(encode_batches, "_model"):
            encode_batches._model = SentenceTransformer(
                executor_model_path,
                device="cpu",
                trust_remote_code=False,
            )
        model = encode_batches._model
        for batch in iterator:
            embeddings = model.encode(
                batch["embedding_text"].fillna("").astype(str).tolist(),
                batch_size=inference_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            yield batch.assign(embedding=embeddings.astype(float).tolist())

    return missing.repartition(inference_partitions).mapInPandas(
        encode_batches,
        schema=schema,
    )


def build_product_embeddings_frame(
    source,
    existing,
    *,
    binding,
    model_path: Path,
) -> tuple[object, ProductEmbeddingBuildEvidence]:
    """Reuse exact rows, infer changed products, and return a full snapshot."""
    from pyspark.sql import functions as F

    source = source.select(
        F.col("item_id").cast("string"),
        F.col("embedding_text").cast("string"),
        F.col("embedding_text_hash").cast("string"),
    ).cache()
    source_row_count = _raise_for_invalid_product_source(source)

    model_name = binding.model.registered_model_name
    model_version = str(binding.model.registered_model_version)
    current_model = _exact_model_cache(existing, binding=binding, F=F)
    valid_existing = (
        current_model.where(
            F.col("embedding_model_uri") == F.lit(binding.model.model_uri)
        )
        .where(
            F.col("embedding_source_run_id")
            == F.lit(binding.source_run_id)
        )
        .where(
            F.col("embedding_artifact_sha256")
            == F.lit(binding.artifact_sha256)
        )
        .where(
            F.col("embedding_dimension")
            == F.lit(EXPECTED_EMBEDDING_DIMENSION)
        )
        .where(_valid_embedding_expression(F, "embedding"))
        .where(F.col("created_at").isNotNull())
        .select(
            "item_id",
            "embedding_text_hash",
            "embedding",
            "created_at",
        )
    )
    reused = source.join(
        valid_existing,
        on=["item_id", "embedding_text_hash"],
        how="inner",
    ).select(
        "item_id",
        "embedding_text",
        "embedding_text_hash",
        "embedding",
        "created_at",
    )
    reused_row_count = reused.count()
    missing = source.join(
        reused.select("item_id"),
        on="item_id",
        how="left_anti",
    ).select("item_id", "embedding_text", "embedding_text_hash")
    generated_row_count = source_row_count - reused_row_count
    if generated_row_count:
        generated = _encode_missing_products(
            missing,
            model_path=model_path,
            inference_partitions=min(
                binding.inference_partitions,
                generated_row_count,
            ),
            inference_batch_size=binding.inference_batch_size,
        ).withColumn("created_at", F.current_timestamp())
        assembled = reused.unionByName(generated)
    else:
        assembled = reused

    build_time = F.current_timestamp()
    output = (
        assembled.withColumn("embedding_model_name", F.lit(model_name))
        .withColumn("embedding_model_version", F.lit(model_version))
        .withColumn("embedding_model_uri", F.lit(binding.model.model_uri))
        .withColumn(
            "embedding_source_run_id",
            F.lit(binding.source_run_id),
        )
        .withColumn(
            "embedding_artifact_sha256",
            F.lit(binding.artifact_sha256),
        )
        .withColumn(
            "embedding_dimension",
            F.lit(EXPECTED_EMBEDDING_DIMENSION).cast("int"),
        )
        .withColumn("updated_at", build_time)
        .select(*PRODUCT_EMBEDDING_OUTPUT_COLUMNS)
        .localCheckpoint(eager=True)
    )
    invalid_output = output.where(
        ~_valid_embedding_expression(F, "embedding")
    ).limit(1).collect()
    if invalid_output:
        raise ValueError(
            "Product embedding inference produced an invalid 384-value "
            "L2-normalised vector"
        )
    output_row_count = output.count()
    if output_row_count != source_row_count:
        raise ValueError(
            "Product embedding output does not cover the current product "
            f"source: source={source_row_count}, output={output_row_count}"
        )
    return output, ProductEmbeddingBuildEvidence(
        source_row_count=source_row_count,
        reused_row_count=reused_row_count,
        generated_row_count=generated_row_count,
        output_row_count=output_row_count,
    )


__all__ = [
    "PRODUCT_EMBEDDING_OUTPUT_COLUMNS",
    "ProductEmbeddingBuildEvidence",
    "build_product_embeddings_frame",
    "prepare_validated_model_for_executors",
    "stage_validated_model_artifact",
    "validate_materialization_runtime",
    "validate_promoted_model_provenance",
]
