"""Build the complete current product embedding Feature Store snapshot."""

from __future__ import annotations

import json
import logging

from _registry_job import (
    configure_job_logging,
    log_owned_tables,
    parse_common_args,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.embedding_contract import (
    load_product_embedding_definition,
    load_product_embedding_materialization_binding,
)
from next_ads.features.materialization import write_feature_table
from next_ads.features.nextads_core import (
    resolve_reference_date_from_theme,
    source_table,
)
from next_ads.features.product_embedding_inference import (
    build_product_embeddings_frame,
    prepare_validated_model_for_executors,
    validate_materialization_runtime,
)
from next_ads.features.product_embedding_transforms import (
    build_current_product_text_source,
)


LOGGER = logging.getLogger(__name__)

BUILDER = "build_product_embeddings_latest"
OUTPUT_TABLE = "next_uk_nextads_fs_product_embeddings_latest"
PRODUCT_SOURCE_TABLE = "product_catalog_history"
BUILD_MANIFEST_PREFIX = "PRODUCT_EMBEDDING_BUILD="


def main() -> None:
    """Build and atomically replace the latest product embedding snapshot."""
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = resolve_reference_date_from_theme(spark, args)
    definition = load_product_embedding_definition()
    binding = load_product_embedding_materialization_binding()
    validate_builder_output_tables(BUILDER, (OUTPUT_TABLE,), registry)

    runtime_evidence = validate_materialization_runtime(spark, definition)
    import mlflow

    model_path, model_evidence = prepare_validated_model_for_executors(
        mlflow,
        binding,
        definition,
    )
    product_source = spark.table(
        source_table(
            args.source_catalog,
            args.source_schema,
            PRODUCT_SOURCE_TABLE,
        )
    )
    product_text = build_current_product_text_source(
        product_source,
        reference_date=reference_date,
    )
    target_path = registry.resolved_table_path(
        OUTPUT_TABLE,
        catalog=target_catalog,
        schema=target_schema,
    )
    existing = spark.table(target_path)
    output, build_evidence = build_product_embeddings_frame(
        product_text,
        existing,
        binding=binding,
        model_path=model_path,
    )
    table_path = write_feature_table(
        spark,
        OUTPUT_TABLE,
        output,
        catalog=target_catalog,
        schema=target_schema,
        reference_date=reference_date,
        mode=registry.table_spec(OUTPUT_TABLE).write_mode,
        registry=registry,
    )
    manifest = {
        "status": "PASS",
        "reference_date": reference_date,
        "table_path": table_path,
        **build_evidence.__dict__,
        **runtime_evidence,
        **model_evidence,
    }
    LOGGER.info(
        "%s%s",
        BUILD_MANIFEST_PREFIX,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()
