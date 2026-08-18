"""Build the complete current product embedding Feature Store snapshot."""

from __future__ import annotations

import json
import logging

from _registry_job import (
    configure_job_logging,
    feature_group_identity,
    log_owned_tables,
    parse_common_args,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.analytics_pctr_source import parse_reference_date
from next_ads.features.embedding_contract import (
    load_product_embedding_definition,
    load_product_embedding_materialization_binding,
    validate_materialization_binding_target,
)
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
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


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
    identity = feature_group_identity(args, BUILDER)
    pinned_spark = PinnedSourceSession(
        spark,
        feature_build_id=identity["feature_build_id"],
        feature_build_attempt_id=identity["feature_build_attempt_id"],
        reference_date=parse_reference_date(reference_date),
        target_catalog=target_catalog,
        target_schema=target_schema,
        registry=registry,
        allow_unready_feature_ids=(OUTPUT_TABLE,),
    )
    definition = load_product_embedding_definition()
    binding = load_product_embedding_materialization_binding(
        args.product_embedding_binding
    )
    validate_materialization_binding_target(
        binding,
        catalog=target_catalog,
        schema=target_schema,
    )
    validate_builder_output_tables(BUILDER, (OUTPUT_TABLE,), registry)

    runtime_evidence = validate_materialization_runtime(spark, definition)
    import mlflow

    model_path, model_evidence = prepare_validated_model_for_executors(
        mlflow,
        binding,
        definition,
    )
    product_source = pinned_spark.table(
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
    existing = pinned_spark.table(target_path)
    output, build_evidence = build_product_embeddings_frame(
        product_text,
        existing,
        binding=binding,
        model_path=model_path,
    )
    _ready_build, snapshot = write_and_publish_feature_group(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        group_id=BUILDER,
        reference_date=parse_reference_date(reference_date),
        frames={OUTPUT_TABLE: output},
        sources=pinned_spark.source_bindings,
        registry=registry,
        **identity,
    )
    manifest = {
        "status": "PASS",
        "reference_date": reference_date,
        "feature_snapshot_id": snapshot.feature_snapshot_id,
        "feature_snapshot_attempt_id": (
            snapshot.feature_snapshot_attempt_id
        ),
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
