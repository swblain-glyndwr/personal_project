"""Build daily semantic advert profiles from repository-owned text."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
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
from next_ads.features.advert_items import build_advert_item_bridge
from next_ads.features.advert_semantic import (
    build_advert_image_flags,
    build_advert_semantic_profile_frame,
    build_advert_semantic_text_source,
    build_advert_semantic_vector_frame,
    select_exact_product_text,
)
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
    prepare_validated_model_for_executors,
    validate_materialization_runtime,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)

BUILDER = "build_advert_semantic_profile_daily"
OUTPUT_TABLE = "next_uk_nextads_fs_advert_semantic_profile_daily"
ADVERT_CORE_TABLE = "next_uk_nextads_fs_advert_core_daily"
ADVERT_ATTRIBUTE_TABLE = (
    "next_uk_nextads_fs_advert_attribute_profile_daily"
)
PRODUCT_EMBEDDING_TABLE = "next_uk_nextads_fs_product_embeddings_latest"
LEGACY_SORT_HISTORY_SCHEMA_DDL = (
    "UniqueAdID STRING, items STRING, item_pos BIGINT, rundate DATE"
)
BUILD_MANIFEST_PREFIX = "ADVERT_SEMANTIC_BUILD="


def source_cutoff_date(reference_date: str | date) -> str:
    """Use only assignment inputs available before feature date D."""
    if isinstance(reference_date, date):
        resolved = reference_date
    else:
        try:
            resolved = date.fromisoformat(str(reference_date))
        except ValueError as exc:
            raise ValueError("reference_date must be YYYY-MM-DD") from exc
    return (resolved - timedelta(days=1)).isoformat()


@dataclass(frozen=True)
class AdvertSemanticSourcePaths:
    """Physical repository and warehouse inputs used by this builder."""

    advert_core: str
    advert_attributes: str
    product_embeddings: str
    existing_profiles: str
    control_sheet_latest: str
    v2_sort_history: str
    representative_items: str
    v2_control: str
    v1_control: str


def resolve_advert_semantic_source_paths(
    *,
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
) -> AdvertSemanticSourcePaths:
    """Resolve feature tables plus canonical advert and image sources."""
    return AdvertSemanticSourcePaths(
        advert_core=source_table(
            target_catalog,
            target_schema,
            ADVERT_CORE_TABLE,
        ),
        advert_attributes=source_table(
            target_catalog,
            target_schema,
            ADVERT_ATTRIBUTE_TABLE,
        ),
        product_embeddings=source_table(
            target_catalog,
            target_schema,
            PRODUCT_EMBEDDING_TABLE,
        ),
        existing_profiles=source_table(
            target_catalog,
            target_schema,
            OUTPUT_TABLE,
        ),
        control_sheet_latest=source_table(
            source_catalog,
            source_schema,
            "next_uk_nextads_control_sheet_latest",
        ),
        v2_sort_history=source_table(
            source_catalog,
            source_schema,
            "nextads_sort_order_v2",
        ),
        representative_items=source_table(
            source_catalog,
            source_schema,
            "next_uk_nextads_ad_items",
        ),
        v2_control=source_table(
            source_catalog,
            source_schema,
            "next_uk_nextads_control_sheet_v2",
        ),
        v1_control=source_table(
            source_catalog,
            source_schema,
            "next_uk_nextads_control_sheet",
        ),
    )


def read_advert_semantic_source_frames(
    spark,
    *,
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
) -> dict[str, object]:
    """Read all registered text, image, bridge, and cache inputs."""
    paths = resolve_advert_semantic_source_paths(
        source_catalog=source_catalog,
        source_schema=source_schema,
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    return {
        "advert_core": spark.table(paths.advert_core),
        "advert_attributes": spark.table(paths.advert_attributes),
        "product_embeddings": spark.table(paths.product_embeddings),
        "existing_profiles": spark.table(paths.existing_profiles),
        "control_sheet_latest": spark.table(paths.control_sheet_latest),
        "v2_sort_history": spark.table(paths.v2_sort_history),
        "legacy_sort_history": spark.createDataFrame(
            [],
            LEGACY_SORT_HISTORY_SCHEMA_DDL,
        ),
        "representative_items": spark.table(paths.representative_items),
        "v2_control": spark.table(paths.v2_control),
        "v1_control": spark.table(paths.v1_control),
    }


def _reference_date_partition(frame, reference_date):
    from pyspark.sql import functions as F

    return frame.where(
        F.to_date("feature_date") == F.lit(reference_date).cast("date")
    )


def main() -> None:
    """Build text, exact vectors, semantic evidence, and publish the result."""
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
    replace_reference_date = args.replace_reference_date.lower() == "true"
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
    sources = read_advert_semantic_source_frames(
        pinned_spark,
        source_catalog=args.source_catalog,
        source_schema=args.source_schema,
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    bridge = build_advert_item_bridge(
        v2_sort_history=sources["v2_sort_history"],
        legacy_sort_history=sources["legacy_sort_history"],
        representative_items=sources["representative_items"],
        v2_control=sources["v2_control"],
        v1_control=sources["v1_control"],
        feature_date=reference_date,
        cutoff_date=source_cutoff_date(reference_date),
    )
    image_flags = build_advert_image_flags(
        sources["control_sheet_latest"],
        reference_date,
    )
    product_text = select_exact_product_text(
        sources["product_embeddings"],
        binding,
    )
    text_source = build_advert_semantic_text_source(
        _reference_date_partition(sources["advert_core"], reference_date),
        _reference_date_partition(
            sources["advert_attributes"],
            reference_date,
        ),
        bridge,
        product_text,
        image_flags,
    )
    vectors, build_evidence = build_advert_semantic_vector_frame(
        text_source,
        sources["existing_profiles"],
        binding=binding,
        model_path=model_path,
    )
    profiles = build_advert_semantic_profile_frame(text_source, vectors)
    _ready_build, snapshot = write_and_publish_feature_group(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        group_id=BUILDER,
        reference_date=parse_reference_date(reference_date),
        frames={OUTPUT_TABLE: profiles},
        sources=pinned_spark.source_bindings,
        registry=registry,
        replace_reference_date=replace_reference_date,
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
