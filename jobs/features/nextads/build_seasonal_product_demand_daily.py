"""Build daily seasonal product demand for account and advert membership."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging

from _registry_job import (
    configure_job_logging,
    feature_write_kwargs,
    log_owned_tables,
    parse_common_args,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.advert_items import build_advert_item_bridge
from next_ads.features.embedding_contract import (
    load_product_embedding_materialization_binding,
    validate_materialization_binding_target,
)
from next_ads.features.materialization import (
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.nextads_core import (
    resolve_reference_date_from_theme,
    source_table,
)
from next_ads.features.seasonal_product_demand import (
    build_seasonal_product_demand_frame,
)


LOGGER = logging.getLogger(__name__)

BUILDER = "build_seasonal_product_demand_daily"
OUTPUT_TABLE = "next_uk_nextads_fs_seasonal_product_demand_daily"
PRODUCT_EMBEDDING_TABLE = "next_uk_nextads_fs_product_embeddings_latest"
LEGACY_SORT_HISTORY_SCHEMA_DDL = (
    "UniqueAdID STRING, items STRING, item_pos BIGINT, rundate DATE"
)


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
class SeasonalProductDemandSourcePaths:
    """Physical inputs used by the registered seasonal-demand builder."""

    account_views: str
    account_purchases: str
    v2_sort_history: str
    representative_items: str
    v2_control: str
    v1_control: str
    product_embeddings: str


def resolve_seasonal_product_demand_source_paths(
    *,
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
) -> SeasonalProductDemandSourcePaths:
    """Resolve warehouse events, canonical bridge inputs, and embeddings."""
    return SeasonalProductDemandSourcePaths(
        account_views=source_table(
            source_catalog,
            source_schema,
            "bq_views_sessions_next_uk_with_accounts",
        ),
        account_purchases=source_table(
            source_catalog,
            source_schema,
            "baskets_uk_3y",
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
        product_embeddings=source_table(
            target_catalog,
            target_schema,
            PRODUCT_EMBEDDING_TABLE,
        ),
    )


def read_seasonal_product_demand_source_frames(
    spark,
    *,
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
) -> dict[str, object]:
    """Read live inputs and supply a typed empty legacy sort source."""
    paths = resolve_seasonal_product_demand_source_paths(
        source_catalog=source_catalog,
        source_schema=source_schema,
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    return {
        "account_views": spark.table(paths.account_views),
        "account_purchases": spark.table(paths.account_purchases),
        "v2_sort_history": spark.table(paths.v2_sort_history),
        "legacy_sort_history": spark.createDataFrame(
            [],
            LEGACY_SORT_HISTORY_SCHEMA_DDL,
        ),
        "representative_items": spark.table(paths.representative_items),
        "v2_control": spark.table(paths.v2_control),
        "v1_control": spark.table(paths.v1_control),
        "product_embeddings": spark.table(paths.product_embeddings),
    }


def main() -> None:
    """Build and publish the registered seasonal-demand date partition."""
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    embedding_binding = load_product_embedding_materialization_binding(
        args.product_embedding_binding
    )
    validate_materialization_binding_target(
        embedding_binding,
        catalog=target_catalog,
        schema=target_schema,
    )
    reference_date = resolve_reference_date_from_theme(spark, args)
    replace_reference_date = args.replace_reference_date.lower() == "true"
    feature_engineering_client = create_feature_engineering_client()

    validate_builder_output_tables(
        BUILDER,
        (OUTPUT_TABLE,),
        registry,
    )
    sources = read_seasonal_product_demand_source_frames(
        spark,
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
    seasonal_demand = build_seasonal_product_demand_frame(
        account_views=sources["account_views"],
        account_purchases=sources["account_purchases"],
        advert_item_bridge=bridge,
        product_embeddings=sources["product_embeddings"],
        approved_binding=embedding_binding,
        reference_date=reference_date,
    )

    table = registry.table_spec(OUTPUT_TABLE)
    table_path = write_feature_table(
        spark,
        OUTPUT_TABLE,
        seasonal_demand,
        catalog=target_catalog,
        schema=target_schema,
        reference_date=reference_date,
        reference_date_column=table.timestamp_key or "feature_date",
        replace_reference_date=replace_reference_date,
        mode=table.write_mode,
        registry=registry,
        feature_engineering_client=feature_engineering_client,
        **feature_write_kwargs(args),
    )
    LOGGER.info("Wrote seasonal product demand table: %s", table_path)


if __name__ == "__main__":
    main()
