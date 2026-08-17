"""Build Analytics pCTR, account-advert, and session feature tables."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging

from _registry_job import (
    configure_job_logging,
    feature_write_kwargs,
    log_owned_tables,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.analytics_pctr_source import (
    load_analytics_pctr_source_binding,
    load_analytics_pctr_source_definition,
    read_analytics_pctr_source_binding,
    serialise_source_binding,
)
from next_ads.features.materialization import (
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.nextads_core import resolve_reference_date_from_theme
from next_ads.features.pctr_affinity import (
    build_analytics_pctr_model_input_frame,
    build_account_advert_affinity_frame,
    build_session_context_frame,
)


LOGGER = logging.getLogger(__name__)
BUILDER = "build_pctr_affinity_features"
AFFINITY_TABLE = "next_uk_nextads_fs_account_advert_affinity_daily"
SESSION_TABLE = "next_uk_nextads_fs_session_context_daily"
PCTR_MODEL_INPUT_TABLE = "next_uk_nextads_fs_pctr_model_input"


@dataclass(frozen=True)
class PctrAffinitySourcePaths:
    web_sessions: str
    app_sessions: str
    rpid_accounts: str
    customer_accounts: str
    web_pages: str
    country_mapping: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference_date", default="predict")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--source_catalog", default="marketingdata_prod")
    parser.add_argument("--source_schema", default="warehouse")
    parser.add_argument("--theme_source_catalog", default=None)
    parser.add_argument("--theme_source_schema", default="warehouse")
    parser.add_argument(
        "--theme_table_prefix",
        default="next_uk_nextads_account_theme_foundation",
    )
    parser.add_argument(
        "--analytics_pctr_source_binding",
        default="configs/features/analytics_pctr_source_personal_dev.yaml",
    )
    parser.add_argument("--analytics_pctr_source_catalog", default=None)
    parser.add_argument("--analytics_pctr_source_schema", default=None)
    parser.add_argument(
        "--analytics_pctr_receipt_correlation_id",
        required=True,
    )
    parser.add_argument("--country_mapping_schema", default="search")
    parser.add_argument("--replace_reference_date", default="true")
    parser.add_argument("--feature_build_id", default=None)
    parser.add_argument("--feature_build_attempt_id", default=None)
    parser.add_argument("--git_commit", default=None)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def resolve_source_paths(args: argparse.Namespace) -> PctrAffinitySourcePaths:
    return PctrAffinitySourcePaths(
        web_sessions=(
            f"{args.source_catalog}.{args.source_schema}.bq_sessions_next_uk"
        ),
        app_sessions=(
            f"{args.source_catalog}.{args.source_schema}."
            "bq_sessions_next_uk_app"
        ),
        rpid_accounts=(
            f"{args.source_catalog}.{args.source_schema}.rpid_with_accounts"
        ),
        customer_accounts=(
            f"{args.source_catalog}.{args.source_schema}.svoccust"
        ),
        web_pages=(
            f"{args.source_catalog}.{args.source_schema}.bq_pages_next_uk"
        ),
        country_mapping=(
            f"{args.source_catalog}.{args.country_mapping_schema}."
            "nov_country_mapping"
        ),
    )


def read_session_source_frames(
    spark,
    paths: PctrAffinitySourcePaths,
) -> dict[str, object]:
    return {
        "web_sessions": spark.table(paths.web_sessions),
        "app_sessions": spark.table(paths.app_sessions),
        "account_mappings": spark.table(paths.rpid_accounts),
        "customer_accounts": spark.table(paths.customer_accounts),
        "page_events": spark.table(paths.web_pages),
        "country_mapping": spark.table(paths.country_mapping),
    }


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = resolve_reference_date_from_theme(spark, args)
    replace_reference_date = args.replace_reference_date.lower() == "true"
    source_paths = resolve_source_paths(args)
    source_definition = load_analytics_pctr_source_definition(
        args.analytics_pctr_source_binding,
        catalog=args.analytics_pctr_source_catalog,
        schema=args.analytics_pctr_source_schema,
    )
    source_definition.validate_target(
        catalog=target_catalog,
        schema=target_schema,
    )

    source_binding = load_analytics_pctr_source_binding(
        spark,
        definition=source_definition,
        receipt_correlation_id=(
            args.analytics_pctr_receipt_correlation_id
        ),
        reference_date=reference_date,
    )
    analytics_output = read_analytics_pctr_source_binding(
        spark,
        definition=source_definition,
        binding=source_binding,
    )
    LOGGER.info(
        "FEATURE_STORE_SOURCE_BINDING=%s",
        serialise_source_binding(source_binding),
    )
    session_sources = read_session_source_frames(spark, source_paths)

    writes = {
        AFFINITY_TABLE: build_account_advert_affinity_frame(
            analytics_output,
            reference_date,
        ),
        PCTR_MODEL_INPUT_TABLE: (
            build_analytics_pctr_model_input_frame(
                analytics_output,
                reference_date,
            )
        ),
        SESSION_TABLE: build_session_context_frame(
            session_sources["web_sessions"],
            session_sources["app_sessions"],
            session_sources["account_mappings"],
            session_sources["customer_accounts"],
            session_sources["page_events"],
            session_sources["country_mapping"],
            reference_date,
        ),
    }
    validate_builder_output_tables(BUILDER, writes, registry)
    feature_engineering_client = create_feature_engineering_client()

    for table_name, frame in writes.items():
        table = registry.table_spec(table_name)
        table_path = write_feature_table(
            spark,
            table_name,
            frame,
            catalog=target_catalog,
            schema=target_schema,
            reference_date=reference_date,
            reference_date_column=table.timestamp_key,
            replace_reference_date=replace_reference_date,
            mode=table.write_mode,
            registry=registry,
            feature_engineering_client=feature_engineering_client,
            **feature_write_kwargs(args),
        )
        LOGGER.info("Wrote pCTR feature table: %s", table_path)


if __name__ == "__main__":
    main()
