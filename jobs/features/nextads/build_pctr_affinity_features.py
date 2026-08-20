"""Build Analytics pCTR, account-advert, and session feature tables."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
import re

from _registry_job import (
    configure_job_logging,
    feature_write_kwargs,
    log_owned_tables,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    schema_checksum,
)
from next_ads.common.output_locations import log_output_location
from next_ads.features import load_feature_store_registry
from next_ads.features.analytics_pctr_source import (
    latest_delta_version,
    load_analytics_pctr_source_binding,
    load_analytics_pctr_source_definition,
    read_analytics_pctr_source_binding,
    parse_reference_date,
    serialise_source_binding,
)
from next_ads.features.feature_build_store import persist_feature_build
from next_ads.features.feature_builds import mark_feature_build_failed
from next_ads.features.materialization import (
    FeatureMaterializationResult,
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.nextads_core import resolve_reference_date_from_theme
from next_ads.features.pctr_affinity import (
    build_analytics_pctr_model_input_frame,
    build_account_advert_affinity_frame,
    build_session_context_frame,
)
from next_ads.features.snapshot_publication import (
    begin_feature_build,
    external_delta_source,
    publish_ready_feature_group,
)


LOGGER = logging.getLogger(__name__)
BUILDER = "build_pctr_affinity_features"
AFFINITY_TABLE = "next_uk_nextads_fs_account_advert_affinity_daily"
SESSION_TABLE = "next_uk_nextads_fs_session_context_daily"
PCTR_MODEL_INPUT_TABLE = "next_uk_nextads_fs_pctr_model_input"
ANALYTICS_PCTR_SNAPSHOT_FEATURES = (
    AFFINITY_TABLE,
    PCTR_MODEL_INPUT_TABLE,
    SESSION_TABLE,
)
FAILURE_INJECTION_NONE = "none"
FAILURE_INJECTION_AFTER_FIRST_WRITE = "after_first_write"


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
    parser.add_argument("--publish_ready_snapshot", default="false")
    parser.add_argument("--failure_injection", default=FAILURE_INJECTION_NONE)
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


def read_pinned_session_sources(
    spark,
    paths: PctrAffinitySourcePaths,
    *,
    feature_build_id: str,
    feature_build_attempt_id: str,
    reference_date,
    captured_at: datetime,
    target_catalog: str,
    target_schema: str,
):
    """Read every session input from one immutable Delta version."""
    path_by_role = {
        "web_sessions": paths.web_sessions,
        "app_sessions": paths.app_sessions,
        "account_mappings": paths.rpid_accounts,
        "customer_accounts": paths.customer_accounts,
        "page_events": paths.web_pages,
        "country_mapping": paths.country_mapping,
    }
    frames = {}
    sources = []
    for source_name, table_path in path_by_role.items():
        source_type = spark.catalog.getTable(table_path).tableType.upper()
        pinned_table = table_path
        if source_type in {"VIEW", "TEMPORARY"}:
            pinned_table = snapshot_view_source(
                spark,
                source_name=source_name,
                source_view=table_path,
                feature_build_attempt_id=feature_build_attempt_id,
                target_catalog=target_catalog,
                target_schema=target_schema,
            )
        delta_version = latest_delta_version(spark, pinned_table)
        frame = spark.read.option("versionAsOf", delta_version).table(
            pinned_table
        )
        frames[source_name] = frame
        sources.append(
            external_delta_source(
                feature_build_id=feature_build_id,
                feature_build_attempt_id=feature_build_attempt_id,
                reference_date=reference_date,
                source_name=source_name,
                source_table=pinned_table,
                delta_version=delta_version,
                schema_checksum_value=schema_checksum(frame),
                captured_at=captured_at,
            )
        )
    return frames, tuple(sources)


def snapshot_view_source(
    spark,
    *,
    source_name: str,
    source_view: str,
    feature_build_attempt_id: str,
    target_catalog: str,
    target_schema: str,
) -> str:
    """Materialise a view once so a build can retain its exact input."""
    safe_source_name = re.sub(r"[^a-z0-9_]", "_", source_name.lower())
    identity = hashlib.sha256(
        f"{source_view}:{feature_build_attempt_id}".encode("utf-8")
    ).hexdigest()[:16]
    table_name = f"next_uk_nextads_fs_source_{safe_source_name}_{identity}"
    target = f"{target_catalog}.{target_schema}.{table_name}"
    if spark.catalog.tableExists(target):
        log_output_location(
            target,
            kind="delta_table",
            details={"reused": True, "role": "pinned_source"},
        )
        return target
    (
        spark.table(source_view)
        .write.format("delta")
        .mode("errorifexists")
        .saveAsTable(target)
    )
    escaped_source_view = source_view.replace("'", "''")
    spark.sql(
        "ALTER TABLE "
        f"{quote_qualified_identifier(target)} SET TBLPROPERTIES "
        f"('nextads.source_view' = '{escaped_source_view}')"
    )
    log_output_location(
        target,
        kind="delta_table",
        details={"reused": False, "role": "pinned_source"},
    )
    return target


def validate_failure_injection(
    failure_injection: str,
    *,
    catalog: str,
    schema: str,
) -> str:
    """Allow controlled failure evidence only in a personal DEV schema."""
    mode = failure_injection.strip().lower()
    allowed = {FAILURE_INJECTION_NONE, FAILURE_INJECTION_AFTER_FIRST_WRITE}
    if mode not in allowed:
        raise ValueError(f"Unsupported failure injection: {failure_injection}")
    protected_schemas = {
        "nextads_feature_store",
        "nextads_integration",
        "ds_sandbox",
    }
    if mode != FAILURE_INJECTION_NONE and (
        catalog != "marketingdata_dev" or schema.lower() in protected_schemas
    ):
        raise ValueError(
            "Feature publication failure injection is restricted to a "
            "personal DEV schema"
        )
    return mode


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
    publish_ready_snapshot = (
        getattr(args, "publish_ready_snapshot", "false").strip().lower()
        == "true"
    )
    write_identity = feature_write_kwargs(args)
    failure_injection = validate_failure_injection(
        getattr(args, "failure_injection", FAILURE_INJECTION_NONE),
        catalog=target_catalog,
        schema=target_schema,
    )
    if publish_ready_snapshot:
        missing_identity = sorted(
            name
            for name in ("build_id", "attempt_id", "git_commit")
            if not write_identity.get(name)
        )
        if missing_identity:
            raise ValueError(
                "READY snapshot publication needs a complete feature build "
                "identity; missing " + ", ".join(missing_identity)
            )
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
    captured_at = datetime.now(timezone.utc)
    resolved_date = parse_reference_date(reference_date)
    if publish_ready_snapshot:
        session_sources, session_source_bindings = read_pinned_session_sources(
            spark,
            source_paths,
            feature_build_id=write_identity["build_id"],
            feature_build_attempt_id=write_identity["attempt_id"],
            reference_date=resolved_date,
            captured_at=captured_at,
            target_catalog=target_catalog,
            target_schema=target_schema,
        )
    else:
        session_sources = read_session_source_frames(spark, source_paths)
        session_source_bindings = ()

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
    build = None
    if publish_ready_snapshot:
        build = begin_feature_build(
            spark,
            catalog=target_catalog,
            schema=target_schema,
            feature_build_id=write_identity["build_id"],
            feature_build_attempt_id=write_identity["attempt_id"],
            reference_date=resolved_date,
            git_commit=write_identity["git_commit"],
            required_feature_ids=ANALYTICS_PCTR_SNAPSHOT_FEATURES,
            sources=(
                external_delta_source(
                    feature_build_id=write_identity["build_id"],
                    feature_build_attempt_id=write_identity["attempt_id"],
                    reference_date=resolved_date,
                    source_name=source_binding.source_role,
                    source_table=source_binding.table_path,
                    delta_version=source_binding.delta_version,
                    schema_checksum_value=source_binding.schema_sha256,
                    captured_at=captured_at,
                    row_count=source_binding.reference_date_row_count,
                ),
                *session_source_bindings,
            ),
            started_at=captured_at,
        )

    results = {}
    try:
        for table_name, frame in writes.items():
            table = registry.table_spec(table_name)
            result = write_feature_table(
                spark,
                table_name,
                frame,
                catalog=target_catalog,
                schema=target_schema,
                reference_date=reference_date,
                reference_date_column=table.snapshot_date_key,
                replace_reference_date=replace_reference_date,
                mode=table.write_mode,
                registry=registry,
                feature_engineering_client=feature_engineering_client,
                return_receipt=(
                    publish_ready_snapshot
                    and table_name in ANALYTICS_PCTR_SNAPSHOT_FEATURES
                ),
                **write_identity,
            )
            if isinstance(result, FeatureMaterializationResult):
                results[table_name] = result.receipt
                table_path = result.table_path
            else:
                table_path = result
            LOGGER.info("Wrote pCTR feature table: %s", table_path)
            if (
                failure_injection == FAILURE_INJECTION_AFTER_FIRST_WRITE
                and len(results) == 1
            ):
                raise RuntimeError(
                    "Intentional personal DEV failure after the first "
                    "feature output"
                )

        if build is not None:
            ready_build, ready_snapshot = publish_ready_feature_group(
                spark,
                catalog=target_catalog,
                schema=target_schema,
                group_id="analytics_pctr",
                build=build,
                frames={
                    feature_id: writes[feature_id]
                    for feature_id in ANALYTICS_PCTR_SNAPSHOT_FEATURES
                },
                receipts=results,
                registry=registry,
            )
            build = ready_build
            LOGGER.info(
                "Published READY Analytics pCTR feature snapshot: %s attempt %s",
                ready_snapshot.feature_snapshot_id,
                ready_snapshot.feature_snapshot_attempt_id,
            )
    except Exception as exc:
        if build is not None and build.status != "READY":
            failed_build = mark_feature_build_failed(
                build,
                failure_reason=f"{type(exc).__name__}: {exc}"[:1000],
                completed_at=datetime.now(timezone.utc),
            )
            persist_feature_build(
                spark,
                catalog=target_catalog,
                schema=target_schema,
                build=failed_build,
            )
        raise


if __name__ == "__main__":
    main()
