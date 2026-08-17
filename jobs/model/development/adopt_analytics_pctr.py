"""Adopt one exact Analytics pCTR output as an evaluation provider."""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import logging
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from dsutils.dbc import configure_spark
from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    replace_scope_by_name,
)
from next_ads.features.feature_builds import feature_value_checksum
from next_ads.model_development import (
    AccountAdvertCandidateAdapter,
    ExternalModelComponent,
    adapt_external_advert_scores,
    bind_external_score_output,
    create_model_development_tables,
    persist_external_score_output_receipt,
    verify_external_model_components,
)
from next_ads.ranking.provider_publication import PROVIDER_SIGNAL_COLUMNS


LOGGER = logging.getLogger(__name__)
MANIFEST_PREFIX = "ANALYTICS_PCTR_ADOPTION_EVIDENCE="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--source_delta_version", type=int, default=None)
    parser.add_argument("--run_date", required=True)
    parser.add_argument("--producing_run_id", required=True)
    parser.add_argument("--receipt_catalog", required=True)
    parser.add_argument("--receipt_schema", required=True)
    parser.add_argument("--evaluation_scores_table", required=True)
    parser.add_argument("--classifier_model_uri", required=True)
    parser.add_argument("--classifier_run_id", required=True)
    parser.add_argument("--regressor_model_uri", required=True)
    parser.add_argument("--regressor_run_id", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _components(args: argparse.Namespace) -> tuple[ExternalModelComponent, ...]:
    return (
        ExternalModelComponent(
            role="popularity_classifier",
            model_uri=args.classifier_model_uri,
            expected_run_id=args.classifier_run_id,
        ),
        ExternalModelComponent(
            role="affinity_regressor",
            model_uri=args.regressor_model_uri,
            expected_run_id=args.regressor_run_id,
        ),
    )


def _provider_build_id(receipt_id: str) -> str:
    return hashlib.sha256(
        f"analytics_pctr:{receipt_id}".encode("utf-8")
    ).hexdigest()


def _ensure_evaluation_score_table(spark, table_path: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_qualified_identifier(table_path)} (
          ProviderBuildID STRING NOT NULL,
          AccountNumber STRING NOT NULL,
          EntityType STRING NOT NULL,
          EntityID STRING NOT NULL,
          ProviderID STRING NOT NULL,
          RunDate DATE NOT NULL,
          RawScore DOUBLE,
          Score DOUBLE NOT NULL,
          ProviderRank INT NOT NULL
        ) USING DELTA
        """
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    spark = configure_spark()
    run_date = date.fromisoformat(args.run_date)
    components = _components(args)

    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_registry_uri("databricks-uc")
    verify_external_model_components(MlflowClient(), components)
    output, receipt = bind_external_score_output(
        spark,
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table=args.source_table,
        source_delta_version=args.source_delta_version,
        run_date=run_date,
        run_date_column=None,
        producing_run_id=args.producing_run_id,
        components=components,
    )
    create_model_development_tables(
        spark,
        catalog=args.receipt_catalog,
        schema=args.receipt_schema,
    )
    persist_external_score_output_receipt(
        spark,
        catalog=args.receipt_catalog,
        schema=args.receipt_schema,
        receipt=receipt,
    )

    provider_build_id = _provider_build_id(receipt.receipt_id)
    scores = adapt_external_advert_scores(
        output,
        receipt,
        provider_build_id=provider_build_id,
        account_column="account_number",
        advert_column="UniqueAdID",
        raw_score_column="combined_weighted_score",
        score_column="combined_weighted_score",
    )
    eligible = output.select("account_number", "UniqueAdID").dropDuplicates()
    adapter = AccountAdvertCandidateAdapter(
        account_column="account_number",
        advert_column="UniqueAdID",
    )
    first_candidates = adapter.apply(scores, eligible)
    second_candidates = adapter.apply(scores, eligible)
    candidate_checksum = feature_value_checksum(first_candidates)
    if candidate_checksum != feature_value_checksum(second_candidates):
        raise ValueError("Candidate adapter output changed for identical inputs")

    _ensure_evaluation_score_table(spark, args.evaluation_scores_table)
    write_receipt = replace_scope_by_name(
        scores.select(*PROVIDER_SIGNAL_COLUMNS),
        args.evaluation_scores_table,
        {"ProviderBuildID": provider_build_id},
        PROVIDER_SIGNAL_COLUMNS,
        spark=spark,
        build_id=provider_build_id,
        attempt_id=receipt.receipt_id,
        commit_metadata={
            "operation": "analytics_pctr_evaluation_adoption",
            "source_delta_version": receipt.source_delta_version,
        },
    )
    evidence = {
        "activation_mode": "EVALUATE",
        "candidate_checksum": candidate_checksum,
        "component_models": [
            {
                "model_uri": component.model_uri,
                "role": component.role,
                "run_id": component.expected_run_id,
            }
            for component in receipt.components
        ],
        "evaluation_scores_delta_version": write_receipt.delta_version,
        "evaluation_scores_rows": write_receipt.row_count,
        "external_receipt_id": receipt.receipt_id,
        "provider_build_id": provider_build_id,
        "source_delta_version": receipt.source_delta_version,
        "source_rows": receipt.row_count,
        "source_schema_checksum": receipt.schema_checksum,
        "source_table": receipt.source_table,
    }
    LOGGER.info(
        "%s%s",
        MANIFEST_PREFIX,
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()
