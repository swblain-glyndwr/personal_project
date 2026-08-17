"""Build, train, promote and score one declared Feature Store model."""

from __future__ import annotations

import argparse
from datetime import date
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
    ModelPluginRegistry,
    build_training_set_from_feature_store,
    create_model_development_tables,
    load_model_definition,
    persist_training_set_receipt,
    promote_exact_model_build,
    train_or_reuse_model,
)
from next_ads.ranking.provider_publication import PROVIDER_SIGNAL_COLUMNS


LOGGER = logging.getLogger(__name__)
MANIFEST_PREFIX = "MODEL_DEVELOPMENT_EVIDENCE="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--feature_catalog", required=True)
    parser.add_argument("--feature_schema", required=True)
    parser.add_argument("--feature_reference_dates", required=True)
    parser.add_argument("--label_end", required=True)
    parser.add_argument("--code_sha", required=True)
    parser.add_argument("--registered_model_name", required=True)
    parser.add_argument("--experiment_path", required=True)
    parser.add_argument("--evaluation_scores_table", required=True)
    parser.add_argument("--promotion_model_name", default=None)
    parser.add_argument("--promotion_alias", default="integration_candidate")
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _reference_dates(value: str) -> tuple[str, ...]:
    dates = tuple(item.strip() for item in value.split(",") if item.strip())
    if not dates:
        raise ValueError("feature_reference_dates must not be empty")
    for value in dates:
        date.fromisoformat(value)
    return dates


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
    definition = load_model_definition(args.model_name)
    create_model_development_tables(
        spark,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
    )
    training = build_training_set_from_feature_store(
        spark,
        definition,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
        feature_reference_dates=_reference_dates(
            args.feature_reference_dates
        ),
        label_end=date.fromisoformat(args.label_end),
        code_sha=args.code_sha,
    )
    persist_training_set_receipt(
        spark,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
        receipt=training.receipt,
    )

    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment_path)
    plugins = ModelPluginRegistry()
    trainer = plugins.trainer(
        definition,
        registered_model_name=args.registered_model_name,
    )
    build, reused = train_or_reuse_model(
        spark,
        catalog=args.feature_catalog,
        schema=args.feature_schema,
        definition=definition,
        receipt=training.receipt,
        training_frame=training.frame,
        trainer=trainer,
    )
    promotion = None
    promotion_reused = None
    if args.promotion_model_name:
        promotion, promotion_reused = promote_exact_model_build(
            MlflowClient(),
            build,
            destination_model_name=args.promotion_model_name,
            alias=args.promotion_alias,
        )

    run_date = training.receipt.observation_end
    scores = plugins.score_provider(
        definition,
        run_date=run_date,
    ).score(definition, build, training.frame)
    eligible = training.frame.select(
        "account_number",
        "advert_id",
    ).dropDuplicates()
    adapter = plugins.candidate_adapter(
        definition,
        account_column="account_number",
        advert_column="advert_id",
    )
    first_candidates = adapter.apply(scores, eligible)
    second_candidates = adapter.apply(scores, eligible)
    first_checksum = feature_value_checksum(first_candidates)
    second_checksum = feature_value_checksum(second_candidates)
    if first_checksum != second_checksum:
        raise ValueError("Candidate adapter output changed for identical inputs")

    _ensure_evaluation_score_table(spark, args.evaluation_scores_table)
    output_receipt = replace_scope_by_name(
        scores.select(*PROVIDER_SIGNAL_COLUMNS),
        args.evaluation_scores_table,
        {"ProviderBuildID": build.model_build_id},
        PROVIDER_SIGNAL_COLUMNS,
        spark=spark,
        build_id=build.model_build_id,
        attempt_id=training.receipt.receipt_id,
        git_commit=args.code_sha,
    )
    evidence = {
        "artifact_digest": build.artifact_digest,
        "candidate_checksum": first_checksum,
        "evaluation_scores_delta_version": output_receipt.delta_version,
        "evaluation_scores_rows": output_receipt.row_count,
        "feature_bindings": len(training.receipt.feature_bindings),
        "mlflow_run_id": build.mlflow_run_id,
        "model_build_id": build.model_build_id,
        "model_definition_checksum": definition.checksum,
        "model_reused": reused,
        "model_uri": build.model_uri,
        "promotion": promotion.__dict__ if promotion else None,
        "promotion_reused": promotion_reused,
        "runtime_profile": build.runtime_profile,
        "status": build.status,
        "training_receipt_id": training.receipt.receipt_id,
    }
    LOGGER.info(
        "%s%s",
        MANIFEST_PREFIX,
        json.dumps(evidence, default=str, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()
