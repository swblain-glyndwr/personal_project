from __future__ import annotations

from datetime import date
from typing import Any

from next_ads.candidates.publication import (
    CandidateBuild,
    CandidateBuildPublisher,
    build_candidate_context,
    candidate_policy_checksum,
    group_serving_entries,
    load_serving_portfolio_entries,
    validate_assignment_rank_limit,
)
from next_ads.ranking.portfolio_resolution import unchanged_provider_themes
from next_ads.ranking.scoring_inputs import latest_delta_version
from next_ads.ranking.theme_score_mapping import run_theme_score_mapping


def run_portfolio_candidate_build(
    *,
    spark: Any,
    config: Any,
    cfg: dict,
    client: str,
    job_env: str,
    run_date: date | str,
    route: str,
    output_grain: str,
    portfolio_id: str,
    portfolio_attempt_id: str,
    current_input_snapshot_id: str,
    candidate_foundation_snapshot_id: str,
    foundation_inputs: Any,
    control_table: str,
    output_preranked_table: str,
    task_run_id: int,
    execution_count: int,
    compatibility_top_count: int,
    apply_ad_feedback: bool,
    ad_feedback_weight: float,
    write_score_components: bool,
    logger: Any,
) -> CandidateBuild:
    """Build every serving entry and publish one accepted candidate attempt."""
    logical_date = (
        date.fromisoformat(run_date) if isinstance(run_date, str) else run_date
    )
    route_contracts = {
        "v1": ("location", "locations", "Location"),
        "v2": ("page_type", "page_types", "PageType"),
    }
    if route not in route_contracts:
        raise ValueError(f"Unsupported candidate route: {route}")
    expected_grain, configuration_key, group_column = route_contracts[route]
    if output_grain != expected_grain:
        raise ValueError(
            f"Route {route} requires output grain {expected_grain}, "
            f"received {output_grain}"
        )
    route_configuration = cfg[configuration_key]
    validate_assignment_rank_limit(route_configuration)
    entries = load_serving_portfolio_entries(
        spark,
        entries_table=config.tables_write.scoring_portfolio_entries,
        portfolio_id=portfolio_id,
        portfolio_attempt_id=portfolio_attempt_id,
    )
    best_entries = [entry for entry in entries if entry.serving_slot == "best"]
    if len(best_entries) != 1:
        raise ValueError("Portfolio must contain exactly one best serving entry")

    control_delta_version = latest_delta_version(spark, control_table)
    policy_checksum = candidate_policy_checksum(
        cfg,
        output_grain=output_grain,
        apply_ad_feedback=apply_ad_feedback,
        ad_feedback_weight=ad_feedback_weight,
    )
    context = build_candidate_context(
        run_date=logical_date,
        route=route,
        output_grain=output_grain,
        entries=entries,
        candidate_foundation_snapshot_id=candidate_foundation_snapshot_id,
        control_table=control_table,
        control_delta_version=control_delta_version,
        candidate_policy_checksum_value=policy_checksum,
        task_run_id=task_run_id,
        execution_count=execution_count,
    )
    publisher = CandidateBuildPublisher(
        spark,
        context,
        builds_table=config.tables_write.candidate_builds,
        scores_table=config.tables_write.candidate_scores,
        ad_sets_table=config.tables_write.candidate_ad_sets,
        group_column=group_column,
    )

    for provider_entries in group_serving_entries(entries):
        binding = provider_entries[0]
        publishes_best = any(
            entry.serving_slot == "best" for entry in provider_entries
        )
        allowed_provider_themes = (
            None
            if binding.input_snapshot_id == current_input_snapshot_id
            else unchanged_provider_themes(
                spark,
                item_themes_table=(
                    config.tables_write.scoring_input_item_themes
                ),
                provider_input_snapshot_id=binding.input_snapshot_id,
                current_input_snapshot_id=current_input_snapshot_id,
            )
        )

        def publish_frames(ranked, ad_set_to_group, ad_to_ad_set):
            publisher.publish_provider(
                provider_entries,
                ranked,
                ad_set_to_group,
                ad_to_ad_set,
            )

        run_theme_score_mapping(
            spark=spark,
            config=config,
            cfg=cfg,
            client=client,
            job_env=job_env,
            run_date=logical_date,
            provider_build_id=binding.provider_build_id,
            provider_signals_table=binding.provider_output_table,
            provider_signals_delta_version=(
                binding.provider_output_delta_version
            ),
            provider_source_run_date=binding.provider_source_run_date,
            apply_ad_feedback=apply_ad_feedback,
            ad_feedback_weight=ad_feedback_weight,
            control_sheet_latest_table=control_table,
            control_sheet_delta_version=control_delta_version,
            output_preranked_table=output_preranked_table,
            output_grain=output_grain,
            top_ads_per_group=compatibility_top_count,
            write_score_components=(
                write_score_components and publishes_best
            ),
            foundation_inputs=foundation_inputs,
            allowed_provider_themes=allowed_provider_themes,
            candidate_publisher=publish_frames,
            publish_compatibility=publishes_best,
            logger=logger,
        )

    return publisher.finalize(entries)


__all__ = ["run_portfolio_candidate_build"]
