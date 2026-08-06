from datetime import date

from dsutils.logtools import get_logger
from next_ads.candidates.foundation import CandidateFoundationInputs
from next_ads.common.snapshot_writes import (
    publish_history_and_latest,
    replace_validated_snapshot,
    with_run_date,
)
from next_ads.ranking.theme_score_eligibility import (
    append_ad_feedback_scores,
    apply_auto_trading_filter,
    apply_greedy_theme_assignment,
    assert_eligible_groups,
    load_customer_age_preferences,
    select_feedback_scaling_population,
)
from next_ads.ranking.theme_score_ranking import (
    apply_multi_session_downweighting,
    build_score_components,
    calculate_score_range,
    map_ranked_ads_to_groups,
    rank_top_ads_per_adset,
)
from next_ads.ranking.theme_score_retrieval import (
    build_ad_group_mappings,
    build_theme_to_ad_mapping,
    load_control_ads,
    customer_base_from_cells,
    load_customer_base,
    load_provider_theme_scores,
)
from next_ads.common import etl


def run_theme_score_mapping(
    *,
    spark,
    config,
    cfg: dict,
    client: str,
    job_env: str,
    run_date: date | str,
    provider_build_id: str,
    provider_signals_table: str,
    provider_signals_delta_version: int,
    provider_source_run_date: date | str,
    apply_ad_feedback: bool = False,
    ad_feedback_weight=0.05,
    top_ads_per_location: int = 20,
    control_sheet_latest_table: str | None = None,
    control_sheet_delta_version: int | None = None,
    output_preranked_table: str | None = None,
    output_grain: str = "location",
    top_ads_per_group: int | None = None,
    write_score_components: bool = True,
    foundation_inputs: CandidateFoundationInputs | None = None,
    allowed_provider_themes=None,
    candidate_publisher=None,
    publish_compatibility: bool = True,
    logger=None,
):
    logger = logger or get_logger(__name__)
    if isinstance(run_date, str):
        run_date = date.fromisoformat(run_date)
    if isinstance(provider_source_run_date, str):
        provider_source_run_date = date.fromisoformat(provider_source_run_date)
    if not provider_build_id:
        raise ValueError("provider_build_id is required")
    if not provider_signals_table:
        raise ValueError("provider_signals_table is required")
    if (
        isinstance(provider_signals_delta_version, bool)
        or not isinstance(provider_signals_delta_version, int)
        or provider_signals_delta_version < 0
    ):
        raise ValueError(
            "provider_signals_delta_version must be a non-negative integer"
        )
    if control_sheet_delta_version is not None and (
        isinstance(control_sheet_delta_version, bool)
        or not isinstance(control_sheet_delta_version, int)
        or control_sheet_delta_version < 0
    ):
        raise ValueError(
            "control_sheet_delta_version must be a non-negative integer"
        )
    top_ads = int(
        top_ads_per_group
        if top_ads_per_group is not None
        else top_ads_per_location
    )
    assert top_ads > 0, "top ads per group must be greater than zero"

    output_group_cols = {
        "location": "Location",
        "page_type": "PageType",
    }
    output_grain = output_grain.lower()
    if output_grain not in output_group_cols:
        raise ValueError(
            "output_grain must be one of: "
            + ", ".join(sorted(output_group_cols))
        )
    output_group_col = output_group_cols[output_grain]

    min_c_sessions = cfg["results_prm"]["min_c_sessions"]
    incremental_lookback = cfg["incrementality"]["incremental_lookback"]
    auto_trading_switch = cfg["incrementality"]["auto_trading_switch"]
    tbls = cfg["tables"]["write"]
    schema = config.schema_write
    logger.info(f"Write schema set to {schema}")

    tbl_args = {
        "catalog": config.catalog_write,
        "schema": schema,
        "client": client,
    }
    control_sheet_latest = control_sheet_latest_table or etl.map_tbl(
        tbls["control_sheet_latest"], **tbl_args
    )
    customer_cells_latest = etl.map_tbl(
        tbls["customer_cells_latest"], **tbl_args
    )
    kids_age_groups = cfg["tables"]["read"]["kids_age_groups_latest"]
    sessions = cfg["tables"]["read"]["bq_sessions"]
    actions = cfg["tables"]["read"]["bq_actions"]

    theme_score_components_latest = etl.map_tbl(
        tbls["theme_score_components_latest"],
        **tbl_args,
    )
    theme_score_components = etl.map_tbl(
        tbls["theme_score_components"],
        **tbl_args,
    )
    preranked_ads_from_themes_latest = output_preranked_table or etl.map_tbl(
        tbls["preranked_ads_from_themes_latest"],
        **tbl_args,
    )
    webhook_url = cfg["webhooks"]["DS Warnings"]
    ad_results = etl.map_tbl(
        cfg["tables"]["write"]["results_ads"],
        catalog="marketingdata_prod",
        schema="warehouse",
        client=client,
    )

    logger.info(f"Getting theme to ad mappings from {control_sheet_latest}")
    df_control_ads = load_control_ads(
        spark,
        control_sheet_latest,
        control_sheet_delta_version,
    )
    df_ads = df_control_ads
    # Feedback scaling historically uses every active advert in the route.
    # Keep that population separate from the theme/AudienceOnly/AutoTrading
    # eligibility filters so moving the calculation earlier cannot alter all
    # remaining adverts' relative multipliers.
    df_feedback_scaling_ads = select_feedback_scaling_population(df_ads)
    df_ads = apply_auto_trading_filter(
        df_ads,
        auto_trading_switch,
        logger,
    )
    df_theme2ad = build_theme_to_ad_mapping(df_ads)

    if foundation_inputs is None:
        logger.info(f"Getting customer base from {customer_cells_latest}")
        df_cust = load_customer_base(spark, customer_cells_latest)
    else:
        logger.info(
            "Using pinned candidate foundation %s from %s",
            foundation_inputs.snapshot_id,
            foundation_inputs.source_run_date,
        )
        df_cust = customer_base_from_cells(foundation_inputs.customer_cells)

    logger.info(
        "Getting theme scores from provider build %s in %s at Delta "
        "version %s (source run date %s)",
        provider_build_id,
        provider_signals_table,
        provider_signals_delta_version,
        provider_source_run_date,
    )
    df_theme_scores = load_provider_theme_scores(
        spark,
        provider_signals_table=provider_signals_table,
        provider_signals_delta_version=provider_signals_delta_version,
        provider_build_id=provider_build_id,
        provider_source_run_date=provider_source_run_date,
        customer_base_df=df_cust,
        allowed_themes_df=allowed_provider_themes,
    )

    logger.info("Normalising theme scores")
    min_score, score_range = calculate_score_range(df_theme_scores, logger)
    df_theme_scores = apply_greedy_theme_assignment(
        df_theme_scores,
        cfg.get("greedy_themes", {}),
        job_env,
        webhook_url,
        logger,
    )
    df_theme2ad = append_ad_feedback_scores(
        df_theme2ad,
        enabled=apply_ad_feedback,
        ad_results_table=ad_results,
        control_sheet_latest_table=control_sheet_latest,
        ad_feedback_weight=ad_feedback_weight,
        sessions_threshold=min_c_sessions,
        lookback_period_days=incremental_lookback,
        logger=logger,
        ad_feedback_metrics_df=(
            None
            if foundation_inputs is None
            else foundation_inputs.ad_feedback_metrics
        ),
        active_ads_df=(
            None if foundation_inputs is None else df_feedback_scaling_ads
        ),
    )

    logger.info("Normalising theme scores and mapping to ads")
    df_score_components = build_score_components(
        df_theme_scores,
        df_theme2ad,
        min_score,
        score_range,
    )

    logger.info("Getting multi-session ad score")
    logger.info(
        "Joining multi-sessions onto score_components, and downweighting ads "
        "seen more than 3 times in 7 days"
    )
    if foundation_inputs is None:
        df_score_components = apply_multi_session_downweighting(
            df_score_components,
            sessions,
            actions,
        )
    else:
        df_score_components = apply_multi_session_downweighting(
            df_score_components,
            repeat_ad_exposure_df=foundation_inputs.repeat_ad_exposure,
        )
    df_score_components_for_write = df_score_components.drop(
        "AdVariant",
        "TriggerScore",
    )
    if write_score_components:
        logger.info(f"Loading score components to {theme_score_components}")
        logger.info(
            f"Loading score components to {theme_score_components_latest}"
        )
        publish_history_and_latest(
            spark,
            df_score_components_for_write,
            history_table=theme_score_components,
            latest_table=theme_score_components_latest,
            key_columns=["AccountNumber", "Theme", "UniqueAdID"],
            run_date=run_date,
        )
    else:
        logger.info("Skipping score component table writes for this route")

    logger.info(f"Fetching ad {output_group_col} mappings")
    logger.info(
        f"Finding distinct ad sets across {output_group_col} "
        "to minimise repeated ranking"
    )
    df_ad2group, df_adset2group, df_ad2adset = build_ad_group_mappings(
        spark,
        control_sheet_latest,
        logger,
        group_col=output_group_col,
        control_ads_df=df_control_ads,
    )

    logger.info(f"Ranking and returning top {top_ads} ads per ad set")
    customer_prefs, age_order_map = load_customer_age_preferences(
        spark, kids_age_groups
    )
    df_adset_scores = rank_top_ads_per_adset(
        df_score_components,
        df_ad2adset,
        customer_prefs,
        age_order_map,
        top_ads,
    )
    logger.info(f"Mapping ranked ads back to {output_group_col}")
    df_ad_scores = map_ranked_ads_to_groups(
        df_adset_scores,
        df_adset2group,
        group_col=output_group_col,
    )

    if candidate_publisher is not None:
        candidate_publisher(
            df_adset_scores,
            df_adset2group,
            df_ad2adset,
        )

    if publish_compatibility:
        logger.info(
            f"Checking for ads assigned to ineligible {output_group_col}"
        )
        assert_eligible_groups(
            df_ad_scores,
            df_ad2group,
            group_col=output_group_col,
        )
        logger.info(
            "Loading preranked theme ads to "
            f"{preranked_ads_from_themes_latest}"
        )
        replace_validated_snapshot(
            spark,
            with_run_date(df_ad_scores, run_date),
            table=preranked_ads_from_themes_latest,
            key_columns=["AccountNumber", "UniqueAdID", output_group_col],
        )
    else:
        logger.info("Skipping preranked compatibility publication")

    logger.info("Run complete")
