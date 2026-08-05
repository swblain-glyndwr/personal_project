from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Window
from pyspark.sql import functions as F


VALID_SCORE_DIRECTIONS = frozenset(
    {"higher_is_better", "lower_is_better"}
)


def _config_value(config, name):
    if isinstance(config, Mapping):
        return config[name]
    return getattr(config, name)


def adapt_account_entity_scores(
    df,
    *,
    provider_build_id: str,
    provider_id: str,
    entity_type: str,
    run_date,
    account_column: str,
    entity_column: str,
    raw_score_column: str,
    score_column: str,
    score_direction: str = "higher_is_better",
    max_entities_per_account: int | None = None,
):
    """Adapt one account/entity provider output to the canonical contract."""
    required = {
        account_column,
        entity_column,
        raw_score_column,
        score_column,
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing provider score columns: {', '.join(missing)}")
    if score_direction not in VALID_SCORE_DIRECTIONS:
        raise ValueError(f"Unsupported score direction: {score_direction}")
    if max_entities_per_account is not None and (
        isinstance(max_entities_per_account, bool)
        or not isinstance(max_entities_per_account, int)
        or max_entities_per_account < 1
    ):
        raise ValueError("max_entities_per_account must be a positive integer")

    selected = df.select(
        F.col(account_column).cast("string").alias("AccountNumber"),
        F.col(entity_column).cast("string").alias("EntityID"),
        F.col(raw_score_column).cast("double").alias("RawScore"),
        F.col(score_column).cast("double").alias("_provider_score"),
    )
    canonical_score = F.col("_provider_score")
    if score_direction == "lower_is_better":
        canonical_score = -canonical_score
    selected = selected.withColumn("Score", canonical_score)

    rank_window = Window.partitionBy("AccountNumber").orderBy(
        F.col("Score").desc_nulls_last(),
        F.col("EntityID").asc_nulls_last(),
    )
    ranked = selected.withColumn(
        "ProviderRank",
        F.row_number().over(rank_window),
    )
    if max_entities_per_account is not None:
        ranked = ranked.where(
            F.col("ProviderRank") <= max_entities_per_account
        )
    return ranked.select(
        F.lit(provider_build_id).alias("ProviderBuildID"),
        "AccountNumber",
        F.lit(entity_type).alias("EntityType"),
        "EntityID",
        F.lit(provider_id).alias("ProviderID"),
        F.lit(run_date).cast("date").alias("RunDate"),
        "RawScore",
        "Score",
        "ProviderRank",
    )


def adapt_account_theme_scores(
    df,
    *,
    provider_build_id: str,
    provider_id: str,
    run_date,
    account_column: str,
    theme_column: str,
    raw_score_column: str,
    score_column: str,
):
    """Adapt a provider's account-theme output to the canonical signal shape."""
    return adapt_account_entity_scores(
        df,
        provider_build_id=provider_build_id,
        provider_id=provider_id,
        entity_type="theme",
        run_date=run_date,
        account_column=account_column,
        entity_column=theme_column,
        raw_score_column=raw_score_column,
        score_column=score_column,
    )


def adapt_configured_provider_scores(
    df,
    *,
    context,
    provider_config,
):
    """Adapt configured account/entity output without model-specific code."""
    adapter = _config_value(provider_config, "adapter")
    if adapter != "legacy_account_entity_table":
        raise ValueError(f"Unsupported provider output adapter: {adapter}")
    expected = {
        "provider_id": context.provider_id,
        "capability": context.capability,
        "entity_type": context.capability.removeprefix("account_"),
    }
    mismatched = [
        field
        for field, value in expected.items()
        if _config_value(provider_config, field) != value
    ]
    if mismatched:
        raise ValueError(
            "Provider output configuration does not match its context: "
            + ", ".join(mismatched)
        )
    return adapt_account_entity_scores(
        df,
        provider_build_id=context.provider_build_id,
        provider_id=context.provider_id,
        entity_type=_config_value(provider_config, "entity_type"),
        run_date=context.run_date,
        account_column=_config_value(
            provider_config,
            "account_number_column",
        ),
        entity_column=_config_value(provider_config, "entity_id_column"),
        raw_score_column=_config_value(provider_config, "raw_score_column"),
        score_column=_config_value(provider_config, "score_column"),
        score_direction=_config_value(provider_config, "score_direction"),
        max_entities_per_account=int(
            _config_value(provider_config, "max_entities_per_account")
        ),
    )


__all__ = [
    "VALID_SCORE_DIRECTIONS",
    "adapt_account_entity_scores",
    "adapt_account_theme_scores",
    "adapt_configured_provider_scores",
]
