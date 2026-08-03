from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from next_ads.common.delta_writes import validate_unique_non_null_keys


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
    required = {
        account_column,
        theme_column,
        raw_score_column,
        score_column,
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing provider score columns: {', '.join(missing)}")
    ranked = df.select(
        F.col(account_column).cast("string").alias("AccountNumber"),
        F.col(theme_column).cast("string").alias("EntityID"),
        F.col(raw_score_column).cast("double").alias("RawScore"),
        F.col(score_column).cast("double").alias("Score"),
    ).persist()
    try:
        keys = validate_unique_non_null_keys(
            ranked,
            ["AccountNumber", "EntityID"],
        )
        if keys.row_count == 0:
            raise ValueError("Provider score output is empty")
        invalid_scores = (
            ranked.agg(
                F.sum(
                    F.when(
                        F.col("RawScore").isNull()
                        | F.isnan("RawScore")
                        | F.col("RawScore").isin(
                            float("inf"),
                            float("-inf"),
                        )
                        | F.col("Score").isNull()
                        | F.isnan("Score")
                        | F.col("Score").isin(
                            float("inf"),
                            float("-inf"),
                        ),
                        1,
                    ).otherwise(0)
                ).alias("invalid_score_count")
            )
            .first()["invalid_score_count"]
        )
        if invalid_scores:
            raise ValueError(
                f"Provider output contains {invalid_scores} invalid scores"
            )
        validated = ranked
    finally:
        ranked.unpersist()
    rank_window = Window.partitionBy("AccountNumber").orderBy(
        F.col("Score").desc_nulls_last(),
        F.col("EntityID").asc(),
    )
    return validated.select(
        F.lit(provider_build_id).alias("ProviderBuildID"),
        "AccountNumber",
        F.lit("theme").alias("EntityType"),
        "EntityID",
        F.lit(provider_id).alias("ProviderID"),
        F.lit(run_date).cast("date").alias("RunDate"),
        "RawScore",
        "Score",
        F.row_number().over(rank_window).alias("ProviderRank"),
    )
