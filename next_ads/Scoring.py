"""Compatibility wrapper for scoring helpers moved into the ranking package."""

from next_ads.ranking.scoring import (  # noqa: F401
    aggregate_model_scores,
    append_targeting_criteria,
    get_model_scores,
)
