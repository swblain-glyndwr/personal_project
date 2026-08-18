from datetime import date
import inspect

import pytest

from next_ads.model_development import scoring_sets


def test_scoring_dates_are_explicit_or_auto_but_never_ambiguous():
    assert scoring_sets._normalise_reference_dates(None) is None
    assert scoring_sets._normalise_reference_dates(
        ("2026-08-17", date(2026, 8, 16))
    ) == (date(2026, 8, 16), date(2026, 8, 17))

    with pytest.raises(ValueError, match="cannot be empty"):
        scoring_sets._normalise_reference_dates(())
    with pytest.raises(ValueError, match="must be unique"):
        scoring_sets._normalise_reference_dates(("2026-08-17", "2026-08-17"))


def test_scoring_set_is_label_free_and_uses_declared_point_in_time_lookups():
    source = inspect.getsource(scoring_sets.build_label_free_scoring_set)

    assert "definition.feature_lookups" in source
    assert "definition.model_feature_columns" in source
    assert "_apply_point_in_time_lookup" in source
    assert "availability_lag_days" in source
    assert "_latest_ready_reference_date" in source
    assert "validate_snapshot_time_boundary" in source
    assert "training_observation" not in source
    assert "definition.label" not in source
    assert "label_column" not in source


def test_scoring_set_keeps_exact_ready_snapshot_receipts():
    annotations = scoring_sets.ScoringFeatureBinding.__annotations__

    assert {
        "feature_snapshot_id",
        "feature_snapshot_attempt_id",
        "feature_build_id",
        "feature_build_attempt_id",
        "backing_table",
        "delta_version",
        "row_count",
        "schema_checksum",
        "value_checksum",
        "write_receipt_id",
    }.issubset(annotations)
