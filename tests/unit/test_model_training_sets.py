from datetime import date, datetime, timezone
import inspect

import pytest

from next_ads.features.snapshot_reader import ReadyFeatureBinding
from next_ads.model_development import (
    load_model_definition,
    validate_snapshot_time_boundary,
)
from next_ads.model_development import training_sets
from next_ads.features import load_feature_store_registry


def _binding(reference_date):
    return ReadyFeatureBinding(
        feature_snapshot_id=f"analytics_pctr:{reference_date}",
        feature_snapshot_attempt_id="123",
        feature_build_id="123",
        feature_build_attempt_id="123",
        reference_date=reference_date,
        registry_checksum="a" * 64,
        git_commit="abc123",
        completed_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        feature_id="next_uk_nextads_fs_pctr_model_input",
        backing_table="catalog.schema.pctr",
        delta_version=42,
        row_count=10,
        output_schema_checksum="b" * 64,
        backing_schema_checksum="b" * 64,
        value_checksum="c" * 64,
        write_receipt_id="receipt",
    )


def test_future_snapshot_is_rejected_before_a_receipt_can_be_ready():
    with pytest.raises(ValueError, match="after observation end"):
        validate_snapshot_time_boundary(
            _binding(date(2026, 8, 2)),
            date(2026, 8, 1),
        )


def test_training_receipt_id_pins_definition_versions_and_windows():
    definition = load_model_definition("analytics_pctr")
    binding = training_sets._training_feature_binding(
        _binding(date(2026, 8, 1))
    )
    values = {
        "observation_start": date(2026, 1, 1),
        "observation_end": date(2026, 7, 31),
        "label_end": date(2026, 8, 1),
        "code_sha": "abc123",
    }

    first = training_sets._receipt_id(
        definition,
        (binding,),
        **values,
    )
    second = training_sets._receipt_id(
        definition,
        (binding,),
        **values,
    )
    changed = training_sets._receipt_id(
        definition,
        (binding,),
        **{**values, "code_sha": "def456"},
    )

    assert first == second
    assert first != changed


def test_feature_history_reads_and_unions_every_ready_snapshot(monkeypatch):
    class Frame:
        def __init__(self, names):
            self.names = tuple(names)

        def unionByName(self, other, *, allowMissingColumns):  # noqa: N802
            assert allowMissingColumns is False
            return Frame(self.names + other.names)

    requested = []

    def read(_spark, feature_id, **kwargs):
        requested.append((feature_id, kwargs["reference_date"]))
        reference_date = date.fromisoformat(kwargs["reference_date"])
        return Frame((kwargs["reference_date"],)), _binding(reference_date)

    monkeypatch.setattr(training_sets, "read_ready_feature", read)
    frame, bindings = training_sets._read_feature_history(
        object(),
        feature_id="next_uk_nextads_fs_pctr_model_input",
        catalog="catalog",
        schema="schema",
        reference_dates=("2026-07-01", "2026-08-01"),
        registry=object(),
    )

    assert frame.names == ("2026-07-01", "2026-08-01")
    assert [binding.reference_date for binding in bindings] == [
        date(2026, 7, 1),
        date(2026, 8, 1),
    ]
    assert len(requested) == 2


def test_feature_reference_dates_reject_ambiguous_or_duplicate_inputs():
    with pytest.raises(ValueError, match="not both"):
        training_sets._normalise_feature_reference_dates(
            feature_reference_date="2026-08-01",
            feature_reference_dates=("2026-07-01",),
        )
    with pytest.raises(ValueError, match="must be unique"):
        training_sets._normalise_feature_reference_dates(
            feature_reference_date=None,
            feature_reference_dates=("2026-08-01", "2026-08-01"),
        )


def test_legacy_aggregate_click_label_is_rejected_for_training():
    registry = load_feature_store_registry()

    with pytest.raises(ValueError, match="not approved for model training"):
        training_sets._require_training_safe_feature(
            registry,
            "next_uk_nextads_fs_labels_clicks",
        )

    approved = training_sets._require_training_safe_feature(
        registry,
        "next_uk_nextads_fs_shopping_bag_click_labels",
    )
    assert approved.training_safe is True


def test_training_set_builder_reaches_feature_lookups_and_receipt_creation():
    source = inspect.getsource(training_sets.build_training_set)

    assert "_normalise_feature_reference_dates" in source
    assert "_apply_point_in_time_lookup" in source
    assert "return TrainingSetBuildResult" in source


def test_point_in_time_lookup_applies_the_declared_availability_lag():
    source = inspect.getsource(training_sets._apply_point_in_time_lookup)

    assert "lookup.availability_lag_days" in source
    assert "INTERVAL" in source
