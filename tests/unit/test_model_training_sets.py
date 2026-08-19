from datetime import date, datetime, timezone
import hashlib
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

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
    audited = training_sets._receipt_id(
        definition,
        (binding,),
        include_feature_audit_columns=True,
        **values,
    )
    legacy_payload = {
        "code_sha": values["code_sha"],
        "definition_checksum": definition.checksum,
        "feature_bindings": [
            {
                "feature_id": binding.feature_id,
                "snapshot_id": binding.feature_snapshot_id,
                "snapshot_attempt_id": (binding.feature_snapshot_attempt_id),
                "delta_version": binding.delta_version,
                "value_checksum": binding.value_checksum,
            }
        ],
        "label_end": values["label_end"].isoformat(),
        "observation_end": values["observation_end"].isoformat(),
        "observation_start": values["observation_start"].isoformat(),
    }
    expected_legacy = hashlib.sha256(
        json.dumps(
            legacy_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    assert first == second
    assert first == expected_legacy
    assert first != changed
    assert first != audited


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


def test_receipt_window_uses_session_date_for_cross_midnight_exposures(
    monkeypatch,
):
    definition = load_model_definition("shopping_bag_pctr")

    class Frame:
        def __init__(self, rows, columns=None):
            self.rows = tuple(rows)
            self.columns = list(
                columns if columns is not None else rows[0].keys()
            )

    observations = Frame(
        (
            {
                **{
                    column: None
                    for column in (
                        definition.training_observation.selected_columns
                    )
                },
                "exposure_id": "exposure-1",
                "label_horizon_days": 0,
                "session_date": date(2026, 8, 7),
                "exposure_timestamp": datetime(
                    2026,
                    8,
                    6,
                    22,
                    46,
                    tzinfo=timezone.utc,
                ),
                "clicked": 0,
            },
            {
                **{
                    column: None
                    for column in (
                        definition.training_observation.selected_columns
                    )
                },
                "exposure_id": "exposure-2",
                "label_horizon_days": 0,
                "session_date": date(2026, 8, 7),
                "exposure_timestamp": datetime(
                    2026,
                    8,
                    6,
                    22,
                    47,
                    tzinfo=timezone.utc,
                ),
                "clicked": 1,
            },
        )
    )
    ready_binding = _binding(date(2026, 8, 6))

    def date_window(frame, column):
        values = tuple(row[column] for row in frame.rows)
        return min(values), max(values)

    def apply_lookup(
        frame,
        _feature_frame,
        lookup,
        **_kwargs,
    ):
        return Frame(
            frame.rows,
            (*frame.columns, *training_sets._lookup_output_names(lookup)),
        )

    monkeypatch.setattr(
        "next_ads.features.load_feature_store_registry",
        lambda: object(),
    )
    monkeypatch.setattr(
        training_sets,
        "validate_unique_non_null_keys",
        lambda *_args: None,
    )
    monkeypatch.setattr(training_sets, "_date_window", date_window)
    monkeypatch.setattr(
        training_sets,
        "_validate_label_maturity",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        training_sets,
        "_require_training_safe_feature",
        lambda *_args: SimpleNamespace(timestamp_key="reference_date"),
    )
    monkeypatch.setattr(
        training_sets,
        "_read_feature_history",
        lambda *_args, **_kwargs: (Frame(({},), ()), (ready_binding,)),
    )
    monkeypatch.setattr(
        training_sets,
        "_apply_point_in_time_lookup",
        apply_lookup,
    )
    monkeypatch.setattr(
        training_sets,
        "summarise_binary_labels",
        lambda *_args: (2, 1),
    )
    monkeypatch.setattr(
        training_sets,
        "schema_checksum",
        lambda *_args: "d" * 64,
    )
    monkeypatch.setattr(
        training_sets,
        "feature_value_checksum",
        lambda *_args: "e" * 64,
    )

    result = training_sets.build_training_set(
        object(),
        definition,
        observations,
        catalog="catalog",
        schema="schema",
        feature_reference_date="2026-08-06",
        label_end=date(2026, 8, 7),
        code_sha="sha",
    )

    assert result.receipt.observation_start == date(2026, 8, 7)
    assert result.receipt.observation_end == date(2026, 8, 7)


def test_point_in_time_lookup_applies_the_declared_availability_lag():
    source = inspect.getsource(training_sets._apply_point_in_time_lookup)

    assert "lookup.availability_lag_days" in source
    assert "INTERVAL" in source
    assert "if include_feature_audit_columns" in source


def test_feature_audit_columns_are_opt_in(monkeypatch):
    class Frame:
        def __init__(self, columns):
            self.columns = list(columns)

    definition = load_model_definition("analytics_pctr")
    observations = Frame(definition.training_observation.selected_columns)
    ready_binding = _binding(date(2026, 8, 1))

    def apply_lookup(
        frame,
        _feature_frame,
        lookup,
        *,
        feature_timestamp_key,
        observation_keys,
        include_feature_audit_columns=False,
    ):
        assert feature_timestamp_key == "reference_date"
        assert observation_keys == definition.observation_keys
        outputs = training_sets._lookup_output_names(lookup)
        audits = ()
        if include_feature_audit_columns:
            audits = tuple(
                audit_column(output)
                for output in outputs
                for audit_column in (
                    training_sets.feature_missing_audit_column,
                    training_sets.feature_default_audit_column,
                )
            )
        return Frame((*frame.columns, *outputs, *audits))

    def frame_digest(frame):
        return hashlib.sha256(
            json.dumps(frame.columns, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    monkeypatch.setattr(
        "next_ads.features.load_feature_store_registry",
        lambda: object(),
    )
    monkeypatch.setattr(
        training_sets,
        "validate_unique_non_null_keys",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        training_sets,
        "_date_window",
        lambda *_args: (date(2026, 8, 1), date(2026, 8, 1)),
    )
    monkeypatch.setattr(
        training_sets,
        "_validate_label_maturity",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        training_sets,
        "_require_training_safe_feature",
        lambda *_args: SimpleNamespace(timestamp_key="reference_date"),
    )
    monkeypatch.setattr(
        training_sets,
        "_read_feature_history",
        lambda *_args, **_kwargs: (Frame(()), (ready_binding,)),
    )
    monkeypatch.setattr(
        training_sets, "_apply_point_in_time_lookup", apply_lookup
    )
    monkeypatch.setattr(
        training_sets,
        "summarise_binary_labels",
        lambda *_args: (2, 1),
    )
    monkeypatch.setattr(training_sets, "schema_checksum", frame_digest)
    monkeypatch.setattr(training_sets, "feature_value_checksum", frame_digest)

    arguments = {
        "catalog": "catalog",
        "schema": "schema",
        "feature_reference_date": "2026-08-01",
        "label_end": date(2026, 8, 2),
        "code_sha": "abc123",
    }
    legacy = training_sets.build_training_set(
        object(), definition, observations, **arguments
    )
    explicit_legacy = training_sets.build_training_set(
        object(),
        definition,
        observations,
        include_feature_audit_columns=False,
        **arguments,
    )
    research = training_sets.build_training_set(
        object(),
        definition,
        observations,
        include_feature_audit_columns=True,
        **arguments,
    )

    assert explicit_legacy.frame.columns == legacy.frame.columns
    assert explicit_legacy.receipt.receipt_id == legacy.receipt.receipt_id
    assert explicit_legacy.receipt.schema_checksum == (
        legacy.receipt.schema_checksum
    )
    assert (
        explicit_legacy.receipt.data_checksum == legacy.receipt.data_checksum
    )
    assert not any(
        column.startswith("__feature_") for column in legacy.frame.columns
    )
    assert research.receipt.receipt_id != legacy.receipt.receipt_id
    assert research.receipt.schema_checksum != legacy.receipt.schema_checksum
    assert research.receipt.data_checksum != legacy.receipt.data_checksum
    assert sum(
        column.startswith("__feature_") for column in research.frame.columns
    ) == 2 * len(definition.model_feature_columns)


def test_public_builders_default_to_legacy_schema_and_research_opts_in():
    for builder in (
        training_sets.build_training_set,
        training_sets.build_training_set_from_feature_store,
    ):
        parameter = inspect.signature(builder).parameters[
            "include_feature_audit_columns"
        ]
        assert parameter.default is False

    project_root = Path(__file__).resolve().parents[2]
    research_job = (
        project_root
        / "jobs"
        / "model"
        / "research"
        / "run_declared_research.py"
    ).read_text()
    legacy_job = (
        project_root
        / "jobs"
        / "model"
        / "development"
        / "run_declared_model.py"
    ).read_text()

    assert "include_feature_audit_columns=True" in research_job
    assert "include_feature_audit_columns" not in legacy_job
