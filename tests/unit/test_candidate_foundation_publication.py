import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from jobs.nextads_candidates.publish_candidate_foundation import (
    _validated_customer_cell_status,
)
from next_ads.candidates import foundation_manifest as manifest_module
from next_ads.candidates.foundation import FALLBACK_PREVIOUS, READY_FOR_NEXTADS
from next_ads.candidates.foundation_manifest import (
    canonical_json,
    parse_output_bindings,
    publish_candidate_foundation_manifest,
)


RUN_DATE = date(2026, 8, 3)


def _outputs():
    return {
        name: {
            "table": f"catalog.schema.{name}",
            "delta_version": index,
            "schema_version": "v1",
            "row_count": 1,
            "content_checksum": f"checksum-{name}",
        }
        for index, name in enumerate(
            ("customer_cells", "repeat_ad_exposure", "ad_feedback"),
            start=1,
        )
    }


def test_output_binding_json_is_canonical_and_order_independent():
    bindings = _outputs()
    reversed_bindings = dict(reversed(list(bindings.items())))

    assert canonical_json(bindings) == canonical_json(reversed_bindings)
    assert parse_output_bindings(canonical_json(bindings)) == bindings


def test_output_bindings_reject_missing_required_input():
    incomplete = _outputs()
    incomplete.pop("ad_feedback")

    with pytest.raises(ValueError, match="missing: ad_feedback"):
        parse_output_bindings(json.dumps(incomplete))


def test_customer_cell_status_accepts_only_same_day_or_previous_day_fallback():
    assert (
        _validated_customer_cell_status(
            RUN_DATE,
            RUN_DATE,
            READY_FOR_NEXTADS,
        )
        == READY_FOR_NEXTADS
    )
    assert (
        _validated_customer_cell_status(
            RUN_DATE,
            date(2026, 8, 2),
            FALLBACK_PREVIOUS,
        )
        == FALLBACK_PREVIOUS
    )

    with pytest.raises(ValueError, match="must match"):
        _validated_customer_cell_status(
            RUN_DATE,
            date(2026, 8, 2),
            READY_FOR_NEXTADS,
        )
    with pytest.raises(ValueError, match="previous logical day"):
        _validated_customer_cell_status(
            RUN_DATE,
            date(2026, 8, 1),
            FALLBACK_PREVIOUS,
        )
    with pytest.raises(ValueError, match="must be READY_FOR_NEXTADS"):
        _validated_customer_cell_status(RUN_DATE, RUN_DATE, "UNKNOWN")


def test_manifest_writes_sources_before_ready_build(monkeypatch):
    writes = []

    class _Spark:
        def createDataFrame(self, rows, schema):  # noqa: N802
            return SimpleNamespace(rows=rows, schema=schema)

    def _replace(frame, table, scope, columns, *, spark):
        writes.append((table, scope, tuple(columns), frame.rows))

    monkeypatch.setattr(manifest_module, "replace_scope_by_name", _replace)
    build = publish_candidate_foundation_manifest(
        _Spark(),
        snapshot_id="foundation",
        run_date=RUN_DATE,
        source_bindings=(
            {
                "name": "customer_cells",
                "role": "eligibility",
                "table": "catalog.schema.cells",
                "delta_version": 7,
                "schema_version": "v1",
                "schema_checksum": "schema",
                "required": True,
            },
        ),
        output_bindings=_outputs(),
        warning_count=0,
        status=READY_FOR_NEXTADS,
        task_run_id=123,
        execution_count=0,
        builds_table="catalog.schema.builds",
        sources_table="catalog.schema.sources",
        completed_at=datetime(2026, 8, 3, 16, 30, tzinfo=timezone.utc),
    )

    assert [write[0] for write in writes] == [
        "catalog.schema.sources",
        "catalog.schema.builds",
    ]
    assert build.status == READY_FOR_NEXTADS
    assert build.attempt_id == "foundation:attempt:0:123"


def test_duplicate_source_names_fail_before_any_write(monkeypatch):
    writes = []
    monkeypatch.setattr(
        manifest_module,
        "replace_scope_by_name",
        lambda *args, **kwargs: writes.append(args),
    )
    source = {
        "name": "customer_cells",
        "role": "eligibility",
        "table": "catalog.schema.cells",
        "delta_version": 7,
        "schema_version": "v1",
        "schema_checksum": "schema",
        "required": True,
    }

    with pytest.raises(ValueError, match="source names must be unique"):
        publish_candidate_foundation_manifest(
            SimpleNamespace(),
            snapshot_id="foundation",
            run_date=RUN_DATE,
            source_bindings=(source, source),
            output_bindings=_outputs(),
            warning_count=0,
            status=READY_FOR_NEXTADS,
            task_run_id=123,
            execution_count=0,
            builds_table="catalog.schema.builds",
            sources_table="catalog.schema.sources",
        )

    assert writes == []
