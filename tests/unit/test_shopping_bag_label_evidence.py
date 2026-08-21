from datetime import date

import pytest

from next_ads.features.shopping_bag_label_evidence import (
    EVIDENCE_CONTRACT,
    bounded_evidence_section,
    reporting_sanity_payload,
)


def test_bounded_evidence_marks_omitted_groups_and_serializes_dates():
    section = bounded_evidence_section(
        [
            {"stage": "first", "reference_date": date(2026, 8, 10)},
            {"stage": "second"},
            {"stage": "third"},
        ],
        max_groups=2,
    )

    assert section == {
        "total_groups": 3,
        "returned_groups": 2,
        "truncated": True,
        "rows": [
            {"stage": "first", "reference_date": "2026-08-10"},
            {"stage": "second"},
        ],
    }


def test_bounded_evidence_rejects_an_unbounded_zero_limit():
    with pytest.raises(ValueError, match="max_groups must be positive"):
        bounded_evidence_section([], max_groups=0)


def test_reporting_comparison_is_explicitly_directional_and_non_gating():
    payload = reporting_sanity_payload(
        reference_date="2026-08-10",
        label_impressions=100,
        label_positive_exposures=5,
        reporting_soft_impressions=80,
        reporting_soft_clicks=8,
        reporting_table="marketingdata_prod.warehouse.results",
        status="AVAILABLE",
    )

    assert EVIDENCE_CONTRACT.endswith("/v1")
    assert payload["is_gate"] is False
    assert payload["observed_event_labels"]["click_rate"] == pytest.approx(0.05)
    assert payload["reporting_soft_metrics"]["click_rate"] == pytest.approx(0.1)
    assert payload["directional_click_rate_difference"] == pytest.approx(-0.05)
    assert "must not be asserted equal" in payload["comparison_rule"]


def test_unavailable_reporting_evidence_keeps_label_counts():
    payload = reporting_sanity_payload(
        reference_date="2026-08-10",
        label_impressions=12,
        label_positive_exposures=2,
        reporting_soft_impressions=None,
        reporting_soft_clicks=None,
        reporting_table="missing.results",
        status="UNAVAILABLE",
        detail="not readable",
    )

    assert payload["status"] == "UNAVAILABLE"
    assert payload["is_gate"] is False
    assert payload["observed_event_labels"]["impressions"] == 12
    assert payload["reporting_soft_metrics"]["click_rate"] is None
    assert payload["directional_click_rate_difference"] is None
