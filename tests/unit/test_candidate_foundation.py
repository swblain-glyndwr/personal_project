from datetime import date

import pytest

from next_ads.candidates import foundation as foundation_module
from next_ads.candidates.foundation import (
    build_ad_feedback_metrics,
    build_repeat_ad_exposure,
    load_candidate_foundation_inputs,
    score_ad_feedback_metrics,
)
from next_ads.ranking.theme_score_eligibility import (
    select_feedback_scaling_population,
)


RUN_DATE = date(2026, 8, 3)


def _sessions(spark, *, app=False):
    rows = [
        ("v1", date(2026, 8, 1), "UK", "Mobile", "A"),
        ("v2", date(2026, 7, 31), "UK", "Desktop", "A"),
        ("v3", date(2026, 7, 30), "UK", "Mobile", "A"),
        ("v4", date(2026, 7, 29), "UK", "Mobile", "A"),
        ("today", RUN_DATE, "UK", "Mobile", "A"),
    ]
    frame = spark.createDataFrame(
        rows,
        [
            "UniqueVisitID",
            "Date",
            "SiteCountry",
            "Device",
            "AccountNumber_RPID",
        ],
    )
    return frame.drop("Device") if app else frame


def _actions(spark, *, app=False):
    rows = [
        (visit, day, "Banner Impression - Next Ads", "AD_A")
        for visit, day in (
            ("v1", date(2026, 8, 1)),
            ("v2", date(2026, 7, 31)),
            ("v3", date(2026, 7, 30)),
            ("v4", date(2026, 7, 29)),
            ("today", RUN_DATE),
        )
    ]
    if app:
        return spark.createDataFrame(
            [(*row, "PLP") for row in rows],
            ["UniqueVisitID", "Date", "Action", "Level2", "ScreenName"],
        )
    return spark.createDataFrame(
        [(*row, "/shoppingbag") for row in rows],
        ["UniqueVisitID", "Date", "Action", "Level2", "PagePath"],
    )


def test_repeat_exposure_uses_logical_date_and_is_partition_stable(spark):
    sessions = _sessions(spark)
    sessions_app = _sessions(spark, app=True)
    actions = _actions(spark)
    actions_app = _actions(spark, app=True)

    one = build_repeat_ad_exposure(
        sessions.repartition(1),
        sessions_app.repartition(1),
        actions.repartition(1),
        actions_app.repartition(1),
        run_date=RUN_DATE,
    ).collect()
    four = build_repeat_ad_exposure(
        sessions.repartition(4),
        sessions_app.repartition(4),
        actions.repartition(4),
        actions_app.repartition(4),
        run_date=RUN_DATE,
    ).collect()

    assert sorted(one) == sorted(four)
    assert len(one) == 1
    assert one[0]["sessions_seen_ad_in_last_7_days"] == 4
    assert one[0]["MultiSessionDownweightScore"] == pytest.approx(0.8)


def _feedback_results(spark):
    return spark.createDataFrame(
        [
            (
                "AD_A",
                date(2026, 7, 31),
                100,
                200.0,
                100,
                100.0,
                1.0,
            ),
            (
                "AD_B",
                date(2026, 7, 31),
                100,
                50.0,
                100,
                100.0,
                1.0,
            ),
            (
                "OUTSIDE",
                date(2026, 8, 3),
                100,
                1000.0,
                100,
                100.0,
                1.0,
            ),
        ],
        [
            "UniqueAdID",
            "SessionDate",
            "Sessions",
            "ApportionedRevenue",
            "C_Sessions",
            "C_ApportionedRevenue",
            "SessionOverlapRatio",
        ],
    )


def test_feedback_is_preaggregated_then_scaled_for_route_active_ads(spark):
    metrics = build_ad_feedback_metrics(
        _feedback_results(spark),
        run_date=RUN_DATE,
        sessions_threshold=1,
        lookback_period_days=7,
    )
    active = spark.createDataFrame([("AD_A",)], ["UniqueAdID"])

    result = score_ad_feedback_metrics(
        metrics,
        active,
        ad_feedback_weight=0.05,
    ).collect()

    assert [row["UniqueAdID"] for row in result] == ["AD_A"]
    assert result[0]["AdFeedbackScore"] == pytest.approx(1.05)
    assert "OUTSIDE" not in {row["UniqueAdID"] for row in metrics.collect()}


def test_constant_zero_feedback_is_neutral(spark):
    metrics = spark.createDataFrame(
        [("AD_A", 0.0)],
        ["UniqueAdID", "IncARPSAdjPct"],
    )

    result = score_ad_feedback_metrics(
        metrics,
        metrics.select("UniqueAdID"),
        ad_feedback_weight=0.05,
    ).first()

    assert result["AdFeedbackScore"] == 1.0


def test_feedback_scaling_population_includes_every_active_route_ad(spark):
    ads = spark.createDataFrame(
        [
            ("THEME", 0, "Shoes", None),
            ("BASIC", 0, None, None),
            ("AUDIENCE", 1, None, None),
            ("UNDERPERFORMING", 0, "Shoes", True),
            ("THEME", 0, "Shoes", None),
        ],
        ["UniqueAdID", "AudienceOnly", "Themes", "IsUnderperforming"],
    )

    population = {
        row["UniqueAdID"]
        for row in select_feedback_scaling_population(ads).collect()
    }

    assert population == {
        "THEME",
        "BASIC",
        "AUDIENCE",
        "UNDERPERFORMING",
    }


def test_candidate_inputs_read_only_manifest_delta_versions(spark, monkeypatch):
    calls = []
    frames = {
        "cells": spark.createDataFrame([("A",)], ["AccountNumber"]),
        "exposure": spark.createDataFrame(
            [("foundation", RUN_DATE, "A", "AD_A", 3, 0.84)],
            [
                "CandidateFoundationSnapshotID",
                "RunDate",
                "AccountNumber",
                "AdSeen",
                "sessions_seen_ad_in_last_7_days",
                "MultiSessionDownweightScore",
            ],
        ),
        "feedback": spark.createDataFrame(
            [("foundation", RUN_DATE, "AD_A", 0.2)],
            [
                "CandidateFoundationSnapshotID",
                "RunDate",
                "UniqueAdID",
                "IncARPSAdjPct",
            ],
        ),
    }

    def _read(_spark, table, version):
        calls.append((table, version))
        return frames[table]

    monkeypatch.setattr(foundation_module, "read_delta_version", _read)

    selected = load_candidate_foundation_inputs(
        spark,
        snapshot_id="foundation",
        source_run_date=RUN_DATE,
        customer_cells_table="cells",
        customer_cells_delta_version=7,
        repeat_ad_exposure_table="exposure",
        repeat_ad_exposure_delta_version=11,
        ad_feedback_table="feedback",
        ad_feedback_delta_version=13,
    )

    assert calls == [("cells", 7), ("exposure", 11), ("feedback", 13)]
    assert selected.customer_cells.first()["AccountNumber"] == "A"
    assert selected.repeat_ad_exposure.first()["AdSeen"] == "AD_A"
