from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from next_ads.control import control_sheet_audit as audit_module
from next_ads.control.control_sheet_audit import (
    REVIEW,
    WARNING,
    ControlSheetAuditFinding,
    ControlSheetAuditReport,
    ControlSheetAuditSpec,
    audit_control_sheet,
)


def test_cms_audit_reference_grain_retains_the_control_url():
    assert audit_module._CMS_AUDIT_REFERENCE_GRAIN == (
        "_UniqueAdID",
        "_CMSPageID",
        "_ControlURL",
    )


@pytest.fixture(scope="module")
def audit_spark():
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("control-sheet-audit-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def _raw(
    spark,
    rows,
    *,
    placements=("HomePage", "ShoppingBagPage"),
):
    columns = [
        "UniqueAdID",
        "CMSPageID",
        "StartDate",
        "EndDate",
        "Status",
        "AudienceOnly",
        "AdVariant",
        *placements,
    ]
    return spark.createDataFrame(rows, columns).withColumn(
        "URL",
        F.lit(""),
    )


def _processed(spark, rows):
    return spark.createDataFrame(
        rows,
        [
            "UniqueAdID",
            "CMSPageID",
            "PageType",
            "StartDate",
            "EndDate",
            "AudienceOnly",
            "AdVariant",
        ],
    )


def _cms(spark, rows):
    return spark.createDataFrame(rows, ["CMSPageID", "cms_data"])


def _spec(max_examples=10):
    return ControlSheetAuditSpec(
        route="v2",
        run_date=date(2026, 7, 29),
        placement_columns=("HomePage", "ShoppingBagPage"),
        expected_scopes=("HomePage", "ShoppingBagPage"),
        max_examples=max_examples,
    )


def _findings_by_code(report):
    return {finding.code: finding for finding in report.findings}


def test_audit_reports_raw_processed_and_cms_facts_without_blocking(audit_spark):
    raw = _raw(
        audit_spark,
        [
            (
                "ad-duplicate",
                "cms-shared",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "TRUE",
                "younger",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-duplicate",
                "cms-shared",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "TRUE",
                "younger",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-shared",
                "cms-shared",
                "29/07/2026",
                "31/07/2026",
                "Inactive",
                "FALSE",
                "younger",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-no-placement",
                "cms-absent",
                "29/07/2026",
                "31/07/2026",
                "unknown",
                "maybe",
                "Younger",
                "yes",
                "FALSE",
            ),
            (
                "ad-cms-absent",
                "cms-absent",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-bad-flags",
                "cms-empty",
                "31/07/2026",
                "30/07/2026",
                "Active",
                "",
                "",
                "yes",
                "",
            ),
            (
                "ad-empty-content",
                "cms-empty",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "older",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-bad-dates",
                "",
                "31/02/2026",
                "",
                "unknown",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                None,
                "cms-good",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-mismatch",
                "cms-mismatch",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "older",
                "TRUE",
                "FALSE",
            ),
        ],
    )
    processed = _processed(
        audit_spark,
        [
            (
                "ad-duplicate",
                "cms-shared",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                1,
                "younger",
            ),
            (
                "ad-shared",
                "cms-shared",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                1,
                "YOUNGER",
            ),
            (
                "ad-invalid-scope",
                "cms-good",
                "OtherPage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
            (
                "ad-old",
                "cms-good",
                "ShoppingBagPage",
                date(2026, 7, 1),
                date(2026, 7, 2),
                0,
                "older",
            ),
            (
                "ad-duplicate-key",
                "cms-good",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
            (
                "ad-duplicate-key",
                "cms-good",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
        ],
    )
    cms = _cms(
        audit_spark,
        [
            (
                "cms-shared",
                '{"data":{"externalPageId":"cms-shared","title":"Shared"}}',
            ),
            ("cms-empty", '{"error":"not found"}'),
            (
                "cms-good",
                '{"data":{"externalPageId":"cms-good","title":"Good"}}',
            ),
            (
                "cms-mismatch",
                '{"data":{"externalPageId":"different-id","title":"Other"}}',
            ),
        ],
    )

    report = audit_control_sheet(
        raw_current=raw,
        processed_current=processed,
        cms_latest=cms,
        spec=_spec(),
    )
    findings = _findings_by_code(report)

    assert report.effective_date == date(2026, 7, 30)
    assert findings["DUPLICATE_UNIQUE_AD_ID"].count == 1
    assert findings["BLANK_UNIQUE_AD_ID"].count == 1
    assert findings["MALFORMED_START_DATE"].count == 1
    assert findings["MALFORMED_END_DATE"].count == 1
    assert findings["START_AFTER_END_DATE"].count == 1
    assert findings["INVALID_STATUS"].count == 1
    assert "STATUS_ACTIVE_OUTSIDE_DATE_WINDOW" not in findings
    assert findings["STATUS_INACTIVE_INSIDE_DATE_WINDOW"].count == 1
    assert findings["INVALID_PLACEMENT_FLAG"].count == 1
    assert findings["ACTIVE_WITH_NO_SELECTED_PLACEMENT"].count == 1
    assert findings["INVALID_AUDIENCE_ONLY"].count == 1
    assert findings["UNKNOWN_AD_VARIANT"].count == 1
    assert findings["INVALID_PROCESSED_SCOPE"].count == 1
    assert findings["PROCESSED_OUT_OF_WINDOW"].count == 1
    assert findings["DUPLICATE_PROCESSED_KEY"].count == 1
    assert findings["AMBIGUOUS_CMS_DECISION_SIGNATURE"].count == 1
    assert findings["CMS_NOT_IN_LATEST_PULL"].count == 1
    assert findings["CMS_CONTENT_MISSING"].count == 1
    assert findings["CMS_EXTERNAL_ID_MISMATCH"].count == 1

    shared = findings["SHARED_CMS_PAGE_ID"]
    assert shared.severity == REVIEW
    assert "review only" in shared.message
    assert all(
        finding.severity in {WARNING, REVIEW}
        for finding in report.findings
    )


def test_previous_inputs_report_added_removed_changed_and_zero_scope(
    audit_spark,
):
    previous_raw = _raw(
        audit_spark,
        [
            (
                "ad-removed",
                "cms-removed",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "FALSE",
                "older",
                "FALSE",
                "TRUE",
            ),
            (
                "ad-changed",
                "cms-changed",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "FALSE",
                "older",
                "FALSE",
                "TRUE",
            ),
        ],
    )
    current_raw = _raw(
        audit_spark,
        [
            (
                "ad-added",
                "cms-added",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "FALSE",
                "older",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-changed",
                "cms-changed",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "TRUE",
                "older",
                "TRUE",
                "FALSE",
            ),
        ],
    )
    previous_processed = _processed(
        audit_spark,
        [
            (
                "ad-removed",
                "cms-removed",
                "ShoppingBagPage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
            (
                "ad-changed",
                "cms-changed",
                "ShoppingBagPage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
        ],
    )
    current_processed = _processed(
        audit_spark,
        [
            (
                "ad-added",
                "cms-added",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "older",
            ),
            (
                "ad-changed",
                "cms-changed",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                1,
                "older",
            ),
        ],
    )
    cms = _cms(
        audit_spark,
        [
            (
                "cms-added",
                '{"data":{"externalPageId":"cms-added","title":"Added"}}',
            ),
            (
                "cms-changed",
                '{"data":{"externalPageId":"cms-changed","title":"Changed"}}',
            ),
        ],
    )

    report = audit_control_sheet(
        raw_current=current_raw,
        processed_current=current_processed,
        cms_latest=cms,
        spec=_spec(),
        previous_raw=previous_raw,
        previous_processed=previous_processed,
    )
    findings = _findings_by_code(report)

    assert findings["CONTROL_AD_ADDED"].count == 1
    assert findings["CONTROL_AD_REMOVED"].count == 1
    assert findings["CONTROL_AD_CHANGED"].count == 1
    assert findings["PROCESSED_ROUTE_ADDED"].count == 2
    assert findings["PROCESSED_ROUTE_REMOVED"].count == 2
    assert findings["PROCESSED_ROUTE_SET_CHANGED"].count == 1
    assert findings["SCOPE_DROPPED_TO_ZERO"].count == 1
    assert findings["SCOPE_DROPPED_TO_ZERO"].examples == (
        "PageType=ShoppingBagPage|previous_rows=2",
    )


def test_shared_cms_content_with_different_targeting_is_review_only(
    audit_spark,
):
    raw = _raw(
        audit_spark,
        [
            (
                "ad-theme-a",
                "cms-shared",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "FALSE",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-theme-b",
                "cms-shared",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "FALSE",
                "",
                "TRUE",
                "FALSE",
            ),
        ],
    )
    processed = audit_spark.createDataFrame(
        [
            (
                "ad-theme-a",
                "cms-shared",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
                "older boys footwear",
            ),
            (
                "ad-theme-b",
                "cms-shared",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
                "younger boys footwear",
            ),
        ],
        [
            "UniqueAdID",
            "CMSPageID",
            "PageType",
            "StartDate",
            "EndDate",
            "AudienceOnly",
            "AdVariant",
            "Themes",
        ],
    )
    cms = _cms(
        audit_spark,
        [("cms-shared", '{"data":{"externalPageId":"cms-shared"}}')],
    )

    report = audit_control_sheet(
        raw_current=raw,
        processed_current=processed,
        cms_latest=cms,
        spec=_spec(),
    )
    findings = _findings_by_code(report)

    assert findings["SHARED_CMS_PAGE_ID"].severity == REVIEW
    assert "AMBIGUOUS_CMS_DECISION_SIGNATURE" not in findings


def test_cms_target_url_checks_cover_missing_and_wrong_links(audit_spark):
    raw = _raw(
        audit_spark,
        [
            (
                "ad-target-missing",
                "cms-target-missing",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-target-wrong",
                "cms-target-wrong",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
            (
                "ad-target-match",
                "cms-target-match",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            ),
        ],
    ).withColumn("URL", F.lit("/expected"))
    processed = _processed(
        audit_spark,
        [
            (
                ad_id,
                cms_id,
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
            )
            for ad_id, cms_id in (
                ("ad-target-missing", "cms-target-missing"),
                ("ad-target-wrong", "cms-target-wrong"),
                ("ad-target-match", "cms-target-match"),
            )
        ],
    )
    cms = _cms(
        audit_spark,
        [
            (
                "cms-target-missing",
                (
                    '{"data":{"externalPageId":"cms-target-missing",'
                    '"title":"Missing target"}}'
                ),
            ),
            (
                "cms-target-wrong",
                (
                    '{"data":{"externalPageId":"cms-target-wrong",'
                    '"title":"Wrong target",'
                    '"placements":[{"content":[{"items":'
                    '[{"target":"/wrong"}]}]}]}}'
                ),
            ),
            (
                "cms-target-match",
                (
                    '{"data":{"externalPageId":"cms-target-match",'
                    '"title":"Matching target",'
                    '"placements":[{"content":[{"items":'
                    '[{"target":"/expected"}]}]}]}}'
                ),
            ),
        ],
    )

    report = audit_control_sheet(
        raw_current=raw,
        processed_current=processed,
        cms_latest=cms,
        spec=_spec(),
    )
    findings = _findings_by_code(report)

    assert findings["CMS_TARGET_URL_MISSING"].count == 1
    assert findings["CMS_TARGET_URL_MISMATCH"].count == 1
    assert "ad-target-match" not in " ".join(
        findings["CMS_TARGET_URL_MISMATCH"].examples
    )


def test_cms_target_url_checks_preserve_every_distinct_control_url(
    audit_spark,
):
    raw = _raw(
        audit_spark,
        [
            (
                "ad-multiple-targets",
                "cms-multiple-targets",
                "29/07/2026",
                "31/07/2026",
                "Active",
                "",
                "",
                "TRUE",
                "FALSE",
            )
        ],
    ).withColumn(
        "URL",
        F.explode(F.array(F.lit("/expected"), F.lit("/wrong"))),
    )
    processed = _processed(
        audit_spark,
        [
            (
                "ad-multiple-targets",
                "cms-multiple-targets",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
            )
        ],
    )
    cms = _cms(
        audit_spark,
        [
            (
                "cms-multiple-targets",
                (
                    '{"data":{"externalPageId":"cms-multiple-targets",'
                    '"title":"Multiple targets","placements":[{"content":'
                    '[{"items":[{"target":"/expected"}]}]}]}}'
                ),
            )
        ],
    )

    report = audit_control_sheet(
        raw_current=raw,
        processed_current=processed,
        cms_latest=cms,
        spec=_spec(),
    )
    mismatch = _findings_by_code(report)["CMS_TARGET_URL_MISMATCH"]

    assert mismatch.count == 1
    assert "/wrong" in " ".join(mismatch.examples)


def test_report_is_partition_stable_sorted_and_capped(audit_spark):
    raw_rows = [
        (
            f"ad-{index:02d}",
            f"cms-{index:02d}",
            "29/07/2026",
            "31/07/2026",
            "Active",
            "invalid",
            "",
            "TRUE",
            "FALSE",
        )
        for index in range(8)
    ]
    raw = _raw(audit_spark, raw_rows)
    processed = _processed(
        audit_spark,
        [
            (
                f"ad-{index:02d}",
                f"cms-{index:02d}",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
            )
            for index in range(8)
        ],
    )
    cms = _cms(
        audit_spark,
        [
            (
                f"cms-{index:02d}",
                (
                    '{"data":{"externalPageId":'
                    f'"cms-{index:02d}","title":"Ad {index:02d}"'
                    "}}"
                ),
            )
            for index in range(8)
        ],
    )

    one_partition = audit_control_sheet(
        raw_current=raw.repartition(1),
        processed_current=processed.repartition(1),
        cms_latest=cms.repartition(1),
        spec=_spec(max_examples=3),
    )
    four_partitions = audit_control_sheet(
        raw_current=raw.repartition(4),
        processed_current=processed.repartition(4),
        cms_latest=cms.repartition(4),
        spec=_spec(max_examples=3),
    )

    assert one_partition == four_partitions
    finding = _findings_by_code(one_partition)["INVALID_AUDIENCE_ONLY"]
    assert finding.count == 8
    assert finding.examples == (
        "UniqueAdID=ad-00|AudienceOnly=invalid",
        "UniqueAdID=ad-01|AudienceOnly=invalid",
        "UniqueAdID=ad-02|AudienceOnly=invalid",
    )
    assert [item.code for item in one_partition.findings] == sorted(
        item.code for item in one_partition.findings
    )


def test_report_rendering_is_explicitly_warning_only_and_bounded():
    report = ControlSheetAuditReport(
        route="v2",
        effective_date=date(2026, 7, 30),
        findings=(
            ControlSheetAuditFinding(
                severity=WARNING,
                code="INVALID_AUDIENCE_ONLY",
                count=1,
                examples=("UniqueAdID=ad-1|AudienceOnly=invalid",),
                message="AudienceOnly is invalid.",
            ),
        ),
    )

    assert report.has_warnings
    assert "Warning-only: no data was changed or blocked." in report.render()
    compact = report.compact_message(max_chars=180)
    assert len(compact) <= 180
    assert "Warning-only" in compact


def test_spec_normalises_route_defaults_and_rejects_unknown_route():
    spec = ControlSheetAuditSpec(
        route=" V1 ",
        run_date=date(2026, 7, 29),
        placement_columns=("HomePage", "HomePage"),
        expected_scopes=("HomePage",),
    )

    assert spec.route == "v1"
    assert spec.scope_column == "Location"
    assert spec.placement_columns == ("HomePage",)
    assert spec.effective_date == date(2026, 7, 30)

    with pytest.raises(ValueError, match="route must be"):
        ControlSheetAuditSpec(
            route="v3",
            run_date=date(2026, 7, 29),
            placement_columns=("HomePage",),
            expected_scopes=("HomePage",),
        )


def test_input_contracts_fail_before_audit_actions(audit_spark):
    incomplete_raw = audit_spark.createDataFrame(
        [("ad-1",)],
        ["UniqueAdID"],
    )
    processed = _processed(
        audit_spark,
        [
            (
                "ad-1",
                "cms-1",
                "HomePage",
                date(2026, 7, 29),
                date(2026, 7, 31),
                0,
                "",
            )
        ],
    )
    cms = _cms(
        audit_spark,
        [("cms-1", '{"data":{"externalPageId":"cms-1"}}')],
    )

    with pytest.raises(ValueError, match="raw_current is missing"):
        audit_control_sheet(
            raw_current=incomplete_raw,
            processed_current=processed,
            cms_latest=cms,
            spec=_spec(),
        )
