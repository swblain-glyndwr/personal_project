from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = (
    PROJECT_ROOT
    / "jobs"
    / "features"
    / "nextads"
    / "build_shopping_bag_click_labels.py"
)


def test_label_builder_logs_a_bounded_non_gating_evidence_funnel():
    job_source = (
        PROJECT_ROOT
        / "jobs"
        / "features"
        / "nextads"
        / "build_shopping_bag_click_labels.py"
    ).read_text()
    evidence_source = (
        PROJECT_ROOT
        / "src"
        / "next_ads"
        / "features"
        / "shopping_bag_label_evidence.py"
    ).read_text()

    assert "SHOPPING_BAG_LABEL_FUNNEL=" in job_source
    assert "next_uk_nextads_results_ads_location" in job_source
    assert "pinned_spark.table(reporting_table)" not in job_source
    assert '"is_gate": False' in evidence_source
    assert "AMBIGUOUS_ACCOUNT" in evidence_source
    assert "AMBIGUOUS_MATCH" in evidence_source
    assert "PRE_REFRESH" in evidence_source
    assert "UNKNOWN_TREATMENT" in evidence_source
    assert "event_cms_page_id" in evidence_source
    assert "assignment_cms_page_id" in evidence_source
    assert "label_horizon_days" in evidence_source
    assert '"quality_checks"' in evidence_source


def test_label_publication_uses_the_registry_snapshot_date_scope():
    source = BUILDER_PATH.read_text()
    registry = yaml.safe_load(
        (
            PROJECT_ROOT
            / "configs"
            / "features"
            / "nextads_feature_store.yaml"
        ).read_text()
    )
    observed_labels = next(
        table
        for table in registry["feature_store"]["physical_tables"]
        if table["name"]
        == "next_uk_nextads_fs_shopping_bag_click_labels"
    )

    assert "write_options=" not in source
    assert observed_labels["timestamp_key"] == "exposure_timestamp"
    assert observed_labels["snapshot_date_key"] == "session_date"
