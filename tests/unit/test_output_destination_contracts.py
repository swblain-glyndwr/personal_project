from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

DIRECT_WRITE_TOKENS = (
    ".saveAsTable(",
    ".save()",
    "client.write_table(",
)
PATH_WRITE_TOKENS = (".csv(", ".parquet(")
ESTABLISHED_EXACT_PATH_WRITERS = {
    "jobs/nextads_delivery/build_v2_payload.py",
    "jobs/nextads_reporting/results_1.py",
    "jobs/nextads_reporting/results_2.py",
    "jobs/nextads_reporting/results_3.py",
    "src/next_ads/delivery/google_sheets.py",
}
API_OUTPUT_WRITERS = (
    "jobs/model/theme_affinity/monitor_model.py",
    "jobs/model/theme_affinity/setup_quality_monitor.py",
)
RAW_DURABLE_WRITERS = (
    "src/next_ads/model_development/automl_claims.py",
    "src/next_ads/model_development/research_claims.py",
    "src/next_ads/model_development/research_store.py",
    "src/next_ads/ranking/foundation_context.py",
    "src/next_ads/ranking/provider_context.py",
)
RECEIPT_REUSE_WRITERS = (
    "jobs/nextads_candidates/build_theme_scores.py",
    "jobs/orchestration/publish_theme_affinity.py",
    "src/next_ads/candidates/publication.py",
    "src/next_ads/decisioning/assignment_publication.py",
    "src/next_ads/model_development/research_store.py",
    "src/next_ads/ranking/foundation_publication.py",
    "src/next_ads/ranking/provider_publication.py",
)


def test_direct_output_writers_emit_searchable_destination_evidence():
    direct_writers = {
        path
        for root in (ROOT / "jobs", ROOT / "src")
        for path in root.rglob("*.py")
        if any(
            token in path.read_text(encoding="utf-8", errors="replace")
            for token in DIRECT_WRITE_TOKENS
        )
    }

    assert direct_writers
    for path in direct_writers:
        source = path.read_text(encoding="utf-8", errors="replace")
        assert "log_output_location(" in source, path.relative_to(ROOT)


def test_path_writers_use_structured_or_established_exact_evidence():
    path_writers = {
        path
        for root in (ROOT / "jobs", ROOT / "src")
        for path in root.rglob("*.py")
        if ".write"
        in path.read_text(encoding="utf-8", errors="replace")
        and any(
            token in path.read_text(encoding="utf-8", errors="replace")
            for token in PATH_WRITE_TOKENS
        )
    }

    assert path_writers
    for path in path_writers:
        relative_path = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8", errors="replace")
        assert (
            "log_output_location(" in source
            or relative_path in ESTABLISHED_EXACT_PATH_WRITERS
        ), relative_path


def test_non_table_api_outputs_emit_searchable_destination_evidence():
    for relative_path in API_OUTPUT_WRITERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "log_output_location(" in source, relative_path


def test_raw_durable_writers_emit_searchable_resolved_destinations():
    for relative_path in RAW_DURABLE_WRITERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "log_output_location(" in source, relative_path


def test_receipt_reuse_paths_repeat_the_resolved_destination():
    for relative_path in RECEIPT_REUSE_WRITERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "log_output_location(" in source, relative_path
        assert '"reused": True' in source, relative_path


def test_external_and_managed_outputs_are_fully_resolved():
    bigquery = (
        ROOT / "jobs/nextads_control/parse_attributes.py"
    ).read_text(encoding="utf-8")
    monitor = (
        ROOT / "jobs/model/theme_affinity/setup_quality_monitor.py"
    ).read_text(encoding="utf-8")

    assert "bq_options['parentProject']" in bigquery
    assert '"profile_metrics_table_name"' in monitor
    assert '"drift_metrics_table_name"' in monitor


def test_shared_delta_writer_emits_destination_after_the_write():
    source = (ROOT / "src/next_ads/common/delta_writes.py").read_text(
        encoding="utf-8"
    )
    write_end = source.index("spark.catalog.dropTempView(source_view)")
    output_log = source.index("log_output_location(", write_end)
    return_receipt = source.index("return receipt", output_log)

    assert write_end < output_log < return_receipt
