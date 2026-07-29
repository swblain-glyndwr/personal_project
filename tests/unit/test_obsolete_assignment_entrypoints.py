from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_obsolete_assignment_truncation_entrypoint_is_not_deployable():
    obsolete_path = (
        PROJECT_ROOT
        / "jobs/table_operations/truncate_assignments_latest.py"
    )
    assert not obsolete_path.exists()

    bundle_definition = (PROJECT_ROOT / "databricks.yml").read_text()
    job_resources = "\n".join(
        path.read_text()
        for path in (
            PROJECT_ROOT / "pipelines/databricks/jobs"
        ).glob("*.y*ml")
    )

    assert "truncate_assignments_latest" not in bundle_definition
    assert "truncate_assignments_latest" not in job_resources
