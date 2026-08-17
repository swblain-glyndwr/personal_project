"""Regression checks for the Databricks CLI pipeline installer."""

from pathlib import Path


INSTALLER = (
    Path(__file__).parents[2] / "devops" / "scripts" / "install_databricks_cli.sh"
)


def test_installer_retries_transient_download_failures_and_fails_the_pipe() -> None:
    script = INSTALLER.read_text(encoding="utf-8")

    assert "set -euo pipefail" in script
    assert "--retry 5" in script
    assert "--retry-all-errors" in script
    assert "--fail" in script
