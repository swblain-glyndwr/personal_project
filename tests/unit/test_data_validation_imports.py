import os
import subprocess
import sys
from pathlib import Path

from next_ads.data.validation import custom_checks, schemas


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_data_validation_package_exposes_expected_contracts():
    assert schemas.ControlSheetInputModel is not None
    assert schemas.GlobalSolutionOutputModel is not None
    assert callable(custom_checks.isin_spark)
    assert callable(custom_checks.str_matches_spark)
    assert callable(custom_checks.unique_spark)


def test_src_package_imports_from_an_isolated_working_directory(tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")

    script = """
import next_ads

assert "src" in next_ads.__file__.replace("\\\\", "/")
import next_ads.common.config_manager
import next_ads.common.etl
import next_ads.control.attributes
import next_ads.data.validation
import next_ads.decisioning.assignment
import next_ads.delivery.export
import next_ads.ranking.scoring
import next_ads.reporting.plotting
import next_ads.reporting.results
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        check=True,
    )
