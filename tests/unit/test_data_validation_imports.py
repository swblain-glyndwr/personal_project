import os
import subprocess
import sys
from pathlib import Path

from next_ads.data.validation import custom_checks as new_custom_checks
from next_ads.data.validation import schemas as new_schemas
from next_ads.data_validation import custom_checks as legacy_custom_checks
from next_ads.data_validation import schemas as legacy_schemas


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_data_validation_schema_imports_work_from_new_and_legacy_paths():
    assert (
        legacy_schemas.ControlSheetInputModel
        is new_schemas.ControlSheetInputModel
    )
    assert (
        legacy_schemas.GlobalSolutionOutputModel
        is new_schemas.GlobalSolutionOutputModel
    )


def test_data_validation_custom_check_imports_work_from_new_and_legacy_paths():
    assert legacy_custom_checks.isin_spark is new_custom_checks.isin_spark
    assert (
        legacy_custom_checks.str_matches_spark
        is new_custom_checks.str_matches_spark
    )
    assert legacy_custom_checks.unique_spark is new_custom_checks.unique_spark


def test_src_first_package_keeps_legacy_imports_available(tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")

    script = """
import next_ads

assert "src" in next_ads.__file__.replace("\\\\", "/")
import next_ads.Assignment
import next_ads.Attributes
import next_ads.Export
import next_ads.Plotting
import next_ads.Results
import next_ads.Scoring
import next_ads.data.validation
import next_ads.data_validation
from next_ads.utils import config_manager, etl
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        check=True,
    )
