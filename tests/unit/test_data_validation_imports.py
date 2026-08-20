import os
import subprocess
import sys
from pathlib import Path

import pytest

from next_ads.data.validation import custom_checks, schemas


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_data_validation_package_exposes_expected_contracts():
    assert schemas.ControlSheetInputModel is not None
    assert schemas.GlobalSolutionOutputModel is not None
    assert callable(custom_checks.isin_spark)
    assert callable(custom_checks.str_matches_spark)
    assert callable(custom_checks.unique_spark)


@pytest.mark.parametrize(
    "model",
    [
        schemas.ControlSheetInputModel,
        schemas.ControlSheetInputModelv2,
    ],
)
def test_control_sheet_identity_is_unique_ad_id_not_cms_page_id(model):
    schema = model.to_schema()

    unique_ad_checks = {check.name for check in schema.columns["UniqueAdID"].checks}
    cms_page_checks = {check.name for check in schema.columns["CMSPageID"].checks}

    assert "unique_spark" in unique_ad_checks
    assert "unique_spark" not in cms_page_checks


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
