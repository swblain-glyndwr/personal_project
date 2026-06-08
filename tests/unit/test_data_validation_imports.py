from next_ads.data.validation import custom_checks as new_custom_checks
from next_ads.data.validation import schemas as new_schemas
from next_ads.data_validation import custom_checks as legacy_custom_checks
from next_ads.data_validation import schemas as legacy_schemas


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
