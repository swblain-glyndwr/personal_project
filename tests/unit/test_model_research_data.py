from datetime import date
import inspect
import json
from types import SimpleNamespace

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from next_ads.model_development import research_data


def _plan(**overrides):
    values = {
        "observation_date_column": "observation_date",
        "label_column": "clicked",
        "raw_key_columns": ("account_number", "exposure_id"),
        "feature_columns": ("advert_ctr", "device_type"),
        "slice_columns": ("location",),
        "train_dates": (
            date(2026, 8, 5),
            date(2026, 8, 6),
            date(2026, 8, 7),
            date(2026, 8, 8),
        ),
        "validation_dates": (date(2026, 8, 9), date(2026, 8, 10)),
        "test_dates": (date(2026, 8, 11),),
    }
    values.update(overrides)
    return research_data.ResearchFramePlan(**values)


def test_frame_plan_has_stable_three_way_split_identity():
    first = _plan()
    second = _plan()

    assert first.checksum == second.checksum
    assert first.train_dates[0] == "2026-08-05"
    assert first.validation_dates == ("2026-08-09", "2026-08-10")
    assert first.test_dates == ("2026-08-11",)
    with pytest.raises(ValueError, match="train and test dates overlap"):
        _plan(test_dates=(date(2026, 8, 5),))


def test_frame_plan_forbids_raw_keys_from_features_and_slices():
    with pytest.raises(
        ValueError, match="raw keys overlap features or slices"
    ):
        _plan(feature_columns=("account_number", "advert_ctr"))

    assert "account_number" not in research_data.RESEARCH_FRAME_COLUMNS
    assert "exposure_id" not in research_data.RESEARCH_FRAME_COLUMNS
    assert research_data.RESEARCH_SPLITS == ("train", "validate", "test")


def test_frame_plan_rejects_identity_columns_even_when_not_declared_as_keys():
    with pytest.raises(
        ValueError, match="raw identity columns: account_number"
    ):
        _plan(
            raw_key_columns=("observation_key",),
            feature_columns=("account_number", "advert_ctr"),
        )
    with pytest.raises(
        ValueError, match="raw identity columns: email_address"
    ):
        _plan(
            raw_key_columns=("observation_key",),
            slice_columns=("email_address",),
        )

    plan = _plan(feature_columns=("customer_total_clicks", "advert_ctr"))
    assert "customer_total_clicks" in plan.feature_columns


def test_reporting_slice_can_also_be_a_model_feature():
    plan = _plan(
        feature_columns=("advert_ctr", "location"),
        slice_columns=("location",),
    )

    assert plan.feature_columns == ("advert_ctr", "location")
    assert plan.slice_columns == ("location",)


def test_declared_schemas_retain_original_spark_types_for_unpack():
    frame = SimpleNamespace(
        columns=["advert_ctr", "device_type", "location"],
        schema=StructType(
            [
                StructField("advert_ctr", DoubleType(), True),
                StructField("device_type", StringType(), True),
                StructField("location", StringType(), True),
            ]
        ),
    )
    schemas = research_data.declared_research_schemas(frame, plan=_plan())
    feature_schema = StructType.fromJson(
        json.loads(schemas.feature_schema_json)
    )
    slice_schema = StructType.fromJson(json.loads(schemas.slice_schema_json))

    assert feature_schema["advert_ctr"].dataType == DoubleType()
    assert feature_schema["device_type"].dataType == StringType()
    assert slice_schema.fieldNames() == ["location"]


def test_public_partitions_withhold_test_until_selection():
    automl_source = inspect.getsource(research_data.automl_discovery_partition)
    selected_test_source = inspect.getsource(
        research_data.selected_test_partition
    )

    assert "TRAIN, VALIDATE" in automl_source
    assert "TEST" not in automl_source
    assert "selection_decision_id" in selected_test_source
    assert "F.lit(TEST)" in selected_test_source
