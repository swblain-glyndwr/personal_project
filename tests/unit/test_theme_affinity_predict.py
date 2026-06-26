import importlib

from next_ads.ranking.theme_affinity.predict import (
    _configure_mlflow_for_model_uri,
    _install_numpy_pickle_compat,
    _is_unity_catalog_model_uri,
    _load_mlflow_model,
    _prediction_input_columns,
)


def test_prediction_input_columns_keep_output_passthrough_fields():
    columns = _prediction_input_columns(
        model_input_cols=[
            "month",
            "baskets_behavior__frequency",
            "simple_rules_rank",
        ],
        output_cols=[
            "account_number",
            "theme",
            "month",
            "baskets_behavior__recency_rank",
            "prediction",
        ],
    )

    assert columns == [
        "month",
        "baskets_behavior__frequency",
        "simple_rules_rank",
        "account_number",
        "baskets_behavior__recency_rank",
        "theme_clean",
    ]


def test_numpy_pickle_compat_supports_numpy_2_serialized_artifacts():
    _install_numpy_pickle_compat()

    assert importlib.import_module("numpy._core.multiarray")


def test_unity_catalog_model_uri_detection():
    assert _is_unity_catalog_model_uri(
        "models:/marketingdata_prod.ds_sandbox.nextads_hackathon_model/1"
    )
    assert _is_unity_catalog_model_uri(
        "models:/marketingdata_prod.ds_sandbox.nextads_hackathon_model@champion"
    )
    assert not _is_unity_catalog_model_uri("models:/nextads_hackathon_model/1")
    assert not _is_unity_catalog_model_uri("runs:/abc123/model")


def test_configure_mlflow_uses_unity_catalog_registry_for_uc_model_uri():
    class FakeMlflow:
        def __init__(self):
            self.tracking_uri = None
            self.registry_uri = None

        def set_tracking_uri(self, value):
            self.tracking_uri = value

        def set_registry_uri(self, value):
            self.registry_uri = value

    mlflow = FakeMlflow()

    _configure_mlflow_for_model_uri(
        mlflow,
        "models:/marketingdata_prod.ds_sandbox.nextads_hackathon_model/1",
    )

    assert mlflow.tracking_uri == "databricks"
    assert mlflow.registry_uri == "databricks-uc"


def test_load_mlflow_model_falls_back_to_pyfunc_flavor():
    pyfunc_model = object()

    class FakeXgboost:
        @staticmethod
        def load_model(_model_uri):
            raise RuntimeError("missing xgboost flavor")

    class FakePyfunc:
        @staticmethod
        def load_model(_model_uri):
            return pyfunc_model

    class FakeMlflow:
        xgboost = FakeXgboost()
        pyfunc = FakePyfunc()

    assert _load_mlflow_model(FakeMlflow(), "models:/catalog.schema.model/1") == (
        "pyfunc",
        pyfunc_model,
    )


def test_load_mlflow_model_prefers_spark_flavor_when_allowed():
    spark_model = object()

    class FakeSpark:
        @staticmethod
        def load_model(_model_uri):
            return spark_model

    class FakeMlflow:
        spark = FakeSpark()

    assert _load_mlflow_model(
        FakeMlflow(),
        "models:/catalog.schema.model/1",
        allow_spark=True,
    ) == ("spark", spark_model)


def test_load_mlflow_model_does_not_load_spark_flavor_in_partition_path():
    calls = []
    xgboost_model = object()

    class FakeSpark:
        @staticmethod
        def load_model(_model_uri):
            calls.append("spark")
            raise AssertionError("spark flavor should not be loaded")

    class FakeXgboost:
        @staticmethod
        def load_model(_model_uri):
            calls.append("xgboost")
            return xgboost_model

    class FakePyfunc:
        @staticmethod
        def load_model(_model_uri):
            calls.append("pyfunc")
            return object()

    class FakeMlflow:
        spark = FakeSpark()
        xgboost = FakeXgboost()
        pyfunc = FakePyfunc()

    assert _load_mlflow_model(FakeMlflow(), "models:/catalog.schema.model/1") == (
        "xgboost",
        xgboost_model,
    )
    assert calls == ["xgboost"]
