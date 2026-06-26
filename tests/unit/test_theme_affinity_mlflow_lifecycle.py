import sys
import types

from next_ads.common.config_manager import load_config
from next_ads.ml.lifecycle.spec import ModelLifecycleSpec
from next_ads.ranking.theme_affinity.mlflow_lifecycle import (
    configure_mlflow,
    copy_model_alias_to_registered_model,
    copy_model_version_to_registered_model,
    model_uri_for_alias,
    model_uri_for_version,
    resolve_lifecycle_config,
    set_model_alias,
)
from next_ads.ranking.theme_affinity.spark_model import build_spark_model_signature


def _load_config(job_env, monkeypatch, user_schema="test_user"):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", user_schema)
    return load_config(job_env)


def test_lifecycle_config_resolves_dev_registered_model(monkeypatch):
    config = _load_config("dev", monkeypatch)

    lifecycle_config = resolve_lifecycle_config(config)

    assert isinstance(lifecycle_config, ModelLifecycleSpec)
    assert lifecycle_config.registered_model_name == (
        "marketingdata_dev.test_user.nextads_theme_affinity_ranker"
    )
    assert lifecycle_config.experiment_path == (
        "/Shared/mlflow/nextads/dev/experiments/theme_affinity_ranker"
    )
    assert lifecycle_config.train_table == (
        "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_predict_ranked"
    )
    assert lifecycle_config.categorical_cols == ("repurchase_stage", "GmaName")
    assert lifecycle_config.drift_thresholds.numeric_psi_warn == 0.1
    assert config.ranking_model.training_backend == "spark_xgb_ranker"
    assert config.ranking_model.spark_num_workers == 4
    assert config.ranking_model.training_frame.max_accounts == 50000
    assert config.ranking_model.training_frame.max_candidates_per_account == 256
    assert config.ranking_model.training_frame.max_rows == 15000000
    assert config.ranking_model.training_frame.max_pandas_rows == 15000000
    assert config.ranking_model.training_frame.rank_filter_threshold == 256
    assert config.ranking_model.xgb_params.tree_method == "hist"
    assert config.ranking_model.xgb_params.device == "cpu"


def test_lifecycle_config_resolves_preprod_and_prod_models(monkeypatch):
    preprod = resolve_lifecycle_config(_load_config("preprod", monkeypatch))
    prod = resolve_lifecycle_config(_load_config("prod", monkeypatch))

    assert preprod.registered_model_name == (
        "marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker"
    )
    assert prod.registered_model_name == (
        "marketingdata_prod.warehouse.nextads_theme_affinity_ranker"
    )


def test_configure_mlflow_uses_databricks_unity_catalog():
    class FakeMlflow:
        tracking_uri = None
        registry_uri = None

        @classmethod
        def set_tracking_uri(cls, value):
            cls.tracking_uri = value

        @classmethod
        def set_registry_uri(cls, value):
            cls.registry_uri = value

    configure_mlflow(FakeMlflow)

    assert FakeMlflow.tracking_uri == "databricks"
    assert FakeMlflow.registry_uri == "databricks-uc"


def test_model_uri_for_alias_uses_registered_model_alias():
    assert model_uri_for_alias("catalog.schema.model", "preprod") == (
        "models:/catalog.schema.model@preprod"
    )


def test_model_uri_for_version_uses_fixed_registered_model_version():
    assert model_uri_for_version("catalog.schema.model", 17) == (
        "models:/catalog.schema.model/17"
    )


def test_set_model_alias_delegates_to_mlflow_client():
    calls = []

    class FakeClient:
        def set_registered_model_alias(self, **kwargs):
            calls.append(kwargs)

    set_model_alias(FakeClient(), "catalog.schema.model", 12, "preprod")

    assert calls == [
        {
            "name": "catalog.schema.model",
            "alias": "preprod",
            "version": "12",
        }
    ]


def test_copy_model_alias_uses_native_mlflow_register_model():
    calls = []

    class RegisteredModel:
        version = "42"

    class FakeClient:
        def set_registered_model_alias(self, **kwargs):
            calls.append(("alias", kwargs))

    class FakeTracking:
        pass

    FakeTracking.MlflowClient = staticmethod(lambda: FakeClient())

    class FakeMlflow:
        tracking = FakeTracking()

        @staticmethod
        def register_model(model_uri, name):
            calls.append(("register", model_uri, name))
            return RegisteredModel()

    result = copy_model_alias_to_registered_model(
        FakeMlflow,
        "marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker",
        "preprod",
        "marketingdata_prod.warehouse.nextads_theme_affinity_ranker",
        "prod",
    )

    assert result.version == "42"
    assert calls == [
        (
            "register",
            (
                "models:/marketingdata_prod.ds_sandbox."
                "nextads_theme_affinity_ranker@preprod"
            ),
            "marketingdata_prod.warehouse.nextads_theme_affinity_ranker",
        ),
        (
            "alias",
            {
                "name": (
                    "marketingdata_prod.warehouse."
                    "nextads_theme_affinity_ranker"
                ),
                "alias": "prod",
                "version": "42",
            },
        ),
    ]


def test_copy_model_version_tags_source_model_details():
    calls = []

    class RegisteredModel:
        version = "8"

    class FakeClient:
        def set_registered_model_alias(self, **kwargs):
            calls.append(("alias", kwargs))

        def set_model_version_tag(self, **kwargs):
            calls.append(("tag", kwargs))

    class FakeTracking:
        pass

    FakeTracking.MlflowClient = staticmethod(lambda: FakeClient())

    class FakeMlflow:
        tracking = FakeTracking()

        @staticmethod
        def register_model(model_uri, name):
            calls.append(("register", model_uri, name))
            return RegisteredModel()

    result = copy_model_version_to_registered_model(
        FakeMlflow,
        "marketingdata_dev.nextads_integration.nextads_theme_affinity_ranker",
        "17",
        "marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker",
        "preprod_gpu_xgboost",
    )

    assert result.version == "8"
    assert calls == [
        (
            "register",
            (
                "models:/marketingdata_dev.nextads_integration."
                "nextads_theme_affinity_ranker/17"
            ),
            "marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker",
        ),
        (
            "alias",
            {
                "name": (
                    "marketingdata_prod.ds_sandbox."
                    "nextads_theme_affinity_ranker"
                ),
                "alias": "preprod_gpu_xgboost",
                "version": "8",
            },
        ),
        (
            "tag",
            {
                "name": (
                    "marketingdata_prod.ds_sandbox."
                    "nextads_theme_affinity_ranker"
                ),
                "version": "8",
                "key": "source_registered_model_name",
                "value": (
                    "marketingdata_dev.nextads_integration."
                    "nextads_theme_affinity_ranker"
                ),
            },
        ),
        (
            "tag",
            {
                "name": (
                    "marketingdata_prod.ds_sandbox."
                    "nextads_theme_affinity_ranker"
                ),
                "version": "8",
                "key": "source_model_version",
                "value": "17",
            },
        ),
    ]


def test_train_and_promote_scripts_use_native_mlflow_not_marketingdata_utils():
    train_script = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "scripts/theme_affinity/train_model.py"
    ).read_text()
    promote_script = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "jobs/model/lifecycle/promote_model.py"
    ).read_text()
    monitor_script = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "scripts/theme_affinity/monitor_model.py"
    ).read_text()
    gpu_train_script = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "scripts/theme_affinity/train_gpu_xgboost_model.py"
    ).read_text()
    assert "marketingdata_utils" not in train_script
    assert "marketingdata_utils" not in gpu_train_script
    assert "marketingdata_utils" not in promote_script
    assert "marketingdata_utils" not in monitor_script
    assert "mlflow.tracking.MlflowClient" in promote_script
    assert "copy_model_version_to_registered_model" in promote_script
    assert "Theme Affinity" not in promote_script
    assert "log_table_drift_to_mlflow" in monitor_script


def test_train_lifecycle_uses_spark_xgboost_not_pandas_collection():
    lifecycle_source = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "src/next_ads/ranking/theme_affinity/mlflow_lifecycle.py"
    ).read_text()
    spark_model_source = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "src/next_ads/ranking/theme_affinity/spark_model.py"
    ).read_text()

    assert ".toPandas()" not in lifecycle_source
    assert "build_bounded_training_frame" in lifecycle_source
    assert "training_frame_stats" in lifecycle_source
    assert "fit_spark_xgb_ranker" in lifecycle_source
    assert "mlflow_module.spark.log_model" in lifecycle_source
    assert "signature=build_spark_model_signature" in lifecycle_source
    assert '_spark_xgboost"' in lifecycle_source
    assert "SparkXGBRanker" in spark_model_source
    assert "VectorAssembler" in spark_model_source
    assert 'features_col="features"' in spark_model_source
    assert "validation_indicator_col" in spark_model_source


def test_gpu_xgboost_lifecycle_uses_cuda_pyfunc_and_challenger_alias():
    gpu_source = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "src/next_ads/ranking/theme_affinity/gpu_xgboost_lifecycle.py"
    ).read_text()

    assert ".toPandas()" in gpu_source
    assert "build_bounded_training_frame" in gpu_source
    assert "max_pandas_rows" in gpu_source
    assert 'resolved_params["device"] = "cuda"' in gpu_source
    assert 'alias_suffix: str = "gpu_xgboost"' in gpu_source
    assert "mlflow_module.pyfunc.log_model" in gpu_source
    assert "signature=build_spark_model_signature" in gpu_source


def test_spark_model_signature_declares_features_and_prediction(monkeypatch):
    class FakeColSpec:
        def __init__(self, dtype, name):
            self.dtype = dtype
            self.name = name

    class FakeSchema:
        def __init__(self, columns):
            self.columns = columns

        def input_names(self):
            return [column.name for column in self.columns]

        def input_types(self):
            return [column.dtype for column in self.columns]

    class FakeModelSignature:
        def __init__(self, inputs, outputs):
            self.inputs = inputs
            self.outputs = outputs

    signature_module = types.ModuleType("mlflow.models.signature")
    signature_module.ModelSignature = FakeModelSignature
    schema_module = types.ModuleType("mlflow.types.schema")
    schema_module.ColSpec = FakeColSpec
    schema_module.Schema = FakeSchema

    monkeypatch.setitem(sys.modules, "mlflow", types.ModuleType("mlflow"))
    monkeypatch.setitem(sys.modules, "mlflow.models", types.ModuleType("mlflow.models"))
    monkeypatch.setitem(sys.modules, "mlflow.models.signature", signature_module)
    monkeypatch.setitem(sys.modules, "mlflow.types", types.ModuleType("mlflow.types"))
    monkeypatch.setitem(sys.modules, "mlflow.types.schema", schema_module)

    signature = build_spark_model_signature(
        ["month", "repurchase_stage", "GmaName", "simple_rules_rank"],
        ["repurchase_stage", "GmaName"],
    )

    assert signature.inputs.input_names() == [
        "month",
        "repurchase_stage",
        "GmaName",
        "simple_rules_rank",
    ]
    assert signature.inputs.input_types() == [
        "double",
        "string",
        "string",
        "double",
    ]
    assert signature.outputs.input_names() == ["prediction"]
    assert signature.outputs.input_types() == ["double"]


def test_training_frame_builder_bounds_operational_ranked_table():
    source = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "src/next_ads/ranking/theme_affinity/training_data.py"
    ).read_text()

    assert "class TrainingFrameConfig" in source
    assert "rank_filter_threshold" in source
    assert "(F.col(RANK_COL) <= F.lit(rank_filter_threshold))" in source
    assert "| (F.col(LABEL_COL) > F.lit(0))" in source
    assert "frame_config.max_accounts" in source
    assert "frame_config.max_candidates_per_account" in source
    assert "exceeds configured max_rows" in source
    assert "training_frame_row_count" in source
