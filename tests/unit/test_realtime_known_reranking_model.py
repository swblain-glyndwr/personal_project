import importlib
import importlib.util
import sys
import types

import pandas as pd
import pytest


MODEL_MODULE = (
    "next_ads.realtime.decisioning.realtime_known_reranking_model"
)


@pytest.fixture
def model_module(monkeypatch):
    if importlib.util.find_spec("mlflow") is None:
        mlflow_module = types.ModuleType("mlflow")
        pyfunc_module = types.ModuleType("mlflow.pyfunc")
        pyfunc_module.PythonModel = type("PythonModel", (), {})
        mlflow_module.pyfunc = pyfunc_module
        monkeypatch.setitem(sys.modules, "mlflow", mlflow_module)
        monkeypatch.setitem(sys.modules, "mlflow.pyfunc", pyfunc_module)

    if importlib.util.find_spec("psycopg2") is None:
        psycopg2_module = types.ModuleType("psycopg2")
        psycopg2_module.connect = None
        monkeypatch.setitem(sys.modules, "psycopg2", psycopg2_module)

    sys.modules.pop(MODEL_MODULE, None)
    module = importlib.import_module(MODEL_MODULE)
    yield module
    sys.modules.pop(MODEL_MODULE, None)


class FakeLakebaseConnector:
    def __init__(self, results):
        self.results = results

    def run_query(self, _query):
        return self.results.copy()


def test_incrementality_retains_cms_page_id_for_payload_builders(
    model_module,
):
    model = object.__new__(model_module.RealtimeKnownRerankingModel)
    model.table_catalog = "catalog"
    model.table_schema = "schema"
    model.lakebaseconnector = FakeLakebaseConnector(
        pd.DataFrame(
            [
                {
                    "ViewUniqueAdID": "view-ad",
                    "AtbUniqueAdID": "next-ad",
                    "AtbCMSPageID": "next-fragment",
                    "lift_adjusted": 2.5,
                    "HomePage": True,
                }
            ]
        )
    )
    best_ads = pd.DataFrame(
        [
            {
                "roamingprofileid": 123,
                "UniqueAdID": "view-ad",
                "PageType": "HomePage",
                "AdjustedTriggerScore": 1.25,
            }
        ]
    )

    results = model.incrementality(best_ads)
    payload_generator = model_module.NextAdsPayloadGenerator()

    assert results["AtbCMSPageID"].tolist() == ["next-fragment"]
    assert payload_generator.build_triggers(results) == [
        {"t": 2.5, "id": "next-fragment"}
    ]
    assert payload_generator.build_fragments(results) == [
        {
            "pageTypes": ["HomePage"],
            "enableAdFatigueRotation": False,
            "fragmentIds": ["next-fragment"],
        }
    ]
