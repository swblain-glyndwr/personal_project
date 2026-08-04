import importlib
import importlib.util
import sys
import types

import pandas as pd
import pytest

from next_ads.realtime.decisioning.reranking_data_build import (
    _filter_top_feature_coverage,
)


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


class FakePredicate:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def __and__(self, other):
        """Combine two predicates."""
        return FakePredicate(
            lambda row: self.evaluate(row) and other.evaluate(row)
        )


class FakeColumn:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        """Build an equality predicate."""
        return FakePredicate(lambda row: row[self.name] == value)

    def __gt__(self, value):
        """Build a greater-than predicate."""
        return FakePredicate(lambda row: row[self.name] > value)


class FakeFeatureFrame:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, predicate):
        return FakeFeatureFrame(
            [row for row in self.rows if predicate.evaluate(row)]
        )

    def collect(self):
        return self.rows


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


def _customer_cells(fallow_control):
    return pd.DataFrame(
        [
            {
                "AccountNumber": "account-1",
                "FallowControl": fallow_control,
                "ShoppingBagTest1": "Best",
                "PageTypeIsolation": "AllPages",
                "AdHocABTest1": "A",
            }
        ]
    )


def _payload_results():
    return pd.DataFrame(
        [
            {
                "roamingprofileid": 123,
                "AtbCMSPageID": "next-fragment",
                "lift_adjusted": 2.5,
                "PageType": "HomePage",
            }
        ]
    )


def test_payload_control_is_calculated_per_request(model_module):
    payload_generator = model_module.NextAdsPayloadGenerator()
    results = _payload_results()

    control_payload = payload_generator.build_payload_structure(
        _customer_cells("NoAds"), results
    )
    ads_payload = payload_generator.build_payload_structure(
        _customer_cells("Ads"), results
    )

    assert control_payload["ads"]["control"] is True
    assert ads_payload["ads"]["control"] is False


def test_top_feature_filter_applies_coverage_threshold(monkeypatch):
    monkeypatch.setattr(
        "pyspark.sql.functions.col", lambda name: FakeColumn(name)
    )
    features = FakeFeatureFrame(
        [
            {
                "UniqueAdID": "below",
                "brand_ranking": 1,
                "brand_perc_coverage": 0.1,
            },
            {
                "UniqueAdID": "equal",
                "brand_ranking": 1,
                "brand_perc_coverage": 0.2,
            },
            {
                "UniqueAdID": "above",
                "brand_ranking": 1,
                "brand_perc_coverage": 0.3,
            },
            {
                "UniqueAdID": "wrong-rank",
                "brand_ranking": 2,
                "brand_perc_coverage": 0.8,
            },
        ]
    )

    filtered = _filter_top_feature_coverage(features, "brand", 0.2)

    assert [row["UniqueAdID"] for row in filtered.collect()] == ["above"]
