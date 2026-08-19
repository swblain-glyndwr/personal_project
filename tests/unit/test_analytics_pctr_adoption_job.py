from argparse import Namespace

from jobs.model.development import adopt_analytics_pctr as job


def test_analytics_adoption_keeps_both_exact_component_models():
    components = job._components(
        Namespace(
            classifier_model_uri="models:/catalog.schema.classifier/2",
            classifier_run_id="classifier-run",
            regressor_model_uri="models:/catalog.schema.regressor/3",
            regressor_run_id="regressor-run",
        )
    )

    assert [(item.role, item.model_uri, item.expected_run_id) for item in components] == [
        (
            "popularity_classifier",
            "models:/catalog.schema.classifier/2",
            "classifier-run",
        ),
        (
            "affinity_regressor",
            "models:/catalog.schema.regressor/3",
            "regressor-run",
        ),
    ]


def test_analytics_provider_build_is_stable_and_model_specific():
    assert job._provider_build_id("receipt") == job._provider_build_id("receipt")
    assert job._provider_build_id("receipt") != job._provider_build_id("other")
