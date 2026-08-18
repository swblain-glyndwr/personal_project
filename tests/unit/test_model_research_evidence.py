from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from next_ads.model_development.plugins import SparkAccountAdvertScoreProvider
from next_ads.model_development.research_evaluation import (
    COMPLETE,
    EvaluationConfig,
    FeatureCoverageSpec,
    SliceEvaluationSpec,
    binary_metrics_from_score_bins,
    profile_feature_coverage,
    require_complete_binary_evaluation,
    require_complete_confidence_intervals,
)
from next_ads.model_development import research_evaluation, research_evidence
from next_ads.model_development.research_evidence import (
    MandatoryEvidenceError,
    write_candidate_evidence,
)
from next_ads.model_development.research_explainability import (
    FAILED,
    FeatureNameMapping,
    GlobalExplanation,
    deterministic_permutation_importance,
    explain_logistic_regression,
    explain_tree_importance,
    explain_xgboost,
    produce_global_explanation,
)
from next_ads.model_development.research_scoring import (
    PositiveClassScoreTransformer,
)


class _Vector:
    def __init__(self, values):
        self._values = values

    def toArray(self):  # noqa: N802 - mirrors Spark API
        return self._values


def _mapping():
    return (
        FeatureNameMapping(0, "advert_ctr_30d"),
        FeatureNameMapping(1, "device", "mobile"),
    )


def _complete_evaluation():
    metrics = {
        "auc_pr": 0.21,
        "prevalence": 0.02,
        "auc_roc": 0.71,
        "log_loss": 0.12,
        "observed_click_rate": 0.02,
        "predicted_click_rate": 0.021,
        "calibration_gap": 0.001,
    }
    for percentage in (1, 5, 10):
        metrics.update(
            {
                f"precision_at_{percentage}_percent": 0.1,
                f"recall_at_{percentage}_percent": 0.2,
                f"lift_at_{percentage}_percent": 5.0,
            }
        )
    return {
        "status": COMPLETE,
        "profile": {"rows": 1000, "positives": 20, "negatives": 980},
        "metrics": metrics,
        "precision_recall_curve": [
            {"score_bin": None, "precision": 1.0, "recall": 0.0},
            {"score_bin": 9, "precision": 0.2, "recall": 1.0},
        ],
        "roc_curve": [
            {"score_bin": None, "false_positive_rate": 0.0, "recall": 0.0},
            {"score_bin": 9, "false_positive_rate": 1.0, "recall": 1.0},
        ],
        "calibration": [
            {
                "score_bin": 0,
                "rows": 1000,
                "mean_score": 0.02,
                "observed_rate": 0.02,
            }
        ],
        "lift_gain": [
            {
                "rank_bin": 1,
                "population_fraction": 1.0,
                "cumulative_gain": 1.0,
                "cumulative_lift": 1.0,
                "minimum_score": 0.0,
                "maximum_score": 1.0,
            }
        ],
        "score_distribution": [
            {"label": 0, "score_bin": 0, "rows": 980},
            {"label": 1, "score_bin": 1, "rows": 20},
        ],
        "top_confusion": [
            {
                "percentage": 5,
                "selected_rows": 50,
                "threshold": 0.4,
                "tp": 5,
                "fp": 45,
                "fn": 15,
                "tn": 935,
                "precision": 0.1,
                "recall": 0.25,
                "lift": 5.0,
            }
        ],
        "slices": [
            {
                "slice_column": "location",
                "slice_value": "SB1",
                "status": "INSUFFICIENT",
                "reason": "Positive-class volume is below minimum 5",
                "profile": {"rows": 100},
                "metrics": {},
            }
        ],
    }


def _coverage():
    return [
        {
            "feature": "advert_ctr_30d",
            "rows": 1000,
            "missing_rows": 0,
            "missing_rate": 0.0,
            "default_rows": 10,
            "default_rate": 0.01,
        }
    ]


def _explanation():
    return GlobalExplanation(
        status=COMPLETE,
        method="native_tree_importance",
        features=(
            {
                "feature": "advert_ctr_30d",
                "absolute_importance": 0.8,
            },
        ),
    )


def test_positive_class_transformer_is_ml_writable_and_declares_double_outputs():
    from pyspark.ml.util import MLWritable
    from pyspark.ml.linalg import VectorUDT
    from pyspark.sql.types import DoubleType, StructField, StructType

    transformer = PositiveClassScoreTransformer()
    schema = transformer.transformSchema(
        StructType(
            (
                StructField("probability", VectorUDT(), nullable=False),
                StructField("prediction", DoubleType(), nullable=False),
            )
        )
    )

    assert isinstance(transformer, MLWritable)
    assert transformer.getInputCol() == "probability"
    assert transformer.getOutputCol() == "score"
    assert schema["prediction"].dataType == DoubleType()
    assert schema["score"].dataType == DoubleType()
    with pytest.raises(ValueError, match="threshold"):
        PositiveClassScoreTransformer(threshold=1.1)


@pytest.mark.parametrize(
    ("columns", "expected_source", "vector_expected"),
    [
        (["score"], "score", False),
        (["probability"], "probability", True),
    ],
)
def test_score_provider_prefers_scalar_score_and_keeps_probability_fallback(
    monkeypatch,
    columns,
    expected_source,
    vector_expected,
):
    from pyspark.ml import functions as ml_functions
    from pyspark.sql import functions as F

    calls = []

    class Expression:
        def __init__(self, source):
            self.source = source

        def cast(self, _type):
            return self

        def getItem(self, _index):  # noqa: N802 - mirrors Spark API
            return self

    class Frame:
        def __init__(self):
            self.columns = columns

        def withColumn(self, name, expression):  # noqa: N802
            calls.append((name, expression.source))
            return self

    class Model:
        def transform(self, _frame):
            return Frame()

    mlflow_module = SimpleNamespace(
        spark=SimpleNamespace(load_model=lambda _uri: Model())
    )
    monkeypatch.setitem(sys.modules, "mlflow", mlflow_module)
    monkeypatch.setattr(F, "col", lambda name: Expression(name))

    def vector_to_array(expression):
        calls.append(("vector_to_array", expression.source))
        return expression

    monkeypatch.setattr(ml_functions, "vector_to_array", vector_to_array)
    definition = SimpleNamespace(model_name="model", checksum="checksum")
    build = SimpleNamespace(
        status="READY",
        model_uri="models:/catalog.schema.model/1",
        model_name="model",
        model_definition_checksum="checksum",
    )

    SparkAccountAdvertScoreProvider(run_date=None)._predictions(
        definition,
        build,
        Frame(),
    )

    assert calls[-1] == ("__model_pctr", expected_source)
    assert (
        any(call[0] == "vector_to_array" for call in calls) is vector_expected
    )


def test_binned_metrics_use_prevalence_for_constant_scores_and_one_for_perfect():
    constant = binary_metrics_from_score_bins(
        ({"score_bin": 1, "rows": 100, "positives": 10},)
    )
    perfect = binary_metrics_from_score_bins(
        (
            {"score_bin": 9, "rows": 10, "positives": 10},
            {"score_bin": 0, "rows": 90, "positives": 0},
        )
    )

    assert constant["auc_pr"] == pytest.approx(0.1)
    assert constant["auc_roc"] == pytest.approx(0.5)
    assert perfect["auc_pr"] == pytest.approx(1.0)
    assert perfect["auc_roc"] == pytest.approx(1.0)


def test_feature_contracts_reject_anonymous_names_and_invalid_limits():
    with pytest.raises(ValueError, match="readable"):
        FeatureCoverageSpec("input", display_name="feature_42")
    with pytest.raises(ValueError, match="readable"):
        FeatureNameMapping(0, "feature_42")
    with pytest.raises(ValueError, match="curve_bins"):
        EvaluationConfig(curve_bins=2)


def test_feature_coverage_indicators_are_part_of_the_input_contract():
    spec = FeatureCoverageSpec(
        "advert_ctr_30d",
        missing_indicator_column="advert_ctr_lookup_missing",
        default_indicator_column="advert_ctr_default_applied",
    )
    frame = SimpleNamespace(columns=["advert_ctr_30d"])

    with pytest.raises(ValueError, match="advert_ctr_default_applied"):
        profile_feature_coverage(frame, (spec,))

    assert spec.missing_indicator_column == "advert_ctr_lookup_missing"
    assert spec.default_indicator_column == "advert_ctr_default_applied"


def test_supported_explanations_return_readable_model_specific_evidence():
    logistic = explain_logistic_regression(
        SimpleNamespace(coefficients=_Vector((0.5, -0.25))),
        _mapping(),
    )
    tree = explain_tree_importance(
        SimpleNamespace(featureImportances=_Vector((0.2, 0.8))),
        _mapping(),
    )
    xgboost = explain_xgboost(
        SimpleNamespace(
            get_feature_importances=lambda importance_type: {
                "f0": 4.0,
                "f1": 2.0,
            }
        ),
        _mapping(),
        contribution_summary=(
            {
                "vector_index": 0,
                "mean_contribution": 0.1,
                "mean_absolute_contribution": 0.3,
                "rows": 100,
            },
            {
                "vector_index": 1,
                "mean_contribution": -0.05,
                "mean_absolute_contribution": 0.2,
                "rows": 100,
            },
        ),
    )

    assert logistic.features[0]["odds_ratio"] == pytest.approx(1.648721)
    assert logistic.features[1]["feature"] == "device=mobile"
    assert tree.features[0]["feature"] == "device=mobile"
    assert xgboost.features[0]["gain_importance"] == 4.0
    assert xgboost.method == "xgboost_gain_and_bounded_contributions"


def test_xgboost_explanation_requires_bounded_contributions():
    with pytest.raises(ValueError, match="bounded contribution"):
        explain_xgboost(
            SimpleNamespace(
                get_feature_importances=lambda importance_type: {"f0": 1.0}
            ),
            _mapping(),
        )

    with pytest.raises(ValueError, match="cover every mapped feature"):
        explain_xgboost(
            SimpleNamespace(
                get_feature_importances=lambda importance_type: {"f0": 1.0}
            ),
            _mapping(),
            contribution_summary=(
                {
                    "vector_index": 0,
                    "mean_contribution": 0.1,
                    "mean_absolute_contribution": 0.2,
                    "rows": 10,
                },
            ),
        )

    with pytest.raises(ValueError, match="positive and consistent"):
        explain_xgboost(
            SimpleNamespace(
                get_feature_importances=lambda importance_type: {"f0": 1.0}
            ),
            _mapping(),
            contribution_summary=(
                {
                    "vector_index": 0,
                    "mean_contribution": 0.1,
                    "mean_absolute_contribution": 0.2,
                    "rows": 10,
                },
                {
                    "vector_index": 1,
                    "mean_contribution": 0.1,
                    "mean_absolute_contribution": 0.2,
                    "rows": 9,
                },
            ),
        )


def test_permutation_fallback_seeds_are_deterministic():
    first_seeds = []
    second_seeds = []

    first = deterministic_permutation_importance(
        _mapping(),
        baseline_metric=0.3,
        evaluate_permuted=lambda _feature, seed: (
            first_seeds.append(seed) or 0.2
        ),
    )
    second = deterministic_permutation_importance(
        _mapping(),
        baseline_metric=0.3,
        evaluate_permuted=lambda _feature, seed: (
            second_seeds.append(seed) or 0.2
        ),
    )

    assert first_seeds == second_seeds
    assert first.features == second.features


def test_custom_candidate_names_always_use_permutation_explanations():
    explanation = produce_global_explanation(
        "next_ads.custom.LogisticCandidate",
        object(),
        _mapping(),
        permutation_baseline_metric=0.3,
        permutation_evaluator=lambda _feature, _seed: 0.2,
    )

    assert explanation.method == "deterministic_permutation_importance"


def test_failed_explanation_cannot_be_completed_without_a_reason():
    with pytest.raises(ValueError, match="reason"):
        GlobalExplanation(status=FAILED, method="custom")


def test_candidate_evidence_is_manifested_and_low_volume_slices_are_honest(
    tmp_path,
    monkeypatch,
):
    def write_plot(path, *_args, **_kwargs):
        Path(path).write_bytes(b"fixed png")

    for name in (
        "_plot_precision_recall",
        "_plot_roc",
        "_plot_calibration",
        "_plot_lift_gain",
        "_plot_score_distribution",
        "_plot_top_confusion",
        "_plot_slices",
        "_plot_feature_coverage",
        "_plot_feature_importance",
    ):
        monkeypatch.setattr(research_evidence, name, write_plot)

    first = write_candidate_evidence(
        tmp_path,
        candidate_id="spark_random_forest",
        evaluation=_complete_evaluation(),
        feature_coverage=_coverage(),
        explanation=_explanation(),
    )
    second = write_candidate_evidence(
        tmp_path,
        candidate_id="spark_random_forest",
        evaluation=_complete_evaluation(),
        feature_coverage=_coverage(),
        explanation=_explanation(),
    )

    assert first.selectable is True
    assert first.manifest_sha256 == second.manifest_sha256
    assert (tmp_path / "artifact_manifest.json").is_file()
    assert "INSUFFICIENT" in (tmp_path / "slice_metrics.csv").read_text()


def test_artifact_guard_rejects_row_identity_and_missing_evidence_blocks_selection(
    tmp_path,
):
    with pytest.raises(ValueError, match="row identity"):
        write_candidate_evidence(
            tmp_path,
            candidate_id="candidate",
            evaluation={"row_id_hash": "secret"},
            feature_coverage=_coverage(),
            explanation=_explanation(),
        )

    bundle = research_evidence.EvidenceBundle(
        root=tmp_path,
        status="FAILED",
        artifacts=(),
        manifest_sha256="a" * 64,
        failures=("precision_recall_curve evidence is missing",),
    )
    with pytest.raises(MandatoryEvidenceError, match="precision_recall_curve"):
        bundle.require_selectable()


def test_optional_evidence_must_record_its_outcome(tmp_path):
    with pytest.raises(ValueError, match="Optional evidence"):
        write_candidate_evidence(
            tmp_path,
            candidate_id="candidate",
            evaluation=_complete_evaluation(),
            feature_coverage=_coverage(),
            explanation=_explanation(),
            optional_evidence={"custom_plot": {"value": 1}},
        )


def test_optional_evidence_rejects_row_records_and_arbitrary_objects(tmp_path):
    common = {
        "candidate_id": "candidate",
        "evaluation": _complete_evaluation(),
        "feature_coverage": _coverage(),
        "explanation": _explanation(),
    }
    with pytest.raises(ValueError, match="record-shaped row lists"):
        write_candidate_evidence(
            tmp_path,
            **common,
            optional_evidence={
                "custom_table": {
                    "status": COMPLETE,
                    "evidence": {"data": [{"value": 12345678}]},
                }
            },
        )
    with pytest.raises(ValueError, match="aggregate numeric JSON"):
        write_candidate_evidence(
            tmp_path,
            **common,
            optional_evidence={
                "custom_table": {
                    "status": COMPLETE,
                    "evidence": {"payload": object()},
                }
            },
        )


def test_insufficient_slice_artifacts_reject_outcome_counts(tmp_path):
    evaluation = _complete_evaluation()
    evaluation["slices"][0]["profile"] = {"rows": 10, "positives": 1}

    with pytest.raises(ValueError, match="aggregate row count"):
        write_candidate_evidence(
            tmp_path,
            candidate_id="candidate",
            evaluation=evaluation,
            feature_coverage=_coverage(),
            explanation=_explanation(),
        )


def test_selected_test_bundle_uses_distinct_mlflow_parameters(tmp_path):
    bundle = research_evidence.EvidenceBundle(
        root=tmp_path,
        status=COMPLETE,
        artifacts=(),
        manifest_sha256="a" * 64,
    )
    logged = {}
    mlflow = SimpleNamespace(
        log_artifacts=lambda *_args, **_kwargs: None,
        log_param=lambda name, value: logged.setdefault(name, value),
    )

    research_evidence.log_evidence_bundle(
        mlflow,
        bundle,
        artifact_path="selected_test_evidence",
        parameter_prefix="selected_test_",
    )

    assert logged == {
        "selected_test_evidence_manifest_sha256": "a" * 64,
        "selected_test_evidence_status": COMPLETE,
    }


def test_candidate_comparison_plots_ready_selectable_candidates_and_baseline():
    rows = [
        {
            "candidate_id": "ready_candidate",
            "status": "READY",
            "selectable": True,
            "auc_pr": 0.2,
        },
        {
            "candidate_id": "failed_candidate",
            "status": "FAILED",
            "selectable": False,
            "auc_pr": 0.3,
        },
        {
            "candidate_id": "prevalence_only_baseline",
            "status": COMPLETE,
            "selectable": False,
            "auc_pr": 0.02,
        },
    ]

    plotted = research_evidence._candidate_comparison_plot_rows(rows)

    assert [row["candidate_id"] for row in plotted] == [
        "ready_candidate",
        "prevalence_only_baseline",
    ]


def test_declared_slice_values_and_thresholds_control_evaluation():
    spec = SliceEvaluationSpec(
        slice_id="shopping_bag_location",
        column="location",
        values=("SB1", "SB2"),
        minimum_rows=4,
    )
    config = EvaluationConfig(
        min_rows=1,
        min_positive_rows=1,
        min_negative_rows=1,
    )

    assert research_evaluation._bounded_slice_values(
        spec,
        discovered_values=("OTHER",),
        max_values=config.max_slice_values,
    ) == ("SB1", "SB2")
    slice_config = research_evaluation._config_for_slice(config, spec)
    assert slice_config.min_rows == 4
    assert research_evaluation._empty_slice_evaluation(slice_config)[
        "reason"
    ] == ("Row volume is below minimum 4")


def test_incomplete_training_or_test_uncertainty_blocks_selection():
    with pytest.raises(ValueError, match="training evidence is not COMPLETE"):
        require_complete_binary_evaluation(
            {"status": "INSUFFICIENT", "reason": "too few rows"},
            required_metrics=("auc_pr",),
            context="training",
        )
    with pytest.raises(ValueError, match="missing mandatory metrics"):
        require_complete_binary_evaluation(
            {"status": COMPLETE, "metrics": {"auc_pr": 0.2}},
            required_metrics=("auc_pr", "log_loss"),
            context="validation",
        )
    with pytest.raises(
        ValueError, match="confidence intervals are not COMPLETE"
    ):
        require_complete_confidence_intervals(
            {"status": "INSUFFICIENT", "reason": "too few blocks"},
            context="Selected test",
        )
