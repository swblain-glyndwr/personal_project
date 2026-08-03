from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from next_ads.common.config_manager import load_config
from next_ads.ranking.foundation_context import (
    build_foundation_invocation_checksum,
    build_scoring_foundation_build_id,
)
from next_ads.ranking.scoring_manifest import (
    READY_FOR_NEXTADS,
    READY_FOR_PROVIDERS,
    ScoreProviderBuild,
    ScoringFoundationBuild,
    ScoringFoundationOutput,
    validate_scoring_config,
    validate_scoring_foundation_builds,
)


RUN_DATE = date(2026, 7, 30)
COMPLETED_AT = datetime(2026, 7, 30, 16, 0, tzinfo=timezone.utc)


def _output(name, *, required=True, attempt=0):
    return ScoringFoundationOutput(
        scoring_foundation_build_id="account_theme_20260730",
        scoring_foundation_build_attempt_id=f"foundation:attempt:{attempt}",
        run_date=RUN_DATE,
        output_name=name,
        source_table=f"catalog.pipeline.{name}",
        source_delta_version=41,
        source_schema_checksum=f"schema-{name}",
        output_table=f"catalog.schema.{name}",
        output_delta_version=42,
        output_schema_version=f"{name}/v1",
        output_schema_checksum=f"schema-{name}",
        is_required=required,
        row_count=100,
        account_count=10,
        entity_count=20,
        null_key_count=0,
        duplicate_key_count=0,
        invalid_value_count=0,
        output_checksum=f"checksum-{name}",
        published_at=COMPLETED_AT,
    )


def _foundation(*, attempt=0):
    return ScoringFoundationBuild(
        scoring_foundation_build_id="account_theme_20260730",
        scoring_foundation_build_attempt_id=f"foundation:attempt:{attempt}",
        input_snapshot_id="inputs_20260730",
        input_snapshot_attempt_id="inputs:attempt:0",
        run_date=RUN_DATE,
        foundation_id="account_theme_features",
        foundation_version="account_theme_features/v2",
        capability="account_theme",
        contract_version="account_theme_foundation/v1",
        invocation_checksum="checksum",
        required_output_names=("ranked", "complete"),
        status=READY_FOR_PROVIDERS,
        warning_count=0,
        task_run_id=100 + attempt,
        execution_count=attempt,
        completed_at=COMPLETED_AT + timedelta(minutes=attempt),
        outputs=(
            _output("ranked", attempt=attempt),
            _output("complete", attempt=attempt),
        ),
        input_bindings_json='{"item_themes":{"delta_version":42}}',
        pipeline_id="pipeline-123",
        pipeline_update_id="pipeline-update",
        pipeline_task_run_id=200 + attempt,
        pipeline_update_type="REFRESH",
    )


def _markov_build(**overrides):
    values = {
        "provider_build_id": "markov_20260730",
        "provider_build_attempt_id": "markov:attempt:0",
        "input_snapshot_id": "inputs_20260730",
        "run_date": RUN_DATE,
        "capability": "account_theme",
        "use_case": "theme_ranking",
        "provider_id": "markov",
        "provider_version": "markov/v1",
        "contract_version": "account_entity_scores/v1",
        "model_name": "markov",
        "model_version": "1",
        "model_uri": "legacy://markov/1",
        "pipeline_update_id": None,
        "row_count": 100,
        "account_count": 10,
        "entity_count": 20,
        "null_key_count": 0,
        "duplicate_key_count": 0,
        "invalid_score_count": 0,
        "output_checksum": "checksum",
        "warning_count": 0,
        "status": READY_FOR_NEXTADS,
        "task_run_id": 123,
        "execution_count": 0,
        "completed_at": COMPLETED_AT,
        "output_snapshot_id": "markov-output",
        "output_table": "catalog.schema.signals",
        "output_delta_version": 42,
    }
    values.update(overrides)
    return ScoreProviderBuild(**values)


def test_foundation_identity_is_reusable_across_providers_and_environments():
    definition = {
        "foundation_id": "account_theme_features",
        "foundation_version": "account_theme_features/v2",
        "capability": "account_theme",
        "contract_version": "account_theme_foundation/v1",
        "required_outputs": {"ranked": "ranked/v1", "complete": "complete/v1"},
        "input_bindings": {
            "item_themes": {
                "table": "marketingdata_dev.user.item_themes",
                "schema_version": "item_themes/v1",
            }
        },
    }
    other_environment = deepcopy(definition)
    other_environment["input_bindings"]["item_themes"]["table"] = (
        "marketingdata_prod.warehouse.item_themes"
    )
    checksum = build_foundation_invocation_checksum(definition)
    assert checksum == build_foundation_invocation_checksum(other_environment)
    assert build_scoring_foundation_build_id(
        foundation_id=definition["foundation_id"],
        foundation_version=definition["foundation_version"],
        input_snapshot_id="inputs_20260730",
        invocation_checksum=checksum,
        run_date=RUN_DATE,
    ) == build_scoring_foundation_build_id(
        foundation_id=other_environment["foundation_id"],
        foundation_version=other_environment["foundation_version"],
        input_snapshot_id="inputs_20260730",
        invocation_checksum=checksum,
        run_date=RUN_DATE,
    )


def test_ready_foundation_requires_its_complete_valid_output_contract():
    build = _foundation()
    assert build.status == READY_FOR_PROVIDERS
    assert build.pipeline_task_run_id == 200
    assert "item_themes" in build.input_bindings_json
    with pytest.raises(ValueError, match="required contract"):
        replace(build, outputs=build.outputs[:1])
    with pytest.raises(ValueError, match="match the build attempt"):
        replace(
            build,
            outputs=(
                replace(
                    build.outputs[0],
                    scoring_foundation_build_id="another-build",
                ),
                build.outputs[1],
            ),
        )
    with pytest.raises(ValueError, match="input_bindings_json"):
        replace(build, input_bindings_json="{}")


def test_foundation_repair_selection_uses_execution_completion_and_task_order():
    original = _foundation()
    repaired = _foundation(attempt=1)
    assert validate_scoring_foundation_builds((original, repaired)) == (
        repaired,
    )


def test_provider_foundation_link_is_nullable_but_must_be_a_complete_pair():
    assert _markov_build().scoring_foundation_build_id is None
    linked = _markov_build(
        scoring_foundation_build_id="foundation",
        scoring_foundation_build_attempt_id="foundation:attempt:0",
    )
    assert linked.scoring_foundation_build_id == "foundation"
    with pytest.raises(ValueError, match="supplied together"):
        _markov_build(scoring_foundation_build_id="foundation")


def test_multiple_providers_can_share_one_foundation(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")
    scoring = deepcopy(load_config("dev", client="next_uk").scoring.to_dict())
    challenger = deepcopy(scoring["providers"]["theme_affinity"])
    challenger["provider_id"] = "theme_affinity_challenger"
    challenger["provider_version"] = "theme_affinity_challenger/v1"
    challenger["implementation"] = "theme_affinity_challenger"
    scoring["providers"]["theme_affinity_challenger"] = challenger

    validate_scoring_config(scoring)

    assert challenger["foundation_id"] == "account_theme_features"
    assert scoring["providers"]["markov"]["foundation_id"] is None
