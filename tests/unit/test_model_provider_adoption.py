from datetime import date, datetime, timezone
from types import SimpleNamespace

from next_ads.model_development import provider_adoption


NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


def test_evaluation_provider_uses_the_existing_canonical_contract(monkeypatch):
    calls = []
    receipt = SimpleNamespace(delta_version=7)
    result = SimpleNamespace(build=SimpleNamespace(status="READY_FOR_NEXTADS"))

    monkeypatch.setattr(
        provider_adoption,
        "validate_provider_publication_contract",
        lambda *_a, **kwargs: calls.append(("validate", kwargs)),
    )
    monkeypatch.setattr(
        provider_adoption,
        "stage_provider_signals",
        lambda *_a, **kwargs: calls.append(("stage", kwargs)) or receipt,
    )
    monkeypatch.setattr(
        provider_adoption,
        "publish_provider_build",
        lambda *_a, **kwargs: calls.append(("publish", kwargs)) or result,
    )

    actual = provider_adoption.publish_evaluation_provider(
        "spark",
        "scores",
        provider_id="shopping_bag_pctr",
        provider_version="shopping_bag_pctr/v1",
        provider_build_id="build",
        provider_build_attempt_id="attempt",
        input_snapshot_id="receipt",
        run_date=date(2026, 8, 11),
        model_uri="models:/catalog.schema.model/3",
        signals_table="catalog.schema.signals",
        builds_table="catalog.schema.builds",
        git_commit="abc123",
        orchestration_run_id=10,
        task_run_id=11,
        execution_count=0,
        completed_at=NOW,
    )

    assert actual is result
    assert [name for name, _ in calls] == ["validate", "stage", "publish"]
    context = calls[1][1]["context"]
    assert context.capability == "account_ad"
    assert context.use_case == "advert_ranking"
    assert context.input_snapshot_id == "receipt"
    assert calls[2][1]["signals_delta_version"] == 7
    assert calls[2][1]["provider_config"] == {
        "provider_id": "shopping_bag_pctr",
        "provider_version": "shopping_bag_pctr/v1",
        "capability": "account_ad",
        "entity_type": "ad",
    }
