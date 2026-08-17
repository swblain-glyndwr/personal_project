from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from next_ads.model_development.contracts import ModelBuild
import next_ads.model_development.promotion as promotion


def _build():
    return ModelBuild(
        model_build_id="build-1",
        model_name="shopping_bag_pctr",
        training_receipt_id="receipt-1",
        model_definition_checksum="a" * 64,
        runtime_profile="dbr_15_4_spark_cpu",
        status="READY",
        created_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
        mlflow_run_id="run-1",
        registered_model_name="catalog.dev.model",
        registered_model_version=2,
        model_uri="models:/catalog.dev.model/2",
        artifact_digest="b" * 64,
        completed_at=datetime(2026, 8, 17, 1, tzinfo=timezone.utc),
    )


class Client:
    def __init__(self, *, existing=False):
        self.existing = existing
        self.copies = []
        self.tags = []
        self.aliases = []

    def search_model_versions(self, _query):
        return [SimpleNamespace(version=7)] if self.existing else []

    def get_model_version(self, _name, version):
        tags = {"nextads.model_build_id": "build-1"} if self.existing else {}
        return SimpleNamespace(version=version, run_id="run-1", tags=tags)

    def copy_model_version(self, **kwargs):
        self.copies.append(kwargs)
        return SimpleNamespace(version=7)

    def set_model_version_tag(self, **kwargs):
        self.tags.append(kwargs)

    def download_artifacts(self, run_id, artifact_path):
        assert (run_id, artifact_path) == ("run-1", "model")
        return "downloaded"

    def set_registered_model_alias(self, **kwargs):
        self.aliases.append(kwargs)


def test_promotion_copies_and_verifies_the_exact_artifact(monkeypatch):
    client = Client()
    monkeypatch.setattr(
        promotion,
        "artifact_directory_digest",
        lambda _path: "b" * 64,
    )

    receipt, reused = promotion.promote_exact_model_build(
        client,
        _build(),
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is False
    assert client.copies == [
        {
            "src_model_uri": "models:/catalog.dev.model/2",
            "dst_name": "catalog.integration.model",
        }
    ]
    assert receipt.destination_model_version == 7
    assert receipt.artifact_digest == "b" * 64
    assert len(client.tags) == 2
    assert client.aliases[0]["version"] == 7


def test_promotion_retry_reuses_the_tagged_destination(monkeypatch):
    client = Client(existing=True)
    monkeypatch.setattr(
        promotion,
        "artifact_directory_digest",
        lambda _path: "b" * 64,
    )

    _receipt, reused = promotion.promote_exact_model_build(
        client,
        _build(),
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is True
    assert client.copies == []


def test_promotion_rejects_an_artifact_digest_change(monkeypatch):
    client = Client()
    monkeypatch.setattr(
        promotion,
        "artifact_directory_digest",
        lambda _path: "c" * 64,
    )

    with pytest.raises(ValueError, match="digest does not match"):
        promotion.promote_exact_model_build(
            client,
            _build(),
            destination_model_name="catalog.integration.model",
            alias="integration_candidate",
        )
