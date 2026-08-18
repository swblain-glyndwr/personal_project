import hashlib
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
    def __init__(self, *, existing=False, destination_run_id="run-1"):
        self.existing = existing
        self.destination_run_id = destination_run_id
        self.copies = []
        self.tags = []
        self.aliases = []

    def search_model_versions(self, _query):
        return [SimpleNamespace(version=7)] if self.existing else []

    def get_model_version(self, _name, version):
        tags = {"nextads.model_build_id": "build-1"} if self.existing else {}
        return SimpleNamespace(
            version=version,
            run_id=self.destination_run_id,
            tags=tags,
        )

    def copy_model_version(self, **kwargs):
        self.copies.append(kwargs)
        return SimpleNamespace(version=7)

    def set_model_version_tag(self, **kwargs):
        self.tags.append(kwargs)

    def download_artifacts(self, run_id, artifact_path):
        raise AssertionError(
            "Promotion must resolve artifacts from exact registered model URIs, "
            f"not run {run_id}/{artifact_path}"
        )

    def set_registered_model_alias(self, **kwargs):
        self.aliases.append(kwargs)


class ExactVersionClient(Client):
    def get_model_version(self, name, version):
        if name == "catalog.dev.model":
            return SimpleNamespace(
                version=version,
                run_id="source-workspace-run",
                tags={
                    "nextads.model_build_id": "build-1",
                    "nextads.artifact_digest": "b" * 64,
                },
            )
        tags = {"nextads.model_build_id": "build-1"} if self.existing else {}
        return SimpleNamespace(
            version=version,
            run_id=self.destination_run_id,
            tags=tags,
        )


def _record_uri_digests(
    monkeypatch,
    *,
    source_digest="b" * 64,
    destination_digest="b" * 64,
):
    calls = []

    def digest(model_uri):
        calls.append(model_uri)
        if model_uri.startswith("models:/catalog.dev.model/"):
            return source_digest
        return destination_digest

    monkeypatch.setattr(
        promotion,
        "registered_model_artifact_digest",
        digest,
    )
    return calls


def test_registered_model_digest_downloads_one_exact_numeric_uri(tmp_path):
    model = tmp_path / "legacy-run-backed-model"
    model.mkdir()
    (model / "MLmodel").write_text("artifact_path: model\n")
    (model / "python_model.pkl").write_bytes(b"legacy-model-bytes")
    calls = []

    def download_artifacts(*, artifact_uri):
        calls.append(artifact_uri)
        return str(model)

    digest = promotion.registered_model_artifact_digest(
        "models:/catalog.dev.model/2",
        artifact_downloader=download_artifacts,
    )

    assert calls == ["models:/catalog.dev.model/2"]
    assert digest == promotion.artifact_directory_digest(model)
    assert len(digest) == hashlib.sha256().digest_size * 2


@pytest.mark.parametrize(
    "model_uri",
    [
        "models:/catalog.dev.model@candidate",
        "models:/catalog.dev.model/Production",
        "models:/catalog.dev.model/latest",
    ],
)
def test_registered_model_digest_rejects_a_moving_model_uri(model_uri):
    with pytest.raises(ValueError, match="numeric registered model version"):
        promotion.registered_model_artifact_digest(
            model_uri,
            artifact_downloader=lambda **_kwargs: "unused",
        )


def test_promotion_copies_and_verifies_the_exact_artifact(monkeypatch):
    client = Client()
    digest_uris = _record_uri_digests(monkeypatch)

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
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]
    assert len(client.tags) == 2
    assert client.aliases[0]["version"] == 7


def test_promotion_retry_reuses_the_tagged_destination(monkeypatch):
    client = Client(existing=True)
    digest_uris = _record_uri_digests(monkeypatch)

    _receipt, reused = promotion.promote_exact_model_build(
        client,
        _build(),
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is True
    assert client.copies == []
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]


def test_promotion_rejects_an_artifact_digest_change(monkeypatch):
    client = Client()
    _record_uri_digests(
        monkeypatch,
        source_digest="b" * 64,
        destination_digest="c" * 64,
    )

    with pytest.raises(ValueError, match="digest does not match"):
        promotion.promote_exact_model_build(
            client,
            _build(),
            destination_model_name="catalog.integration.model",
            alias="integration_candidate",
        )


def test_promotion_rejects_changed_source_bytes_before_copy(monkeypatch):
    client = Client()
    _record_uri_digests(
        monkeypatch,
        source_digest="c" * 64,
    )

    with pytest.raises(
        ValueError, match="Source artifact digest does not match"
    ):
        promotion.promote_exact_model_build(
            client,
            _build(),
            destination_model_name="catalog.integration.model",
            alias="integration_candidate",
        )

    assert client.copies == []


def test_source_alias_rehearsal_needs_no_destination_model_copy(monkeypatch):
    client = Client()
    digest_uris = _record_uri_digests(monkeypatch)

    receipt, reused = promotion.promote_exact_model_build(
        client,
        _build(),
        destination_model_name=None,
        alias="preprod_candidate",
        promotion_mode=promotion.SOURCE_ALIAS_REHEARSAL,
    )

    assert reused is True
    assert client.copies == []
    assert receipt.destination_model_name == "catalog.dev.model"
    assert receipt.destination_model_version == 2
    assert receipt.promotion_mode == promotion.SOURCE_ALIAS_REHEARSAL
    assert client.aliases == [
        {
            "name": "catalog.dev.model",
            "alias": "preprod_candidate",
            "version": 2,
        }
    ]
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.dev.model/2",
    ]


def test_ready_build_validation_checks_tags_and_artifact_bytes(monkeypatch):
    client = Client()
    client.get_model_version = lambda _name, version: SimpleNamespace(
        version=version,
        run_id="run-1",
        tags={
            "nextads.model_build_id": "build-1",
            "nextads.artifact_digest": "b" * 64,
        },
    )
    digest_uris = _record_uri_digests(monkeypatch)

    promotion.validate_registered_model_build(client, _build())
    assert digest_uris == ["models:/catalog.dev.model/2"]


def test_ready_build_validation_rejects_a_changed_digest_tag(monkeypatch):
    client = Client()
    client.get_model_version = lambda _name, version: SimpleNamespace(
        version=version,
        run_id="run-1",
        tags={
            "nextads.model_build_id": "build-1",
            "nextads.artifact_digest": "c" * 64,
        },
    )
    _record_uri_digests(monkeypatch)

    with pytest.raises(ValueError, match="different digest tag"):
        promotion.validate_registered_model_build(client, _build())


def test_exact_registered_version_copy_is_digest_checked_and_idempotent(
    monkeypatch,
):
    client = ExactVersionClient()
    digest_uris = _record_uri_digests(monkeypatch)

    receipt, reused = promotion.promote_exact_registered_version(
        client,
        source_model_name="catalog.dev.model",
        source_model_version=2,
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is False
    assert receipt.artifact_digest == "b" * 64
    assert client.copies == [
        {
            "src_model_uri": "models:/catalog.dev.model/2",
            "dst_name": "catalog.integration.model",
        }
    ]
    assert len(client.tags) == 4
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]


def test_exact_registered_version_retry_does_not_create_a_duplicate(
    monkeypatch,
):
    client = ExactVersionClient(existing=True)
    digest_uris = _record_uri_digests(monkeypatch)

    _receipt, reused = promotion.promote_exact_registered_version(
        client,
        source_model_name="catalog.dev.model",
        source_model_version=2,
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is True
    assert client.copies == []
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]


def test_cross_workspace_copy_does_not_require_a_destination_run_id(
    monkeypatch,
):
    client = ExactVersionClient(destination_run_id=None)
    digest_uris = _record_uri_digests(monkeypatch)

    receipt, reused = promotion.promote_exact_registered_version(
        client,
        source_model_name="catalog.dev.model",
        source_model_version=2,
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is False
    assert receipt.destination_run_id is None
    assert receipt.artifact_digest == "b" * 64
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]


def test_model_build_copy_receipt_allows_a_missing_destination_run_id(
    monkeypatch,
):
    client = Client(destination_run_id=None)
    digest_uris = _record_uri_digests(monkeypatch)

    receipt, reused = promotion.promote_exact_model_build(
        client,
        _build(),
        destination_model_name="catalog.integration.model",
        alias="integration_candidate",
    )

    assert reused is False
    assert receipt.destination_run_id is None
    assert digest_uris == [
        "models:/catalog.dev.model/2",
        "models:/catalog.integration.model/7",
    ]
