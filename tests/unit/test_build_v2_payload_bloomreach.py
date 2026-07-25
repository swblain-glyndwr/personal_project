from types import SimpleNamespace

import pytest

from jobs.nextads_delivery import build_v2_payload


def _config(import_config=None):
    return SimpleNamespace(bloomreach_import=import_config)


def _import_config(**overrides):
    values = {
        "enabled": True,
        "url": "https://api.example.test",
        "project_token": "project-token",
        "secret_scope": "scope",
        "secret_key_id": "key-id",
        "secret_key_name": "key-name",
        "import_id": "import-id",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_bloomreach_import_disabled_when_config_missing():
    assert not build_v2_payload.is_bloomreach_import_enabled(
        SimpleNamespace()
    )


def test_bloomreach_import_disabled_when_flag_false():
    config = _config(SimpleNamespace(enabled=False))

    assert not build_v2_payload.is_bloomreach_import_enabled(config)


def test_bloomreach_import_requires_all_enabled_config_fields():
    config = _config(_import_config(import_id=""))

    with pytest.raises(ValueError, match="import_id"):
        build_v2_payload.require_bloomreach_import_config(config)


def test_start_bloomreach_import_posts_to_expected_endpoint(monkeypatch):
    posted = {}

    class FakeSecrets:
        def get(self, scope, key):
            return f"{scope}:{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    class FakeResponse:
        status_code = 200
        text = "ok"

    def fake_post(url, auth):
        posted["url"] = url
        posted["auth"] = auth
        return FakeResponse()

    monkeypatch.setattr(
        build_v2_payload, "dbutils", FakeDbutils(), raising=False
    )
    monkeypatch.setattr(build_v2_payload.requests, "post", fake_post)

    logger = SimpleNamespace(info=lambda message: posted.setdefault("log", message))

    build_v2_payload.start_bloomreach_import(
        _config(_import_config()), logger
    )

    assert posted["url"] == (
        "https://api.example.test/data/v2/projects/"
        "project-token/imports/import-id/start"
    )
    assert posted["auth"].username == "scope:key-id"
    assert posted["auth"].password == "scope:key-name"
    assert posted["log"] == "Bloomreach import triggered successfully"


def test_start_bloomreach_import_raises_on_failed_response(monkeypatch):
    class FakeSecrets:
        def get(self, scope, key):
            return f"{scope}:{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    class FakeResponse:
        status_code = 500
        text = "failed"

    monkeypatch.setattr(
        build_v2_payload, "dbutils", FakeDbutils(), raising=False
    )
    monkeypatch.setattr(
        build_v2_payload.requests, "post", lambda url, auth: FakeResponse()
    )

    logger = SimpleNamespace(info=lambda message: None)

    with pytest.raises(
        RuntimeError,
        match="Bloomreach import trigger failed with status 500",
    ):
        build_v2_payload.start_bloomreach_import(
            _config(_import_config()), logger
        )
