from types import SimpleNamespace

import pytest

from next_ads.delivery import cosmos


@pytest.fixture
def cosmos_config_args():
    return {
        "url": "https://example.documents.azure.com",
        "db_name": "nextads",
        "container": "exclusions",
        "subscriptionid": "subscription",
        "rg_name": "resource-group",
        "tenantId": "tenant",
        "clientId": "client",
        "clientSecret": "secret",
    }


@pytest.mark.parametrize(
    ("operation", "expected_strategy"),
    [
        ("read", None),
        ("upsert", "ItemOverwrite"),
        ("delete", "ItemDelete"),
        ("default_upsert", "ItemOverwrite"),
    ],
)
def test_get_cosmos_config_preserves_operation_contract(
    cosmos_config_args,
    operation,
    expected_strategy,
):
    config = cosmos.get_cosmos_config(operation, **cosmos_config_args)

    assert config["spark.cosmos.database"] == "nextads"
    assert config["spark.cosmos.container"] == "exclusions"
    assert config["spark.cosmos.read.inferSchema.enabled"] == "true"
    assert config.get("spark.cosmos.write.strategy") == expected_strategy


def test_get_cosmos_config_supports_throughput_and_direct_mode(
    cosmos_config_args,
):
    config = cosmos.get_cosmos_config(
        "upsert",
        **cosmos_config_args,
        TC=True,
        gateway_mode=False,
    )

    assert config["spark.cosmos.useGatewayMode"] == "false"
    assert config["spark.cosmos.throughputControl.enabled"] == "true"
    assert (
        config["spark.cosmos.throughputControl.globalControl.container"]
        == "ThroughputControl"
    )


def test_get_cosmos_config_rejects_unknown_operation(cosmos_config_args):
    with pytest.raises(Exception, match="ctype must be"):
        cosmos.get_cosmos_config("unknown", **cosmos_config_args)


class FakeRow:
    def __init__(self, value):
        self.value = value

    def asDict(self, recursive=False):  # noqa: N802 - Spark Row API
        assert recursive is True
        return {"id": self.value}


class FakeContainer:
    def __init__(self):
        self.documents = []

    def read(self):
        return None

    def upsert_item(self, document):
        self.documents.append(document)

    def query_items(
        self,
        query,
        parameters=None,
        enable_cross_partition_query=False,
    ):
        assert isinstance(query, str)
        assert enable_cross_partition_query is True

        if not parameters:
            return list(self.documents)

        parameter_map = {item["name"]: item["value"] for item in parameters}
        doc_id = parameter_map.get("@id")
        return [doc for doc in self.documents if doc.get("id") == doc_id]


class FakeDatabase:
    def __init__(self, container):
        self.container = container

    def read(self):
        return None

    def get_container_client(self, name):
        assert name == "exclusions"
        return self.container


class FakeClient:
    def __init__(self):
        self.container = FakeContainer()
        self.closed = False

    def list_databases(self, max_item_count):
        assert max_item_count == 1
        return iter([{"id": "nextads"}])

    def get_database_client(self, name):
        assert name == "nextads"
        return FakeDatabase(self.container)

    def close(self):
        self.closed = True


def test_sdk_write_to_cosmos_upserts_documents_and_closes_client(monkeypatch):
    fake_client = FakeClient()
    secret_values = {
        ("realtime", "DataPlatform-Dev-TenantId"): "tenant",
        ("scope", "client-id"): "client",
        ("scope", "client-secret"): "secret",
    }
    fake_dbutils = SimpleNamespace(
        secrets=SimpleNamespace(
            get=lambda scope, key: secret_values[(scope, key)]
        )
    )
    credential_args = {}

    monkeypatch.setattr(cosmos, "get_dbutils", lambda: fake_dbutils)
    monkeypatch.setattr(
        cosmos,
        "ClientSecretCredential",
        lambda **kwargs: credential_args.update(kwargs) or object(),
    )
    monkeypatch.setattr(
        cosmos,
        "CosmosClient",
        lambda url, credential: fake_client,
    )

    config = SimpleNamespace(
        az_tenant_id=None,
        dbutils_secret_scope="scope",
        secret_key_spn_clientid="client-id",
        secret_key_spn_secret="client-secret",
        cosmos_url="https://example.documents.azure.com",
        cosmos_database="nextads",
        cosmos_container="exclusions",
    )
    dataframe = SimpleNamespace(
        collect=lambda: [FakeRow("one"), FakeRow("two")]
    )

    cosmos.sdk_write_to_cosmos(config, "dev", dataframe)

    assert credential_args == {
        "tenant_id": "tenant",
        "client_id": "client",
        "client_secret": "secret",
    }
    assert fake_client.container.documents == [{"id": "one"}, {"id": "two"}]
    assert fake_client.closed is True


class FakeCosmosError(Exception):
    def __init__(self, status_code, message):
        super().__init__(message)
        self.status_code = status_code


def test_cosmos_errors_add_permission_and_not_found_context():
    config = SimpleNamespace(
        cosmos_url="endpoint",
        cosmos_database="database",
        cosmos_container="container",
    )

    with pytest.raises(PermissionError, match="metadata access"):
        cosmos._raise_cosmos_error_with_context(
            "write",
            FakeCosmosError(403, "databaseAccounts/readMetadata"),
            config,
        )

    with pytest.raises(FileNotFoundError, match="resource not found"):
        cosmos._raise_cosmos_error_with_context(
            "write",
            FakeCosmosError(404, "missing"),
            config,
        )
