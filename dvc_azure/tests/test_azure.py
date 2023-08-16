import pytest

from dvc_azure import AzureAuthError, AzureFileSystem

container_name = "container-name"
connection_string = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def test_strip_protocol_env_var(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", container_name)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", connection_string)

    actual = AzureFileSystem._strip_protocol(  # pylint: disable=W0212
        "azure://"
    )
    assert actual == container_name


def test_strip_protocol():
    actual = AzureFileSystem._strip_protocol(  # pylint: disable=W0212
        f"azure://{container_name}"
    )
    assert actual == container_name


def test_init():
    config = {"connection_string": connection_string}
    fs = AzureFileSystem(**config)
    assert fs.fs_args["connection_string"] == connection_string


def test_azure_login_methods():
    def get_login_method(config):
        fs = AzureFileSystem(**config)
        return fs._login_method  # pylint: disable=W0212

    with pytest.raises(AzureAuthError):
        get_login_method({})

    assert (
        get_login_method({"connection_string": "test"}) == "connection string"
    )
    assert get_login_method({"account_name": "test"}).startswith(
        "default credentials"
    )
    assert (
        get_login_method(
            {"account_name": "test", "allow_anonymous_login": True}
        )
        == "anonymous login"
    )

    with pytest.raises(AzureAuthError):
        get_login_method(
            {"tenant_id": "test", "client_id": "test", "client_secret": "test"}
        )

    assert (
        get_login_method(
            {
                "account_name": "test",
                "tenant_id": "test",
                "client_id": "test",
                "client_secret": "test",
            }
        )
        == "AD service principal"
    )

    assert (
        get_login_method({"account_name": "test", "account_key": "test"})
        == "account key"
    )
    assert (
        get_login_method({"account_name": "test", "sas_token": "test"})
        == "SAS token"
    )
    assert (
        get_login_method(
            {
                "connection_string": "test",
                "account_name": "test",
                "sas_token": "test",
            }
        )
        == "connection string"
    )
    assert (
        get_login_method({"connection_string": "test", "sas_token": "test"})
        == "connection string"
    )
