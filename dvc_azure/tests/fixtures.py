import os
import uuid

import pytest
from funcy import suppress

from .cloud import Azure

TEST_AZURE_CONTAINER = "tests"
TEST_AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSR"
    "Z6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
)


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(os.path.dirname(__file__), "docker-compose.yml")


@pytest.fixture(scope="session")
def azure_server(docker_compose, docker_services):
    from azure.core.exceptions import (  # pylint: disable=no-name-in-module
        AzureError,
    )
    from azure.storage.blob import (  # pylint: disable=no-name-in-module
        BlobServiceClient,
    )

    port = docker_services.port_for("azurite", 10000)
    connection_string = TEST_AZURE_CONNECTION_STRING.format(port=port)

    def _check():
        try:
            BlobServiceClient.from_connection_string(
                connection_string
            ).list_containers()
            return True
        except AzureError:
            return False

    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=_check
    )

    Azure.CONNECTION_STRING = connection_string
    return connection_string


@pytest.fixture
def make_azure(azure_server):
    def _make_azure():
        from azure.core.exceptions import ResourceExistsError

        url = f"azure://{TEST_AZURE_CONTAINER}/{uuid.uuid4()}"
        ret = Azure(url, connection_string=azure_server)

        container = ret.service_client.get_container_client(
            TEST_AZURE_CONTAINER
        )
        with suppress(ResourceExistsError):
            container.create_container()

        return ret

    return _make_azure


@pytest.fixture
def azure(make_azure):
    return make_azure()
