import os
import shlex
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

import pytest

from .cloud import Azure

AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # noqa: E501
AZURITE_CONNECTION_STRING = f"DefaultEndpointsProtocol=http;AccountName={AZURITE_ACCOUNT_NAME};AccountKey={AZURITE_KEY};BlobEndpoint={{host}}/{AZURITE_ACCOUNT_NAME};"  # noqa: E501
CONTAINER_NAME = "dvc_azure_test_azurite"


def stop_docker():
    container_id = subprocess.check_output(
        ["docker", "ps", "-q", "-f", f"name={CONTAINER_NAME}"],
        text=True,
    ).strip()
    if container_id:
        subprocess.call(["docker", "rm", "-f", "-v", container_id])


def start_docker():
    cmd = (
        "docker run -d -p 10000:10000 "
        f"--name {CONTAINER_NAME} "
        "mcr.microsoft.com/azure-storage/azurite "
        "azurite-blob --loose --blobHost 0.0.0.0 --skipApiVersionCheck"
    )
    subprocess.check_output(shlex.split(cmd))
    return "http://localhost:10000"


def wait_for_azurite(url, timeout=30):
    p = urlparse(url)
    port, host = p.port, p.hostname
    for _ in range(timeout):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            try:
                sock.connect((host, port))
            except (socket.timeout, ConnectionRefusedError):
                time.sleep(1)
            else:
                return True
    raise TimeoutError("Azurite did not start in time")


@pytest.fixture(scope="session")
def azurite(host):
    if host:
        wait_for_azurite(host)
        yield AZURITE_CONNECTION_STRING.format(host=host)
        return

    if os.environ.get("CI") and sys.platform == "darwin":
        pytest.skip("disabled for macOS on GitHub Actions")
    if sys.platform == "win32":
        pytest.skip("Cannot run azurite on Windows")

    stop_docker()
    host = start_docker()
    wait_for_azurite(host)
    try:
        yield AZURITE_CONNECTION_STRING.format(host=host)
    finally:
        stop_docker()


@pytest.fixture
def azure_container(azurite):
    from azure.storage.blob import BlobServiceClient

    container_name = "dvc-test"
    service_client = BlobServiceClient.from_connection_string(azurite)
    service_client.create_container(container_name)
    try:
        yield container_name
    finally:
        service_client.delete_container(container_name)


@pytest.fixture
def make_azure(request):
    def _make_azure():
        if os.environ.get("DVC_TEST_AZURE_PATH"):
            path = Azure.get_storagepath()
            path = path.replace("az://", "azure://")
        else:
            path = None
        connection_string = os.environ.get("DVC_TEST_AZURE_CONNECTION_STRING")
        if not (path and connection_string):
            connection_string = request.getfixturevalue("azurite")
            container_name = request.getfixturevalue("azure_container")
            path = "azure://" + container_name
        return Azure(path, connection_string=connection_string)

    return _make_azure


@pytest.fixture
def azure(make_azure):
    return make_azure()


@pytest.fixture
def remote(make_remote):
    return make_remote(name="upstream", typ="azure")
