import os

# pylint: disable=redefined-outer-name
import pytest

from .cloud import Azure


@pytest.fixture
def make_azure(request):
    def _make_azure():
        if os.environ.get("DVC_TEST_AZURE_PATH"):
            path = Azure.get_storagepath()
        else:
            path = None
        connection_string = os.environ.get("DVC_TEST_AZURE_CONNECTION_STRING")
        if not (path and connection_string):
            path = request.getfixturevalue("tmp_azure_path")
            connection_string = request.getfixturevalue("azurite")
        return Azure(
            str(path).replace("az://", "azure://"),
            connection_string=connection_string,
        )

    return _make_azure


@pytest.fixture
def azure(make_azure):
    return make_azure()
