# pylint: disable=redefined-outer-name
import pytest

from .cloud import Azure


@pytest.fixture
def make_azure(tmp_azure_path, azurite):
    def _make_azure():
        return Azure(
            str(tmp_azure_path).replace("az://", "azure://"),
            connection_string=azurite,
        )

    return _make_azure


@pytest.fixture
def azure(make_azure):
    return make_azure()
