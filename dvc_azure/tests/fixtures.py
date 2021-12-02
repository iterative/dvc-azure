import os

import pytest


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "dvc_azure", "tests", "docker-compose.yml"
    )


@pytest.fixture
def make_azure():
    def _make_azure():
        raise NotImplementedError

    return _make_azure


@pytest.fixture
def azure(make_azure):
    return make_azure()

