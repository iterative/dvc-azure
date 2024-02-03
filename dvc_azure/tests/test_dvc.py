import pytest

from dvc.testing.api_tests import (  # noqa: F401
    TestAPI,
)
from dvc.testing.remote_tests import (  # noqa: F401
    TestRemote,
)
from dvc.testing.workspace_tests import (  # noqa: F401  # noqa: F401
    TestGetUrl,
    TestLsUrl,
)
from dvc.testing.workspace_tests import TestImport as _TestImport


@pytest.fixture
def cloud(make_cloud):
    return make_cloud(typ="azure")


@pytest.fixture
def remote(make_remote):
    return make_remote(name="upstream", typ="azure")


@pytest.fixture
def workspace(make_workspace):
    return make_workspace(name="workspace", typ="azure")


@pytest.mark.xfail(reason="waiting for https://github.com/fsspec/adlfs/pull/333")
class TestImport(_TestImport):
    @pytest.fixture
    def stage_md5(self):
        return "ffe462bbb08432b7a1c3985fcf82ad3a"

    @pytest.fixture
    def is_object_storage(self):
        return True

    @pytest.fixture
    def dir_md5(self):
        return "ec602a6ba97b2dd07bd6d2cd89674a60.dir"
