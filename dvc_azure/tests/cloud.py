import os
import uuid

from funcy import cached_property

from dvc.testing.cloud import Cloud
from dvc.testing.path_info import CloudURLInfo


class Azure(Cloud, CloudURLInfo):
    IS_OBJECT_STORAGE = True

    def __init__(self, url, **kwargs):
        super().__init__(url)
        self.opts = kwargs
        if "connection_string" not in kwargs:
            raise ValueError("Must provide connection_string")

    def __truediv__(self, key):
        ret = super().__truediv__(key)
        ret.opts = self.opts
        return ret

    @cached_property
    def service_client(self):
        # pylint: disable=no-name-in-module
        from azure.storage.blob import BlobServiceClient

        return BlobServiceClient.from_connection_string(self.opts["connection_string"])

    @property
    def container_client(self):
        return self.service_client.get_container_client(container=self.bucket)

    @property
    def blob_client(self):
        return self.service_client.get_blob_client(self.bucket, self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        self.blob_client.upload_blob(contents, overwrite=True)

    def unlink(self, missing_ok: bool = False) -> None:
        if not self.exists():
            if not missing_ok:
                raise FileNotFoundError(str(self))
            return
        self.blob_client.delete_blob()

    def rmdir(self, recursive: bool = True) -> None:
        if not self.is_dir():
            raise NotADirectoryError(str(self))

        blobs = [
            blob.name
            for blob in self.container_client.list_blobs(
                name_starts_with=(self / "").path
            )
        ]
        if not blobs:
            return

        if not recursive:
            raise OSError(f"Not recursive and directory not empty: {self}")

        for blob in blobs:
            self.container_client.delete_blob(blob)

    def read_bytes(self):
        stream = self.blob_client.download_blob()
        return stream.readall()

    @property
    def fs_path(self):
        bucket = self.bucket.rstrip("/")
        path = self.path.lstrip("/")
        return f"{bucket}/{path}"

    def is_file(self):
        raise NotImplementedError

    def is_dir(self):
        path = self.path.rstrip("/") + "/"
        cc = self.service_client.get_container_client(self.bucket)
        for _ in cc.list_blobs(name_starts_with=path):
            return True
        return False

    def exists(self):
        raise NotImplementedError

    @property
    def config(self):
        return {"url": self.url, **self.opts}

    @staticmethod
    def get_storagepath():
        path = os.environ.get("DVC_TEST_AZURE_PATH")
        assert path
        return path + "/" + "dvc_test_caches" + "/" + str(uuid.uuid4())
