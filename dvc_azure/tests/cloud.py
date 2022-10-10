from dvc.testing.cloud import Cloud
from dvc.testing.path_info import CloudURLInfo
from funcy import cached_property


class Azure(Cloud, CloudURLInfo):

    IS_OBJECT_STORAGE = True
    CONNECTION_STRING = None

    def __init__(self, url, **kwargs):
        super().__init__(url)
        self.opts = kwargs

    def __truediv__(self, key):
        ret = super().__truediv__(key)
        ret.opts = self.opts
        return ret

    @cached_property
    def service_client(self):
        # pylint: disable=no-name-in-module
        from azure.storage.blob import BlobServiceClient

        service_client = BlobServiceClient.from_connection_string(
            self.CONNECTION_STRING
        )

        return service_client

    @property
    def blob_client(self):
        return self.service_client.get_blob_client(self.bucket, self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        self.blob_client.upload_blob(contents, overwrite=True)

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
