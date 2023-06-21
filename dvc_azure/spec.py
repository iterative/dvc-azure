from adlfs import AzureBlobFileSystem as _AzureBlobFileSystem


class AzureBlobFileSystem(  # pylint: disable=abstract-method
    _AzureBlobFileSystem
):
    def put_file(self, *args, **kwargs) -> None:
        kwargs["overwrite"] = True
        return super().put_file(*args, **kwargs)

    async def _put_file(self, *args, **kwargs) -> None:
        kwargs["overwrite"] = True
        return await super()._put_file(*args, **kwargs)

    def rm(self, *args, **kwargs) -> None:
        kwargs["expand_path"] = False
        return super().rm(*args, **kwargs)

    async def _rm(self, *args, **kwargs) -> None:
        kwargs["expand_path"] = False
        return await super()._rm(*args, **kwargs)
