import logging
import os
import threading
from typing import Any, Dict

from dvc_objects.fs.base import ObjectFileSystem
from dvc_objects.fs.errors import AuthError
from fsspec.utils import infer_storage_options
from funcy import cached_property, memoize, wrap_prop

from .path import AzurePath

logger = logging.getLogger(__name__)
_DEFAULT_CREDS_STEPS = (
    "https://azuresdkdocs.blob.core.windows.net/$web/python/"
    "azure-identity/1.4.0/azure.identity.html#azure.identity"
    ".DefaultAzureCredential"
)


class AzureAuthError(AuthError):
    pass


@memoize
def _az_config():
    # NOTE: ideally we would've used get_default_cli().config from
    # azure.cli.core, but azure-cli-core has a lot of conflicts with other
    # dependencies. So instead we are just use knack directly
    from knack.config import CLIConfig

    config_dir = os.getenv(
        "AZURE_CONFIG_DIR", os.path.expanduser(os.path.join("~", ".azure"))
    )
    return CLIConfig(config_dir=config_dir, config_env_var_prefix="AZURE")


# pylint:disable=abstract-method
class AzureFileSystem(ObjectFileSystem):
    protocol = "azure"
    PARAM_CHECKSUM = "etag"
    REQUIRES = {
        "adlfs": "adlfs",
        "knack": "knack",
        "azure-identity": "azure.identity",
    }

    @cached_property
    def path(self) -> AzurePath:
        def _getcwd():
            return self.fs.root_marker

        return AzurePath(self.sep, getcwd=_getcwd)

    @classmethod
    def _strip_protocol(cls, path: str):
        opts = infer_storage_options(path)
        if opts.get("host"):
            if "url_query" in opts:
                query = f"?{opts['url_query']}"
            else:
                query = ""
            return f"{opts['host']}{opts['path']}{query}"

        return _az_config().get("storage", "container_name", None)

    def unstrip_protocol(self, path: str) -> str:
        return "azure://" + path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str) -> Dict[str, Any]:
        ret = {}
        ops = infer_storage_options(urlpath)
        if "host" in ops:
            ret["bucket"] = ops["host"]

        url_query = ops.get("url_query")
        if url_query is not None:
            from urllib.parse import parse_qs

            parsed = parse_qs(url_query)
            if "versionid" in parsed:
                ret["version_aware"] = True

        return ret

    def _prepare_credentials(self, **config):
        az_config = _az_config()

        defaults = {
            "version_aware": None,
            "connection_string": az_config.get(
                "storage", "connection_string", None
            ),
            "account_name": az_config.get("storage", "account", None),
            "account_key": az_config.get("storage", "key", None),
            "sas_token": az_config.get("storage", "sas_token", None),
            "tenant_id": None,
            "client_id": None,
            "client_secret": None,
            "exclude_environment_credential": None,
            "exclude_shared_token_cache_credential": None,
            "exclude_managed_identity_credential": None,
            # NOTE: these two are True by default in DefaultAzureCredential,
            # so we need to explicitly default to False here.
            "exclude_interactive_browser_credential": False,
            "exclude_visual_studio_code_credential": False,
        }

        ret = {}
        for name, default in defaults.items():
            value = config.get(name, default)
            if value is not None:
                ret[name] = value

        ret["anon"] = config.get("allow_anonymous_login", False)

        return ret

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from azure.core.exceptions import AzureError

        from .spec import AzureBlobFileSystem

        # Disable spam from failed cred types for DefaultAzureCredential
        logging.getLogger("azure.identity.aio").setLevel(logging.ERROR)

        try:
            return AzureBlobFileSystem(**self.fs_args)
        except (ValueError, AzureError) as e:
            raise AzureAuthError(
                "Authentication to Azure Blob Storage failed"
            ) from e
