import logging
import os
import threading
from typing import Any, Dict

from dvc_objects.fs.base import ObjectFileSystem
from dvc_objects.fs.errors import AuthError
from fsspec.asyn import fsspec_loop
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
        from azure.identity.aio import DefaultAzureCredential

        # Disable spam from failed cred types for DefaultAzureCredential
        logging.getLogger("azure.identity.aio").setLevel(logging.ERROR)

        login_info = {}
        login_info["version_aware"] = config.get("version_aware", False)
        login_info["connection_string"] = config.get(
            "connection_string",
            _az_config().get("storage", "connection_string", None),
        )
        login_info["account_name"] = config.get(
            "account_name", _az_config().get("storage", "account", None)
        )
        login_info["account_key"] = config.get(
            "account_key", _az_config().get("storage", "key", None)
        )
        login_info["sas_token"] = config.get(
            "sas_token", _az_config().get("storage", "sas_token", None)
        )
        login_info["tenant_id"] = config.get("tenant_id")
        login_info["client_id"] = config.get("client_id")
        login_info["client_secret"] = config.get("client_secret")

        if not (login_info["account_name"] or login_info["connection_string"]):
            raise AzureAuthError(
                "Authentication to Azure Blob Storage requires either "
                "account_name or connection_string."
            )

        any_secondary = any(
            value for key, value in login_info.items() if key != "account_name"
        )
        if (
            login_info["account_name"]
            and not any_secondary
            and not config.get("allow_anonymous_login", False)
        ):
            with fsspec_loop():
                login_info["credential"] = DefaultAzureCredential(
                    exclude_interactive_browser_credential=False,
                    exclude_environment_credential=config.get(
                        "exclude_environment_credential", False
                    ),
                    exclude_visual_studio_code_credential=config.get(
                        "exclude_visual_studio_code_credential", False
                    ),
                    exclude_shared_token_cache_credential=config.get(
                        "exclude_shared_token_cache_credential", False
                    ),
                    exclude_managed_identity_credential=config.get(
                        "exclude_managed_identity_credential", False
                    ),
                )

        for login_method, required_keys in [  # noqa
            ("connection string", ["connection_string"]),
            (
                "AD service principal",
                ["tenant_id", "client_id", "client_secret"],
            ),
            ("account key", ["account_name", "account_key"]),
            ("SAS token", ["account_name", "sas_token"]),
            (
                f"default credentials ({_DEFAULT_CREDS_STEPS})",
                ["account_name", "credential"],
            ),
            ("anonymous login", ["account_name"]),
        ]:
            if all(login_info.get(key) is not None for key in required_keys):
                break
        else:
            login_method = None

        self.login_method = login_method
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from adlfs import AzureBlobFileSystem
        from azure.core.exceptions import AzureError

        try:
            return AzureBlobFileSystem(**self.fs_args)
        except (ValueError, AzureError) as e:
            raise AzureAuthError(
                f"Authentication to Azure Blob Storage via {self.login_method}"
                " failed."
            ) from e

    def put_file(self, *args, **kwargs) -> None:
        kwargs["overwrite"] = True
        super().put_file(*args, **kwargs)
