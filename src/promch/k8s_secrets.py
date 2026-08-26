"""Read a K8s Secret key from inside the cluster, with no k8s client library.

kopf owns all other k8s I/O, but it has no public helper to read an arbitrary
Secret. This does exactly one authenticated GET against the API server, using the
mounted ServiceAccount token + CA. The token is re-read on every call because the
kubelet rotates the projected token in place; a cached token would eventually 401.
"""

from __future__ import annotations

import base64
import logging
import os
import ssl
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
_TOKEN_PATH = f"{_SA_DIR}/token"
_CA_PATH = f"{_SA_DIR}/ca.crt"
_NS_PATH = f"{_SA_DIR}/namespace"


class SecretResolutionError(Exception):
    """A referenced Secret or key could not be read."""


class SecretReader(Protocol):
    async def read_secret(self, namespace: str, name: str) -> dict[str, str]: ...


def operator_namespace() -> str:
    """The operator's own namespace: POD_NAMESPACE env, else the mounted file."""
    ns = os.environ.get("POD_NAMESPACE")
    if ns:
        return ns
    try:
        with open(_NS_PATH) as f:
            return f.read().strip()
    except OSError:
        return "default"


# (namespace, name) -> (status_code, decoded_json_body). Injected so tests never
# touch disk or the network.
Transport = Callable[[str, str], Awaitable[tuple[int, dict[str, Any]]]]


class InClusterSecretReader:
    def __init__(self, transport: Transport | None = None) -> None:
        self._transport: Transport = transport or self._in_cluster_get

    async def read_secret(self, namespace: str, name: str) -> dict[str, str]:
        """Return the Secret's `data` as a decoded {key: value} map.

        Values that are not valid base64/utf-8 are skipped (auth keys like
        username/password are text; unrelated binary keys are ignored).
        """
        status, body = await self._transport(namespace, name)
        if status == 404:
            raise SecretResolutionError(f"secret {namespace}/{name} not found")
        if status != 200:
            raise SecretResolutionError(
                f"reading secret {namespace}/{name}: API server returned HTTP {status}"
            )
        raw = body.get("data") or {}
        out: dict[str, str] = {}
        for k, v in raw.items():
            try:
                out[k] = base64.b64decode(v).decode("utf-8")
            except (ValueError, UnicodeDecodeError):
                continue
        return out

    async def _in_cluster_get(self, namespace: str, name: str) -> tuple[int, dict[str, Any]]:
        import aiohttp

        # Re-read token + CA on every call (SA token rotates in place).
        with open(_TOKEN_PATH) as f:
            token = f.read().strip()
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        url = f"https://{host}:{port}/api/v1/namespaces/{namespace}/secrets/{name}"
        ssl_ctx = ssl.create_default_context(cafile=_CA_PATH)
        async with (
            aiohttp.ClientSession() as session,
            session.get(
                url, headers={"Authorization": f"Bearer {token}"}, ssl=ssl_ctx
            ) as resp,
        ):
            if resp.status != 200:
                return resp.status, {}
            return resp.status, await resp.json()
