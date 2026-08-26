"""kopf daemon for Connection resources (Phase A: discovery + status only).

This module has two parts: pure mapping helpers (unit-tested) and the kopf
handlers that drive the ClickHouse client (validated by a live smoke test).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import Mapping
from typing import Any

import kopf

from ..clickhouse.connection import ClickHouseConnection
from ..clickhouse.types import ConnectionSpec, ConnectionStatus, NodeStatus
from ..k8s_secrets import (
    InClusterSecretReader,
    SecretReader,
    SecretResolutionError,
    operator_namespace,
)

logger = logging.getLogger(__name__)

_CRD = ("prometheus-ch-exporter.io", "v1alpha1", "clickhouseconnections")
# kopf reads timer intervals at import time, so these are process-wide (env-tunable),
# not per-CRD. Two cadences: frequent liveness re-check, infrequent topology discovery.
_RECHECK_INTERVAL = float(os.environ.get("PROMCH_RECHECK_INTERVAL", "60"))
_TOPOLOGY_INTERVAL = float(os.environ.get("PROMCH_TOPOLOGY_INTERVAL", "1800"))

# One ClickHouseConnection per resource, keyed by (namespace, name). A module-level
# registry (not kopf `memo`) guarantees a single shared instance across the
# create/resume handler, the timer, and delete. kopf `memo` did not reliably share
# the instance here, which spawned duplicate connections and leaked aiohttp sessions.
_connections: dict[tuple[str, str], ClickHouseConnection] = {}

# Injectable seam (tests replace with a fake). Reads a referenced Secret's
# password from the operator's own namespace.
_secret_reader: SecretReader = InClusterSecretReader()


def _spec_to_connection_spec(
    spec: Mapping[str, Any], password: str = "", username_override: str | None = None
) -> ConnectionSpec:
    """Map a Connection CRD `spec` into the client's ConnectionSpec.

    Credentials are resolved separately (from a Secret) and passed in. When the
    auth Secret carries a username it overrides `spec.username`; otherwise
    `spec.username` (default "default") is used. Other tuning fields keep their
    ConnectionSpec defaults.
    """
    if username_override is not None:
        username = username_override
    else:
        username = str(spec.get("username", "default"))
    return ConnectionSpec(
        seed_hosts=list(spec["seedHosts"]),
        cluster_name=str(spec["clusterName"]),
        port=int(spec.get("port", 8443)),
        username=username,
        password=password,
        secure=bool(spec.get("secure", True)),
        verify=bool(spec.get("verify", True)),
        max_failovers=spec.get("maxFailovers"),
        system_query_retries=int(spec.get("systemQueryRetries", 1)),
        liveness_mode=spec.get("livenessMode", "active"),
        connect_timeout=float(spec.get("connectTimeout", 10.0)),
    )


async def _resolve_credentials(spec: Mapping[str, Any]) -> tuple[str | None, str]:
    """Resolve (username_override, password) from the auth Secret.

    Returns (None, "") when no authSecretRef is set. When set, the passwordKey
    must exist; the usernameKey is optional (None => fall back to spec.username).
    """
    ref = spec.get("authSecretRef")
    if not ref:
        return None, ""
    name = str(ref["name"])
    username_key = str(ref.get("usernameKey", "username"))
    password_key = str(ref.get("passwordKey", "password"))
    namespace = operator_namespace()
    data = await _secret_reader.read_secret(namespace, name)
    if password_key not in data:
        raise SecretResolutionError(f"secret {namespace}/{name} has no key {password_key!r}")
    return data.get(username_key), data[password_key]


def _node_status_to_dict(node: NodeStatus) -> dict[str, Any]:
    """Serialize one NodeStatus, omitting null optional fields.

    Omitting None keeps the patched status identical to what the API server
    stores (it prunes nulls), so kopf's post-patch consistency check stays quiet.
    """
    out: dict[str, Any] = {"host": node.host, "alive": node.alive, "inCluster": node.in_cluster}
    if node.last_checked is not None:
        out["lastChecked"] = node.last_checked.isoformat()
    if node.last_error is not None:
        out["lastError"] = node.last_error
    return out


def _status_to_dict(status: ConnectionStatus) -> dict[str, Any]:
    """Serialize ConnectionStatus into the plain dict kopf patches into `.status`."""
    out: dict[str, Any] = {
        "phase": status.phase,
        "totalNodes": status.total_nodes,
        "aliveNodes": status.alive_nodes,
        "nodes": [_node_status_to_dict(n) for n in status.nodes],
    }
    if status.last_discovery is not None:
        out["lastDiscovery"] = status.last_discovery.isoformat()
    return out


def _resource_key(namespace: str | None, name: str) -> tuple[str, str]:
    # Connection is a namespaced resource, so namespace is always set at runtime.
    return (namespace or "", name)


async def _ensure_connection(
    namespace: str | None, name: str, spec: Mapping[str, Any]
) -> ClickHouseConnection:
    """Get-or-create the single ClickHouseConnection for this resource.

    On a cache miss we resolve the password (a Secret read) before building the
    spec; steady-state timers hit the cache and never touch the API server. The
    registry is re-checked after the await so concurrent handlers (resume +
    timer) never build duplicates.
    """
    key = _resource_key(namespace, name)
    conn = _connections.get(key)
    if conn is not None:
        return conn
    try:
        username_override, password = await _resolve_credentials(spec)
    except SecretResolutionError as exc:
        raise kopf.TemporaryError(f"connection {name}: {exc}", delay=30) from exc
    cspec = _spec_to_connection_spec(spec, password, username_override)
    conn = _connections.get(key)  # re-check after await (race guard)
    if conn is None:
        conn = ClickHouseConnection(cspec)
        _connections[key] = conn
    return conn


def get_connection(name: str) -> ClickHouseConnection | None:
    """Look up a cluster-scoped connection by name (namespace is always None)."""
    return _connections.get(_resource_key(None, name))


async def _discover(conn: ClickHouseConnection) -> dict[str, Any]:
    return _status_to_dict(await conn.refresh())


@kopf.on.create(*_CRD)
@kopf.on.resume(*_CRD)
async def on_connection_present(
    spec: kopf.Spec, namespace: str | None, name: str, patch: kopf.Patch, **kwargs: Any
) -> None:
    conn = await _ensure_connection(namespace, name, spec)
    patch.status.update(await _discover(conn))


@kopf.on.update(*_CRD)
async def on_connection_update(
    spec: kopf.Spec, namespace: str | None, name: str, patch: kopf.Patch, **kwargs: Any
) -> None:
    # Spec changed: drop and rebuild the connection from the new spec so seed and
    # config edits take effect without an operator restart.
    old = _connections.pop(_resource_key(namespace, name), None)
    old_seeds = old.seed_hosts if old is not None else set()
    if old is not None:
        await old.close()
    conn = await _ensure_connection(namespace, name, spec)
    logger.info("connection %s: spec updated, rebuilding", name)
    added = conn.seed_hosts - old_seeds
    removed = old_seeds - conn.seed_hosts
    if added:
        logger.info("connection %s: seed hosts added: %s", name, ", ".join(sorted(added)))
    if removed:
        logger.info("connection %s: seed hosts removed: %s", name, ", ".join(sorted(removed)))
    patch.status.update(await _discover(conn))


@kopf.timer(*_CRD, interval=_RECHECK_INTERVAL)
async def on_connection_recheck(
    spec: kopf.Spec, namespace: str | None, name: str, patch: kopf.Patch, **kwargs: Any
) -> None:
    conn = await _ensure_connection(namespace, name, spec)
    patch.status.update(_status_to_dict(await conn.recheck()))


@kopf.timer(*_CRD, interval=_TOPOLOGY_INTERVAL)
async def on_connection_topology(
    spec: kopf.Spec, namespace: str | None, name: str, patch: kopf.Patch, **kwargs: Any
) -> None:
    conn = await _ensure_connection(namespace, name, spec)
    patch.status.update(_status_to_dict(await conn.rediscover()))


# optional=True: no finalizer is added, so the resource can be deleted even when
# the operator is not running (delete cleanup is best-effort local session close).
@kopf.on.delete(*_CRD, optional=True)
async def on_connection_delete(namespace: str | None, name: str, **kwargs: Any) -> None:
    conn = _connections.pop(_resource_key(namespace, name), None)
    if conn is not None:
        await conn.close()


# Bound per-connection close on shutdown: if a node's socket is stuck (e.g. every
# node blocked, so closes/pings hang on the connect timeout), we still exit promptly
# instead of waiting on each one. Unclosed sockets are reclaimed by the OS on exit.
_CLEANUP_TIMEOUT = 3.0


async def _close_bounded(conn: ClickHouseConnection) -> None:
    with contextlib.suppress(Exception, TimeoutError):
        await asyncio.wait_for(conn.close(), timeout=_CLEANUP_TIMEOUT)


@kopf.on.cleanup()
async def on_operator_cleanup(**kwargs: Any) -> None:
    """Close every open connection when the operator shuts down (no aiohttp leaks)."""
    conns = list(_connections.values())
    _connections.clear()
    if conns:
        await asyncio.gather(*(_close_bounded(c) for c in conns))
