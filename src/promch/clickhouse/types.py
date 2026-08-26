"""Data types and the injectable client seam for the ClickHouse client."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol, cast

logger = logging.getLogger(__name__)

# One raw result row: column name -> value. Unvalidated on purpose — the client
# is "dumb" about metrics; the collector validates the `value` contract later.
Row = dict[str, object]


@dataclass
class SystemQueryResult:
    """Fan-out result — partial by design (see execute_system_query)."""

    rows: list[Row]
    failed_nodes: list[str]


@dataclass
class NodeStatus:
    host: str
    alive: bool
    last_checked: datetime | None
    last_error: str | None
    in_cluster: bool = False


@dataclass
class ConnectionStatus:
    phase: str  # "Healthy" | "Degraded" | "Down"
    total_nodes: int
    alive_nodes: int
    last_discovery: datetime | None
    nodes: list[NodeStatus]


@dataclass
class ConnectionSpec:
    """Everything the client needs, populated (later) from a Connection CRD."""

    seed_hosts: list[str]
    cluster_name: str
    port: int = 8443
    username: str = "default"
    password: str = ""
    secure: bool = True
    verify: bool = True
    connect_timeout: float = 10.0
    max_failovers: int | None = None  # None = "try all live nodes"
    system_query_retries: int = 1
    liveness_mode: Literal["active", "passive"] = "active"
    recheck_interval: float = 60.0
    topology_interval: float = 1800.0
    idle_ttl: float = 300.0


class QueryResultProtocol(Protocol):
    """The subset of clickhouse-connect's QueryResult that Node reads."""

    column_names: Sequence[str]
    result_rows: Sequence[Sequence[object]]


class ClientProtocol(Protocol):
    """The subset of clickhouse-connect's AsyncClient that Node uses.

    Declaring only what we need keeps fakes tiny and the seam narrow.
    """

    async def query(
        self, query: str, settings: Mapping[str, object] | None = ...
    ) -> QueryResultProtocol: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


# Given a host and the spec, produce a connected async client.
ClientFactory = Callable[[str, ConnectionSpec], Awaitable[ClientProtocol]]


async def default_client_factory(host: str, spec: ConnectionSpec) -> ClientProtocol:
    """The real factory: a clickhouse-connect async client bound to one host.

    query_retries=0 hands all retry/failover control to our own logic.
    """
    import clickhouse_connect

    if spec.secure and not spec.verify:
        logger.warning(
            "TLS certificate verification is DISABLED for %s (verify=false): "
            "connection is vulnerable to MITM — use only for dev/test",
            host,
        )
    client = await clickhouse_connect.get_async_client(
        host=host,
        port=spec.port,
        username=spec.username,
        password=spec.password,
        secure=spec.secure,
        verify=spec.verify,
        connect_timeout=int(spec.connect_timeout),
        query_retries=0,
    )
    return cast(ClientProtocol, client)


# `field` is re-exported so downstream dataclasses can import from one place.
__all__ = [
    "Row",
    "SystemQueryResult",
    "NodeStatus",
    "ConnectionStatus",
    "ConnectionSpec",
    "QueryResultProtocol",
    "ClientProtocol",
    "ClientFactory",
    "default_client_factory",
    "field",
]
