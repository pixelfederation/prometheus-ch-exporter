"""The facade the collector and the kopf daemon use."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable

from .errors import NodeConnectionError, NodeQueryError, NoLiveNodesError
from .node import Node
from .topology import ClusterTopology
from .types import (
    ClientFactory,
    ConnectionSpec,
    ConnectionStatus,
    NodeStatus,
    Row,
    SystemQueryResult,
    default_client_factory,
)

logger = logging.getLogger(__name__)


class ClickHouseConnection:
    def __init__(
        self,
        spec: ConnectionSpec,
        client_factory: ClientFactory = default_client_factory,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._spec = spec
        self._topology = ClusterTopology(spec, client_factory, rng=rng, clock=clock)
        self._discovery_lock = asyncio.Lock()
        self._ready = False

    @property
    def seed_hosts(self) -> set[str]:
        return self._topology.seed_hosts

    def member_hosts(self) -> list[str]:
        """All known cluster-member hosts (used to report 'all nodes down' when a
        system query can't run at all). Falls back to every tracked host if
        discovery has not confirmed membership yet."""
        nodes = self._topology.all_nodes()
        members = [n.host for n in nodes if n.in_cluster]
        return sorted(members or [n.host for n in nodes])

    @property
    def ready(self) -> bool:
        """True once the first discovery cycle has completed (regardless of
        outcome). Until then, queries should wait rather than fail — this avoids
        a spurious NoLiveNodes right after an operator/pod restart."""
        return self._ready

    # ---- collector-facing ----

    async def execute_data_query(self, sql: str, timeout: float) -> list[Row]:
        tried: set[str] = set()
        attempts = 0
        last_error: NodeConnectionError | None = None
        while True:
            node = self._topology.pick_one_live(exclude=tried)
            if node is None:
                if last_error is not None:
                    raise last_error
                raise NoLiveNodesError(f"no live nodes for cluster {self._spec.cluster_name}")
            try:
                logger.info(
                    "data query on cluster %s -> node %s%s",
                    self._spec.cluster_name,
                    node.host,
                    f" (attempt {attempts + 1})" if attempts else "",
                )
                return await node.query(sql, timeout)
            except NodeConnectionError as exc:
                last_error = exc
                tried.add(node.host)
                attempts += 1
                logger.warning("data query node %s failed, failing over: %s", node.host, exc)
                if self._spec.max_failovers is not None and attempts > self._spec.max_failovers:
                    raise
                # loop: pick another live node not in `tried`
            except NodeQueryError:
                raise  # deterministic — failover would not help

    async def execute_system_query(self, sql: str, timeout: float) -> SystemQueryResult:
        nodes = self._topology.all_live()
        if not nodes:
            raise NoLiveNodesError(f"no live nodes for cluster {self._spec.cluster_name}")

        results = await asyncio.gather(*(self._run_on_node(node, sql, timeout) for node in nodes))

        rows: list[Row] = []
        failed: list[str] = []
        for node, node_rows in zip(nodes, results, strict=True):
            if node_rows is None:
                failed.append(node.host)
                continue
            for row in node_rows:
                if "node" in row:
                    raise NodeQueryError(f"query result uses reserved column 'node' on {node.host}")
                row["node"] = node.host
                rows.append(row)

        # A system query should reflect the WHOLE cluster: include members that
        # are currently down (not even in all_live(), so never attempted above).
        # Before discovery has run (e.g. operator restarted while every node was
        # down, so system.clusters could not be read), in_cluster is unset — fall
        # back to treating every configured seed as an expected member. Otherwise
        # a partial recovery would look fully Healthy while nodes are still down.
        discovered = self._topology.last_discovery is not None
        down_members = [
            n.host
            for n in self._topology.all_nodes()
            if not n.alive and (n.in_cluster or not discovered)
        ]
        failed_nodes = sorted(set(failed) | set(down_members))
        return SystemQueryResult(rows=rows, failed_nodes=failed_nodes)

    async def _run_on_node(self, node: Node, sql: str, timeout: float) -> list[Row] | None:
        """Return rows, or None if the node is unreachable.

        Fail fast on a connection error: it means the node is down, so retrying
        it in-band just burns another connect timeout and makes the whole fan-out
        (and thus the status update) lag. node.query() already marked it down, so
        it is excluded from the next run. NodeQueryError propagates (fail-fast for
        the whole system query — a bad query fails the same everywhere)."""
        try:
            return await node.query(sql, timeout)
        except NodeConnectionError:
            return None

    # ---- daemon-facing ----

    async def refresh_topology(self) -> None:
        await self._topology.refresh_topology()

    async def recheck_dead(self) -> None:
        await self._topology.recheck_dead()

    async def health_tick(self) -> None:
        await self._topology.health_tick()

    async def refresh(self) -> ConnectionStatus:
        """One serialized discovery pass (liveness + topology) → status snapshot.

        The lock stops the create/resume handler and the timer from running
        discovery concurrently on the same connection, which would race on
        per-node client creation and double the work.
        """
        async with self._discovery_lock:
            await self._topology.health_tick()
            await self._topology.refresh_topology()
            self._ready = True
            return self.status_snapshot()

    async def recheck(self) -> ConnectionStatus:
        """Liveness-only pass (no topology discovery), serialized like refresh()."""
        async with self._discovery_lock:
            await self._topology.health_tick()
            self._ready = True
            return self.status_snapshot()

    async def rediscover(self) -> ConnectionStatus:
        """Topology discovery pass (no separate liveness sweep), serialized."""
        async with self._discovery_lock:
            await self._topology.refresh_topology()
            self._ready = True
            return self.status_snapshot()

    def status_snapshot(self) -> ConnectionStatus:
        nodes = self._topology.all_nodes()
        total = len(nodes)
        alive = sum(1 for n in nodes if n.alive)
        if alive == 0:
            phase = "Down"
        elif alive == total:
            phase = "Healthy"
        else:
            phase = "Degraded"
        return ConnectionStatus(
            phase=phase,
            total_nodes=total,
            alive_nodes=alive,
            last_discovery=self._topology.last_discovery,
            nodes=[
                NodeStatus(
                    host=n.host,
                    alive=n.alive,
                    last_checked=n.last_checked,
                    last_error=n.last_error,
                    in_cluster=n.in_cluster,
                )
                for n in nodes
            ],
        )

    async def close(self) -> None:
        # Serialize with discovery so we never close nodes while a refresh is
        # mid-flight creating clients (which would leak sessions on shutdown).
        async with self._discovery_lock:
            await self._topology.close()
