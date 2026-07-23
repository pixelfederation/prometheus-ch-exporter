"""The live pool of cluster nodes.

Membership is defined by `system.clusters` (authoritative). Liveness is defined
by ping. A node that is unreachable but still a cluster member stays in the pool
marked dead (maintenance / upgrade / transient outage).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime

from .errors import NodeConnectionError, NodeQueryError
from .node import Node
from .types import ClientFactory, ConnectionSpec

logger = logging.getLogger(__name__)

_DISCOVERY_SQL = "SELECT cluster, host_name FROM system.clusters"


class ClusterTopology:
    def __init__(
        self,
        spec: ConnectionSpec,
        client_factory: ClientFactory,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cluster_name = spec.cluster_name
        self.last_discovery: datetime | None = None
        self._spec = spec
        self._factory = client_factory
        self._rng = rng if rng is not None else random.Random()
        self._clock = clock
        # Deduplicate seed hosts (a repeated host would otherwise build a node
        # object twice). Silently harmless before, but worth surfacing.
        self._nodes: dict[str, Node] = {}
        duplicates: list[str] = []
        for host in spec.seed_hosts:
            if host in self._nodes:
                duplicates.append(host)
                continue
            self._nodes[host] = self._make_node(host)
        if duplicates:
            logger.warning(
                "cluster %s: duplicate seed hosts ignored: %s",
                spec.cluster_name,
                ", ".join(duplicates),
            )
        # Declared seeds are never dropped by discovery — a seed that is not a
        # cluster member is kept and flagged (config drift), not removed.
        self._seed_hosts: set[str] = set(self._nodes)

    def _make_node(self, host: str) -> Node:
        return Node(host, self._spec, self._factory, clock=self._clock)

    def all_nodes(self) -> list[Node]:
        return list(self._nodes.values())

    @property
    def seed_hosts(self) -> set[str]:
        return set(self._seed_hosts)

    def all_live(self) -> list[Node]:
        return [n for n in self._nodes.values() if n.alive]

    def pick_one_live(self, exclude: set[str] | None = None) -> Node | None:
        exclude = exclude or set()
        candidates = [n for n in self._nodes.values() if n.alive and n.host not in exclude]
        if not candidates:
            return None
        return self._rng.choice(candidates)

    async def recheck_dead(self) -> None:
        nodes = list(self._nodes.values())
        dead = [n for n in nodes if not n.alive]
        # Ping dead nodes in parallel so several blocked/recovering nodes don't
        # each add a serial connect-timeout to the pass.
        await asyncio.gather(*(n.ping() for n in dead))
        for node in dead:
            if node.alive:
                logger.info("node %s back up", node.host)
        for node in nodes:
            if node.alive and node.is_idle(self._spec.idle_ttl):
                await node.close()

    async def check_all(self) -> None:
        """Active liveness: ping every node each tick and record transitions."""
        nodes = list(self._nodes.values())
        prev = [(n, n.alive, n.last_checked is None) for n in nodes]
        # Ping all nodes in parallel: N blocked nodes cost one connect-timeout,
        # not N of them in series — so they are marked down quickly and excluded
        # from queries, instead of slowing every fan-out until they time out.
        await asyncio.gather(*(n.ping() for n in nodes))
        for node, was_alive, first_check in prev:
            if node.alive and not was_alive:
                logger.info("node %s back up", node.host)
            elif not node.alive and (was_alive or first_check):
                # Log a fresh failure once — on a transition, or a never-seen node
                # that fails its first check (e.g. a bad seed IP). A persistently
                # dead node stays quiet on later ticks.
                logger.warning("node %s is unreachable: %s", node.host, node.last_error)

    async def health_tick(self) -> None:
        """One liveness pass, per the connection's configured mode."""
        if self._spec.liveness_mode == "passive":
            await self.recheck_dead()
        else:
            await self.check_all()

    async def refresh_topology(self, timeout: float = 10.0) -> None:
        seed = self.pick_one_live()
        if seed is None:
            logger.warning(
                "cannot discover topology for cluster %s: no live nodes", self.cluster_name
            )
            return
        try:
            rows = await seed.query(_DISCOVERY_SQL, timeout=timeout)
        except (NodeConnectionError, NodeQueryError) as exc:
            logger.warning("topology discovery query failed on %s: %s", seed.host, exc)
            return

        members = {str(r["host_name"]) for r in rows if r.get("cluster") == self.cluster_name}

        # New cluster members we didn't know about.
        for host in members - self._nodes.keys():
            logger.warning("node %s identified in cluster but not in CRD", host)
            node = self._make_node(host)
            node.in_cluster = True
            self._nodes[host] = node
            await node.ping()  # bring a healthy new member into rotation this cycle

        # Reconcile the nodes we already track.
        for host in list(self._nodes.keys()):
            node = self._nodes[host]
            if host in members:
                node.in_cluster = True
            elif host in self._seed_hosts:
                # A declared seed that is not a cluster member — keep it visible
                # as config drift (wrong IP / migration remnant / mid-migration).
                if node.in_cluster:
                    logger.warning(
                        "seed host %s is no longer a member of cluster %s",
                        host,
                        self.cluster_name,
                    )
                node.in_cluster = False
            else:
                # A previously-discovered member that left the cluster topology.
                logger.info("node %s removed from cluster %s", host, self.cluster_name)
                await node.close()
                del self._nodes[host]

        self.last_discovery = datetime.now(UTC)

    async def close(self) -> None:
        for node in list(self._nodes.values()):
            await node.close()
