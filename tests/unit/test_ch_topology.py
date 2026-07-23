import random
from collections.abc import Mapping, Sequence

from promch.clickhouse.types import ConnectionSpec


class FakeQueryResult:
    def __init__(self, column_names: Sequence[str], result_rows: Sequence[Sequence[object]]):
        self.column_names = column_names
        self.result_rows = result_rows


class FakeClient:
    def __init__(self, host):
        self.host = host
        self.ping_ok = True
        self.clusters_rows = []  # list of (cluster, host_name)
        self.closed = False

    async def query(self, query: str, settings: Mapping[str, object] | None = None):
        return FakeQueryResult(["cluster", "host_name"], self.clusters_rows)

    async def ping(self) -> bool:
        if not self.ping_ok:
            from clickhouse_connect.driver.exceptions import OperationalError

            raise OperationalError("down")
        return True

    async def close(self) -> None:
        self.closed = True


def make_topology(clients: dict):
    """clients: host -> FakeClient. Factory hands out the matching one."""
    from promch.clickhouse.topology import ClusterTopology

    async def factory(host: str, spec: ConnectionSpec):
        return clients[host]

    spec = ConnectionSpec(seed_hosts=list(clients), cluster_name="c")
    return ClusterTopology(spec, factory, rng=random.Random(0))


async def test_seeds_start_dead_until_pinged() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    topo = make_topology(clients)
    assert topo.all_live() == []
    assert topo.pick_one_live() is None


async def test_recheck_dead_revives_responders() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    clients["h2"].ping_ok = False
    topo = make_topology(clients)
    await topo.recheck_dead()
    live = {n.host for n in topo.all_live()}
    assert live == {"h1"}


async def test_pick_one_live_only_returns_live_and_respects_exclude() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    topo = make_topology(clients)
    await topo.recheck_dead()  # both up
    node = topo.pick_one_live(exclude={"h1"})
    assert node is not None and node.host == "h2"


async def test_all_live_returns_snapshot_copy() -> None:
    clients = {"h1": FakeClient("h1")}
    topo = make_topology(clients)
    await topo.recheck_dead()
    a = topo.all_live()
    a.clear()
    assert len(topo.all_live()) == 1  # mutating the returned list must not affect internal state


async def test_refresh_adds_new_member_and_pings_it() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2"), "h3": FakeClient("h3")}
    from promch.clickhouse.topology import ClusterTopology

    async def factory(host: str, spec: ConnectionSpec):
        return clients[host]

    spec = ConnectionSpec(seed_hosts=["h1", "h2"], cluster_name="c")
    topo = ClusterTopology(spec, factory, rng=random.Random(0))
    # system.clusters returns the same view from any node discovery may hit.
    members = [("c", "h1"), ("c", "h2"), ("c", "h3")]
    clients["h1"].clusters_rows = members
    clients["h2"].clusters_rows = members
    await topo.recheck_dead()  # bring seeds up so discovery has a live node
    await topo.refresh_topology()
    hosts = {n.host for n in topo.all_nodes()}
    assert hosts == {"h1", "h2", "h3"}
    h3 = next(n for n in topo.all_nodes() if n.host == "h3")
    assert h3.alive is True  # pinged immediately during discovery


async def test_refresh_removes_vanished_non_seed_member() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    from promch.clickhouse.topology import ClusterTopology

    async def factory(host: str, spec: ConnectionSpec):
        return clients[host]

    # only h1 is a seed; h2 is discovered from the cluster
    spec = ConnectionSpec(seed_hosts=["h1"], cluster_name="c")
    topo = ClusterTopology(spec, factory, rng=random.Random(0))
    for c in clients.values():
        c.clusters_rows = [("c", "h1"), ("c", "h2")]
    await topo.recheck_dead()
    await topo.refresh_topology()  # discovers h2 as a member
    assert {n.host for n in topo.all_nodes()} == {"h1", "h2"}

    # h2 leaves the cluster topology
    for c in clients.values():
        c.clusters_rows = [("c", "h1")]
    await topo.refresh_topology()
    assert {n.host for n in topo.all_nodes()} == {"h1"}  # non-seed member removed
    assert clients["h2"].closed is True


async def test_seed_not_in_cluster_is_kept_and_flagged() -> None:
    clients = {"h1": FakeClient("h1"), "bogus": FakeClient("bogus")}
    from promch.clickhouse.topology import ClusterTopology

    async def factory(host: str, spec: ConnectionSpec):
        return clients[host]

    # both are seeds, but "bogus" is not a member of the cluster
    spec = ConnectionSpec(seed_hosts=["h1", "bogus"], cluster_name="c")
    topo = ClusterTopology(spec, factory, rng=random.Random(0))
    for c in clients.values():
        c.clusters_rows = [("c", "h1")]
    await topo.recheck_dead()
    await topo.refresh_topology()
    # the orphan seed is KEPT (visible as config drift), not dropped
    assert {n.host for n in topo.all_nodes()} == {"h1", "bogus"}
    by_host = {n.host: n for n in topo.all_nodes()}
    assert by_host["h1"].in_cluster is True
    assert by_host["bogus"].in_cluster is False


async def test_refresh_keeps_down_but_member_node() -> None:
    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    clients["h2"].ping_ok = False  # h2 is down but still a cluster member
    topo = make_topology(clients)
    clients["h1"].clusters_rows = [("c", "h1"), ("c", "h2")]
    await topo.recheck_dead()
    await topo.refresh_topology()
    hosts = {n.host for n in topo.all_nodes()}
    assert hosts == {"h1", "h2"}  # down member stays listed
    h2 = next(n for n in topo.all_nodes() if n.host == "h2")
    assert h2.alive is False


async def test_refresh_skips_when_no_live_node() -> None:
    clients = {"h1": FakeClient("h1")}
    clients["h1"].ping_ok = False
    topo = make_topology(clients)
    await topo.refresh_topology()  # must not raise
    assert topo.last_discovery is None


async def test_duplicate_seed_hosts_deduped_with_warning(caplog) -> None:
    import logging

    from promch.clickhouse.topology import ClusterTopology

    async def factory(host: str, spec: ConnectionSpec):
        return FakeClient(host)

    spec = ConnectionSpec(seed_hosts=["h1", "h1", "h2"], cluster_name="c")
    with caplog.at_level(logging.WARNING):
        topo = ClusterTopology(spec, factory, rng=random.Random(0))
    assert {n.host for n in topo.all_nodes()} == {"h1", "h2"}
    assert len(topo.all_nodes()) == 2  # the duplicate is dropped, not doubled
    assert any("duplicate seed hosts" in r.message for r in caplog.records)


async def test_check_all_pings_every_node_and_logs_transitions(caplog) -> None:
    import logging

    clients = {"h1": FakeClient("h1"), "h2": FakeClient("h2")}
    topo = make_topology(clients)
    await topo.check_all()  # both start dead -> both come up
    assert {n.host for n in topo.all_live()} == {"h1", "h2"}

    clients["h2"].ping_ok = False  # h2 silently fails
    with caplog.at_level(logging.WARNING):
        await topo.check_all()  # active check pings h2 -> marks it down
    assert {n.host for n in topo.all_live()} == {"h1"}
    assert any("is unreachable" in r.message for r in caplog.records)


async def test_health_tick_active_repings_live_but_passive_does_not() -> None:
    from promch.clickhouse.topology import ClusterTopology

    # active (default): a live node that starts failing is caught on the next tick
    clients = {"h1": FakeClient("h1")}
    topo = make_topology(clients)
    await topo.check_all()  # h1 up
    clients["h1"].ping_ok = False
    await topo.health_tick()  # active -> re-pings h1 -> down
    assert topo.all_live() == []

    # passive: live nodes are not re-pinged, so a silent failure is not noticed here
    clients2 = {"h2": FakeClient("h2")}

    async def factory(host: str, spec: ConnectionSpec):
        return clients2[host]

    spec = ConnectionSpec(seed_hosts=["h2"], cluster_name="c", liveness_mode="passive")
    topo2 = ClusterTopology(spec, factory, rng=random.Random(0))
    await topo2.check_all()  # bring h2 up
    clients2["h2"].ping_ok = False
    await topo2.health_tick()  # passive -> does NOT re-ping the live node
    assert {n.host for n in topo2.all_live()} == {"h2"}
