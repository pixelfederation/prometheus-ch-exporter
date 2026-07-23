import random
from collections.abc import Mapping, Sequence

import pytest
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError

from promch.clickhouse.errors import NodeQueryError, NoLiveNodesError
from promch.clickhouse.types import ConnectionSpec


class FakeQueryResult:
    def __init__(self, column_names: Sequence[str], result_rows: Sequence[Sequence[object]]):
        self.column_names = column_names
        self.result_rows = result_rows


class FakeClient:
    def __init__(self, host, on_query):
        self.host = host
        self._on_query = on_query

    async def query(self, query: str, settings: Mapping[str, object] | None = None):
        return self._on_query(self.host)

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass


def make_connection(on_query, hosts, max_failovers=None, system_query_retries=1):
    from promch.clickhouse.connection import ClickHouseConnection

    async def factory(host: str, spec: ConnectionSpec):
        return FakeClient(host, on_query)

    spec = ConnectionSpec(
        seed_hosts=list(hosts),
        cluster_name="c",
        max_failovers=max_failovers,
        system_query_retries=system_query_retries,
    )
    return ClickHouseConnection(spec, client_factory=factory, rng=random.Random(0))


async def _bring_up(conn):
    await conn.recheck_dead()


async def test_data_query_success() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(7,)]), ["h1"])
    await _bring_up(conn)
    rows = await conn.execute_data_query("SELECT 7 AS value", timeout=5)
    assert rows == [{"value": 7}]


async def test_data_query_no_live_nodes_raises() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1"])
    # never brought up
    with pytest.raises(NoLiveNodesError):
        await conn.execute_data_query("SELECT 1 AS value", timeout=5)


async def test_data_query_fails_over_then_succeeds() -> None:
    calls = {"n": 0}

    def on_query(host):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("first node down")
        return FakeQueryResult(["value"], [(1,)])

    conn = make_connection(on_query, ["h1", "h2"], max_failovers=None)
    await _bring_up(conn)
    rows = await conn.execute_data_query("SELECT 1 AS value", timeout=5)
    assert rows == [{"value": 1}]
    assert calls["n"] == 2


async def test_data_query_exhausts_failovers_and_raises() -> None:
    def on_query(host):
        raise OperationalError("all down")

    conn = make_connection(on_query, ["h1", "h2"], max_failovers=None)
    await _bring_up(conn)
    from promch.clickhouse.errors import NodeConnectionError

    with pytest.raises(NodeConnectionError):
        await conn.execute_data_query("SELECT 1 AS value", timeout=5)


async def test_data_query_sql_error_is_not_retried() -> None:
    calls = {"n": 0}

    def on_query(host):
        calls["n"] += 1
        raise ProgrammingError("bad sql")

    conn = make_connection(on_query, ["h1", "h2"])
    await _bring_up(conn)
    with pytest.raises(NodeQueryError):
        await conn.execute_data_query("SELECT bad", timeout=5)
    assert calls["n"] == 1  # no failover on SQL error


async def test_system_query_stamps_node_and_returns_all() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1", "h2"])
    await _bring_up(conn)
    result = await conn.execute_system_query("SELECT 1 AS value", timeout=5)
    by_node = {r["node"]: r["value"] for r in result.rows}
    assert by_node == {"h1": 1, "h2": 1}
    assert result.failed_nodes == []


async def test_system_query_partial_on_node_failure() -> None:
    def on_query(host):
        if host == "h2":
            raise OperationalError("h2 down")
        return FakeQueryResult(["value"], [(1,)])

    conn = make_connection(on_query, ["h1", "h2"], system_query_retries=1)
    await _bring_up(conn)
    result = await conn.execute_system_query("SELECT 1 AS value", timeout=5)
    assert [r["node"] for r in result.rows] == ["h1"]
    assert result.failed_nodes == ["h2"]


async def test_system_query_reports_down_member_as_failed() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1", "h2"])
    await _bring_up(conn)
    # h2 is a cluster member that is currently down → not in all_live(), so never
    # queried, but it must still surface as failed (query reflects the whole cluster).
    for n in conn._topology.all_nodes():
        if n.host == "h2":
            n.alive = False
            n.in_cluster = True
    result = await conn.execute_system_query("SELECT 1 AS value", timeout=5)
    assert [r["node"] for r in result.rows] == ["h1"]
    assert result.failed_nodes == ["h2"]


async def test_system_query_counts_down_seeds_before_discovery() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1", "h2"])
    await _bring_up(conn)  # both up; discovery has not run (last_discovery is None)
    for n in conn._topology.all_nodes():
        if n.host == "h2":
            n.alive = False  # a down seed, in_cluster still False
    result = await conn.execute_system_query("SELECT 1 AS value", timeout=5)
    assert [r["node"] for r in result.rows] == ["h1"]
    # down seed is reported even though discovery never confirmed membership
    assert result.failed_nodes == ["h2"]


async def test_member_hosts_lists_cluster_members() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1", "h2"])
    await _bring_up(conn)
    for n in conn._topology.all_nodes():
        n.in_cluster = True
    assert conn.member_hosts() == ["h1", "h2"]


async def test_system_query_sql_error_fails_fast() -> None:
    def on_query(host):
        raise ProgrammingError("bad sql")

    conn = make_connection(on_query, ["h1", "h2"])
    await _bring_up(conn)
    with pytest.raises(NodeQueryError):
        await conn.execute_system_query("SELECT bad", timeout=5)


async def test_system_query_reserved_node_column_errors() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value", "node"], [(1, "x")]), ["h1"])
    await _bring_up(conn)
    with pytest.raises(NodeQueryError, match="reserved column 'node'"):
        await conn.execute_system_query("SELECT 1 AS value, 'x' AS node", timeout=5)


async def test_status_snapshot_phases() -> None:
    conn = make_connection(lambda host: FakeQueryResult(["value"], [(1,)]), ["h1", "h2"])
    # nothing up yet -> Down
    down = conn.status_snapshot()
    assert down.phase == "Down"
    assert down.total_nodes == 2
    assert down.alive_nodes == 0
    await _bring_up(conn)  # both up -> Healthy
    healthy = conn.status_snapshot()
    assert healthy.phase == "Healthy"
    assert healthy.alive_nodes == 2
    assert {n.host for n in healthy.nodes} == {"h1", "h2"}
