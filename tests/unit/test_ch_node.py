from collections.abc import Mapping, Sequence

import pytest
from clickhouse_connect.driver.exceptions import (
    OperationalError,
    ProgrammingError,
)

from promch.clickhouse.errors import NodeConnectionError, NodeQueryError
from promch.clickhouse.node import Node
from promch.clickhouse.types import ConnectionSpec


class FakeQueryResult:
    def __init__(self, column_names: Sequence[str], result_rows: Sequence[Sequence[object]]):
        self.column_names = column_names
        self.result_rows = result_rows


class FakeClient:
    """A canned client. `behavior` is a callable invoked per query()."""

    def __init__(self, behavior):
        self.behavior = behavior
        self.closed = False
        self.ping_ok = True

    async def query(self, query: str, settings: Mapping[str, object] | None = None):
        return self.behavior(query, settings)

    async def ping(self) -> bool:
        if not self.ping_ok:
            raise OperationalError("down")
        return True

    async def close(self) -> None:
        self.closed = True


def _spec() -> ConnectionSpec:
    return ConnectionSpec(seed_hosts=["h1"], cluster_name="c")


def _factory_returning(client: FakeClient):
    async def factory(host: str, spec: ConnectionSpec):
        return client

    return factory


async def test_node_starts_dead() -> None:
    node = Node("h1", _spec(), _factory_returning(FakeClient(lambda q, s: None)))
    assert node.host == "h1"
    assert node.alive is False
    assert node.last_error is None


async def test_query_returns_row_dicts() -> None:
    client = FakeClient(lambda q, s: FakeQueryResult(["value", "app"], [(1, "a"), (2, "b")]))
    node = Node("h1", _spec(), _factory_returning(client))
    rows = await node.query("SELECT 1", timeout=5)
    assert rows == [{"value": 1, "app": "a"}, {"value": 2, "app": "b"}]


async def test_query_operational_error_marks_down_and_raises_connection() -> None:
    def boom(q, s):
        raise OperationalError("disconnected")

    node = Node("h1", _spec(), _factory_returning(FakeClient(boom)))
    with pytest.raises(NodeConnectionError):
        await node.query("SELECT 1", timeout=5)
    assert node.alive is False
    assert node.last_error is not None


async def test_query_transport_error_raises_connection() -> None:
    def boom(q, s):
        raise OSError("connection refused")

    node = Node("h1", _spec(), _factory_returning(FakeClient(boom)))
    with pytest.raises(NodeConnectionError):
        await node.query("SELECT 1", timeout=5)
    assert node.alive is False


async def test_query_sql_error_raises_query_error_without_marking_down() -> None:
    def boom(q, s):
        raise ProgrammingError("unknown table")

    node = Node("h1", _spec(), _factory_returning(FakeClient(boom)))
    node.alive = True  # simulate a node believed alive
    with pytest.raises(NodeQueryError):
        await node.query("SELECT 1", timeout=5)
    assert node.alive is True  # SQL error does not change liveness


async def test_ping_success_sets_alive_and_last_checked() -> None:
    node = Node("h1", _spec(), _factory_returning(FakeClient(lambda q, s: None)))
    ok = await node.ping()
    assert ok is True
    assert node.alive is True
    assert node.last_checked is not None


async def test_ping_failure_sets_dead() -> None:
    client = FakeClient(lambda q, s: None)
    client.ping_ok = False
    node = Node("h1", _spec(), _factory_returning(client))
    node.alive = True
    ok = await node.ping()
    assert ok is False
    assert node.alive is False
    assert node.last_error is not None


async def test_client_is_created_once_and_reused() -> None:
    created = 0
    client = FakeClient(lambda q, s: FakeQueryResult([], []))

    async def factory(host: str, spec: ConnectionSpec):
        nonlocal created
        created += 1
        return client

    node = Node("h1", _spec(), factory)
    await node.query("SELECT 1", timeout=5)
    await node.query("SELECT 1", timeout=5)
    assert created == 1


async def test_is_idle_uses_injected_clock() -> None:
    t = [100.0]
    client = FakeClient(lambda q, s: FakeQueryResult([], []))
    node = Node("h1", _spec(), _factory_returning(client), clock=lambda: t[0])
    await node.query("SELECT 1", timeout=5)  # marks used at t=100
    t[0] = 150.0
    assert node.is_idle(ttl=30.0) is True
    t[0] = 120.0
    assert node.is_idle(ttl=30.0) is False


async def test_close_closes_client() -> None:
    client = FakeClient(lambda q, s: FakeQueryResult([], []))
    node = Node("h1", _spec(), _factory_returning(client))
    await node.query("SELECT 1", timeout=5)
    await node.close()
    assert client.closed is True


async def test_ensure_client_is_concurrency_safe() -> None:
    import asyncio

    created = 0

    async def factory(host: str, spec: ConnectionSpec):
        nonlocal created
        await asyncio.sleep(0)  # force the two callers to interleave
        created += 1
        return FakeClient(lambda q, s: FakeQueryResult([], []))

    node = Node("h1", _spec(), factory)
    await asyncio.gather(node.ping(), node.ping())  # concurrent connect attempts
    assert created == 1  # one client despite concurrent _ensure_client


async def test_query_success_updates_last_checked() -> None:
    client = FakeClient(lambda q, s: FakeQueryResult(["value"], [(1,)]))
    node = Node("h1", _spec(), _factory_returning(client))
    assert node.last_checked is None
    await node.query("SELECT 1", timeout=5)
    assert node.alive is True  # a successful query confirms the node is alive
    assert node.last_checked is not None
