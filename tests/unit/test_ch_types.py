from datetime import datetime

from promch.clickhouse.errors import (
    NodeConnectionError,
    NodeQueryError,
    NoLiveNodesError,
)
from promch.clickhouse.types import (
    ConnectionSpec,
    ConnectionStatus,
    NodeStatus,
    SystemQueryResult,
)


def test_errors_are_distinct_exceptions() -> None:
    assert issubclass(NodeConnectionError, Exception)
    assert issubclass(NodeQueryError, Exception)
    assert issubclass(NoLiveNodesError, Exception)
    assert not issubclass(NodeConnectionError, NodeQueryError)


def test_connection_spec_defaults() -> None:
    spec = ConnectionSpec(seed_hosts=["h1"], cluster_name="c")
    assert spec.port == 8123
    assert spec.max_failovers is None
    assert spec.system_query_retries == 1
    assert spec.recheck_interval == 60.0
    assert spec.topology_interval == 1800.0
    assert spec.idle_ttl == 300.0


def test_dataclasses_hold_data() -> None:
    now = datetime(2026, 1, 1)
    ns = NodeStatus(host="h1", alive=True, last_checked=now, last_error=None)
    cs = ConnectionStatus(
        phase="Healthy", total_nodes=1, alive_nodes=1, last_discovery=now, nodes=[ns]
    )
    sqr = SystemQueryResult(rows=[{"value": 1, "node": "h1"}], failed_nodes=["h2"])
    assert cs.nodes[0].host == "h1"
    assert sqr.failed_nodes == ["h2"]
    assert sqr.rows[0]["node"] == "h1"
