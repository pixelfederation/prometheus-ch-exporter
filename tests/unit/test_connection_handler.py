from datetime import UTC, datetime

from promch.clickhouse.types import ConnectionStatus, NodeStatus
from promch.handlers import connection as conn_mod
from promch.handlers.connection import (
    _ensure_connection,
    _spec_to_connection_spec,
    _status_to_dict,
    get_connection,
)


def test_spec_to_connection_spec_essentials_and_defaults() -> None:
    spec = {"seedHosts": ["1.2.3.4"], "clusterName": "cluster"}
    cs = _spec_to_connection_spec(spec)
    assert cs.seed_hosts == ["1.2.3.4"]
    assert cs.cluster_name == "cluster"
    assert cs.port == 8123
    assert cs.username == "default"
    assert cs.password == ""
    assert cs.max_failovers is None
    assert cs.system_query_retries == 1
    assert cs.liveness_mode == "active"


def test_spec_to_connection_spec_overrides() -> None:
    spec = {
        "seedHosts": ["a", "b"],
        "clusterName": "c",
        "port": 9000,
        "username": "reader",
        "maxFailovers": 3,
        "systemQueryRetries": 2,
        "livenessMode": "passive",
    }
    cs = _spec_to_connection_spec(spec)
    assert cs.port == 9000
    assert cs.username == "reader"
    assert cs.max_failovers == 3
    assert cs.system_query_retries == 2
    assert cs.liveness_mode == "passive"


def test_status_to_dict_omits_none_optional_fields() -> None:
    now = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    status = ConnectionStatus(
        phase="Degraded",
        total_nodes=2,
        alive_nodes=1,
        last_discovery=now,
        nodes=[
            NodeStatus(host="h1", alive=True, last_checked=now, last_error=None, in_cluster=True),
            NodeStatus(
                host="h2", alive=False, last_checked=None, last_error="boom", in_cluster=False
            ),
        ],
    )
    out = _status_to_dict(status)
    assert out["phase"] == "Degraded"
    assert out["totalNodes"] == 2
    assert out["aliveNodes"] == 1
    assert out["lastDiscovery"] == now.isoformat()
    # None optional fields are omitted, not sent as null (matches what the API stores).
    assert out["nodes"][0] == {
        "host": "h1",
        "alive": True,
        "inCluster": True,
        "lastChecked": now.isoformat(),
    }
    assert out["nodes"][1] == {
        "host": "h2",
        "alive": False,
        "inCluster": False,
        "lastError": "boom",
    }


def test_status_to_dict_omits_none_last_discovery() -> None:
    status = ConnectionStatus(
        phase="Down", total_nodes=0, alive_nodes=0, last_discovery=None, nodes=[]
    )
    out = _status_to_dict(status)
    assert "lastDiscovery" not in out
    assert out["nodes"] == []


def test_ensure_connection_is_singleton_per_key() -> None:
    conn_mod._connections.clear()
    spec = {"seedHosts": ["1.2.3.4"], "clusterName": "cluster"}
    c1 = _ensure_connection("ns", "a", spec)
    c2 = _ensure_connection("ns", "a", spec)
    c3 = _ensure_connection("ns", "b", spec)
    assert c1 is c2  # same resource -> same instance (no duplicate connections)
    assert c1 is not c3  # different resource -> different instance
    conn_mod._connections.clear()


def test_get_connection_looks_up_by_name() -> None:
    conn_mod._connections.clear()
    spec = {"seedHosts": ["1.2.3.4"], "clusterName": "cluster"}
    created = _ensure_connection(None, "shared", spec)  # cluster-scoped: namespace None
    assert get_connection("shared") is created
    assert get_connection("nope") is None
    conn_mod._connections.clear()
