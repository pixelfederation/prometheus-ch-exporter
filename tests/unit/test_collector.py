import pytest
from prometheus_client import CollectorRegistry

from promch.collector.metrics import QueryMetricsCollector, QueryResult


@pytest.fixture
def collector() -> QueryMetricsCollector:
    registry = CollectorRegistry()
    return QueryMetricsCollector(registry=registry)


def test_register_and_collect_no_value(collector: QueryMetricsCollector) -> None:
    """Before the first query runs, only system metrics are emitted (no user metric)."""
    collector.register("ns/test", "my_metric", "help text", {})
    metrics = list(collector.collect())
    names = {m.name for m in metrics}
    assert "my_metric" not in names
    # assert "ch_query_up" in names


def test_update_scalar_exposes_user_metric(collector: QueryMetricsCollector) -> None:
    """A single-row result (scalar) produces one user metric time series."""
    collector.register("ns/test", "my_metric", "help", {})
    collector.update(
        "ns/test",
        rows=[QueryResult(value=42.0, dynamic_labels={})],
        duration_seconds=0.1,
        tick_ts=0.0,
    )

    metrics = {m.name: m for m in collector.collect()}
    assert "my_metric" in metrics
    assert metrics["my_metric"].samples[0].value == 42.0
    assert metrics["clickhouse_query_up"].samples[0].value == 1.0


def test_update_multirow_exposes_multiple_series(collector: QueryMetricsCollector) -> None:
    """A multi-row result produces one time series per row, with correct dynamic labels."""
    collector.register("ns/test", "my_metric", "help", {})
    collector.update(
        "ns/test",
        rows=[
            QueryResult(value=10.0, dynamic_labels={"app_name": "app1", "level": "500"}),
            QueryResult(value=20.0, dynamic_labels={"app_name": "app2", "level": "550"}),
        ],
        duration_seconds=0.2,
        tick_ts=0.0,
    )

    metrics = {m.name: m for m in collector.collect()}
    assert "my_metric" in metrics
    # Two samples — one per row
    assert len(metrics["my_metric"].samples) == 2
    values = {s.labels["app_name"]: s.value for s in metrics["my_metric"].samples}
    assert values["app1"] == 10.0
    assert values["app2"] == 20.0


def test_update_returns_recovered_on_transition_to_healthy(
    collector: QueryMetricsCollector,
) -> None:
    """update() flags transitions INTO healthy so the caller can log them."""
    collector.register("ns/test", "my_metric", "help", {})
    row = [QueryResult(value=1.0, dynamic_labels={})]

    # First-ever success is a transition into healthy.
    assert collector.update("ns/test", row, 0.1, tick_ts=1.0) is True
    # Steady-state success — no transition.
    assert collector.update("ns/test", row, 0.1, tick_ts=2.0) is False

    # After a failure, the next success is a recovery.
    collector.mark_failure("ns/test", "QueryError", "boom")
    assert collector.update("ns/test", row, 0.1, tick_ts=3.0) is True

    # A discarded stale-tick result is not a transition.
    assert collector.update("ns/test", row, 0.1, tick_ts=0.5) is False


def test_mark_failure_keeps_stale_value(collector: QueryMetricsCollector) -> None:
    """After mark_failure(), user metric still served (stale), but ch_query_up=0."""
    collector.register("ns/test", "my_metric", "help", {})
    collector.update(
        "ns/test",
        rows=[QueryResult(value=42.0, dynamic_labels={})],
        duration_seconds=0.1,
        tick_ts=0.0,
    )
    collector.mark_failure("ns/test", "QueryError", "boom")

    metrics = {m.name: m for m in collector.collect()}
    # stale user metric is still present
    assert "my_metric" in metrics
    assert metrics["my_metric"].samples[0].value == 42.0
    # but clickhouse_query_up signals the failure
    assert metrics["clickhouse_query_up"].samples[0].value == 0.0


def test_expire_removes_user_metric(collector: QueryMetricsCollector) -> None:
    """After expire(), user metric is gone from output; ch_query_up reports 0."""
    collector.register("ns/test", "my_metric", "help", {})
    collector.update(
        "ns/test",
        rows=[QueryResult(value=42.0, dynamic_labels={})],
        duration_seconds=0.1,
        tick_ts=0.0,
    )
    collector.expire("ns/test")

    metrics = {m.name: m for m in collector.collect()}
    assert "my_metric" not in metrics
    assert metrics["clickhouse_query_up"].samples[0].value == 0.0


def test_unregister_removes_all(collector: QueryMetricsCollector) -> None:
    """After unregister(), no query metrics remain — only the leader gauge."""
    collector.register("ns/test", "my_metric", "help", {})
    collector.unregister("ns/test")
    assert {m.name for m in collector.collect()} == {"clickhouse_leader"}


def test_standby_emits_only_leader_gauge_zero() -> None:
    """A standby (peering-paused) replica must not export its cached query metrics
    — only the leader gauge (=0) — so Prometheus sees no duplicate series from the
    demoted pod, even though the cache still holds rows from when it was active."""
    registry = CollectorRegistry()
    collector = QueryMetricsCollector(registry=registry, is_active=lambda: False)
    collector.register("ns/test", "my_metric", "help", {})
    collector.update("ns/test", [QueryResult(value=42.0, dynamic_labels={})], 0.1, tick_ts=0.0)

    metrics = {m.name: m for m in collector.collect()}
    assert set(metrics) == {"clickhouse_leader"}
    assert metrics["clickhouse_leader"].samples[0].value == 0.0


def test_active_emits_leader_gauge_one_plus_metrics() -> None:
    """An active replica exports the leader gauge (=1) alongside its query metrics."""
    registry = CollectorRegistry()
    collector = QueryMetricsCollector(registry=registry, is_active=lambda: True)
    collector.register("ns/test", "my_metric", "help", {})
    collector.update("ns/test", [QueryResult(value=42.0, dynamic_labels={})], 0.1, tick_ts=0.0)

    metrics = {m.name: m for m in collector.collect()}
    assert metrics["clickhouse_leader"].samples[0].value == 1.0
    assert metrics["my_metric"].samples[0].value == 42.0
    assert metrics["clickhouse_query_up"].samples[0].value == 1.0


def test_inflight_and_skip_counters(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    assert collector.inflight("ns/q") == 0
    collector.inc_inflight("ns/q")
    collector.inc_inflight("ns/q")
    assert collector.inflight("ns/q") == 2
    collector.dec_inflight("ns/q")
    assert collector.inflight("ns/q") == 1
    collector.record_skip("ns/q")
    assert collector.snapshot("ns/q")["skipped_total"] == 1


def test_mark_failure_then_success_resets(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    collector.mark_failure("ns/q", "QueryError", "boom")
    snap = collector.snapshot("ns/q")
    assert snap["consecutive_failures"] == 1
    assert snap["last_error"]["type"] == "QueryError"
    assert snap["up"] is False
    collector.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=1.0)
    snap = collector.snapshot("ns/q")
    assert snap["consecutive_failures"] == 0
    assert snap["up"] is True
    # last_error is retained after recovery (aged out later by derive_status)
    assert snap["last_error"] is not None


def test_mark_failure_can_set_failed_nodes(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    # all-down system query reports every member as failed
    collector.mark_failure("ns/q", "NoLiveNodes", "no live nodes", ["h1", "h2", "h3"])
    assert collector.snapshot("ns/q")["failed_nodes"] == ["h1", "h2", "h3"]


def test_snapshot_absent_key(collector: QueryMetricsCollector) -> None:
    assert collector.snapshot("missing") is None


def test_collect_emits_inflight_and_skipped(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    collector.inc_inflight("ns/q")
    collector.record_skip("ns/q")
    names = {s.name for fam in collector.collect() for s in fam.samples}
    assert "clickhouse_query_inflight" in names
    assert "clickhouse_query_skipped_total" in names


def test_mark_waiting_then_success_clears(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    collector.mark_waiting("ns/q", "waiting for connection")
    assert collector.snapshot("ns/q")["waiting_reason"] == "waiting for connection"
    assert collector.snapshot("ns/q")["consecutive_failures"] == 0  # not a failure
    collector.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=1.0)
    assert collector.snapshot("ns/q")["waiting_reason"] is None


def test_update_stores_failed_nodes(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "m", "h", {})
    collector.update(
        "ns/q", [QueryResult(1.0, {"node": "h1"})], 0.1, tick_ts=1.0, failed_nodes=["h2"]
    )
    assert collector.snapshot("ns/q")["failed_nodes"] == ["h2"]
    # a later successful run with no failures clears it
    collector.update("ns/q", [QueryResult(1.0, {"node": "h1"})], 0.1, tick_ts=2.0)
    assert collector.snapshot("ns/q")["failed_nodes"] == []


def test_system_metric_names_use_prefix() -> None:
    registry = CollectorRegistry()
    c = QueryMetricsCollector(registry=registry, prefix="clickhouse")
    c.register("ns/q", "m", "h", {})
    c.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=0.0)
    names = {m.name for m in c.collect()}
    assert "clickhouse_query_up" in names
    assert "ch_query_up" not in names


def test_system_metric_names_custom_prefix() -> None:
    registry = CollectorRegistry()
    c = QueryMetricsCollector(registry=registry, prefix="acme_clickhouse")
    c.register("ns/q", "m", "h", {})
    c.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=0.0)
    names = {m.name for m in c.collect()}
    assert "acme_clickhouse_query_up" in names


def test_register_invalid_hides_metric_and_reports_reason(collector: QueryMetricsCollector) -> None:
    collector.register("ns/bad", "m", "h", {}, invalid_reason="reserved label 'query_key'")
    collector.update("ns/bad", [QueryResult(1.0, {})], 0.1, tick_ts=0.0)
    names = {m.name for m in collector.collect()}
    assert "m" not in names  # invalid entries emit no user metric
    assert collector.invalid_reason("ns/bad") == "reserved label 'query_key'"
    assert collector.snapshot("ns/bad")["invalid_reason"] == "reserved label 'query_key'"


def test_invalid_reports_query_up_zero_and_nothing_else(collector: QueryMetricsCollector) -> None:
    """An invalid query stays alertable via query_up=0, but emits no user metric
    and no execution metrics (it never runs)."""
    collector.register("ns/bad", "m", "h", {}, invalid_reason="bad config")
    fams = {m.name: m for m in collector.collect()}
    up = {s.labels["query_key"]: s.value for s in fams["clickhouse_query_up"].samples}
    assert up["ns/bad"] == 0.0
    assert "m" not in fams  # no user metric
    assert "clickhouse_query_duration_seconds" not in fams
    assert "clickhouse_query_last_success_timestamp_seconds" not in fams
    assert "clickhouse_query_inflight" not in fams
    assert "clickhouse_query_skipped_total" not in fams


def test_invalidating_valid_query_forces_up_zero(collector: QueryMetricsCollector) -> None:
    """A query edited from valid into invalid keeps stale runtime state (up=True,
    cached rows); query_up must still report 0 and the user metric must vanish."""
    collector.register("ns/q", "m", "h", {})
    collector.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=0.0)
    up_valid = {
        s.labels["query_key"]: s.value
        for fam in collector.collect()
        if fam.name == "clickhouse_query_up"
        for s in fam.samples
    }
    assert up_valid["ns/q"] == 1.0
    collector.register("ns/q", "m", "h", {}, invalid_reason="now bad")
    fams = {m.name: m for m in collector.collect()}
    up = {s.labels["query_key"]: s.value for s in fams["clickhouse_query_up"].samples}
    assert up["ns/q"] == 0.0
    assert "m" not in fams  # user metric suppressed despite cached rows


def test_re_register_updates_name_and_clears_invalid(collector: QueryMetricsCollector) -> None:
    collector.register("ns/q", "old", "h", {}, invalid_reason="bad")
    assert collector.invalid_reason("ns/q") == "bad"
    collector.register("ns/q", "new_name", "h2", {"env": "prod"})
    assert collector.invalid_reason("ns/q") is None
    entry_snap = collector.snapshot("ns/q")
    assert entry_snap is not None
    collector.update("ns/q", [QueryResult(1.0, {})], 0.1, tick_ts=1.0)
    metrics = {m.name: m for m in collector.collect()}
    assert "new_name" in metrics
    assert "old" not in metrics
