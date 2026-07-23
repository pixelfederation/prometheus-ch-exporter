from datetime import UTC, datetime, timedelta

import pytest

from promch.collector.metrics import QueryResult
from promch.config import OperatorConfig
from promch.handlers.query_status import build_rows, derive_status


def _cfg() -> OperatorConfig:
    return OperatorConfig()  # type: ignore[call-arg]


def test_phase_detail_recovery_ignores_retained_error() -> None:
    from promch.handlers.query import _phase_detail

    # up again, but a stale error is retained → detail must be empty (Healthy),
    # not the old error message.
    snap = {
        "up": True,
        "waiting_reason": None,
        "failed_nodes": [],
        "last_error": {"type": "NoLiveNodes", "message": "no live nodes"},
    }
    assert _phase_detail(snap) == ""


def test_phase_detail_degraded_lists_nodes() -> None:
    from promch.handlers.query import _phase_detail

    snap = {"up": True, "waiting_reason": None, "failed_nodes": ["h2"], "last_error": None}
    assert _phase_detail(snap) == "nodes not responding: h2"


def test_phase_detail_failing_shows_error() -> None:
    from promch.handlers.query import _phase_detail

    snap = {
        "up": False,
        "waiting_reason": None,
        "failed_nodes": [],
        "last_error": {"type": "QueryError", "message": "boom"},
    }
    assert _phase_detail(snap) == "QueryError: boom"


def test_build_rows_scalar() -> None:
    assert build_rows([{"value": 7}]) == [QueryResult(value=7.0, dynamic_labels={})]


def test_build_rows_labels_and_node() -> None:
    rows = build_rows([{"value": 3, "app": "a", "node": "h1"}])
    assert rows[0].value == 3.0
    assert rows[0].dynamic_labels == {"app": "a", "node": "h1"}


def test_build_rows_missing_value() -> None:
    with pytest.raises(ValueError, match="value"):
        build_rows([{"app": "a"}])


def test_build_rows_non_numeric() -> None:
    with pytest.raises(ValueError, match="numeric"):
        build_rows([{"value": "abc"}])


def test_derive_status_pending_when_absent() -> None:
    assert derive_status(None, {}, _cfg())["phase"] == "Pending"


def test_derive_status_healthy() -> None:
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": None,
        "last_success_ts": 1_700_000_000.0,
        "row_count": 2,
        "duration": 0.25,
        "skipped_total": 0,
        "last_skip_ts": None,
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Healthy"
    assert out["lastSuccess"]["rowCount"] == 2
    assert out["consecutiveFailures"] == 0


def test_derive_status_retains_recent_last_error_when_healthy() -> None:
    recent = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": {"type": "QueryError", "message": "blip", "timestamp": recent},
        "last_success_ts": 1_700_000_000.0,
        "row_count": 1,
        "duration": 0.1,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": [],
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Healthy"
    assert out["lastError"] is not None  # kept within TTL for diagnosis


def test_derive_status_clears_aged_last_error() -> None:
    old = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": {"type": "QueryError", "message": "old", "timestamp": old},
        "last_success_ts": 1_700_000_000.0,
        "row_count": 1,
        "duration": 0.1,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": [],
    }
    out = derive_status(snap, {}, _cfg())  # default TTL 10m -> 1h old is cleared
    assert out["lastError"] is None


def test_derive_status_conditions_sorted_newest_first() -> None:
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": None,
        "last_success_ts": 1_700_000_000.0,
        "row_count": 1,
        "duration": 0.1,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": [],
    }
    out = derive_status(snap, {}, _cfg())
    times = [c["lastTransitionTime"] for c in out["conditions"]]
    assert times == sorted(times, reverse=True)


def test_derive_status_partial_system_is_degraded() -> None:
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": None,
        "last_success_ts": 1_700_000_000.0,
        "row_count": 5,
        "duration": 0.3,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": ["10.0.0.9"],
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Degraded"
    assert out["failedNodes"] == ["10.0.0.9"]
    cond = next(c for c in out["conditions"] if c["type"] == "AllNodesResponding")
    assert cond["status"] == "False"
    assert cond["reason"] == "NodesUnreachable"


def test_derive_status_healthy_has_empty_failed_nodes() -> None:
    snap = {
        "up": True,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": None,
        "last_success_ts": 1_700_000_000.0,
        "row_count": 2,
        "duration": 0.1,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": [],
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Healthy"
    assert out["failedNodes"] == []


def test_derive_status_waiting_is_pending_with_ready_false() -> None:
    snap = {
        "up": False,
        "expired": False,
        "consecutive_failures": 0,
        "last_error": None,
        "last_success_ts": None,
        "row_count": 0,
        "duration": None,
        "skipped_total": 0,
        "last_skip_ts": None,
        "failed_nodes": [],
        "waiting_reason": "Waiting for connection 'cluster' to complete its first check",
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Pending"
    ready = next(c for c in out["conditions"] if c["type"] == "Ready")
    assert ready["status"] == "False"
    assert ready["reason"] == "WaitingForConnection"


def test_derive_status_connection_warmup_is_pending() -> None:
    # never succeeded + connection not ready -> Pending (not Failing)
    snap = {
        "up": False,
        "expired": False,
        "consecutive_failures": 1,
        "last_error": {"type": "NoLiveNodes", "message": "no live nodes", "timestamp": "t"},
        "last_success_ts": None,
        "row_count": 0,
        "duration": None,
        "skipped_total": 0,
        "last_skip_ts": None,
    }
    assert derive_status(snap, {}, _cfg())["phase"] == "Pending"


def test_derive_status_outage_after_success_is_failing() -> None:
    # succeeded before, then no live nodes -> a real outage -> Failing
    snap = {
        "up": False,
        "expired": False,
        "consecutive_failures": 1,
        "last_error": {"type": "NoLiveNodes", "message": "no live nodes", "timestamp": "t"},
        "last_success_ts": 1_700_000_000.0,
        "row_count": 1,
        "duration": 0.1,
        "skipped_total": 0,
        "last_skip_ts": None,
    }
    assert derive_status(snap, {}, _cfg())["phase"] == "Failing"


def test_derive_status_failing_and_keeping_up_false() -> None:
    snap = {
        "up": False,
        "expired": False,
        "consecutive_failures": 3,
        "last_error": {
            "type": "QueryError",
            "message": "boom",
            "timestamp": datetime.now(UTC).isoformat(),
        },
        "last_success_ts": None,
        "row_count": 0,
        "duration": None,
        "skipped_total": 5,
        "last_skip_ts": 1_700_000_050.0,
    }
    out = derive_status(snap, {}, _cfg())
    assert out["phase"] == "Failing"
    assert out["lastError"]["type"] == "QueryError"
    assert out["skippedTicks"] == 5
    keeping = next(c for c in out["conditions"] if c["type"] == "KeepingUp")
    assert keeping["status"] == "False"
