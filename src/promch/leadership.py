"""Tracks whether this operator replica is the active peer under kopf peering.

Standalone (peering disabled) is always active. Under active/standby peering,
kopf stops every daemon/timer with reason OPERATOR_PAUSING when a higher-priority
peer takes over. We flip to standby so the metrics collector stops exporting the
rows it cached while it was briefly active — otherwise the demoted replica keeps
serving them and Prometheus scrapes duplicate series from two pods.

A query still finishing in the background on a demoted replica is harmless: its
result is simply not exported while we are standby.

The flag is a threading.Event because it is written from the asyncio loop (the
kopf daemon) and read from the prometheus_client HTTP server thread (collect()).
Those primitives on Event are thread-safe.
"""

from __future__ import annotations

import threading

import kopf

# Active by default: kopf runs active until it discovers a higher-priority peer.
_active = threading.Event()
_active.set()


def is_active() -> bool:
    """True if this replica should export query metrics (active peer / standalone)."""
    return _active.is_set()


def mark_active() -> None:
    """Called when a query daemon starts — kopf only runs daemons while active."""
    _active.set()


def mark_standby() -> None:
    """Force standby. Called at startup when peering is enabled: a replica that
    boots straight into standby never runs a daemon (its watch-stream is frozen),
    so without this it would keep the default "active" forever and wrongly export
    metrics / report clickhouse_leader=1. The query daemon flips us back to active
    iff kopf actually runs it here (i.e. we are the active peer with >=1 query)."""
    _active.clear()


def note_daemon_stopped(reason: kopf.DaemonStoppingReason | None) -> None:
    """Flip to standby only when kopf paused us for a higher-priority peer.

    Resource deletion and operator shutdown stop daemons too, but neither is a
    loss of leadership, so they must not gate metric emission.
    """
    if reason is not None and kopf.DaemonStoppingReason.OPERATOR_PAUSING in reason:
        _active.clear()
