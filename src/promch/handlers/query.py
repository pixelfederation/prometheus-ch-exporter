"""kopf handlers for ClickHouseQuery: scheduler daemon + status reflector.

Execution and status are decoupled:
- @kopf.daemon fires fire-and-forget executions on a fixed interval (overlapping
  up to maxConcurrent); it updates the in-memory collector and NEVER patches status.
- @kopf.timer reflects the collector's state into .status via kopf's patch
  (clean, no direct k8s API).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any

import kopf

from ..clickhouse.errors import NodeQueryError, NoLiveNodesError
from ..collector.metrics import QueryMetricsCollector
from ..config import OperatorConfig
from ..utils import parse_duration_seconds
from .connection import get_connection
from .query_status import build_rows, derive_status, resolve_metric

logger = logging.getLogger(__name__)

_CRD = ("prometheus-ch-exporter.io", "v1alpha1", "clickhousequeries")

# kopf reads timer intervals at import time → env knob (mirrors PROMCH_STATUS_INTERVAL).
_STATUS_INTERVAL = parse_duration_seconds(os.environ.get("PROMCH_STATUS_INTERVAL", "15s"))

_config: OperatorConfig | None = None
_collector: QueryMetricsCollector | None = None
# Fire-and-forget task references, kept to prevent premature GC.
_pending: set[asyncio.Future[None]] = set()


def _resource_key(namespace: str | None, name: str) -> str:
    return f"{namespace or ''}/{name}"


def _event_signature(s: Any) -> tuple[Any, ...]:
    """What makes a status change 'material' enough to emit a new Event:
    the phase, the set of failed nodes, and the error type."""
    err = s.get("lastError") or {}
    return (s.get("phase"), tuple(sorted(s.get("failedNodes") or [])), err.get("type"))


def _phase_detail(snap: dict[str, Any] | None) -> str:
    """A short human reason for the CURRENT phase, for the transition Event.

    Order matters: lastError is retained after recovery (for diagnosis), so it
    must NOT be used to describe a now-successful (up) transition — otherwise a
    'Expired -> Healthy' event would carry the stale 'NoLiveNodes' message.
    """
    if snap is None:
        return ""
    if snap.get("waiting_reason"):
        return str(snap["waiting_reason"])
    if snap.get("failed_nodes"):
        return f"nodes not responding: {', '.join(snap['failed_nodes'])}"
    if not snap.get("up") and snap.get("last_error"):
        err = snap["last_error"]
        return f"{err.get('type', 'Error')}: {err.get('message', '')}"
    return ""


def _resolve(spec: kopf.Spec, field: str, default_seconds: float) -> float:
    raw = spec.get(field)
    return parse_duration_seconds(raw) if raw else default_seconds


@kopf.on.startup()
async def startup(**kwargs: Any) -> None:
    global _config, _collector
    _config = OperatorConfig()
    logging.basicConfig(
        level=_config.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,  # override any handler kopf installed, so timestamps apply
    )
    # kopf logs "Timer/Handler '...' succeeded" per tick at INFO on the
    # `kopf.objects` logger — with a 15s reflector that is pure noise. Quiet it;
    # our own promch.* logs and k8s Events (transitions) still carry the signal.
    logging.getLogger("kopf.objects").setLevel(logging.WARNING)
    # clickhouse-connect logs every failed ping as a DEBUG traceback; those
    # failures are expected and handled by us (Node marks the node down).
    logging.getLogger("clickhouse_connect").setLevel(logging.WARNING)
    # kopf's liveness aiohttp server logs every kube-probe GET /healthz at INFO
    # on `aiohttp.access` — one line per probe interval, pure noise.
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    _collector = QueryMetricsCollector(prefix=_config.metric_prefix_normalized)
    from prometheus_client import start_http_server

    start_http_server(_config.metrics_port)
    logger.info("Metrics server on :%d", _config.metrics_port)


@kopf.on.cleanup()
async def cancel_pending(**kwargs: Any) -> None:
    """Cancel in-flight query executions on shutdown so a run stuck on a blocked
    node (waiting out the connect timeout) doesn't delay the operator's exit."""
    for task in list(_pending):
        task.cancel()


@kopf.on.create(*_CRD)
@kopf.on.resume(*_CRD)
@kopf.on.update(*_CRD)
async def register(spec: kopf.Spec, name: str, namespace: str | None, **kwargs: Any) -> None:
    assert _collector is not None and _config is not None
    key = _resource_key(namespace, name)
    metric = dict(spec["metric"])
    query_type = str(spec.get("queryType", "data"))
    resolved = resolve_metric(
        metric, query_type, _config.metric_prefix_normalized, _config.node_label
    )
    _collector.register(
        key, resolved.name, resolved.help, resolved.labels, invalid_reason=resolved.invalid_reason
    )
    if resolved.invalid_reason:
        logger.warning("query %s invalid: %s", key, resolved.invalid_reason)
    else:
        logger.info(
            "query %s registered (metric=%s connection=%s type=%s interval=%s)",
            key,
            resolved.name,
            spec.get("connectionRef"),
            query_type,
            spec.get("interval", "<default>"),
        )


@kopf.on.delete(*_CRD)
async def deregister(name: str, namespace: str | None, **kwargs: Any) -> None:
    assert _collector is not None
    key = _resource_key(namespace, name)
    _collector.unregister(key)
    logger.info("query %s removed", key)


@kopf.daemon(*_CRD, cancellation_timeout=5.0)
async def query_scheduler(
    spec: kopf.Spec, name: str, namespace: str | None, stopped: kopf.DaemonStopped, **kwargs: Any
) -> None:
    assert _config is not None and _collector is not None
    collector = _collector
    config = _config
    key = _resource_key(namespace, name)
    loop = asyncio.get_event_loop()

    while not stopped.is_set():
        # Re-read spec each tick so live CRD edits (interval, maxConcurrent,
        # query, queryType, connectionRef) take effect on the next cycle —
        # kopf keeps the daemon's `spec` kwarg in sync with the object.
        interval = _resolve(spec, "interval", config.default_interval_seconds)
        timeout = _resolve(spec, "timeout", config.default_timeout_seconds)
        max_concurrent = int(spec.get("maxConcurrent") or config.default_max_concurrent)
        query_type = str(spec.get("queryType", "data"))
        conn_ref = str(spec["connectionRef"])
        sql = str(spec["query"])

        invalid = collector.invalid_reason(key)
        if invalid is not None:
            logger.warning("query %s not run (invalid): %s", key, invalid)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stopped.wait(), timeout=interval)
            continue

        conn = get_connection(conn_ref)
        if conn is None or not conn.ready:
            # Don't run queries until the connection has finished its first
            # discovery (after operator/pod start). Avoids a spurious NoLiveNodes.
            collector.mark_waiting(
                key,
                f"Waiting for connection {conn_ref!r} to complete its first "
                "topology/liveness check after operator start",
            )
            logger.debug("query %s waiting: connection %r not ready yet", key, conn_ref)
        elif collector.inflight(key) >= max_concurrent:
            collector.record_skip(key)
            logger.warning(
                "query %s still running (>= maxConcurrent=%d), skipping tick", key, max_concurrent
            )
        else:
            collector.inc_inflight(key)
            task: asyncio.Task[None] = asyncio.create_task(
                _execute(key, conn_ref, query_type, sql, timeout, loop.time())
            )
            _pending.add(task)
            task.add_done_callback(_pending.discard)
            task.add_done_callback(lambda _t: collector.dec_inflight(key))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stopped.wait(), timeout=interval)


async def _execute(
    key: str, conn_ref: str, query_type: str, sql: str, timeout: float, tick_ts: float
) -> None:
    assert _collector is not None
    collector = _collector
    start = time.monotonic()
    try:
        conn = get_connection(conn_ref)
        if conn is None:
            collector.mark_failure(key, "ConnectionNotReady", f"connection {conn_ref!r} not found")
            logger.warning("query %s: connection %r not found", key, conn_ref)
            return
        if query_type == "system":
            assert _config is not None
            result = await conn.execute_system_query(sql, timeout, _config.node_label)
            if not result.rows and result.failed_nodes:
                _fail(
                    key, "NodesUnreachable", f"all nodes failed: {', '.join(result.failed_nodes)}"
                )
                return
            raw = result.rows
            failed_nodes: list[str] = result.failed_nodes
        else:
            raw = await conn.execute_data_query(sql, timeout)
            failed_nodes = []
        rows = build_rows(raw)
        recovered = collector.update(
            key, rows, time.monotonic() - start, tick_ts, failed_nodes=failed_nodes
        )
        if failed_nodes:
            logger.warning(
                "query %s ok: %d rows; %d node(s) failed: %s",
                key,
                len(rows),
                len(failed_nodes),
                ", ".join(failed_nodes),
            )
        elif recovered:
            # Transition into healthy (first success or recovery after failure) —
            # a production signal, logged at INFO. Steady-state runs stay DEBUG.
            logger.info("query %s healthy: %d rows", key, len(rows))
        else:
            logger.debug("query %s ok: %d rows", key, len(rows))
    except NoLiveNodesError as exc:
        # A system query that can't run at all → report every member as down
        # (it couldn't enumerate them itself). Data queries have no node list.
        failed = None
        if query_type == "system":
            c = get_connection(conn_ref)
            failed = c.member_hosts() if c is not None else None
        _fail(key, "NoLiveNodes", str(exc), failed_nodes=failed)
    except NodeQueryError as exc:
        _fail(key, "QueryError", str(exc))
    except ValueError as exc:  # build_rows value-contract violation
        _fail(key, "InvalidResult", str(exc))
    except Exception as exc:  # noqa: BLE001 - never let a fire-and-forget task die silently
        _fail(key, "QueryError", str(exc))


def _fail(key: str, error_type: str, message: str, failed_nodes: list[str] | None = None) -> None:
    assert _config is not None and _collector is not None
    _collector.mark_failure(key, error_type, message, failed_nodes)
    snap = _collector.snapshot(key)
    if snap is not None and snap["consecutive_failures"] >= _config.expire_after_failures:
        _collector.expire(key)
        logger.warning("query %s expired after %d failures", key, snap["consecutive_failures"])
    else:
        logger.warning("query %s failed (%s): %s", key, error_type, message)


@kopf.timer(*_CRD, interval=_STATUS_INTERVAL, sharp=True)
async def status_reflector(
    name: str,
    namespace: str | None,
    body: kopf.Body,
    status: kopf.Status,
    patch: kopf.Patch,
    **kwargs: Any,
) -> None:
    assert _config is not None and _collector is not None
    key = _resource_key(namespace, name)
    old_phase = status.get("phase")
    snap = _collector.snapshot(key)
    desired = derive_status(snap, status, _config)

    # Emit a k8s Event when the situation MATERIALLY changes — the phase, the set
    # of failed nodes, or the error type — not every tick. This still fires when
    # a different node fails while the phase stays Degraded. Identical repeats are
    # aggregated server-side by k8s (count + lastTimestamp), so no throttle needed.
    new_phase = desired.get("phase")
    if new_phase is not None and _event_signature(status) != _event_signature(desired):
        etype = (
            "Warning" if new_phase in ("Failing", "Expired", "Degraded", "Invalid") else "Normal"
        )
        detail = _phase_detail(snap)
        message = f"{old_phase or 'None'} -> {new_phase}" + (f": {detail}" if detail else "")
        kopf.event(body, type=etype, reason=str(new_phase), message=message)

    # Only patch changed top-level keys (idempotent, avoids churn).
    for field, value in desired.items():
        if status.get(field) != value:
            patch.status[field] = value
