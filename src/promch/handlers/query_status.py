"""Pure helpers for the query path: row -> metric mapping and status derivation.

Kept free of kopf so they are straightforward to unit-test.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..collector.metrics import QueryResult
from ..config import OperatorConfig

# Errors meaning "the cluster/connection is not ready yet", as opposed to a
# broken query. Before the query has ever succeeded (e.g. right after an operator
# restart, while the connection is still verifying nodes), these keep it Pending
# (warming up) rather than Failing.
_AVAILABILITY_ERRORS = {"NoLiveNodes", "ConnectionNotReady", "NodesUnreachable"}


@dataclass
class ResolvedMetric:
    """Final metric identity after prefixing + static-label validation."""

    name: str
    help: str
    labels: dict[str, str]
    invalid_reason: str | None


def apply_prefix(name: str, prefix: str, policy: str) -> str:
    """Namespace a metric name under `prefix`. `policy` decides what happens when
    `name` already starts with `<prefix>_`: 'skip' keeps it, 'append' prepends
    anyway, 'fail' raises. Empty prefix returns the name unchanged."""
    if not prefix:
        return name
    lead = f"{prefix}_"
    if not name.startswith(lead):
        return f"{prefix}_{name}"
    if policy == "skip":
        return name
    if policy == "append":
        return f"{prefix}_{name}"
    raise ValueError(
        f"metric name {name!r} already starts with the configured prefix {prefix!r}; "
        "set spec.metric.prefixPolicy to 'skip' to keep the name or 'append' to add it anyway"
    )


def reserved_label_names(node_label: str, query_type: str) -> set[str]:
    """Labels the exporter injects and users must not define themselves."""
    reserved = {"query_key"}
    if query_type == "system":
        reserved.add(node_label)
    return reserved


def resolve_metric(
    metric_spec: dict[str, Any], query_type: str, prefix: str, node_label: str
) -> ResolvedMetric:
    """Compute the final metric identity, or mark it invalid with a reason."""
    raw_name = str(metric_spec["name"])
    help_text = str(metric_spec.get("help", ""))
    labels = {str(k): str(v) for k, v in (metric_spec.get("labels") or {}).items()}

    reserved = reserved_label_names(node_label, query_type)
    clashes = sorted(set(labels) & reserved)
    if clashes:
        reason = (
            f"metric.labels use reserved label name(s) {clashes}; "
            "these are added by the exporter and must not be set"
        )
        return ResolvedMetric(raw_name, help_text, labels, reason)

    policy = str(metric_spec.get("prefixPolicy", "fail"))
    try:
        final_name = apply_prefix(raw_name, prefix, policy)
    except ValueError as exc:
        return ResolvedMetric(raw_name, help_text, labels, str(exc))
    return ResolvedMetric(final_name, help_text, labels, None)


def build_rows(raw_rows: list[dict[str, Any]]) -> list[QueryResult]:
    """Map raw query rows to QueryResult.

    Enforces the 'value' contract here (the ClickHouse client is metric-agnostic):
    every row must have a numeric 'value'; all other columns become labels.
    """
    results: list[QueryResult] = []
    for raw in raw_rows:
        row = dict(raw)
        if "query_key" in row:
            raise ValueError(
                "query result uses reserved column 'query_key' "
                "(added by the exporter to identify the resource)"
            )
        if "value" not in row:
            raise ValueError(f"query result row missing 'value' column: {list(row)}")
        try:
            value = float(row.pop("value"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'value' is not numeric: {raw.get('value')!r}") from exc
        results.append(QueryResult(value=value, dynamic_labels={k: str(v) for k, v in row.items()}))
    return results


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat()


def _fresh_error(last_error: dict[str, str] | None, ttl_seconds: float) -> dict[str, str] | None:
    """Return the error while it is younger than ttl_seconds, else None."""
    if not last_error:
        return None
    stamp = last_error.get("timestamp")
    try:
        age = (datetime.now(UTC) - datetime.fromisoformat(stamp)).total_seconds() if stamp else 0.0
    except ValueError:
        age = 0.0
    return dict(last_error) if age < ttl_seconds else None


def _set_condition(
    out: dict[str, Any], current: Any, ctype: str, status: str, reason: str, message: str
) -> None:
    now = datetime.now(UTC).isoformat()
    # Deep-copy each condition so we never mutate the live `current`/status dicts
    # in place — that would make the reflector's "patch only if changed" compare
    # equal and silently drop the update.
    by_type: dict[str, dict[str, str]] = {
        c["type"]: dict(c) for c in (out.get("conditions") or current.get("conditions") or [])
    }
    prev = by_type.get(ctype)
    # lastTransitionTime changes only when the status value flips (k8s semantics);
    # keeping it stable also lets an unchanged condition compare equal → no churn.
    transition = prev["lastTransitionTime"] if prev and prev.get("status") == status else now
    by_type[ctype] = {
        "type": ctype,
        "status": status,
        "lastTransitionTime": transition,
        "reason": reason,
        "message": message,
    }
    out["conditions"] = list(by_type.values())


def derive_status(
    snap: dict[str, Any] | None, current: Any, config: OperatorConfig
) -> dict[str, Any]:
    """Build the .status patch from a collector snapshot. Pure; the reflector
    only writes it if it differs from the current status."""
    if snap is None:
        return {"phase": "Pending"}

    # Misconfigured resource (reserved label / prefix collision): soft-fail. The
    # operator is a controller, not an admission webhook, so we surface it as an
    # Invalid phase + Valid=False condition rather than rejecting the apply.
    if snap.get("invalid_reason"):
        invalid_out: dict[str, Any] = {
            "phase": "Invalid",
            "invalidReason": snap["invalid_reason"],
        }
        _set_condition(
            invalid_out, current, "Valid", "False", "InvalidConfiguration", snap["invalid_reason"]
        )
        invalid_out["conditions"].sort(key=lambda c: c.get("lastTransitionTime", ""), reverse=True)
        return invalid_out

    waiting = snap.get("waiting_reason")
    if snap["expired"]:
        phase = "Expired"
    elif snap["up"]:
        phase = "Degraded" if snap.get("failed_nodes") else "Healthy"
    elif waiting:
        phase = "Pending"  # intentionally not running yet (e.g. after restart)
    elif snap["last_error"] is not None:
        etype = snap["last_error"].get("type")
        if etype in _AVAILABILITY_ERRORS and snap["last_success_ts"] is None:
            phase = "Pending"  # warming up / waiting for the connection
        else:
            phase = "Failing"
    else:
        phase = "Pending"

    out: dict[str, Any] = {
        "phase": phase,
        "consecutiveFailures": snap["consecutive_failures"],
        "skippedTicks": snap["skipped_total"],
    }
    if snap["last_success_ts"] is not None:
        out["lastSuccess"] = {
            "timestamp": _iso(snap["last_success_ts"]),
            "rowCount": snap["row_count"],
            "durationMs": int((snap["duration"] or 0.0) * 1000),
        }
    # Retain lastError for `last_error_ttl` after recovery so occasional brief
    # outages stay diagnosable, then clear it. Always present (dict or None) so
    # the reflector clears a stale one (it only patches keys, never removes them).
    out["lastError"] = _fresh_error(snap["last_error"], config.last_error_ttl_seconds)
    out["failedNodes"] = list(snap.get("failed_nodes") or [])

    err = snap["last_error"] or {}
    _set_condition(
        out,
        current,
        "QuerySucceeded",
        "True" if snap["up"] else "False",
        "QueryOK" if snap["up"] else err.get("type", "Unknown"),
        "Last query succeeded" if snap["up"] else err.get("message", ""),
    )

    # KeepingUp: a skip at/after the last success means we are behind.
    ls, lk = snap["last_success_ts"], snap["last_skip_ts"]
    behind = lk is not None and (ls is None or lk >= ls)
    _set_condition(
        out,
        current,
        "KeepingUp",
        "False" if behind else "True",
        "MaxConcurrentReached" if behind else "OnSchedule",
        "Query runs longer than interval x maxConcurrent; ticks skipped"
        if behind
        else "Keeping up",
    )

    # Ready: surfaces WHY a query is Pending without a scary error — e.g. waiting
    # for its connection right after an operator/pod restart.
    _set_condition(
        out,
        current,
        "Ready",
        "False" if waiting else "True",
        "WaitingForConnection" if waiting else "Ready",
        waiting if waiting else "Query is running",
    )

    # AllNodesResponding: distinguishes a partial (Degraded) system query from a
    # fully-healthy one — QuerySucceeded stays True on partial success, so this
    # is what actually flips on a node outage.
    failed = snap.get("failed_nodes") or []
    _set_condition(
        out,
        current,
        "AllNodesResponding",
        "False" if failed else "True",
        "NodesUnreachable" if failed else "AllResponding",
        ("nodes not responding: " + ", ".join(failed)) if failed else "All cluster nodes responded",
    )

    # Newest transition first, so the most recent change is at the top.
    out["conditions"].sort(key=lambda c: c.get("lastTransitionTime", ""), reverse=True)
    return out
