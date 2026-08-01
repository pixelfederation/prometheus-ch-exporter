import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import REGISTRY, Collector, CollectorRegistry

logger = logging.getLogger(__name__)


# --- @dataclass ---
# Instead of writing __init__ manually, we declare fields at class level
# and the decorator generates the constructor for us.
# `@dataclass` is equivalent to writing:
#
#   def __init__(self, name, help, labels, ...):
#       self.name = name
#       self.help = help
#       ...  (for every field)
@dataclass
class QueryResult:
    """One row from a successful query execution → one Prometheus time series.

    The query contract: every row must have a `value` column (numeric).
    All other columns become dynamic label names.

    Example query:
        SELECT count() AS value, app_name, log_level FROM ... GROUP BY ...
    Produces rows like:
        QueryResult(value=142.0, dynamic_labels={"app_name": "app1-prod", "log_level": "500"})
        QueryResult(value=5.0,   dynamic_labels={"app_name": "app2-prod", "log_level": "550"})
    """

    value: float
    dynamic_labels: dict[str, str]  # column_name → string value from query row


@dataclass
class CachedMetric:
    """Holds the last known good rows for one ClickHouseQuery resource.

    One instance of this exists per CRD resource (per namespace/name pair).
    One resource can produce N Prometheus time series (one per QueryResult row).
    """

    # Fields without defaults must come before fields with defaults (Python rule).
    name: str  # Prometheus metric name, e.g. "clickhouse_query_hits"
    help: str  # Prometheus HELP text shown in /metrics output
    labels: dict[str, str]  # static labels from CRD spec, merged into every row

    # Optional fields with defaults — filled in after the first successful query.
    # `rows` replaces the old `value: float | None` — one resource now stores
    # a list of time series rows instead of a single scalar value.
    rows: list[QueryResult] | None = None  # None until first success; then 1+ rows
    last_success_ts: float | None = None  # unix timestamp of last success
    last_duration_seconds: float | None = None
    up: bool = False  # True only after at least one success
    expired: bool = False  # True after too many consecutive failures

    # Failure / skip / inflight tracking — single source of truth for status.
    consecutive_failures: int = 0
    last_error: dict[str, str] | None = None  # {"type","message","timestamp"}
    inflight: int = 0
    skipped_total: int = 0
    last_skip_ts: float | None = None
    failed_nodes: list[str] = field(default_factory=list)  # last run's non-responding nodes
    # Set while the query is intentionally NOT running yet (e.g. its connection
    # has not completed its first discovery after operator start). Not a failure.
    waiting_reason: str | None = None

    # Set when the resource is misconfigured (reserved label, prefix collision):
    # the entry exists (so status can report it) but emits no metric.
    invalid_reason: str | None = None

    # Monotonic timestamp of the tick that produced the current rows.
    # Used to discard results from older ticks that arrive late (race condition
    # when a new tick fires before the previous query finishes).
    # `compare=False, repr=False` — excluded from __eq__ and __repr__.
    tick_ts: float = field(default=0.0, compare=False, repr=False)

    # --- threading.Lock ---
    # A Lock is a synchronisation primitive. When one thread "acquires" the lock,
    # all other threads trying to acquire it must WAIT until it is released.
    #
    # We need this because two things run concurrently:
    #   1. A background thread executes the CH query and wants to UPDATE the rows
    #   2. The Prometheus scrape handler wants to READ the rows
    # Without a lock, a scrape could read a half-updated list (a "race condition").
    #
    # `field(default_factory=..., compare=False, repr=False)`:
    #   - default_factory=threading.Lock  → new Lock per instance (not shared)
    #   - compare=False                   → excluded from auto-generated __eq__
    #   - repr=False                      → excluded from auto-generated __repr__
    lock: threading.Lock = field(default_factory=threading.Lock, compare=False, repr=False)


class QueryMetricsCollector(Collector):
    """Custom Prometheus collector that serves cached ClickHouseQuery results.

    --- Why a custom collector? ---
    The standard prometheus_client approach (`Gauge('name', 'help').set(value)`)
    creates metrics that are immediately visible. We need more control:
    - We want to serve STALE data while a query is running (never block scrapes)
    - We want to REMOVE expired metrics entirely (not serve 0 or NaN)
    - We want to add system metrics (ch_query_up etc.) alongside user metrics

    A custom collector gives us full control over what gets yielded at scrape time
    by implementing the `collect()` method.

    --- Inheritance from Collector ---
    `Collector` is an abstract base class (interface) from prometheus_client.
    By inheriting from it and implementing `collect()`, we tell the Prometheus
    registry: "call MY collect() method when someone scrapes /metrics".
    """

    def __init__(
        self,
        registry: CollectorRegistry = REGISTRY,
        prefix: str = "clickhouse",
        is_active: Callable[[], bool] | None = None,
    ) -> None:
        # `_entries` maps a resource key ("namespace/name") to its CachedMetric.
        # This is the in-memory store for all registered queries.
        self._entries: dict[str, CachedMetric] = {}

        # A second lock protects the `_entries` dict itself.
        # Why separate? We need two levels:
        #   1. `_global_lock` — protects adding/removing entries from the dict
        #   2. `entry.lock`   — protects reading/writing a single entry's rows
        # This avoids holding a big lock during slow operations.
        self._global_lock = threading.Lock()

        # Namespace for the operator's own system metrics (ch_query_* → <prefix>_query_*).
        # A trailing "_" is ignored; an empty prefix disables namespacing.
        self._prefix = prefix.rstrip("_")

        # Leadership gate: return True to export metrics, False to suppress them
        # (standby replica under kopf peering). Defaults to always-active, so a
        # standalone operator and the unit tests behave exactly as before.
        self._is_active = is_active if is_active is not None else (lambda: True)

        # Register this collector with the Prometheus registry.
        # After this call, every /metrics scrape will call our `collect()`.
        registry.register(self)

    def _sys_name(self, suffix: str) -> str:
        """System-metric family name under the configured prefix."""
        return f"{self._prefix}_{suffix}" if self._prefix else suffix

    def register(
        self,
        key: str,
        name: str,
        help_text: str,
        labels: dict[str, str],
        invalid_reason: str | None = None,
    ) -> None:
        """Add or update a CRD resource in the collector.

        Called when a ClickHouseQuery is created or updated. On update, the
        name/help/labels/invalid_reason are refreshed in place — so live spec
        edits and validation-fix (invalid→valid) take effect — while runtime
        state (rows, counters, inflight) is preserved.

        The `with self._global_lock:` block is a context manager — Python
        automatically acquires the lock on entry and releases it on exit,
        even if an exception is raised.
        """
        with self._global_lock:
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = CachedMetric(
                    name=name,
                    help=help_text,
                    labels=dict(labels),
                    invalid_reason=invalid_reason,
                )
                return
        with entry.lock:
            entry.name = name
            entry.help = help_text
            entry.labels = dict(labels)
            entry.invalid_reason = invalid_reason

    def unregister(self, key: str) -> None:
        """Remove a CRD resource from the collector.

        Called when a ClickHouseQuery is deleted.
        `dict.pop(key, None)` removes the key if it exists, returns None if not —
        no KeyError raised.
        """
        with self._global_lock:
            self._entries.pop(key, None)

    def update(
        self,
        key: str,
        rows: list[QueryResult],
        duration_seconds: float,
        tick_ts: float,
        failed_nodes: list[str] | None = None,
    ) -> bool:
        """Store successful query results (one or more rows).

        Called from a background thread after a query completes.
        We take two locks: first the global lock to safely look up the entry,
        then the entry's own lock to update its fields.

        Each element in `rows` is a QueryResult — one per time series to expose.
        A scalar query (SELECT x AS value) produces a single-element list.
        A GROUP BY query can produce many.

        `tick_ts` is the monotonic timestamp of the tick that started this query.
        If a newer tick's result already arrived (faster query on next interval),
        we discard this stale result to avoid overwriting newer data with older.

        Returns True if this success is a transition INTO the healthy state —
        i.e. the first-ever success, or a recovery after failure/expiry — so the
        caller can log that as a production signal. Returns False for a
        steady-state success and for a discarded stale-tick result.
        """
        # Step 1: look up the entry under the global lock, then release it.
        # We don't hold the global lock while updating — that would block
        # register/unregister for the entire duration of the update.
        with self._global_lock:
            entry = self._entries.get(key)

        if entry is None:
            return False  # resource was deleted between query start and completion

        # Step 2: update the entry's fields under its own lock.
        with entry.lock:
            # Discard results from older ticks — a newer tick already updated the cache.
            # This prevents a slow query from overwriting a faster, more recent result.
            if tick_ts < entry.tick_ts:
                return False
            # `up` is False before the first success and is cleared on every
            # failure/expiry — so `not entry.up` marks a transition into healthy.
            recovered = not entry.up
            entry.tick_ts = tick_ts
            entry.rows = rows
            entry.last_success_ts = time.time()  # current unix timestamp
            entry.last_duration_seconds = duration_seconds
            entry.up = True
            entry.expired = False  # a success clears the expired state
            entry.consecutive_failures = 0
            # last_error is intentionally NOT cleared here — it is retained so a
            # recent, now-recovered failure stays visible; derive_status ages it
            # out after last_error_ttl.
            entry.failed_nodes = list(failed_nodes or [])
            entry.waiting_reason = None
            return recovered

    def mark_failure(
        self, key: str, error_type: str, message: str, failed_nodes: list[str] | None = None
    ) -> None:
        """Record a failure: mark down, count it, store the error detail.

        `failed_nodes` lets an all-nodes-down system query report every member as
        failed (the fan-out can't run, so it can't enumerate them itself)."""
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is None:
            return
        with entry.lock:
            entry.up = False
            entry.consecutive_failures += 1
            entry.last_error = {
                "type": error_type,
                "message": message,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            entry.waiting_reason = None
            if failed_nodes is not None:
                entry.failed_nodes = list(failed_nodes)

    def mark_waiting(self, key: str, reason: str) -> None:
        """Mark the query as waiting (not running yet, not a failure)."""
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is not None:
            with entry.lock:
                entry.waiting_reason = reason

    def expire(self, key: str) -> None:
        """Mark a query as expired — its user metrics will be removed from output."""
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is None:
            return
        with entry.lock:
            entry.expired = True
            entry.up = False

    def inc_inflight(self, key: str) -> None:
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is not None:
            with entry.lock:
                entry.inflight += 1

    def dec_inflight(self, key: str) -> None:
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is not None:
            with entry.lock:
                entry.inflight = max(0, entry.inflight - 1)

    def inflight(self, key: str) -> int:
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is None:
            return 0
        with entry.lock:
            return entry.inflight

    def invalid_reason(self, key: str) -> str | None:
        """Return why the resource is misconfigured, or None if it is valid."""
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is None:
            return None
        with entry.lock:
            return entry.invalid_reason

    def record_skip(self, key: str) -> None:
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is not None:
            with entry.lock:
                entry.skipped_total += 1
                entry.last_skip_ts = time.time()

    def snapshot(self, key: str) -> dict[str, Any] | None:
        """Consistent read of one entry's state for the status reflector."""
        with self._global_lock:
            entry = self._entries.get(key)
        if entry is None:
            return None
        with entry.lock:
            return {
                "up": entry.up,
                "expired": entry.expired,
                "consecutive_failures": entry.consecutive_failures,
                "last_error": dict(entry.last_error) if entry.last_error else None,
                "last_success_ts": entry.last_success_ts,
                "row_count": len(entry.rows) if entry.rows else 0,
                "duration": entry.last_duration_seconds,
                "skipped_total": entry.skipped_total,
                "last_skip_ts": entry.last_skip_ts,
                "failed_nodes": list(entry.failed_nodes),
                "waiting_reason": entry.waiting_reason,
                "invalid_reason": entry.invalid_reason,
            }

    def collect(self) -> Iterator[GaugeMetricFamily | CounterMetricFamily]:
        """Yield metrics to Prometheus at scrape time.

        --- How this works ---
        Prometheus calls `collect()` every time it scrapes /metrics.
        We must return (yield) metric objects — NOT raw values.
        `GaugeMetricFamily` is a metric that can go up or down (e.g. a count).

        --- Iterator / yield ---
        `yield` makes this a generator function. Instead of building a list
        and returning it all at once, we produce one item at a time.
        The caller (Prometheus registry) iterates over our yielded values.

        --- One family per unique metric name ---
        A Prometheus metric family is identified by its name. Yielding two
        families with the same name produces duplicate HELP/TYPE lines in the
        exposition format, which is invalid. We must yield exactly one family
        per unique name, with all time series as add_metric() calls within it.

        This has two consequences:
        1. System metrics (ch_query_up etc.) are built ONCE across all entries,
           not once per entry. Label schema: only `query_key` (fixed, no per-CRD
           static labels — those belong only on user metrics).
        2. User metrics are grouped by metric name. All CRDs sharing a name
           are merged into one family. The label schema is the union of all
           static + dynamic label keys across every entry with that name.
           Missing keys for a given row are filled with "" (empty string).

        --- Snapshot pattern ---
        We copy the entries dict under the global lock, then release it.
        This way we don't hold the lock during the actual metric building,
        which could be slow.
        """
        # Leadership indicator — emitted in BOTH states (one series per pod, so no
        # duplication) so `sum(<prefix>_leader) != 1` alerts on split-brain / no
        # leader. Evaluate once so the gauge and the gate below agree.
        is_leader = self._is_active()
        yield GaugeMetricFamily(
            self._sys_name("leader"),
            "1 if this replica is the active peer exporting query metrics, 0 if standby",
            value=1.0 if is_leader else 0.0,
        )
        # A standby (peering-paused) replica still holds the rows it cached while it
        # was briefly active; exporting them would duplicate the active pod's series.
        if not is_leader:
            return

        # Take a snapshot of current entries. `dict(self._entries)` creates
        # a shallow copy so we can iterate safely outside the lock.
        with self._global_lock:
            snapshot = dict(self._entries)

        # --- Step 1: read all entries under their individual locks ---
        # We collect plain data into a list so that no locks are held
        # during the metric-building phase below.
        reads: list[dict[str, Any]] = []
        for key, entry in snapshot.items():
            with entry.lock:
                reads.append(
                    {
                        "key": key,
                        "up": entry.up,
                        "expired": entry.expired,
                        "rows": entry.rows,
                        "last_success_ts": entry.last_success_ts,
                        "duration": entry.last_duration_seconds,
                        "inflight": entry.inflight,
                        "skipped_total": entry.skipped_total,
                        "name": entry.name,
                        "help": entry.help,
                        "static_labels": dict(entry.labels),  # copy, not reference
                        "invalid_reason": entry.invalid_reason,
                    }
                )

        # Invalid entries (reserved label / prefix collision) emit nothing —
        # neither system nor user samples — until the spec is fixed.
        active = [r for r in reads if not r["invalid_reason"]]

        # --- Step 2: system metrics ---
        # One family per system metric name; one sample (add_metric call) per CRD.
        # Label schema is fixed: only `query_key`. Static labels from CRD spec
        # are intentionally excluded here — they belong on user metrics only.
        up_fam = GaugeMetricFamily(
            self._sys_name("query_up"),
            "1 if the last ClickHouseQuery execution succeeded, 0 otherwise",
            labels=["query_key"],
        )
        ts_fam = GaugeMetricFamily(
            self._sys_name("query_last_success_timestamp_seconds"),
            "Unix timestamp of the last successful ClickHouseQuery execution",
            labels=["query_key"],
        )
        dur_fam = GaugeMetricFamily(
            self._sys_name("query_duration_seconds"),
            "Duration of the last ClickHouseQuery execution in seconds",
            labels=["query_key"],
        )
        # query_up covers EVERY registered query — invalid ones report 0 so a
        # misconfiguration stays alertable in Prometheus (not only via .status).
        # An entry that was valid before an invalidating edit keeps stale runtime
        # state (up=True, last_success), so force 0 rather than trusting r["up"].
        for r in reads:
            up = 0.0 if r["invalid_reason"] else (1.0 if r["up"] else 0.0)
            up_fam.add_metric([r["key"]], up)
        # last-success / duration describe an actual execution; invalid queries
        # never run (and stale pre-edit values would mislead), so these — like
        # inflight / skipped below — stay valid-only.
        for r in active:
            if r["last_success_ts"] is not None:
                ts_fam.add_metric([r["key"]], r["last_success_ts"])
            if r["duration"] is not None:
                dur_fam.add_metric([r["key"]], r["duration"])

        # Only yield families that have at least one sample — empty families
        # produce noise in /metrics output and break test assertions.
        if up_fam.samples:
            yield up_fam
        if ts_fam.samples:
            yield ts_fam
        if dur_fam.samples:
            yield dur_fam

        inflight_fam = GaugeMetricFamily(
            self._sys_name("query_inflight"),
            "Number of in-flight executions for this ClickHouseQuery",
            labels=["query_key"],
        )
        skipped_fam = CounterMetricFamily(
            self._sys_name("query_skipped_total"),
            "Ticks skipped because maxConcurrent was reached",
            labels=["query_key"],
        )
        for r in active:
            inflight_fam.add_metric([r["key"]], r["inflight"])
            skipped_fam.add_metric([r["key"]], r["skipped_total"])
        if inflight_fam.samples:
            yield inflight_fam
        if skipped_fam.samples:
            yield skipped_fam

        # --- Step 3: user metrics, grouped by metric name ---
        # Multiple CRDs can share the same metric.name. They are merged into
        # one GaugeMetricFamily. The label schema is the UNION of all static
        # and dynamic label keys across every CRD that shares this name.
        # Missing keys for a given row are filled with "" (empty string).
        #
        # Why union? Because GaugeMetricFamily requires all add_metric() calls
        # to use the same label_names list (same length and order). If CRD A
        # has static label `env` and CRD B has `region`, we need both in the
        # schema; A's rows get region="" and B's rows get env="".
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in active:
            if not r["expired"] and r["rows"]:
                groups[r["name"]].append(r)

        for metric_name, group in groups.items():
            # Compute the union of all static + dynamic label keys in this group.
            # `sorted()` ensures a deterministic key order across scrapes.
            all_static_keys = sorted({k for r in group for k in r["static_labels"]})
            all_dynamic_keys = sorted(
                {k for r in group for row in r["rows"] for k in row.dynamic_labels}
            )
            # `query_key` is always last — it uniquely identifies the CRD resource.
            label_names = all_static_keys + all_dynamic_keys + ["query_key"]

            # Use the HELP text from the first registered entry with this name.
            # If two CRDs share a metric name but have different help text,
            # the first one wins and a warning is logged.
            help_texts = [r["help"] for r in group]
            if len(set(help_texts)) > 1:
                logger.warning(
                    "Metric %r has inconsistent HELP text across CRDs: %s — using first",
                    metric_name,
                    set(help_texts),
                )
            fam = GaugeMetricFamily(metric_name, help_texts[0], labels=label_names)

            for r in group:
                static_vals = [r["static_labels"].get(k, "") for k in all_static_keys]
                for row in r["rows"]:
                    dynamic_vals = [row.dynamic_labels.get(k, "") for k in all_dynamic_keys]
                    label_values = static_vals + dynamic_vals + [r["key"]]
                    fam.add_metric(label_values, row.value)

            yield fam
