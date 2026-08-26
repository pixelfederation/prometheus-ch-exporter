# AGENT.md — prometheus-ch-exporter

Architecture and development reference for contributors and AI assistants. Keep
this in sync with the code when behavior changes.

## Overview

A Kubernetes operator (built on [kopf](https://kopf.readthedocs.io/)) that runs
ClickHouse SQL queries on a schedule and exposes the results as Prometheus
metrics. Two custom resources drive it:

- **`ClickHouseConnection`** (cluster-scoped) — one ClickHouse cluster: seed
  hosts + cluster name. The operator discovers the full node set from
  `system.clusters`, health-checks nodes, and reports topology/health in
  `.status`.
- **`ClickHouseQuery`** (namespaced) — references a connection by name, defines
  SQL + the Prometheus metric. `queryType: data` runs on one node with failover;
  `queryType: system` fans out to every node (adds a `node` label).

API group `prometheus-ch-exporter.io`, version `v1alpha1`. The operator runs
`clusterwide` and `standalone` (no peering CRD, single replica — no leader
election).

## Repository layout

```
charts/prometheus-ch-exporter/
  crds/            clickHouseConnection.yaml, clickHouseQuery.yaml (camelCase)
  templates/       deployment, RBAC, service, servicemonitor, connection & query resources
src/promch/
  clickhouse/      client library: node.py, topology.py, connection.py, types.py, errors.py
  collector/       metrics.py — Prometheus custom collector (cache/expiry/labels)
  handlers/        connection.py, query.py, query_status.py — kopf handlers + pure helpers
  config.py        OperatorConfig (env → PROMCH_*)
  __main__.py      kopf.run entrypoint + liveness endpoint
tests/unit/        unit tests (the CI layer)
tests/integration/ live tests against a real cluster (marker-gated)
deploy/examples/   example CRs
```

## ClickHouse client (`src/promch/clickhouse/`)

Three cooperating classes, each with one responsibility, and a `client_factory`
seam so logic is unit-testable without a real ClickHouse.

- **`Node`** (`node.py`) — one cluster node. Lazy async client
  (`clickhouse_connect`), owns its own health (`alive`, `last_checked`,
  `last_error`, `in_cluster`). Classifies raw exceptions into `NodeConnectionError`
  (connection/connect-timeout → failover) vs `NodeQueryError` (SQL error / query
  timeout → do not fail over). Drops its client on any connection failure so the
  next probe reconnects fresh (avoids a pooled keepalive making `ping` falsely
  succeed while queries fail). A per-node lock makes client creation
  concurrency-safe.
- **`ClusterTopology`** (`topology.py`) — the live node pool. **Membership** is
  authoritative from `system.clusters`; **liveness** is from ping. A node that is
  a member but unreachable stays listed as down (maintenance/outage); a declared
  **seed** that is not a member is kept and flagged (`in_cluster=false`, config
  drift), never silently dropped. Liveness modes: `active` (default) pings all
  nodes each tick; `passive` only re-checks dead nodes + idle-evicts. Pings run
  in parallel so N blocked nodes cost one connect timeout, not N.
- **`ClickHouseConnection`** (`connection.py`) — the facade. `execute_data_query`
  (random live node + failover, `NodeQueryError` re-raised), `execute_system_query`
  (fan-out; partial result; stamps `node`; `failed_nodes` includes down members —
  before discovery, seeds count as members; all-down → every member reported),
  `status_snapshot`, and discovery entrypoints (`refresh`/`recheck`/`rediscover`)
  serialized by a discovery lock. `ready` flips true after the first discovery
  cycle.

Errors: `NodeConnectionError`, `NodeQueryError`, `NoLiveNodesError`. `NodeConnectionError`
never escapes the connection — failover is internal; callers see only
`NoLiveNodesError` (all down) or `NodeQueryError`.

## Query contract

SQL must return a numeric **`value`** column. Every other column becomes a
dynamic label; each row → one time series. Static labels from
`spec.metric.labels` merge into every row. For `system` queries a `node` label is
added; a result already containing a `node` column is an error.

Note (ClickHouse 25.x analyzer): a `WITH col AS toString(x)` alias reused in
`SELECT`/`GROUP BY` can fail (`UNKNOWN_IDENTIFIER`), and an output alias that
shadows a source column can fail `GROUP BY` (`NOT_AN_AGGREGATE`). Inline the
expressions or wrap the transform in a subquery with distinct label names.

## Scheduling & status (`src/promch/handlers/query.py`)

Execution and status reporting are **decoupled**:

- **`@kopf.daemon query_scheduler`** (per query) — fires a fire-and-forget
  execution every `interval` (re-reads `spec` each tick, so edits apply live),
  overlapping up to `maxConcurrent`; excess ticks are **skipped** (not failed).
  It waits for `ClickHouseConnection.ready` before running (avoids a spurious
  `NoLiveNodes` right after a restart). Updates the in-memory collector; never
  patches status.
- **`@kopf.timer status_reflector`** (per query) — fixed interval
  (`PROMCH_STATUS_INTERVAL`), reads a collector snapshot, derives `.status` (pure
  `derive_status` in `query_status.py`), and patches only changed keys via kopf's
  `patch` (no direct k8s API). Emits a Kubernetes Event on a material change
  (phase / failed-node set / error type); k8s aggregates repeats.

Query phases: `Pending` (warming up / waiting for connection), `Healthy`,
`Degraded` (up but some members down → `failedNodes`), `Failing`, `Expired`.
`consecutiveFailures` lives in the collector (single source of truth); `lastError`
is retained for `PROMCH_LAST_ERROR_TTL` after recovery, then cleared.

## Collector (`src/promch/collector/metrics.py`)

A `prometheus_client` custom collector, thread-safe (per-entry + global locks),
because query execution (asyncio) and scrapes (HTTP server thread) run
concurrently. It caches last-good rows, serves stale data while a query is in
flight, removes expired metrics, and merges queries that share a metric name
(label union, missing keys → `""`). `tick_ts` ordering discards a slow older run
overwriting a newer one. System metrics: `ch_query_up`,
`ch_query_last_success_timestamp_seconds`, `ch_query_duration_seconds`,
`ch_query_inflight`, `ch_query_skipped_total`. Every user series gets a
`query_key="<namespace>/<name>"` label.

## Configuration

Env vars, prefix `PROMCH_` (see README for the full table). Connection details
come from the `ClickHouseConnection` CRD, **not** from env. `recheckInterval` /
`topologyInterval` are read with `float()` — plain seconds, not duration strings.

## Deployment

- Multi-stage `Dockerfile` (python:3.12-slim, non-root uid 1000, `python -m promch`).
- Helm chart: Deployment (liveness `/healthz` on 8081, readiness TCP on metrics
  8080), ClusterRole (CRDs + their `/status` + `events`; plus `customresourcedefinitions`
  and `namespaces` list/watch for kopf), Service, optional ServiceMonitor, and
  optional `ClickHouseConnection` / `ClickHouseQuery` resources from values.
- Image published to GHCR via `.github/workflows/docker-publish.yml` (multi-arch).

## Testing

- `tests/unit/` — the CI layer. Pure/logic units: client classes (via fake
  `client_factory`), collector, `build_rows`/`derive_status`, connection handler
  mapping. Run: `.venv/bin/pytest tests/unit/ -q`.
- `tests/integration/test_clickhouse.py` — marker-gated live tests (set
  `PROMCH_IT_SEED`), outside CI.
- Lint/type: `.venv/bin/ruff check src/ tests/` and `.venv/bin/mypy src/promch/`.
- TDD for pure parts; daemon/reflector/kopf handlers are validated by live tests.

## Security

- Non-root container, read-only root FS, dropped caps, seccomp `RuntimeDefault`.
- RBAC is least-privilege (the resources above, plus `get` on Secrets scoped to
  the operator's own namespace, for ClickHouse auth).
- ClickHouse auth: credentials from an auth Secret via `spec.authSecretRef`
  (username + password); TLS via `secure`/`verify`.

## Known limitations / future work

- Per-connection `recheckInterval`/`topologyInterval` are not wired to the kopf
  timers (kopf reads timer intervals at import time); they are process-wide env
  values for now.
- Single replica (no leader election / HA).
- Deferred: global/per-group query barrier, a `system.processes` cluster-load
  metric, validation/conversion webhooks.
