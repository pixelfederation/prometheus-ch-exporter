# prometheus-ch-exporter

A Kubernetes operator that runs ClickHouse SQL queries on a schedule and exposes
the results as Prometheus metrics. Queries and cluster connections are declared
as Kubernetes custom resources, so metric configuration is declarative and
GitOps-friendly.

## How it works

```
ClickHouseConnection (cluster-scoped CRD)   ClickHouseQuery (namespaced CRD)
        │  seed hosts, cluster name                │  connectionRef, SQL, metric
        ▼                                          ▼
   ┌──────────────────────── operator (kopf) ────────────────────────┐
   │  • discovers cluster nodes from system.clusters + health-checks  │
   │  • runs each query on a fixed schedule against its connection    │
   │  • caches results; reflects health into each resource's .status  │
   └───────────────────────────────┬──────────────────────────────────┘
                                    ▼
                        /metrics  ◄── Prometheus scrape
```

- A **`ClickHouseConnection`** describes one ClickHouse cluster: a few seed hosts
  and the cluster name. The operator discovers the full node list from
  `system.clusters`, health-checks each node, and keeps a live view in the
  resource's `.status`.
- A **`ClickHouseQuery`** references a connection by name and defines an SQL query
  plus the Prometheus metric to expose. **Data** queries run on one node (with
  failover); **system** queries fan out to every node (adding a `clickhouse_node`
  label).
- Results are cached and served to Prometheus; a query that fails keeps serving
  its last value for a while, then is removed so stale data can't mislead.

## Install

```bash
# From a published release (chart hosted as an OCI artifact on GHCR):
helm install prometheus-ch-exporter \
  oci://ghcr.io/pixelfederation/charts/prometheus-ch-exporter \
  -n monitoring --create-namespace

# ...or from a local checkout of this repo:
helm install prometheus-ch-exporter ./charts/prometheus-ch-exporter \
  -n monitoring --create-namespace
```

The chart installs the CRDs, a Deployment, RBAC (ClusterRole + binding), a
metrics Service, and — optionally — a ServiceMonitor. See
[`charts/prometheus-ch-exporter/values.yaml`](charts/prometheus-ch-exporter/values.yaml)
for all values.

> Helm installs CRDs only on first `install` and never updates them on `upgrade`.
> After changing a CRD schema, apply it manually:
> `kubectl apply -f charts/prometheus-ch-exporter/crds/`.

## Define a connection

`ClickHouseConnection` is **cluster-scoped** and referenced by name from any
namespace.

```yaml
apiVersion: prometheus-ch-exporter.io/v1alpha1
kind: ClickHouseConnection
metadata:
  name: cluster
spec:
  clusterName: my_cluster        # must match a cluster in system.clusters
  seedHosts:                     # bootstrap only; the rest is discovered
    - "10.0.0.1"
  port: 8123
  username: default
  # livenessMode: active         # active (default) = ping all nodes each tick
  #                              # passive = only re-check dead nodes
  # connectTimeout: 10           # seconds
  # maxFailovers: null           # null = try all live nodes
```

Check discovery:

```bash
kubectl get clickhouseconnections
# NAME      PHASE     ALIVE   TOTAL   AGE
# cluster   Healthy   6       6       2m
```

## Define a query

Every query's SQL must return a numeric **`value`** column. Every other column
becomes a Prometheus label; each result row becomes one time series.

**Data query** — runs on a single node, one time series:

```yaml
apiVersion: prometheus-ch-exporter.io/v1alpha1
kind: ClickHouseQuery
metadata:
  name: errors-by-app
  namespace: monitoring
spec:
  connectionRef: cluster
  queryType: data
  interval: "60s"
  query: |
    SELECT count() AS value, app
    FROM logs.events
    WHERE level >= 500 AND ts >= now() - INTERVAL 1 MINUTE
    GROUP BY app
  metric:
    name: app_errors
    help: "Server errors per app in the last minute"
    labels:
      env: production        # static labels added to every series
```

**System query** — fans out to every node, adds a `clickhouse_node` label (the
label name is configurable, see [Metric naming](#metric-naming--reserved-labels)):

```yaml
apiVersion: prometheus-ch-exporter.io/v1alpha1
kind: ClickHouseQuery
metadata:
  name: uptime-node
  namespace: monitoring
spec:
  connectionRef: cluster
  queryType: system
  interval: "30s"
  query: "SELECT uptime() AS value"
  metric:
    name: node_uptime_seconds        # emitted as clickhouse_node_uptime_seconds
    help: "ClickHouse uptime in seconds, per node"
```

The example above exposes (every metric name is namespaced under the configured
prefix, default `clickhouse`):

```
clickhouse_app_errors{env="production",app="checkout",query_key="monitoring/errors-by-app"} 12
clickhouse_node_uptime_seconds{clickhouse_node="10.0.0.1",query_key="monitoring/uptime-node"} 512345
```

Every series carries a `query_key="<namespace>/<name>"` label identifying the
resource that produced it. Queries that share a `metric.name` are merged into one
metric family.

## Metrics

Besides your user metrics, the operator always exposes the following (shown with
the default `clickhouse` prefix; they follow whatever `metricPrefix` you set):

| Metric | Labels | Meaning |
|---|---|---|
| `clickhouse_query_up` | `query_key` | 1 if the last run succeeded, else 0 |
| `clickhouse_query_last_success_timestamp_seconds` | `query_key` | Unix time of last success |
| `clickhouse_query_duration_seconds` | `query_key` | Last run duration |
| `clickhouse_query_inflight` | `query_key` | Executions currently running |
| `clickhouse_query_skipped_total` | `query_key` | Ticks skipped because `maxConcurrent` was reached |

## Metric naming & reserved labels

Every metric this exporter emits — your user metrics **and** the operator's own
`clickhouse_query_*` metrics — is namespaced under a global prefix, set with
`metricPrefix` (env `PROMCH_METRIC_PREFIX`, default `clickhouse`). A single `_`
joins the prefix and the name; a trailing `_` in the prefix is ignored. Set it to
an empty string to disable prefixing entirely.

`spec.metric.name` is the **unprefixed** name — the operator prepends the prefix.
If your name already starts with `<prefix>_`, `spec.metric.prefixPolicy` decides
what happens:

| `prefixPolicy` | Behaviour when the name already starts with the prefix |
|---|---|
| `fail` (default) | The query is marked **`Invalid`** and emits no metric |
| `skip` | Keep the name as-is (no double prefix) |
| `append` | Prepend the prefix anyway (e.g. `clickhouse_clickhouse_foo`) |

**Reserved labels** — the exporter injects these and rejects any attempt to set
them yourself (the resource goes `Invalid`, or the apply is rejected by the CRD):

- `query_key` — always; identifies the resource (`<namespace>/<name>`). Rejected
  both as a static `metric.labels` key and as a query result column.
- the node label (`nodeLabel`, env `PROMCH_NODE_LABEL`, default `clickhouse_node`)
  — on `system` queries only; carries the source node. Rejected as a static
  label and as a result column on system queries.

## Configuration

Operator-wide defaults are set via environment variables (Helm `operator.*`
values). A `ClickHouseQuery` may override `interval`, `timeout`, and
`maxConcurrent` per query.

| Env var | Default | Description |
|---|---|---|
| `PROMCH_DEFAULT_INTERVAL` | `60s` | Default query interval |
| `PROMCH_DEFAULT_TIMEOUT` | `30s` | Default query timeout |
| `PROMCH_DEFAULT_MAX_CONCURRENT` | `2` | Max overlapping runs of one query |
| `PROMCH_STATUS_INTERVAL` | `15s` | Status reflection interval |
| `PROMCH_LAST_ERROR_TTL` | `10m` | How long a `lastError` stays visible after recovery |
| `PROMCH_EXPIRE_AFTER_FAILURES` | `5` | Consecutive failures before a metric is removed |
| `PROMCH_RECHECK_INTERVAL` | `60` | Node liveness re-check interval (seconds) |
| `PROMCH_TOPOLOGY_INTERVAL` | `1800` | Topology discovery interval (seconds) |
| `PROMCH_METRICS_PORT` | `8080` | Prometheus `/metrics` port |
| `PROMCH_HEALTH_PORT` | `8081` | Liveness `/healthz` port |
| `PROMCH_LOG_LEVEL` | `INFO` | Log level |
| `PROMCH_METRIC_PREFIX` | `clickhouse` | Namespace prepended to every metric (see below); empty disables it |
| `PROMCH_NODE_LABEL` | `clickhouse_node` | Label carrying the source node on system queries |

## Status

Each resource reports health in its `.status` (see `kubectl describe`):

- **Connection:** `phase` (`Healthy` / `Degraded` / `Down`), alive/total node
  counts, and a per-node list with `inCluster` / `alive` / `lastChecked`.
- **Query:** `phase` (`Pending` / `Healthy` / `Degraded` / `Failing` /
  `Expired` / `Invalid`), `lastSuccess`, `lastError`, `failedNodes`, and
  conditions (`QuerySucceeded`, `AllNodesResponding`, `KeepingUp`, `Ready`, and
  `Valid` when misconfigured). `Invalid` means a static misconfiguration
  (reserved label or prefix collision) — see below; it emits no metric until
  fixed. Phase transitions are also emitted as Kubernetes Events.

## Contributing / architecture

See [`AGENT.md`](AGENT.md) for the architecture, design rationale, and
development reference.

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
