"""Exception taxonomy for the ClickHouse client.

The facade decides failover vs. fail-fast purely by which of these it catches.
"""


class NodeConnectionError(Exception):
    """A node-level connection or connect-timeout failure. Triggers failover."""


class NodeQueryError(Exception):
    """A query-level failure — SQL error or query (execution) timeout.

    Never retried across nodes: a bad or slow query behaves the same everywhere.
    """


class NoLiveNodesError(Exception):
    """No live node is available to serve a query."""
