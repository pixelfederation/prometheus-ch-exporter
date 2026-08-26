"""A single ClickHouse cluster node: lazy connection, owns its own health state."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime

from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from .errors import NodeConnectionError, NodeQueryError
from .types import ClientFactory, ClientProtocol, ConnectionSpec, Row

logger = logging.getLogger(__name__)

# Transport-level failures that are not clickhouse-connect exceptions.
_TRANSPORT_ERRORS = (OSError, asyncio.TimeoutError)


class Node:
    """One node. `alive`/`last_error`/`last_checked` are owned here and set only
    by this object — callers read them, never write them (except tests)."""

    def __init__(
        self,
        host: str,
        spec: ConnectionSpec,
        client_factory: ClientFactory,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.host = host
        self.alive = False  # pessimistic: a ping must confirm liveness
        self.in_cluster = False  # confirmed present in system.clusters
        self.last_checked: datetime | None = None
        self.last_error: str | None = None
        self._spec = spec
        self._factory = client_factory
        self._clock = clock
        self._client: ClientProtocol | None = None
        self._last_used: float = clock()
        self._client_lock: asyncio.Lock = asyncio.Lock()

    async def _ensure_client(self) -> ClientProtocol:
        if self._client is None:
            async with self._client_lock:
                # Re-check under the lock: a concurrent caller may have connected
                # while we awaited the lock. Prevents duplicate (leaked) clients.
                if self._client is None:
                    try:
                        self._client = await self._factory(self.host, self._spec)
                    except (DatabaseError, *_TRANSPORT_ERRORS) as exc:
                        # DatabaseError (base) covers OperationalError (host down)
                        # AND plain DatabaseError like AUTHENTICATION_FAILED (516):
                        # any driver error while establishing the client means we
                        # have no usable connection, so treat it as node-down
                        # rather than letting it escape as an ugly traceback.
                        self._mark_down(exc)
                        raise NodeConnectionError(f"{self.host}: connect failed: {exc}") from exc
        return self._client

    def _mark_down(self, exc: BaseException) -> None:
        self.alive = False
        self.last_error = str(exc)
        self.last_checked = datetime.now(UTC)

    def _mark_used(self) -> None:
        self._last_used = self._clock()

    def _mark_alive(self) -> None:
        """A successful ping or query confirms the node is alive."""
        self.alive = True
        self.last_error = None
        self.last_checked = datetime.now(UTC)
        self._mark_used()

    async def _drop_client(self) -> None:
        """Close and discard the client after a connection failure, so the next
        probe reconnects fresh. Without this, a pooled keepalive socket can make
        ping() falsely succeed while real queries fail (node flapping up/down)."""
        client, self._client = self._client, None
        if client is not None:
            # Closing a broken client must not raise.
            with contextlib.suppress(Exception):
                await client.close()

    async def query(self, sql: str, timeout: float) -> list[Row]:
        client = await self._ensure_client()
        try:
            result = await client.query(sql, settings={"max_execution_time": int(timeout)})
        except OperationalError as exc:  # subclass of DatabaseError — catch first
            self._mark_down(exc)
            await self._drop_client()
            raise NodeConnectionError(f"{self.host}: {exc}") from exc
        except _TRANSPORT_ERRORS as exc:
            self._mark_down(exc)
            await self._drop_client()
            raise NodeConnectionError(f"{self.host}: {exc}") from exc
        except DatabaseError as exc:  # SQL error / query timeout — do not fail over
            raise NodeQueryError(f"{self.host}: {exc}") from exc
        self._mark_alive()
        columns = list(result.column_names)
        return [dict(zip(columns, raw, strict=False)) for raw in result.result_rows]

    async def ping(self) -> bool:
        try:
            client = await self._ensure_client()
            await client.ping()
        except (NodeConnectionError, OperationalError, *_TRANSPORT_ERRORS) as exc:
            self._mark_down(exc)
            await self._drop_client()
            logger.debug("node %s ping failed: %s", self.host, exc)
            return False
        self._mark_alive()
        logger.debug("node %s ping ok", self.host)
        return True

    def is_idle(self, ttl: float) -> bool:
        return self._client is not None and (self._clock() - self._last_used) > ttl

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
