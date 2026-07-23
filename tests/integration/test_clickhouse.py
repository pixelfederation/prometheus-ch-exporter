"""Integration tests for the ClickHouse client against a real cluster.

Skipped unless PROMCH_IT_SEED is set. Run against your cluster with, e.g.:

    PROMCH_IT_SEED=10.0.0.1 PROMCH_IT_CLUSTER=my_cluster \
        .venv/bin/pytest tests/integration/ -v

Not part of the unit CI job (which runs tests/unit/ only).
"""

import os

import pytest

from promch.clickhouse.connection import ClickHouseConnection
from promch.clickhouse.types import ConnectionSpec

pytestmark = pytest.mark.skipif(
    not os.getenv("PROMCH_IT_SEED"),
    reason="PROMCH_IT_SEED not set (integration test needs a reachable ClickHouse node)",
)


def _spec() -> ConnectionSpec:
    return ConnectionSpec(
        seed_hosts=os.environ["PROMCH_IT_SEED"].split(","),
        cluster_name=os.getenv("PROMCH_IT_CLUSTER", "default"),
        port=int(os.getenv("PROMCH_IT_PORT", "8123")),
        username=os.getenv("PROMCH_IT_USER", "default"),
        password=os.getenv("PROMCH_IT_PASSWORD", ""),
    )


async def test_discovery_finds_cluster_members() -> None:
    conn = ClickHouseConnection(_spec())
    try:
        await conn.recheck()  # bring seed(s) up
        await conn.rediscover()  # read topology from system.clusters
        status = conn.status_snapshot()
        assert status.alive_nodes >= 1
        assert any(n.in_cluster for n in status.nodes)
    finally:
        await conn.close()


async def test_data_query_returns_value() -> None:
    conn = ClickHouseConnection(_spec())
    try:
        await conn.recheck()
        rows = await conn.execute_data_query("SELECT 1 AS value", timeout=10)
        assert len(rows) == 1
        assert rows[0]["value"] == 1
    finally:
        await conn.close()


async def test_system_query_fans_out_to_all_nodes() -> None:
    conn = ClickHouseConnection(_spec())
    try:
        await conn.recheck()
        await conn.rediscover()
        result = await conn.execute_system_query("SELECT 1 AS value", timeout=10)
        assert len(result.rows) >= 1
        # every row is stamped with its source node; one row per responding node
        assert all("node" in row for row in result.rows)
        assert len({row["node"] for row in result.rows}) == len(result.rows)
    finally:
        await conn.close()
