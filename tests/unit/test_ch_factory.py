import logging

import clickhouse_connect

from promch.clickhouse.types import ConnectionSpec, default_client_factory


async def test_factory_forwards_secure_and_verify(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_get_async_client(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(clickhouse_connect, "get_async_client", fake_get_async_client)
    spec = ConnectionSpec(seed_hosts=["h"], cluster_name="c", secure=True, verify=False)
    await default_client_factory("h", spec)
    assert captured["secure"] is True
    assert captured["verify"] is False


async def test_factory_warns_when_verify_disabled(monkeypatch, caplog) -> None:
    async def fake_get_async_client(**kwargs: object) -> object:
        return object()

    monkeypatch.setattr(clickhouse_connect, "get_async_client", fake_get_async_client)
    spec = ConnectionSpec(seed_hosts=["h"], cluster_name="c", verify=False)
    with caplog.at_level(logging.WARNING):
        await default_client_factory("h", spec)
    assert any("verify" in r.message.lower() for r in caplog.records)
