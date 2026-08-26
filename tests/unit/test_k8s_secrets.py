import base64

import pytest

from promch.k8s_secrets import (
    InClusterSecretReader,
    SecretResolutionError,
    operator_namespace,
)


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


async def test_reads_and_decodes_all_keys() -> None:
    async def transport(ns: str, name: str) -> tuple[int, dict]:
        assert (ns, name) == ("ops", "ch-auth")
        return 200, {"data": {"username": _b64("chexporter"), "password": _b64("s3cret")}}

    reader = InClusterSecretReader(transport=transport)
    data = await reader.read_secret("ops", "ch-auth")
    assert data == {"username": "chexporter", "password": "s3cret"}


async def test_skips_non_utf8_keys() -> None:
    async def transport(ns: str, name: str) -> tuple[int, dict]:
        return 200, {"data": {"password": _b64("s3cret"), "blob": base64.b64encode(b"\xff\xfe").decode()}}

    reader = InClusterSecretReader(transport=transport)
    data = await reader.read_secret("ops", "ch-auth")
    assert data == {"password": "s3cret"}  # non-utf8 key skipped, not an error


async def test_missing_secret_raises() -> None:
    async def transport(ns: str, name: str) -> tuple[int, dict]:
        return 404, {}

    reader = InClusterSecretReader(transport=transport)
    with pytest.raises(SecretResolutionError, match="not found"):
        await reader.read_secret("ops", "nope")


async def test_non_200_raises() -> None:
    async def transport(ns: str, name: str) -> tuple[int, dict]:
        return 403, {}

    reader = InClusterSecretReader(transport=transport)
    with pytest.raises(SecretResolutionError, match="HTTP 403"):
        await reader.read_secret("ops", "ch-auth")


def test_operator_namespace_env_override(monkeypatch) -> None:
    monkeypatch.setenv("POD_NAMESPACE", "my-ns")
    assert operator_namespace() == "my-ns"
