from types import SimpleNamespace

import pytest

from promch.config import OperatorConfig
from promch.peering import configure_peering, current_pod_name, priority_from_pod_name


def test_priority_is_deterministic_and_in_range() -> None:
    p1 = priority_from_pod_name("promch-7d9f8b6c5-abc12")
    p2 = priority_from_pod_name("promch-7d9f8b6c5-abc12")
    assert p1 == p2
    assert 0 <= p1 < 2**31


def test_priority_differs_for_different_pods() -> None:
    a = priority_from_pod_name("promch-7d9f8b6c5-abc12")
    b = priority_from_pod_name("promch-7d9f8b6c5-xyz99")
    assert a != b


def test_current_pod_name_prefers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POD_NAME", "promch-abc")
    assert current_pod_name() == "promch-abc"


def _fake_settings() -> SimpleNamespace:
    return SimpleNamespace(
        peering=SimpleNamespace(
            standalone=False, mandatory=False, name="", lifetime=0.0, priority=0
        )
    )


def test_configure_peering_disabled_forces_standalone() -> None:
    settings = _fake_settings()
    config = OperatorConfig()  # type: ignore[call-arg]  # peering_enabled defaults False
    configure_peering(settings, config)  # type: ignore[arg-type]
    assert settings.peering.standalone is True


def test_configure_peering_enabled_sets_all(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMCH_PEERING_ENABLED", "true")
    monkeypatch.setenv("PROMCH_PEERING_NAME", "prometheus-ch-exporter")
    monkeypatch.setenv("PROMCH_PEERING_LIFETIME", "45")
    monkeypatch.setenv("POD_NAME", "promch-7d9f8b6c5-abc12")
    settings = _fake_settings()
    config = OperatorConfig()  # type: ignore[call-arg]
    configure_peering(settings, config)  # type: ignore[arg-type]
    assert settings.peering.standalone is False
    assert settings.peering.mandatory is True
    assert settings.peering.name == "prometheus-ch-exporter"
    assert settings.peering.lifetime == 45.0
    assert settings.peering.priority == priority_from_pod_name("promch-7d9f8b6c5-abc12")
