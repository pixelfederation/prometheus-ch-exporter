import pytest

from promch.config import OperatorConfig
from promch.utils import parse_duration_seconds


@pytest.mark.parametrize(
    "value,expected",
    [
        ("30s", 30.0),
        ("5m", 300.0),
        ("1h", 3600.0),
        ("120s", 120.0),
    ],
)
def test_parse_duration_seconds(value: str, expected: float) -> None:
    assert parse_duration_seconds(value) == expected


def test_parse_duration_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration_seconds("5d")


def test_operator_config_defaults() -> None:
    config = OperatorConfig()  # type: ignore[call-arg]

    assert config.default_interval_seconds == 60.0
    assert config.default_timeout_seconds == 30.0
    assert config.default_max_concurrent == 2
    assert config.status_interval_seconds == 15.0
    assert config.last_error_ttl_seconds == 600.0
    assert config.expire_after_failures == 5


def test_metric_prefix_and_node_label_defaults() -> None:
    config = OperatorConfig()  # type: ignore[call-arg]
    assert config.metric_prefix == "clickhouse"
    assert config.metric_prefix_normalized == "clickhouse"
    assert config.node_label == "clickhouse_node"


def test_metric_prefix_normalized_strips_trailing_underscore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROMCH_METRIC_PREFIX", "clickhouse_")
    config = OperatorConfig()  # type: ignore[call-arg]
    assert config.metric_prefix_normalized == "clickhouse"


def test_metric_prefix_empty_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMCH_METRIC_PREFIX", "")
    config = OperatorConfig()  # type: ignore[call-arg]
    assert config.metric_prefix_normalized == ""


def test_metric_prefix_invalid_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMCH_METRIC_PREFIX", "1bad-prefix")
    with pytest.raises(ValueError):
        OperatorConfig()  # type: ignore[call-arg]


def test_node_label_invalid_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMCH_NODE_LABEL", "bad-label")
    with pytest.raises(ValueError):
        OperatorConfig()  # type: ignore[call-arg]


def test_node_label_cannot_be_query_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMCH_NODE_LABEL", "query_key")
    with pytest.raises(ValueError):
        OperatorConfig()  # type: ignore[call-arg]
