import re

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .utils import parse_duration_seconds


class OperatorConfig(BaseSettings):
    """All operator configuration, read from environment variables.

    pydantic-settings automatically maps env var PROMCH_CLICKHOUSE_HOST
    to the `clickhouse_host` field (prefix stripped, case-insensitive).
    """

    # `model_config` is a special class-level attribute that pydantic reads
    # to configure how the whole class behaves. It is NOT an instance field —
    # it applies to the class itself.
    #
    # `env_prefix="PROMCH_"` means every field below is read from an env var
    # with that prefix. E.g. `clickhouse_host` → env var `PROMCH_CLICKHOUSE_HOST`.
    model_config = SettingsConfigDict(env_prefix="PROMCH_", case_sensitive=False)

    # --- Field declarations ---
    # Each line below declares one field on the class.
    # Format:  field_name: type = default_value
    # If there is no default, the field is REQUIRED (no default → must be provided).
    #
    # `str` = string, `int` = integer, `bool` = True/False, `float` = decimal number
    # `str | None` = either a string or None (Python 3.10+ union syntax)

    # Query scheduling (defaults; a ClickHouseQuery spec may override per query)
    default_interval: str = "60s"
    default_timeout: str = "30s"
    default_max_concurrent: int = 2  # max overlapping runs of one query
    status_interval: str = "15s"  # how often the status reflector runs
    last_error_ttl: str = "10m"  # keep lastError visible this long after recovery

    # Reliability
    expire_after_failures: int = 5
    # global_barrier: bool = False

    # Status / error history
    error_history_window_minutes: int = 30
    max_error_history_entries: int = 50

    # HTTP server
    metrics_port: int = 8080

    log_level: str = "INFO"

    # Metric naming (global; a forced org-wide namespace)
    metric_prefix: str = "clickhouse"  # "" disables prefixing; trailing "_" ignored
    node_label: str = "clickhouse_node"  # label carrying the source node on system queries

    # --- @field_validator ---
    # A decorator that runs a function automatically when pydantic sets this field.
    # Here we validate that the duration string is well-formed BEFORE the object
    # is fully constructed. If it raises ValueError, pydantic aborts with a clear error.
    #
    # `@classmethod` — this method belongs to the CLASS, not to an instance.
    # Pydantic requires validators to be classmethods. `cls` refers to the class
    # itself (similar to `self` for instance methods).
    @field_validator("default_interval", "default_timeout", "status_interval", "last_error_ttl")
    @classmethod
    def validate_duration(cls, v: str) -> str:
        parse_duration_seconds(v)
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        # `logging` only accepts the canonical upper-case level names, so a
        # value like "debug" would crash basicConfig at startup. Normalise to
        # upper-case and reject anything unknown with a clear message.
        level = v.strip().upper()
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if level not in allowed:
            raise ValueError(f"log_level {v!r} must be one of {', '.join(sorted(allowed))}")
        return level

    @field_validator("metric_prefix")
    @classmethod
    def validate_metric_prefix(cls, v: str) -> str:
        if v and not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", v):
            raise ValueError(f"metric_prefix {v!r} must be empty or match [a-zA-Z_][a-zA-Z0-9_]*")
        return v

    @field_validator("node_label")
    @classmethod
    def validate_node_label(cls, v: str) -> str:
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", v):
            raise ValueError(f"node_label {v!r} is not a valid Prometheus label name")
        if v == "query_key":
            raise ValueError("node_label must not be the reserved label 'query_key'")
        return v

    # --- @property ---
    # A property looks like a plain attribute from the outside (`config.default_interval_seconds`)
    # but actually runs a function every time it is accessed. Useful for derived values
    # that we don't want to store separately.
    #
    # `self` — inside instance methods and properties, `self` refers to the specific
    # object you called the method on. It gives you access to that object's fields.
    @property
    def default_interval_seconds(self) -> float:
        return parse_duration_seconds(self.default_interval)

    @property
    def default_timeout_seconds(self) -> float:
        return parse_duration_seconds(self.default_timeout)

    @property
    def status_interval_seconds(self) -> float:
        return parse_duration_seconds(self.status_interval)

    @property
    def last_error_ttl_seconds(self) -> float:
        return parse_duration_seconds(self.last_error_ttl)

    @property
    def metric_prefix_normalized(self) -> str:
        """Prefix with any trailing underscore removed (join uses a single '_')."""
        return self.metric_prefix.rstrip("_")
