import re


def parse_duration_seconds(value: str) -> float:
    """Convert a duration string to seconds.

    Examples:
        "30s" → 30.0
        "5m"  → 300.0
        "1h"  → 3600.0
    """
    match = re.fullmatch(r"(\d+)(s|m|h)", value)

    if not match:
        raise ValueError(f"Invalid duration: {value!r}. Expected e.g. 30s, 5m, 1h.")

    amount = int(match.group(1))
    unit = match.group(2)

    multipliers = {"s": 1, "m": 60, "h": 3600}
    return amount * multipliers[unit]
