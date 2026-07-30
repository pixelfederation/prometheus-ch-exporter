"""kopf peering configuration for high-availability (active/standby).

Kept free of kopf-handler code so the pure logic is unit-testable: `settings`
is duck-typed (any object exposing a `.peering` with the attributes below).
"""

from __future__ import annotations

import hashlib
import os
import socket

import kopf

from .config import OperatorConfig

# Priority space. kopf pauses peers of strictly-higher priority; equal priority
# makes ALL peers freeze, so replicas must differ. Pod names are unique per pod,
# so a hash over 2**31 gives distinct priorities with negligible collision risk.
_PRIORITY_SPACE = 2**31


def priority_from_pod_name(name: str) -> int:
    """Deterministic peering priority derived from the (unique) pod name."""
    return int(hashlib.sha1(name.encode()).hexdigest(), 16) % _PRIORITY_SPACE


def current_pod_name() -> str:
    """Pod name from the downward-API env var, falling back to the hostname
    (which equals the pod name by default in Kubernetes)."""
    return os.environ.get("POD_NAME") or socket.gethostname()


def configure_peering(settings: kopf.OperatorSettings, config: OperatorConfig) -> None:
    """Apply peering settings from config.

    Disabled -> force standalone (single-replica, today's behavior, no peering
    CRD/keep-alive). Enabled -> active/standby: mandatory peering (fail loudly if
    the peering CRD is absent rather than silently running all replicas active),
    a unique peering name (isolation from other kopf operators), and a
    per-replica priority derived from the pod name.
    """
    if not config.peering_enabled:
        settings.peering.standalone = True
        return
    settings.peering.standalone = False
    settings.peering.mandatory = True
    settings.peering.name = config.peering_name
    settings.peering.lifetime = int(config.peering_lifetime)
    settings.peering.priority = priority_from_pod_name(current_pod_name())
