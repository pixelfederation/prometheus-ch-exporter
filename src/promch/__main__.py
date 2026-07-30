import logging
import os

import kopf

from . import handlers  # noqa: F401 — registers kopf handlers via decorators


def main() -> None:
    logging.basicConfig(level="INFO")
    health_port = int(os.environ.get("PROMCH_HEALTH_PORT", "8081"))
    # Peering (leader election for HA) is enabled per-config in the startup
    # handler via configure_peering(); when disabled it forces standalone, so a
    # single replica behaves exactly as before. liveness_endpoint lets Kubernetes
    # restart a wedged operator on kopf's own aiohttp server (separate from the
    # Prometheus /metrics port).
    kopf.run(
        clusterwide=True,
        liveness_endpoint=f"http://0.0.0.0:{health_port}/healthz",
    )


if __name__ == "__main__":
    main()
