import logging
import os

import kopf

from . import handlers  # noqa: F401 — registers kopf handlers via decorators


def main() -> None:
    logging.basicConfig(level="INFO")
    health_port = int(os.environ.get("PROMCH_HEALTH_PORT", "8081"))
    # standalone=True: no KopfPeering CRD needed for a single-replica operator.
    # liveness_endpoint lets Kubernetes restart a wedged operator; kopf serves it
    # on its own aiohttp server (separate from the Prometheus /metrics port).
    kopf.run(
        clusterwide=True,
        standalone=True,
        liveness_endpoint=f"http://0.0.0.0:{health_port}/healthz",
    )


if __name__ == "__main__":
    main()
