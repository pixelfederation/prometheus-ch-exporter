# syntax=docker/dockerfile:1

# ---- builder ----
# Base image pinned by digest for reproducibility / supply-chain; Dependabot
# bumps the digest. Tag kept in the reference for readability.
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de AS builder
WORKDIR /build
COPY pyproject.toml .
COPY LICENSE README.md .
COPY src/ src/
RUN pip install --no-cache-dir build \
    && python -m build --wheel --outdir /build/dist

# ---- runtime ----
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de AS runtime
# Debian already ships a system group named "operator" (gid 37), so create a
# distinct group/user at uid/gid 1000 to match the chart's runAsUser/runAsGroup.
RUN groupadd -g 1000 promch \
    && useradd -u 1000 -g 1000 -M -s /usr/sbin/nologin promch
WORKDIR /app
COPY --from=builder /build/dist/*.whl .
RUN pip install --no-cache-dir *.whl && rm -f *.whl
USER promch
# 8080 = Prometheus /metrics, 8081 = kopf liveness /healthz
EXPOSE 8080 8081
ENTRYPOINT ["python", "-m", "promch"]
