# syntax=docker/dockerfile:1

# Pinned uv binary. Digest-pin it like the base image below (tag kept for
# readability). Get the digest with:
#   docker buildx imagetools inspect ghcr.io/astral-sh/uv:0.7.13
# then append  @sha256:<digest>  to the reference.
FROM ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1 AS uv

# ---- builder ----
# Base image pinned by digest for reproducibility / supply-chain; Dependabot
# bumps the digest. Tag kept in the reference for readability.
FROM python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
# Use the base image's Python (never let uv download its own); copy-mode links so
# the resulting .venv is self-contained and portable to the runtime stage.
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
# 1) Install locked runtime deps first (cache layer independent of source).
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project
# 2) Install the project itself, copied (non-editable) so runtime needs no src/.
COPY LICENSE README.md ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

# ---- runtime ----
FROM python:3.12-slim@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 AS runtime
# Debian already ships a system group named "operator" (gid 37), so create a
# distinct group/user at uid/gid 1000 to match the chart's runAsUser/runAsGroup.
RUN groupadd -g 1000 promch \
    && useradd -u 1000 -g 1000 -M -s /usr/sbin/nologin promch
WORKDIR /app
# Only the built virtualenv crosses into runtime — no uv, no source tree.
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER promch
# 8080 = Prometheus /metrics, 8081 = kopf liveness /healthz
EXPOSE 8080 8081
ENTRYPOINT ["python", "-m", "promch"]
