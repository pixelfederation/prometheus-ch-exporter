# Security Policy

## Supported Versions

During the PoC phase, only the latest commit on `main` is supported.

| Version | Supported |
|---------|-----------|
| main (PoC) | yes |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report vulnerabilities by emailing the maintainers directly (see `CODEOWNERS` or the repository contact listed on GitHub). Include:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You will receive an acknowledgement within 48 hours and a resolution timeline within 7 days.

## Threat model

The operator's core function is to run **user-supplied SQL** (from `ClickHouseQuery`
resources) against ClickHouse. This has direct security consequences you must
account for when deploying:

- **A `ClickHouseQuery` runs arbitrary SQL as the operator's ClickHouse user.**
  Whoever can *create* a `ClickHouseQuery` can execute any statement that user is
  permitted to run. Because the operator watches **all namespaces** and connects
  through a single shared `ClickHouseConnection`, namespace boundaries do **not**
  restrict what SQL runs — it all executes as one ClickHouse identity.

- **Use a read-only ClickHouse user.** Grant it only `SELECT` on the databases it
  must read (and `system.*` if you use `system` queries). This is the single most
  important control: it turns "arbitrary SQL" into "arbitrary reads" and prevents
  `INSERT` / `ALTER` / `DROP` / `SYSTEM` operations even if a malicious or careless
  query is submitted.

- **Treat `create` on `clickhousequeries` as ClickHouse query access.** Restrict it
  via Kubernetes RBAC to the teams that should be allowed to query ClickHouse.

- **Query text and ClickHouse errors are visible.** A query's SQL, and error
  messages returned by ClickHouse (which may echo query fragments), appear in the
  operator logs and in the resource's `.status` (`lastError`). Anyone with `get`
  on the resource can read them. Do not put secrets in query text.

- **`/metrics` is unauthenticated** (standard for Prometheus). Metric label values
  are query results, i.e. your data. Restrict access with a NetworkPolicy
  (ingress from Prometheus only) and/or scrape over the cluster network only.

## Deployment hardening

- Operator runs **non-root (UID/GID 1000)** with **read-only root filesystem**,
  all Linux capabilities dropped, and seccomp `RuntimeDefault`.
- **RBAC is least-privilege:** the operator can read/patch its own CRDs and their
  status, create/patch Events, and list/watch CRDs + namespaces (required by the
  framework). It has **no access to Secrets**.
- **TLS:** set `secure: true` on the `ClickHouseConnection` for ClickHouse reached
  over untrusted networks. Certificates are verified by default.
- **ClickHouse authentication:** password auth via a Kubernetes Secret
  (`passwordSecretRef`) is **not yet implemented**. Until it is, deploy the
  operator only where it can reach ClickHouse over a trusted path and the CH user
  is appropriately restricted. When added, the password will come only from a
  Secret — never from a CRD spec, logs, or status.

## Supply chain

- Container base image is pinned by digest; images are built multi-arch, **signed
  with cosign (keyless/OIDC)**, and published with an **SBOM and SLSA build
  provenance** attestation.
- Dependencies (Python, GitHub Actions, base image digest) are kept current by
  Dependabot; CI runs lint, type-check, tests, and a container build on every PR.

See `AGENT.md` — Security section — for the architecture-level view.
