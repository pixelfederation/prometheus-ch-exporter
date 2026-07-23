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

## Security Design Notes

See `AGENT.md` — Security Model section for the full threat model, RBAC design, and supply chain security measures.

Key points:
- Operator runs non-root (UID 1000) with read-only root filesystem
- ClickHouse credentials are injected via Kubernetes Secrets, never embedded in CRD specs or logs
- RBAC follows least-privilege; operator has no access to arbitrary Secrets
- TLS for ClickHouse connections is optional but strongly recommended for external hosts
