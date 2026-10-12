# Security

## Reporting a vulnerability

Please report security problems privately, not in a public issue: use **Report a vulnerability** on this repository's **Security** tab (GitHub private vulnerability reporting). Include what you found, how to reproduce it, and what it could let someone do.

## What to know before deploying

This API changes live subscriber equipment, so a weakness here can take customers off the network. Before pointing it at a real CMS or E7:

- **Set `API_KEYS`** with scopes (`name:key:read+change`), one key per caller, each with only what it needs. Without `API_KEYS` the API refuses every request, except in `MOCK_MODE`, where it is open.
- **Use HTTPS to CMS** (`CMS_SCHEME=https`) and keep certificate checks on. Keep SSH host-key checks on for E7s (`E7_KNOWN_HOSTS`).
- **Keep secrets out of the repository.** `.env` is ignored by git. In Kubernetes, the CMS login and API keys come from Secrets (`deploy/k8s/gpon-cms.yaml`).
- **Lock down Kafka.** The events topic is trusted: anything that can write to it can provision ONTs. Restrict producers with ACLs.
- **Scrub captures before committing them.** Output from `app.tools.e7_capture` and `cms_check --dump` can contain customer names and serial numbers; remove them first.

[docs/KNOWN_UNKNOWNS.md](docs/KNOWN_UNKNOWNS.md) lists every known open risk, and [docs/ANTI_PATTERNS.md](docs/ANTI_PATTERNS.md) the operational ones.
