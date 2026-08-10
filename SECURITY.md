# Security Policy

## Reporting

Do not open a public issue containing access tokens, account details, private datasets, or exploit instructions. Use GitHub private vulnerability reporting and include the affected revision, impact, and a safe reproduction.

Revoke any credential that may have been disclosed. Market-data credentials belong only in ignored local environment files.

## Operational scope

Poly is paper-only and defaults to shadow mode with loopback dashboard access. It contains no credentialed execution or external order-submission interface.

The dashboard control endpoint is absent unless `CONTROL_TOKEN` is configured. When enabled, it requires Bearer authentication, and browser WebSocket handshakes must be same-origin.

Supported security fixes target the latest revision.
