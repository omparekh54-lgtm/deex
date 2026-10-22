# DEEX credential security hardening

The current full PMS backtest reads the historical database. Broker verification no longer leaves the API key or JWT in a session. PIN, TOTP, API key and JWT are request-scoped and discarded after verification; only a short-lived verification flag is retained. The browser clears secret input fields before posting, and the server never intentionally writes these values to logs, files, reports or database rows. Physical zeroization of Python strings is not guaranteed.

The deployed browser sends credentials to this server transiently over HTTPS, and the server verifies them with Angel One over HTTPS. This is not client-only authentication or a cryptographic guarantee against a compromised server, browser extension or device.

## Protections implemented

- Random, per-session CSRF tokens protect all mutations. JSON-only request parsing, pinned production host/origin and cross-site request checks apply before route execution.
- Production cookies use the Secure `__Host-` prefix, HttpOnly, SameSite Strict and no Domain attribute. Successful verification rotates the session identifier and CSRF token. Logout revokes previous session/report access and cancels owned work.
- Sessions expire after fifteen minutes of inactivity and cannot outlive two hours. Server-side cleanup also runs without browser activity. Broker verification metadata expires after ten minutes.
- Login throttles span sessions and accounts, with a global ceiling. Only bounded HMAC identifiers are stored in throttle buckets; direct account/IP values are not kept there. These local controls are not a distributed denial-of-service guarantee.
- Streamed and declared request bodies are limited to 16 KiB and fifteen seconds. API query strings and unnecessary Range requests are rejected. The server bounds connection concurrency and keep-alive time.
- Restrictive CSP, HSTS, no-store, framing, content-type, referrer and permissions policies are applied to normal responses and early rejections.
- Broker operations use fixed hosts and an explicit path allowlist. Certificate verification stays enabled. Redirects and environment proxy/CA overrides are disabled. Order endpoints are not permitted by this adapter.
- Error responses do not echo credential input or remote exceptions. Documentation endpoints remain disabled. Secret-bearing local files and Git metadata are excluded from Docker builds. The runtime uses an unprivileged user, suppresses access logs and omits the Uvicorn server banner.
- The complete Python runtime dependency set is pinned. Direct dependencies are recorded separately in requirements.in. docs/dependency-audit.json records the package audit for this change.

## Validation and limits

Automated checks exercise CSRF and cross-origin rejection, credential non-retention, cookie/token rotation, cross-session login throttling, broker TLS/proxy/redirect settings, blocked order paths, error redaction, idle and absolute expiry, report revocation, oversized and slow streamed requests, production HTTPS/host checks and dependency compatibility. Verification uses mocked broker responses; no real broker credentials were used for these tests.

The dependency audit reported no known vulnerabilities across all 28 pinned runtime packages with no skipped packages. This is an advisory scan at the time of the change, not an independent penetration test, container OS audit or permanent security certification. The app remains public and does not claim an owner-only account login. The existing historical dataset connection/availability blockers are separate and unchanged.
