# DEEX Strategy Research

A browser-based, paper-only backtest workspace for `PMS_Rules_v1(3).docx`. Connect Angel One SmartAPI, run a daily NSE technical study, inspect marked-to-market equity and completed position cycles, and download JSON, fills CSV and a print-ready HTML report. Full A/B/C/E and earnings-anchored Setup 2 replay adapters use the existing PMS point-in-time PostgreSQL dataset.

## Run

Python 3.12:

```sh
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 10000 --workers 1 --no-access-log
```

Open http://localhost:10000. Use one worker: sessions and jobs live in memory. Deploy the included Dockerfile on Render (port comes from `PORT`). No database or broker credentials are needed to start the app or run the clearly labelled synthetic demo.

## Connect Angel One

Enter a SmartAPI app key, client code, PIN and current authenticator TOTP, or an existing JWT token. The API key and JWT are held only in the server session. PIN/TOTP are used only during login. The frontend uses no credential storage. Login and candle calls use fixed Angel One hosts; no trade/order endpoint is exposed. Broker IP/app settings may still need to be configured with Angel One.

The API key alone is insufficient to authenticate: use either the interactive login fields or an existing valid session token. The app does not ask for or store a TOTP seed. Sessions expire after two hours; broker sessions locally expire after ninety minutes. Disconnect clears the local broker session and requests cancellation of its active runs.

## Two distinct research paths

1. **Angel technical study**: current NSE EQ tokens, daily candles plus 730 calendar days of warmup, Nifty 50 session calendar, chronological rally state, 15%/1%-per-day/30-session rally and 1.5x pre-rally volume, signal filters, next-three-session buy stop, gap sizing, shared trade lifecycle and cost model. Input checks block missing required historical sessions or fewer than 252 warmup sessions. It enforces 0.5% risk, 10% position cap, seven positions and 3.5% heat. Pending entries use previous-close ADR. An optional 8% ADR cap and 50-session ADR window are available as research settings.

   This is **not the complete PMS strategy**. Candle-only studies cannot apply A, C, E, historical Nifty 50 membership, announced earnings blackout or sector limits. Current selected symbols have survivorship bias. Raw candle corporate-action adjustment basis is unverified. Turnover is approximated with close times volume. Candidates are ordered alphabetically. Signal filters are rechecked at signal time rather than every four sessions. These limitations are embedded in every report and prominently shown before running.

2. **Full PMS**: set `PMS_DATABASE_URL` in the Render environment to the populated database from `omparekh54-lgtm/design_experience`. This adapter uses the inherited `prepared_replay` snapshot and gates, and fresh `MatrixReplay` A/B/C/E decisions. Full variants include A+B, A+B+C, A+B+C+E and research Setup 2, with re-entry on/off and ADR/exit/friction parameters. It reads database history rather than downloading Angel candles into the database. No data gates are bypassed. It does not change database rows or PAPER accounts. A dedicated read-only role with access to the `pms` schema is recommended. Default transaction read-only mode is also set in the driver. Missing schema/history or quota exhaustion blocks a run; no successful metrics are fabricated.

Full database input contracts, ingestion and migrations remain those of the upstream PMS project. They include official prices/calendar, filing timestamps and normalized facts, shareholding/pledge, shares outstanding, sector and index intervals, corporate actions, surveillance histories, announced earnings dates and breadth/benchmark/VIX inputs. Merely providing Angel One credentials cannot fill these historical datasets.

The inherited config explicitly resolves the draft benchmark name to Nifty MidSmallcap 400 and the healthy regime capacity to six. Debt, scoring normalizations and other proposed draft policies remain provisional, and are included verbatim in report configuration. D is parked. This service never enables live trading.

## Reports and realism

Daily NAV powers CAGR, drawdown and annualized daily-return ratios with a zero risk-free-rate assumption. Partials are not double-counted as completed trades. End-of-period open holdings stay marked, not forcibly liquidated. JSON includes fills, events, NAV, complete-cycle outcomes, limitations, configuration, data fingerprint and report hash. Undefined ratios remain null. HTML can be printed to PDF. Costs include delivery STT, stamp, exchange charges, SEBI fees, GST and configurable brokerage/slippage/impact. The inherited pre-2026 fee setting is an assumption, not a complete historical tariff archive. Daily bars cannot establish intraday event order; same-day entry/stop handling is conservative.

All results are exploratory and uncertified, including full dataset runs. A single run is not the full validation matrix or walk-forward acceptance. Dates from 2025 onward use reserved holdout data and must not be called unseen certified evidence. Synthetic demo numbers are explicitly labelled and never mix with historical runs.

## Deployment limits

- One bounded background research worker; at most three queued/running jobs globally and one per session. Fifty symbols and ten years per run. One-hour cooperative run timeout.
- Free Render instances sleep/restart and have ephemeral memory. Jobs, sessions and reports do not survive a restart. Keep the page open while running and download reports. A durable worker/datastore is required for unattended campaigns; this app does not claim that capability.
- Session cookies are HttpOnly/SameSite Strict, and Secure on Render. Reports are accessible only to their owning session. Same-origin mutation checks, bounded session/report memory, sanitized validation/errors and local login rate limits are applied. Broker responses and credentials are not exported or logged.
- Technical runs use fixed broker endpoints; connection IP metadata currently uses loopback placeholders. Broker-specific approved-IP requirements may need the existing JWT sign-in path or a suitable fixed-egress hosting setup. Real broker connectivity must be verified using the user's own active session.

## Provenance and checks

`packages/`, `config/strategy_v1.yaml` and the five research command files were copied from `design_experience` commit `55495ec7bc1ea2e18a80fedb22158c0b0cf56ce2`. The web app is new. The strategy modules are not a new simplified rewrite. Existing command scripts are retained for the engine code fingerprint; this standalone UI does not expose upstream production API/execution routes.

```sh
pip install pytest
pytest -q
```

Tests cover inherited trade timing/rally/matrix behavior and the web adapter's authentication boundaries, session isolation, data blocking, completed report downloads and synthetic-vs-historical labeling. No real broker credentials are required for automated checks.
