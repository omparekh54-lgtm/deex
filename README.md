# DEEX PMS Backtest

## Quick start: download ten years of data and test all 11 strategies

```powershell
git clone -b data/fast-pipeline https://github.com/omparekh54-lgtm/deex.git
cd deex
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-pipeline.txt
python scripts/fetch_all_data.py           # everything for all 11 strategies -> .\deex_data
python scripts/run_all_strategies.py       # reports -> .\deex_data\reports\summary.md
```

`python scripts/fetch_all_data.py --quick` fetches only what S1–S10 need (about 5–10 minutes).
Details, timings, rate limits and data limits: [docs/fast-data-pipeline.md](docs/fast-data-pipeline.md).

Local historical data acquisition is documented in
[docs/local-data-pipeline.md](docs/local-data-pipeline.md). It adds a resumable
SQLite/source-evidence lake for the 11 strategy specifications. The lake is a
staging store; the current PMS replay still reads PostgreSQL, and S1–S10 replay
engines are not implemented by this collection change.

The [ten-year Finstack plan](docs/ten-year-finstack-plan.md) defines the historical
universe, required fields and collection order. The versioned candidate register
is [config/company_universe_seed.csv](config/company_universe_seed.csv); it is a
bootstrap list that expands with historical daily files, not a certified company
count. Our pinned Finstack is the default collection transport.

A browser workspace that runs one fixed strategy: Setup 1 from `PMS_Rules_v1(3).docx`, with Modules A, B, C and E, the shared trade engine and re-entry after an eligible Leg 2 EMA exit. Module D remains parked. Setup 2, technical-only runs, comparison variants, tuning controls and synthetic demos are not exposed by the app.

Open the deployed site, connect Angel One, choose dates and starting capital, and run the strategy. There is no stock-code entry field. The app automatically selects 100 historical NSE equities and returns one consolidated portfolio report. Scanning 100 stocks does not mean holding 100: the documented seven-position, two-per-sector, 3.5% heat and 10% position caps remain in force.

## Fixed strategy

The only public run inputs are `start`, `end` and `capital`. The API rejects additional fields, including `mode`, `symbols`, `adr_min`, `reentry` and exit variants. Defaults in `config/strategy_v1.yaml` govern the run: 3% ADR over 20 sessions, 15% rally, minimum 1% per day velocity, 1.5x baseline rally volume, 0.5% risk, 10% position cap and 5.5% maximum stop distance. Existing proposed policy normalizations remain explicitly provisional and are included in every report. The inherited benchmark policy resolves the draft's benchmark name to Nifty MidSmallcap 400 and healthy capacity to six.

## Automatic 100-stock universe

`app/universe.py` selects a fixed sample using information strictly before the selected start date, inside the same read-only consistent snapshot as the replay. It ranks historically listed NSE EQ stocks by their preceding 63-session average traded value. It requires 252 official pre-start sessions, at least ₹2 crore ADTV, a historical non-financial sector and no Nifty 50 membership at selection. Ties use canonical symbol order. Current `active` flags and end-date price coverage do not select the stocks. The algorithm does not pick future winners or substitute today's constituents.

This selection algorithm is an explicit app execution policy added to satisfy the 100-stock request; the original strategy document did not define a 100-stock sample algorithm. It is a historical sample, not the entire NSE universe, and membership is frozen at the start. Every strategy rule is applied during the replay. If fewer than 100 identities meet the sample requirements, the run blocks instead of silently using fewer stocks. The report records all selected names, their pre-start liquidity and the selection cutoff.

## Historical dataset requirement

**Angel One credentials alone cannot run the complete strategy.** Module A needs filing-time financial facts, shareholding/pledge and historical shares outstanding. B/C/E additionally require reference intervals, announced earnings dates, corporate actions, historical index membership, official calendars and benchmark/breadth/VIX history. The app must have `PMS_DATABASE_URL` pointing to the populated PostgreSQL `pms` schema from `omparekh54-lgtm/design_experience`.

The deployment currently requires this database connection to be configured by the owner, with authorization for the credential transfer. Existing database quota or data-coverage failures must also be resolved. Missing dataset access blocks the app; there is no fallback to an incomplete strategy.

Full runs use the configured historical database. The Angel connection is separate and does not automatically fill missing historical fundamentals. Only fixed broker login, instrument and candle endpoints exist in the adapter; there is no order-submission surface. A dedicated read-only database role is recommended. The database driver also sets default read-only transactions. Fresh A/B/C/E decisions and a simulated account are replayed through the inherited MatrixReplay engine. No stored PAPER selections or production portfolio state drives the run, and no database rows are modified.

## Local run and deployment

Python 3.12:

```sh
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 10000 --workers 1 --no-access-log
```

The included Dockerfile runs on Render and binds to `PORT`. One worker is required because sessions and jobs are held in memory. The service starts without broker or database credentials and shows an honest dataset-connection notice. It cannot generate historical performance until data prerequisites pass.

The browser uses client code, PIN and current TOTP solely to verify Angel One. The backend also supports verifying an existing JWT. **No API key, PIN, TOTP or JWT is retained after verification**: the current full backtest reads the historical PMS database and does not need broker credentials. The session holds only a ten-minute verification flag. Credentials are cleared from the form before submission and never written to browser storage, reports, application logs, files or the database. Python cannot guarantee physical zeroization of every temporary string; the request necessarily processes credentials transiently on the server and sends them to Angel One over HTTPS.

Security controls include per-session CSRF tokens on every mutation, pinned Render host/origin, a Secure `__Host-` HttpOnly/SameSite Strict production cookie, session-cookie and CSRF rotation on verification, 15-minute idle and two-hour absolute session expiry, timed server-side cleanup, report ownership checks, and complete session/report invalidation on logout. Login throttles use bounded HMAC identifiers across browser sessions and accounts, not only a fresh cookie. Request bodies are limited to 16 KiB even when streamed without Content-Length; mutations accept JSON only; API query strings are rejected. Every response receives restrictive CSP, framing/content-type protection, no-store headers and production HSTS. Byte ranges are unnecessary and rejected before the file server parses them.

The broker adapter uses fixed HTTPS hosts and an explicit operation allowlist. TLS verification stays enabled, redirects are disabled, and environment proxy/CA overrides are not trusted. It has no permitted order operation. Errors are sanitized, including unexpected verification failures. The container runs as an unprivileged user, disables access logs and the server banner, and excludes local secrets and Git metadata from Docker builds. The deployed service uses updated, pinned web dependencies. These controls reduce risk; they are not a guarantee against all host compromise, malware, phishing or distributed denial of service. The site remains a public web app, with private per-session reports; no separate owner-only login is claimed.

## Reports and limits

Reports contain daily marked NAV, returns, CAGR, drawdown, risk ratios, complete position cycles, all fills, costs, open inventory, selected stock universe, configuration and reproducibility hashes. Partials are not double-counted as completed trades. EMA exits execute next open; stops use conservative daily-bar timing. Open holdings remain marked at the final close, rather than being forced closed. Download JSON, fills CSV or print-ready HTML; the latter can be printed to PDF.

Reports remain exploratory and uncertified. Draft policies, intraday ordering assumptions, independent survivorship correctness and historical source vintages require review. The inherited pre-2026 transaction cost setting is not a complete dated historical tariff archive. Single runs are not walk-forward or full validation-matrix acceptance. Inspecting 2025 onward consumes reserved holdout data and is not certified unseen evaluation.

One bounded research worker allows three queued/running jobs globally and one per session. Each run uses 100 symbols, at most ten years and a one-hour cooperative timeout. Free Render instances may sleep or restart; sessions, jobs and reports do not survive restart. Download reports before leaving. This app does not claim durable unattended campaign execution.

## Provenance and validation

The strategy modules, base configuration and retained command files originated from `design_experience` commit `55495ec7bc1ea2e18a80fedb22158c0b0cf56ce2`. The fixed web contract and automatic universe adapter are new. Legacy standalone candle/demo helpers remain internal for regression tests and cannot be invoked through the public app.

```sh
pip install pytest
pytest -q
```

Tests cover inherited trade/rally behavior, locked request policy, 100-stock selection, pre-start sampling, missing-history blocking, session isolation, mocked full-report exports and the seven-position portfolio cap. Mocked/synthetic tests do not certify a real 100-stock historical A/B/C/E run. That still requires the connected and complete historical dataset.
