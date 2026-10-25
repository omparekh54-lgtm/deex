# Local data collection for DEEX's 11 strategy specifications

For import-first missing-data collection, use [the one-command guide](free-data-command.md).
The manual commands below do not import the owner's ZIP automatically.

The expanded all-company plan and Finstack source mapping are in
[ten-year-finstack-plan.md](ten-year-finstack-plan.md). Collection now defaults
to our pinned Finstack NSE transport; install `requirements-finstack.txt` too.

This is an acquisition and staging pipeline. It stores real source observations
locally; it does not certify complete data or implement S1–S10 replay engines.
The existing PMS Setup 1 runtime still needs its PostgreSQL `pms` schema. It does
not read this SQLite catalogue. Keep this boundary explicit in reports and UI.

## Universe and dates

Collect the union of historical NSE cash-market identities, including later
delisted, merged, suspended and renamed securities. Do not choose stocks using
today's master or the app's fixed 100-stock sample. Retain dated EQ, BE, BZ, SM,
ST and SZ observations; each strategy's entry filter decides eligibility later.
Rights, REITs and ETFs must be identified separately through dated security-type
metadata. Symbol and ISIN changes are not automatically new companies.

S10 uses NIFTYBEES, JUNIORBEES, MID150BEES, BANKBEES, PSUBNKBEES, ITBEES,
PHARMABEES, CPSEETF, GOLDBEES and SILVERBEES. LIQUIDBEES is an optional cash
implementation, not an eleventh risky allocation. No ETF is eligible before its
actual launch and required lookback.

For the requested ten-year study, use **24 October 2016–23 October 2026**.
The default plan adds prices from **24 October 2014** and filing discovery from
**24 October 2013**. These buffers support price lookbacks, eight-quarter
comparisons and filing delays; they do not guarantee that every required prior
annual report exists. Newly listed stocks are collected from their actual
listing; do not manufacture ten years for them.

There is no defensible exact company count before the historical union and dated
issuer mapping have been audited. Provision for thousands of securities, not
100. A planning capacity of **10,000 security identities** leaves headroom; it
is not a claim that NSE has 10,000 companies. `status` reports observed unique
ISINs, symbols and rows separately and deliberately leaves exact company count
unset. Identifying ordinary-equity issuers and resolving ISIN changes is a
required subsequent identity audit.

## Windows: first run

Open PowerShell in your cloned `deex` folder. Python 3.12 is recommended.

```powershell
git pull --ff-only origin main
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-data.txt -r requirements-finstack.txt
python scripts/collect_local_data.py init --start 2016-10-24 --end 2026-10-23
python scripts/collect_local_data.py fetch --limit 100
python scripts/collect_local_data.py status
```

Repeat `fetch` to continue with pending tasks. For bounded automatic batches:

```powershell
.\scripts\collect_local_data.ps1 -Batches 100 -BatchSize 100
```

This runs locally while the terminal/computer is on. It is not an activated
cloud scheduler. The PowerShell wrapper fetches price/index jobs; the filing
commands below are separate. Ctrl+C is safe: the current uncommitted task rolls
back and is retried. Only one collector should write to a data folder at once.
Do not collect into OneDrive or a network-mounted folder. Use a local disk.

`local_data/catalog.sqlite` stores jobs, prices, indices, normalized/staged
records, document queues and receipts. `local_data/raw/` retains immutable source
bytes by SHA256. Use `--root D:\DEEX_Data` before the subcommand to choose another
drive. Data, dumps and secrets are ignored by Git. A planning allowance of 25–50
GB of free disk provides working/backup room for filings; this is a budget, not
a measured final size.

## Filing acquisition

```powershell
python scripts/collect_local_data.py fetch --datasets actions,earnings,financial_discovery,shareholding_discovery --limit 20
python scripts/collect_local_data.py documents --limit 100
python scripts/collect_local_data.py status
```

Monthly discovery jobs preserve raw exchange responses. Financial discovery
uses the legacy result listing before March 2025 and bounded, count-checked
integrated-result pagination thereafter. Original XBRL documents are queued,
downloaded and parsed using the inherited financial/shareholding parsers.
Quarter and annual companions and revision/source payloads are preserved.
Shareholding links or legacy result fields that cannot be parsed stay blocked.

**Public discovery is not proof of full historical coverage.** Old listing APIs
may ignore date filters, truncate records, omit old attachments or change their
schemas. Raw discovery jobs are STAGED, not complete. A parser failure is recorded
without manufacturing zero facts or backdating a filing. Old disclosures may
require exchange/vendor archives or the source project's existing history.
The March 2025 boundary follows NSE's integrated-filing migration; source
availability around that boundary must still be audited for each issuer.

Actions with an original broadcast time and earnings notices are staged with
their effective and known dates. Unstamped actions remain raw. Adjustment factors,
merger treatment and cancellation semantics still require verification before
constructing tradable replay inputs. No heuristic large-gap split detection is
used. Import reviewed historical action records when original timings are absent.

## Historical reference and missing-source route

The complete contract is in `config/data_requirements.json`: prices, indices/VIX,
calendar, identities, actions, financials, shareholding/pledge/shares, sectors,
membership, surveillance/trading status, announced earnings and dated costs.

Historical calendars (including exceptional weekend sessions), classifications,
ASM/GSM states, index membership, identity changes and cost schedules do **not**
have a verified complete ten-year public adapter here. These must come from
source-backed Design Experience exports or licensed/reviewed archives. The
pipeline supplies an evidence import route rather than silently substituting
today's classifications, EQ-series proxies or guessed holiday calendars.

Each JSONL line for `import` must contain:

```json
{"dataset":"sectors","key":"ISSUER_ID/2016-10-24","effective_date":"2016-10-24","known_at":"2016-10-24T09:00:00+05:30","payload":{"issuer_id":"ISSUER_ID","effective_to":null,"sector":"Industrial"},"source_url":"https://www.nseindia.com/path/to/original/document","source_file":"original_document.csv"}
```

The example demonstrates the schema, not real market evidence. Resolve relative
`source_file` paths beside the JSONL; retain original bytes, original publication
time and source URL. No credentialed/signed URLs are stored. No remote import URLs
are fetched. Records remain AUDIT_REQUIRED after import: semantic field checks,
identity mapping, interval coverage and point-in-time replay validation are still
needed. Rows from a single reference file cannot certify all companies/years.

```powershell
python scripts/collect_local_data.py import D:\DEEX_Exports\references.jsonl
python scripts/collect_local_data.py verify
python scripts/collect_local_data.py backup D:\DEEX_Backup
```

SQLite's backup API produces a consistent database copy; raw files follow it.
Back up while collection is stopped. The catalogue and its raw folder must be
kept together. A backup verifies bytes, not historical data correctness.

## Existing Design Experience snapshot and PMS compatibility

To reuse the existing full PostgreSQL data, set `SNAPSHOT_SOURCE_URL` privately
in your terminal and run:

```powershell
python scripts/export_private_snapshot.py --pg-bin "C:\Program Files\PostgreSQL\17\bin" --out snapshot\pms.dump
```

The export is a read-only, consistent `pg_dump`; credentials are passed through
the child environment, not command arguments or logs. It preserves the full
database, including public dependencies, so treat the dump as private. This tool
does not modify or repair the source data. PostgreSQL client version must be at
least the source server's major version. Transaction poolers may not support the
required session semantics; use a source connection suitable for pg_dump.

DEEX's existing `scripts/start_deex.sh` can restore this private dump into its
local PostgreSQL runtime. Never commit the dump or place it under `static/`.
The SQLite lake is an additional acquisition/staging store; a validated transform
into the PMS PostgreSQL schema is still required for newly collected lake data.
This change deliberately does not point the current replay at incompatible data.

## Status and failures

`DONE`: price file or full index OHLC task stored. `PARTIAL`: available index
closes preserved but an index or OHLC fields are absent. `STAGED`: discovery
evidence only. `MISSING`: HTTP 404, not an inferred holiday. `FAILED`: transport,
schema or validation failure. `--retry` includes missing/failed/partial jobs;
it does not retry completed tasks. Revisit STAGED discovery with an expanded
plan/export if the upstream archive changes; it is not a daily-refresh cache.

The official 2016 MidSmallcap 400 report, for example, can contain a close with
missing open/high/low. Preserve those fields as NULL, not fabricated candles.
The same strictness applies to missing ISINs and malformed bars.

Every strategy remains BLOCKED or AUDIT_REQUIRED until data is audited.
An HTTP success, imported row count or successful SHA256 verification never
turns a strategy into READY. S1–S10 replay implementation and validation are
separate work from this collection infrastructure.

## Optional isolated container

```sh
docker compose -f compose.data.yaml run --rm collector init --start 2016-10-24 --end 2026-10-23
docker compose -f compose.data.yaml run --rm collector fetch --limit 100
docker compose -f compose.data.yaml run --rm collector status
```

The collector has a read-only container filesystem, a local writable `/data`
volume, bounded CPU/memory, and no broker/order API. It is not started by the
web deployment and does not write market data into the production database.

Official source references: [NSE reports](https://www.nseindia.com/all-reports),
[financial results](https://www.nseindia.com/companies-listing/corporate-filings-financial-results),
[shareholding](https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern).
