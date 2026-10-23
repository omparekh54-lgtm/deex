# DEEX: all-company, ten-year collection plan using our Finstack

## Target

Build one reusable local historical dataset for PMS Setup 1 and S1–S10.
Backtest window: **24 October 2016 through 23 October 2026**. Prices start
24 October 2014 for indicator warm-up; financial discovery starts 24 October 2013
for prior quarters, annual cash flows and filing delays. Each company/ETF starts
at its real listing date. Twelve years of price acquisition does not mean twelve
years of evaluated performance.

The machine-readable field checklist is
[`config/company_data_fields.json`](../config/company_data_fields.json).
[`config/data_requirements.json`](../config/data_requirements.json) maps datasets
to all eleven strategy specifications. The full PMS filter set is collected even
though the supplied S1–S9 reference tests used price-only proxies.

## 1. Build the historical union before freezing the company count

1. Fetch NSE's current EQUITY_L metadata through our Finstack NSE client. Use its
   company names/listing-date hints for planning, not as historical membership.
2. Read every cash-market bhavcopy across the acquisition window. One daily file
   discovers that day's identities and supplies all daily prices together.
3. Accumulate `(symbol, ISIN, series)` observations into `company_targets`.
   Preserve companies absent from the current master, old names and old ISINs.
4. Obtain dated security-type and issuer mappings. Exclude rights/REITs from the
   ordinary-equity strategy universe; retain the ten specified S10 ETFs and
   optional LIQUIDBEES separately. EQ alone does not prove ordinary equity.
5. Reconcile mergers, demergers, renames and capital-reorganisation ISIN changes.
   Only then publish the unique issuer/company count and a frozen universe version.

`config/company_universe_seed.csv` is a **bootstrap register**, generated from
current metadata plus downloaded historical observations. It is not an exhaustive
ten-year list until step 2 and identity reconciliation pass. Every row lists the
datasets still required; UNVERIFIED is deliberate. The register expands whenever
a price file adds an identity. Never delete historical candidates because they
are absent from today's master. No fixed list of 100 companies is used for collection.

## 2. Reuse source-backed history already held by Design Experience

Inventory the existing source database/export before making duplicate external
requests. Export privately using `scripts/export_private_snapshot.py`, restore to
an isolated local PostgreSQL database and run the inherited historical integrity
audit. Reuse only source-backed, correctly dated records. An existing row is not
proof of complete coverage. Import audited references into the acquisition store
with their original documents or retain them in the local PMS snapshot.

This reduces fetching; it does not depend on fixing the earlier laptop-to-cloud
database connection before starting public price acquisition.

## 3. Fetch market-wide daily data, not company-by-company prices

The default collector now uses `FinstackHTTP`: our pinned Finstack NSE client and
network governor obtain the original official response; DEEX retains those bytes
and normalizes them with strict archive/date/identity checks. This uses the
inspected Finstack NSE transport because it exposes original response bytes;
normalized `fs.fetch` frames alone do not preserve original source evidence.

Fetch one equity bhavcopy and one index report per candidate date. Use legacy
bhavcopy before 8 July 2024 and UDiFF thereafter. Keep raw OHLCV, rupee turnover,
ISIN and series. Retain non-entry series for historical exits/restrictions.
Missing days remain missing until verified against an official calendar, including
exceptional weekend sessions. Never assume a failed download means a holiday.

The existing plan contains 4,383 calendar dates x two daily feeds = **8,766 daily
tasks**, including warm-up and non-session checks. About 3,000 actual trading
sessions is a sizing approximation, not an official calendar count. Daily files
cover all observed securities, avoiding thousands of per-company price requests.

Finstack's `fs.fetch('india_eod_files', ..., sources=['nse'])` and
`fs.fetch('india_indices', ..., sources=['nse'])` can be used for cross-checks.
Generic fallback quotes/history are not accepted silently; the provider/source,
adjustment convention, currency and dates must match. Retain partial index closes
with missing OHLC as partial evidence rather than inventing a full candle.

## 4. Fetch filing metadata, then the original financial/shareholding documents

Use monthly all-company discovery to find actual events before scheduling
per-document downloads. The current plan has 157 month windows for each of
actions, earnings, financial filings and shareholding: **628 discovery tasks**.
Document counts are determined by discovery and are additional to those tasks.

Finstack exposes `result_filings(start=..., end=...)`,
`fs.fetch('india_corporate_events', what='result_filings', ...)`,
`xbrl_facts`, `xbrl_results`, `quarterly_results`, `annual_results`, `cash_flow`
and `balance_sheet`. For a point-in-time backtest we use the **original filing**
route, original exchange publication timestamp, scope and every revision. Latest
statement snapshots are useful for reconciliation but cannot prove historical
knowability. Current Finstack integrated-result discovery does not establish
complete pagination by itself: the stricter DEEX paged discovery remains required.

Download each discovered original XBRL once through the Finstack transport.
The inherited DEEX parser preserves quarter/annual identities and original facts.
Keep sales, profits, EPS, cash flow, debt/equity/interest and quality-adjustment
facts. Revisions remain separate observations; queries select what was public on
the simulated date. Missing old attachments become explicit repair tasks.

For shareholding, retain promoter holding, pledge denominator, shares outstanding,
filing time and original XBRL. Finstack's bulk `nse_shareholding_history()` is an
additional discovery/reconciliation source: its implementation downloads a
third-party reconstructed CSV, not a new official NSE filing. Stage it and verify
against originals before accepting it. Do not substitute a current snapshot for
older missing filings.

## 5. Collect the historical reference layers

| Dataset | Finstack route | Acceptance / remaining work |
|---|---|---|
| Splits, bonuses, dividends, mergers | `india_corporate_events`, `what='actions'`, dated windows | Verify terms, ex-dates and announcement times; build dated adjustment factors |
| Earnings announcements and changes | Same pipeline, `what='board_meetings'` / `results_calendar` | Preserve publication time, cancellation and reschedule notices |
| Index membership | `nse_index_membership_history()` | Reconstructed third-party intervals; reconcile official releases, baseline coverage and dates |
| ASM/GSM | `nse_surveillance(day)` | Standardised REG_IND history begins in 2024; old exchange circulars/source exports needed before then |
| Sector / industry | `company_profile` for current reconciliation | No verified ten-year sector-history route; use dated source records/export |
| Listing, delisting, ISIN and name changes | Finstack NSE master + historical daily files | Build issuer/security timeline; first/last observed trade is not proof of listing/delisting |
| Calendar | Finstack calendar helpers for planning, official files/circulars for validation | Verify all years, holidays and special sessions |
| Dated fees and taxes | No complete verified Finstack tariff archive | Add reviewed effective-date schedules and explicit execution assumptions |

An empty surveillance frame may mean no file/failed access; it is never treated
as proof that every company was clear. A current-sector snapshot is never
backdated. Third-party reference output stays STAGED_UNVERIFIED.

## 6. Store once, derive indicators locally and audit

Acquisition: `local_data/catalog.sqlite` + immutable `local_data/raw/*.bin`.
Finstack's own cache is under `local_data/finstack_cache`; it is a fetch cache,
not the sole research evidence archive. Retain SHA256, URL, receipt time, provider,
parser version, logical identity and retry state. Back up the catalogue and raw
files together. Reserve 25–50 GB as an initial working-disk budget, then adjust
from the measured pilot. Keep the data directory on a local drive.

Derive adjusted signal series, raw execution bars, weekly bars, SMA/EMA/ADR/ATR,
RSI-2, RS ranks, breadth, rolling liquidity and filing-time market cap locally.
No repeated downloading for eleven strategies. Dividends/cash flows and capital
actions must reconcile; do not use large-gap heuristics as corporate-action facts.

Audit each required company/session and filing period: missing/duplicate bars,
OHLC, volume/turnover units, identities, action discontinuities, revision timing,
financial continuity and statement scope, reference intervals, launch dates and
source receipts. Produce a repair queue, rerun only missing/failed tasks, and
repeat until critical gaps are closed or disclosed as unsupported coverage.

## 7. Publish a frozen local snapshot and connect the engines

After data acceptance, transform the lake into the existing PostgreSQL `pms`
schema or a separately validated common replay contract. Audit that transformation,
then freeze a dated dataset version/hash. The SQLite store is currently acquisition
infrastructure; PMS Setup 1 still reads PostgreSQL. S1–S10 replay engines remain
separate implementation work. Do not label the strategies runnable because a
collector or company list exists.

## Run order on Windows

From the DEEX checkout, with Python 3.12 and Git installed:

```powershell
pip install -r requirements-data.txt -r requirements-finstack.txt
python scripts/collect_local_data.py init --start 2016-10-24 --end 2026-10-23
python scripts/collect_local_data.py universe --refresh-master --out local_data/company_targets.csv
python scripts/collect_local_data.py fetch --provider finstack --limit 100
python scripts/collect_local_data.py fetch --provider finstack --datasets actions,earnings,financial_discovery,shareholding_discovery --limit 20
python scripts/collect_local_data.py documents --provider finstack --limit 100
python scripts/collect_local_data.py finstack-reference --dataset membership
python scripts/collect_local_data.py finstack-reference --dataset shareholding
python scripts/collect_local_data.py universe --out local_data/company_targets.csv
python scripts/collect_local_data.py verify
python scripts/collect_local_data.py status
```

Repeat the bounded fetch/document commands to resume. Failed sources are visible;
no silent provider fallback. `--provider official` explicitly selects the original
direct official transport if troubleshooting requires it. Source limits apply to
Finstack too; use one collector per data directory. Do not turn on large parallel
fetching to try to evade rate limits.

## Milestones and timing

1. Bootstrap target register and verify a pilot across old/new formats.
2. Complete daily history and freeze the historical issuer/security mapping.
3. Complete filing discovery, then document downloads/parsing.
4. Repair reference gaps and audit company/session/period coverage.
5. Freeze and connect the accepted dataset to replay.

Estimate runtime **after a representative pilot**, separately for daily tasks,
discovery pages and filing documents. At one request/second, 8,766 daily tasks
alone imply about 2.4 hours of request spacing, before latency/retries; this is a
lower-bound sizing calculation, not a completion promise. Many filing documents,
source blocks and old-reference repairs can take days or require archive access.
The bottleneck may be historical coverage rather than download speed.

Finstack version inspected: 0.8.0, source commit
`4b336cbdf3750834f67787c8eaf7d311e8b4e9d7`. Dependencies are pinned in
`requirements-finstack.txt`. The whole ten-year download has not been completed
by preparing this plan.
