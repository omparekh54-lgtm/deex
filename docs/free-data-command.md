# One command to collect free public data

First copy your original `data(2).zip` into the DEEX project directory. You do
not need to extract it. Alternatively, put the extracted `data/` folder there.
Then, using Python 3.10+ (3.12 recommended):

```powershell
git pull --ff-only origin main
python fetch_data.py
```

The first run installs the small pinned parser dependencies in `.venv-data` if
needed, then imports the existing dataset and collects missing data. No Angel One login, paid API, database server,
Finstack installation or separate init/fetch/documents commands are required.
An internet connection is needed for initial dependency installation and downloads.
Use a local disk rather than OneDrive/network storage. Keep the computer awake.

## Import first and fetch missing fields

Default behaviour requires existing data. It auto-detects an extracted `data/`
folder or a single `data*.zip` in the project. If several ZIPs exist it asks you
to choose with `--existing`; if none exist it stops before downloading. An
explicit `--fresh` permits a new full collection without previous data.

For a ZIP elsewhere or data stored outside OneDrive:

```powershell
python fetch_data.py --root C:\DEEX_Data --existing "D:\Downloads\data(2).zip"
```

The selected path is remembered in that data root. Keep the source ZIP/folder
available for future runs and XML reuse; move it with an explicit new
`--existing` path. Repeat the same `--root` when resuming.

The import phase validates CSV schemas and OHLCV rows, saves original bytes,
imports valid prices, both index files, quarterly rows and company reference
rows, and indexes the cached XML members. It can read XML directly from the ZIP.
CSV checksums prevent repeated row insertion on resume. Invalid rows are listed
in `import_issues.csv`. If a previously imported CSV changes in place, collection
stops rather than silently retaining stale coverage; use a new data root for a
revised dataset version.

Existing OHLCV dates get `REUSED_LOCAL` price-history status. Uploaded prices stay
in `local_prices` with their adjustment basis marked unverified; original
financial values stay in `local_financials` with their units/timestamp semantics
unverified. This preserves your supplied values without asserting compatibility
with raw exchange prices or normalized point-in-time financial facts.

**Some existing dates still need a bulk exchange download:** your CSVs have no
actual rupee turnover, ISIN or series. Separate `price_supplements` jobs fetch
those missing fields and fill absent/invalid symbol-date bars. Bulk files contain
all stocks on a date; overlapping uploaded OHLCV is not inserted again or
replaced. Only dates with no valid uploaded prices retain full `prices` jobs.
Daily index files are fetched where required benchmarks/sector indices remain
missing; imported index rows are not overwritten.

Quarterly CSV rows do not establish complete issuer/date coverage or annual
cash-flow/debt/pledge fields, so discovery still runs to find missing disclosures.
Matching cached XML is reused before any network request. No historical
eligibility, corporate-action adjustment or missing quality field is invented.

Inspect `missing_data_plan.json` (generated offline by `--plan-only`) for imported
counts, reused tasks and why remaining task types are necessary. This is a field-
and date-aware acquisition plan, not a declaration of backtest completeness.

## Defaults and files

The study defaults to ten years ending yesterday in Asia/Kolkata. It adds the
existing two-year price and three-year financial warm-up periods. You can set a
fixed study range:

```powershell
python fetch_data.py --start 2016-10-24 --end 2026-10-23
```

Everything stays under the project's Git-ignored `local_data/` folder:

- `catalog.sqlite`: normalized prices/indices, staged disclosures, financial
  and shareholding facts, document queues, company register and job status.
- `raw/`: SHA256-addressed original exchange responses used in catalogue receipts.
- `response_cache/` and `http_cache.sqlite`: resumable response cache, checksums,
  original observation timestamps and persistent request/cooldown state.
- `missing_data_plan.json`: imported coverage, reused tasks and pending task reasons.
- `import_issues.csv`: rejected uploaded row locations; originals are preserved.
- `progress.json`: most recently completed download-task checkpoint.
- `coverage_report.json`: dataset counts, remaining blockers, run outcome and
  source error counts. It always retains `NOT_CERTIFIED_FOR_BACKTEST`.
- `failed_tasks.csv`: missing/failed/partial downloads and failed documents.

No downloaded data or local environment is pushed to GitHub. Exit code 0 means
plan-only or a run without reported acquisition blockers; 2 means incomplete
coverage/configuration, 3 means source cooldown, and 130 means Ctrl+C. Processing
all tasks does **not** mean complete historical data. The JSON report is authoritative.

The original ZIP/extracted files are read-only inputs. They are never edited.
`--reuse-cache "D:\OldData\data\_cache\nse"` can also add a loose XML directory.
Filing metadata and the parser must still validate the original document;
filing dates are not inferred from an XML filename.

## What runs automatically

1. Import/check existing data, then plan missing monthly disclosures, daily gaps
   and turnover/identity supplements.
2. Discover legacy quarterly/annual and integrated financial filings, then download and
   parse queued financial documents using DEEX's existing financial parser.
3. Discover and parse shareholding documents (including available pledge/shares).
4. Collect corporate actions and announced board/earnings meetings.
5. Collect the four required market benchmarks and all available Nifty index
   rows, including sector indices, from the same daily index file. Extra index
   rows never hide a missing required benchmark.
6. Download daily bulk exchange bhavcopies only for missing dates or missing
   turnover/identity fields, retaining existing OHLCV. One file covers thousands of
   securities; there is no per-stock candle-download loop.
7. Attempt modern daily REG_IND surveillance archives from 2024 onward.
8. Collect current equity-master and Nifty 50/500 membership/industry snapshots.
   These are explicitly **current snapshots**, never historical classifications.
9. Write the coverage and failure reports.

The progress bar shows tasks **processed**, including failures and 404s. A separate
bar covers documents discovered during each filing phase. The number of
attachments is unknown before discovery; the runner does not invent an overall
percentage for unknown future work. A discovery task is STAGED, not certified
complete merely because the HTTP request succeeded.

## Resume and retries

Press Ctrl+C to stop. Run the same command to resume pending work. Successful
jobs and reused local price-history tasks are skipped, downloaded response bytes are cached, and interrupted SQLite
transactions roll back safely. A process lock prevents two new one-command
collectors writing the same folder; do not also run the legacy collector there.

Previous failures are not retried in a hot loop. To attempt them once more:

```powershell
python fetch_data.py --retry
```

Closed-date responses can be missing. A 404 is **never** automatically classified
as a holiday, including weekends (exceptional weekend sessions exist).

For an offline planning preview after dependency installation:

```powershell
python fetch_data.py --plan-only
```

Choose another disk with `--root D:\DEEX_Data`. To slow collection further use
`--interval 3`; the enforced minimum is one second and default is 1.5 seconds
plus jitter across all NSE/indices hosts. There are no concurrent exchange
requests. Local caching and bulk files provide speed without flooding sources.

## Blocking and historical limits

No downloader can guarantee no bans. This collector uses normal cookie sessions,
bounded transient-error retries and a shared persistent request budget. HTTP
401/403/429 or an HTML challenge stops the run and stores a cooldown. It honours
longer Retry-After values; rerunning early does not bypass the cooldown. It does
not rotate identities/proxies, solve CAPTCHAs or attempt to evade source controls.

Free public endpoints may change, ignore date filters, truncate old results,
miss attachments or omit publication times. Raw discovery is staged for review;
it is not proof of complete point-in-time coverage. Cash flow/debt/equity fields
remain null if the parser cannot establish them. No zero-filled quality filters
or reconstructed consensus estimates are manufactured.

There is still **no verified complete automatic free adapter** here for pre-2024
ASM/GSM intervals, historical sector classifications/index membership, all dated
issuer/capital changes, full historical session calendars or cost schedules.
Modern surveillance files can also be missing. Daily snapshots are kept separate
from audited continuous intervals. Reviewed source-backed JSONL evidence can be
staged using the existing `scripts/collect_local_data.py import` command.

A completed free collection is an acquisition/staging database. It does not
certify an exact full-strategy backtest or replace the existing PostgreSQL PMS
replay schema. Corporate-action adjustment and coverage audits remain required.

## Investigating document failures

A full collector now pauses after five consecutive failed documents, or 15
failures in the last 20 attempts. This is a parser/source failure circuit,
separate from the HTTP rate-limit cooldown. The report uses
`PAUSED_DOCUMENT_FAILURES`; it is not a successful acquisition state.

The parser recognises legacy NSE `toDate` period ends and `seqNumber` filing IDs,
and treats `Non-Consolidated` as standalone. These metadata corrections do not
reconstruct missing XBRL contexts or turn unsupported financial values into zeros.
Some legacy exchange XML renditions reference contexts they do not define.
Those files remain blocked with a specific error; their original bytes are
retained under an unparsed-raw record, outside normalized financial facts.

For an offline metadata report without fetching, importing or retrying:

```powershell
python fetch_data.py --root C:\DEEX_Data --diagnose
```

To retry only a bounded sample of previously failed documents (1 to 20), then stop:

```powershell
python fetch_data.py --root C:\DEEX_Data --probe-documents 10
```

The sample command never starts the full download queue and does not retry
successful records. Normal pacing, original cache reuse and HTTP cooldowns still
apply. It writes `document_diagnostics.json`, showing exact error codes, exception
class names, source-code frame locations, source URLs without query strings and
selected filing metadata. Response bodies and exception messages are not exported.
Historical generic errors stay generic until sampled; the original report alone
cannot identify their root cause. Review the detailed sample before a full retry.
