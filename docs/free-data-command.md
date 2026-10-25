# One command to collect free public data

From the DEEX project directory, using Python 3.10+ (3.12 recommended):

```powershell
git pull --ff-only origin main
python fetch_data.py
```

The first run installs the small pinned parser dependencies in `.venv-data` if
needed, then starts collection. No Angel One login, paid API, database server,
Finstack installation or separate init/fetch/documents commands are required.
An internet connection is needed for initial dependency installation and downloads.
Use a local disk rather than OneDrive/network storage. Keep the computer awake.

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
- `progress.json`: most recently completed download-task checkpoint.
- `coverage_report.json`: dataset counts, remaining blockers, run outcome and
  source error counts. It always retains `NOT_CERTIFIED_FOR_BACKTEST`.
- `failed_tasks.csv`: missing/failed/partial downloads and failed documents.

No downloaded data or local environment is pushed to GitHub. Exit code 0 means
plan-only or a run without reported acquisition blockers; 2 means incomplete
coverage/configuration, 3 means source cooldown, and 130 means Ctrl+C. Processing
all tasks does **not** mean complete historical data. The JSON report is authoritative.

The existing `data/` folder from your ZIP is left untouched. If it is unpacked
at the project root, matching XML files in `data/_cache/nse/` are reused when a
filing discovery supplies their exact source basename. An alternate cache is:

```powershell
python fetch_data.py --reuse-cache "D:\OldData\data\_cache\nse"
```

Reuse is limited to raw XML evidence. Existing adjusted price CSVs and partial
quarterly CSVs are not silently mixed with the new raw-price catalogue. The
financial parser still validates company/period identity; missing metadata
cannot be inferred from a filename.

## What runs automatically

1. Plan resumable monthly filing/action/earnings jobs and daily archive jobs.
2. Discover legacy quarterly/annual and integrated financial filings, then download and
   parse queued financial documents using DEEX's existing financial parser.
3. Discover and parse shareholding documents (including available pledge/shares).
4. Collect corporate actions and announced board/earnings meetings.
5. Collect the four required market benchmarks and all available Nifty index
   rows, including sector indices, from the same daily index file. Extra index
   rows never hide a missing required benchmark.
6. Download daily bulk exchange bhavcopies, including rupee traded value and
   historical symbol/ISIN/series observations. One file covers thousands of
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
jobs are skipped, downloaded response bytes are cached, and interrupted SQLite
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
