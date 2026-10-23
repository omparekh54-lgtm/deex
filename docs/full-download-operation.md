# Running the full historical acquisition

The unattended command is:

```powershell
python scripts/run_full_download.py --root D:\DEEX_Data
```

It drains pending daily files, monthly disclosures and their original statements
for the full configured period, including warm-up. Fair batches prevent document
discovery from starving prices and prevent documents from starving discovery.
The SQLite task ledger commits after each task. There is no default task limit.
`--max-tasks` or `--max-seconds` explicitly stops at a resumable checkpoint.
Ctrl+C records an interruption and safely releases the collector lock.

The root contains `download_progress.json`, `catalog.sqlite` and original
evidence under `raw/`. Keep those together. A second full runner is rejected
while the same folder is locked. Five consecutive authentication/rate-limit or
transport blocks stop the run instead of marking thousands of tasks failed.
Do not bypass an upstream block with higher parallelism or proxies.

## Hosted year-by-year acquisition

`.github/workflows/deex-data-download.yml` runs the same Finstack collector on
standard Ubuntu GitHub Actions runners, one calendar-year shard at a time. It
covers 2013–2026: 2013 is prior financial history; prices begin in October 2014;
the evaluated window remains October 2016–October 2026.

The implementation branch `data/full-ten-year-download` starts the initial run
when this workflow file is published there. It can also be run manually after
the workflow is merged into the default branch. No broker keys, database
passwords, paid infrastructure or private Design Experience export are provided
to the workflow. It only collects the public feeds in the source allowlist.

Each year has a five-hour acquisition budget, followed by verification,
packaging and upload before GitHub's job timeout. A year that needs more time
reports CHECKPOINTED_LIMIT, preserves its data, and fails visibly so its job can
be rerun. Reruns restore the latest saved year cache and continue pending work.
Cache eviction is possible; downloadable year archives remain an additional
checkpoint. Artifacts expire after **seven days** and must be downloaded for
long-term local retention. A successful pass is not a data-acceptance certificate.

Only the workflow file's dedicated implementation-branch push starts a new run;
ordinary project pushes do not continually restart a multi-year backfill.
The job summaries and per-year JSON reports show the actual state, remaining
work and missing/failed sources. A source failure is not silently accepted.

## Joining year archives locally

Download and extract each `deex-data-YYYY` artifact. Its inner
`deex-data-YYYY.zip` contains a `deex_data` directory. Extract each year into a
separate parent, then merge:

```powershell
python scripts/merge_data_checkpoints.py --root D:\DEEX_Data --source D:\Years\2014\deex_data D:\Years\2015\deex_data
python scripts/collect_local_data.py --root D:\DEEX_Data verify
python scripts/run_full_download.py --root D:\DEEX_Data
```

Pass all downloaded year directories to `--source`. The merge verifies SQLite
integrity and every original file's SHA256. It rejects conflicting price/index
observations, preserves separate source receipts, and never replaces a completed
job with a pending or failed one. Resume the merged store to acquire outstanding
documents or dates; it remains separate from the PMS PostgreSQL replay schema.

## Source repairs included

- Older CNX Nifty / CNX 500 index names map to their later NIFTY50 / NIFTY500
  identifiers. MidSmallcap 400 is not invented before its available history.
- NSE issuer IDs are not used as unique filing IDs. Filing identity includes
  symbol, reporting period, scope, publication time and original document link.
- Unstable or repeated integrated-filing pages split into narrower date windows.
  A single-day failure remains explicit. Source counts must still reconcile.
- Legacy `xbrl/.../-` placeholders are replaced by the actual linked official
  HTML statement. These original statements are saved as STAGED; a strict legacy
  table parser is still required before treating them as normalized financial facts.
- Parser failures retain the downloaded original source bytes and receipts.
- Failed document tasks do not sit at the front of the pending queue forever.
  Explicit retries are bounded to three attempts.

## Acceptance boundary

Raw downloads, reference candidates and parsed records are distinct. Original
statement availability, dated issuer/security identity, historic surveillance,
sector/membership coverage, calendar verification and financial normalization
must pass the historical audit before a complete dataset can be claimed.
An acquisition pass can finish with unsupported gaps. The runner never emits
READY and never manufactures values to make the audit pass.
