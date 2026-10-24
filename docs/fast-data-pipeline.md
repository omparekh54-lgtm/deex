# Fast data pipeline and local backtests for all 11 strategies

Two commands: one downloads ten years of everything the strategies need into one folder,
the other backtests all eleven strategies from that folder.

```powershell
pip install -r requirements-pipeline.txt
python scripts/fetch_all_data.py            # downloads into .\deex_data
python scripts/run_all_strategies.py        # writes .\deex_data\reports\summary.md
```

`--quick` downloads only what S1–S10 need (prices, ETFs, indices) and usually finishes in
5–10 minutes. `--root D:\DEEX_Data` puts the data on another drive. Stop at any time with
Ctrl+C and run the same command again: finished files are kept and skipped.

## What is downloaded

| Data | Used by | Source | Requests |
|---|---|---|---|
| Daily OHLCV, turnover, ISIN for every NSE stock and ETF, incl. delisted (2014→) | all 11 | GitHub mirror of NSE's bhavcopy archive (git sparse clone), NSE archive for the newest weeks | ~1 clone + a few dozen |
| Nifty 50 / 500 / MidSmallcap 400 / Midcap 150 / Smallcap 250, India VIX | PMS (C, E), reports | NSE daily index file | 1 per session |
| Shareholding: promoter %, pledge % of promoter, shares outstanding (2018→) | PMS (A) | public reconstruction of NSE SHP XBRL (GitHub CSV) | 1 |
| Historical index membership (Nifty 50 exclusion, sector fallback) | PMS (B) | public reconstruction of NSE press releases (GitHub CSV) | 1 |
| Sector / industry | PMS (A, C) | NSE index constituent lists | 5 |
| ASM/GSM surveillance (Jan 2024→) | PMS (A) | NSE daily REG_IND file | 1 per session |
| Corporate actions, board meetings (results dates) | PMS (dividends, blackout) | NSE APIs, monthly windows | ~290 |
| Quarterly/annual results (sales, profit, EPS, OCF, debt, equity, interest) | PMS (A) | NSE result filings + original XBRL | ~130 discovery + one XBRL per company-quarter |

The fundamentals step downloads only what Module A can use: companies that pass its market-cap
(> Rs 1,000 cr) and liquidity (63-session average ≥ Rs 2 cr) rules on at least one session,
computed exactly as the replay computes them, excluding financial-sector issuers that Module A
rejects anyway, and only the quarters each needs (eight before it first qualifies onward). One
statement scope per quarter (consolidated when filed), every revision kept with its filing time.

## How long it takes

Measured: prices for 2014–2026 build in about 1.5 minutes; S1–S10 backtest in about 6 minutes;
the PMS replay takes about 9 minutes. The NSE steps could not be timed from the development
environment, so these are estimates from request counts and the pacing limits below:

| Step | Requests | Estimate |
|---|---|---|
| Prices | git clone (~290 MB) | 1–3 min |
| Indices + surveillance | ~3,600 archive files | 5–8 min |
| Reference + events | ~300 | 2–3 min |
| Fundamentals | ~130 + 35,000–42,000 XBRL files | 40–60 min |

So `--quick` (S1–S10) is about 5–10 minutes and the full run about 50–75 minutes the first
time, depending on your connection and how NSE responds. Reruns only fetch what is missing.
Ten years of point-in-time fundamentals is the long pole: there is no bulk source, and NSE
publishes one XBRL statement per company per quarter.

## Not getting blocked

`packages/datapipe/net.py` paces every host separately: NSE's website API at most 2 concurrent
requests and 3 per second; NSE's static archive 10 concurrent and 8–16 per second; GitHub 8
concurrent. Pace starts low and rises only while responses stay clean. Any 401/403/429/503
halves the pace, pauses that host (30 s, 60 s, 120 s…) and refreshes the NSE session cookies.
Six refusals in a row stop the step cleanly with a message; wait 15–30 minutes and rerun.
`--pace 0.5` halves every limit if your network is sensitive. No proxies or IP rotation.

## Output folder

`prices.parquet`, `indices.parquet`, `shareholding.parquet`, `membership.parquet`,
`sectors.parquet`, `surveillance.parquet`, `actions.parquet`, `board_meetings.parquet`,
`financials.parquet`, plus original downloads (`bhavcopy/`, `indices/raw/`, `events/`,
`financials/xbrl/*.xml.gz`) and `manifest.json` with counts, coverage and problems per step.
Reports go to `reports/` (one JSON per strategy, `summary.csv`, `summary.md`).

## Known limits (also printed in every PMS report)

* **PMS Setup 1 effectively starts mid-2018.** Public shareholding/pledge history begins with
  NSE's 2018 SHP XBRL taxonomy, so Module A has no market cap or pledge before then. Results
  XBRL is also sparse for older filings, and annual cash flow (OCF/PAT) only appears in results
  XBRL from FY2019-20. The S1–S10 price strategies cover the full ten years.
* Sector is today's NSE classification applied to all dates (historical sectoral-index
  membership for companies no longer listed).
* ASM/GSM history exists from 23 Jan 2024. Earlier dates are treated as clear unless
  `--strict-surveillance` is passed, which makes Module A fail them as the original gate does.
* Splits and bonuses are back-adjusted from opening gaps (identical to the audited S1–S10
  engines); PMS price/volume/market-cap tests use unadjusted levels.
* The PMS replay calls the same Module A/B/C/E functions and trade engine as
  `MatrixReplay`, but reads the local folder instead of the Design Experience database.
  Intraday filing-order checks and held-stock delisting exits are not modelled.

These are research backtests, not certified results or investment advice.
