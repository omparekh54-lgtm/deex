"""Point-in-time quarterly/annual financials for PMS Setup 1 (Module A), from NSE XBRL filings.

Speed comes from fetching only what Module A can use:
* companies that pass Module A's market-cap and liquidity rules on at least one session (computed
  exactly as the replay computes them), excluding financial-sector issuers Module A rejects anyway;
* only the quarters each company needs (eight quarters before it first qualifies onwards);
* one statement scope per quarter (consolidated when filed, otherwise standalone), with every
  revision of that scope kept as a separate point-in-time observation.
Each XBRL is downloaded once, stored gzip-compressed and parsed with the repository's own
extractor (``packages.data_layer.exchange_financials``), so values match the PMS pipeline.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import date, timedelta

import pandas as pd

from .net import HostBlocked, Net, NotFound
from .store import Progress, Store, Window, months

INTEGRATED_FROM = date(2025, 1, 1)
FIN_SECTORS = {"financialservices", "financials", "banks", "banking", "nbfc", "insurance"}
FACT_KEYS = ("sales", "operating_profit", "pat", "diluted_eps", "pbt", "other_income", "exceptional_items",
             "operating_cash_flow", "debt", "equity", "interest_expense")


def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


# ------------------------------------------------------------------------ 1. candidates
def _thresholds():
    from packages.pms.config import load_strategy_config
    f = load_strategy_config()["fundamental"]
    return float(f["min_market_cap_cr"]), float(f["min_avg_traded_value_cr"]) * 1e7, int(f.get("liquidity_sessions", 63))


def candidates(store: Store, window: Window, log=print) -> pd.DataFrame:
    """Companies (by ISIN) and the months in which they pass Module A's size and liquidity rules.

    Uses exactly the quantities the PMS replay uses: unadjusted close x latest filed share count, and
    the 63-session average traded value. Evaluated on every session, then summarised by month."""
    min_cap, min_adtv, n = _thresholds()
    px = pd.read_parquet(store.root / "prices.parquet",
                         columns=["date", "sym", "isin", "series", "close", "turnover"])
    px = px[(px.series == "EQ") & px["isin"].notna() & ~px["isin"].astype(str).str.startswith("INF")]
    px = px.sort_values(["isin", "date"])
    px["adtv"] = px.groupby("isin").turnover.transform(lambda s: s.rolling(n, min_periods=n).mean())
    px = px[px.date >= pd.Timestamp(window.start) - pd.Timedelta(days=120)]
    sh = pd.read_parquet(store.root / "shareholding.parquet", columns=["isin", "filed_at", "total_shares"])
    sh = sh.dropna().sort_values("filed_at").rename(columns={"filed_at": "date"})
    merged = pd.merge_asof(px.sort_values("date"), sh, on="date", by="isin", direction="backward")
    merged["mcap_cr"] = merged.close * merged.total_shares / 1e7
    ok = merged[(merged.mcap_cr > min_cap) & (merged.adtv >= min_adtv)].copy()
    ok["month"] = ok.date.dt.to_period("M")
    sectors = pd.read_parquet(store.root / "sectors.parquet") if (store.root / "sectors.parquet").exists() \
        else pd.DataFrame(columns=["sym", "sector"])
    fin = {s for s, sec in zip(sectors.sym, sectors.sector) if _key(sec) in FIN_SECTORS}
    names = px.groupby("isin").sym.agg(lambda s: list(dict.fromkeys(s[::-1])))  # newest symbol first
    out = ok.groupby("isin").agg(first_month=("month", "min"), last_month=("month", "max"),
                                 sym=("sym", "last")).reset_index()
    out["symbols"] = out["isin"].map(names)
    out["financial_sector"] = out.symbols.map(lambda ss: any(s in fin for s in ss))
    info = dict(candidates=len(out), financial_sector_skipped=int(out.financial_sector.sum()),
                first_month=str(out.first_month.min()) if len(out) else None)
    log(f"  {len(out):,} companies pass Module A's market-cap (> Rs {min_cap:,.0f} cr) and liquidity rules at some "
        f"point ({info['financial_sector_skipped']} financial-sector issuers skipped: Module A excludes them)")
    out = out[~out.financial_sector].reset_index(drop=True)
    out.to_parquet(store.root / "financial_candidates.parquet", index=False)
    store.record("financial_candidates", **info)
    return out


# ------------------------------------------------------------------------ 2. discovery
def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_&-]", "_", name)


def _rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for k in ("data", "rows", "results"):
            if isinstance(payload.get(k), list):
                return [r for r in payload[k] if isinstance(r, dict)]
    return []


def _sym_map(cands: pd.DataFrame) -> dict:
    """Any historical symbol of a candidate -> the candidate's current symbol."""
    out = {}
    for r in cands.itertuples():
        for s in r.symbols:
            out.setdefault(str(s).upper(), r.sym)
    return out


def discover(store: Store, net: Net, window: Window, cands: pd.DataFrame, log=print) -> pd.DataFrame:
    """Legacy result filings by monthly window (one call per month for all companies), then per-symbol
    look-ups only for candidates the windows missed, then integrated filings for 2025 onwards."""
    sym_map = _sym_map(cands)
    isin_of = dict(zip(cands.sym, cands.isin))
    first_needed = max(window.financial_start,
                       (cands.first_month.min().to_timestamp() - pd.Timedelta(days=820)).date()) if len(cands) else window.financial_start
    legacy_end = min(window.end, INTEGRATED_FROM + timedelta(days=89))
    folder = store.dir("financials", "monthly")
    wins = list(months(first_needed, legacy_end))
    todo = [w for w in wins if not (folder / f"{w[0]:%Y-%m}.json").exists()]
    log(f"  filing discovery: {len(wins)} monthly windows ({len(todo)} to fetch)")
    prog = Progress("monthly filing lists", len(todo), log) if todo else None
    failed = []

    def month(w):
        a, b = w
        body = net.api("corporates-financial-results", index="equities", period="Quarterly",
                       from_date=a.strftime("%d-%m-%Y"), to_date=b.strftime("%d-%m-%Y"))
        rows = _rows(json.loads(body))
        store.write_atomic(folder / f"{a:%Y-%m}.json", json.dumps(rows).encode())

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(month, w): w for w in todo}
        for fut in as_completed(futures):
            try:
                fut.result()
            except HostBlocked:
                raise
            except Exception as exc:  # noqa: BLE001 - retried next run
                failed.append(f"{futures[fut][0]:%Y-%m}: {str(exc)[:80]}")
            prog.tick()
    legacy, quarters = [], defaultdict(set)
    for f in sorted(folder.glob("*.json")):
        for row in json.loads(f.read_bytes()):
            sym = sym_map.get(str(row.get("symbol", "")).upper())
            if sym:
                legacy.append({**row, "_isin": isin_of.get(sym), "_sym": sym, "_system": "legacy"})
                quarters[sym].add(str(row.get("toDate")))
    # Per-company fallback when the monthly lists returned too few quarters for a company
    # (renamed symbols, a truncated month, or a filing listed outside its window).
    def expected(r):
        lo = max(first_needed, (r.first_month.to_timestamp() - pd.Timedelta(days=820)).date())
        hi = min(legacy_end - timedelta(days=60), r.last_month.to_timestamp(how="end").date())
        return max(0, (hi - lo).days // 92)
    pfolder = store.dir("financials", "listings")
    missing = [r for r in cands.drop_duplicates("sym").itertuples()
               if r.first_month.to_timestamp().date() < INTEGRATED_FROM
               and len(quarters.get(r.sym, ())) < expected(r) - 1]
    todo = [r for r in missing if not (pfolder / f"{_safe(r.sym)}.json").exists()]
    if todo:
        log(f"  per-company look-ups for {len(todo)} companies with incomplete monthly lists")
        prog = Progress("company filing lists", len(todo), log)

        def one(r):
            rows = []
            for sym in r.symbols[:3]:
                try:
                    rows = _rows(json.loads(net.api("corporates-financial-results", index="equities",
                                                    period="Quarterly", symbol=sym)))
                except NotFound:
                    rows = []
                if rows:
                    break
            store.write_atomic(pfolder / f"{_safe(r.sym)}.json", json.dumps(rows).encode())

        with ThreadPoolExecutor(max_workers=2) as pool:
            for fut in as_completed([pool.submit(one, r) for r in todo]):
                try:
                    fut.result()
                except HostBlocked:
                    raise
                except Exception:  # noqa: BLE001
                    pass
                prog.tick()
    for r in missing:   # duplicates of monthly rows are harmless: documents are keyed by XBRL URL
        f = pfolder / f"{_safe(r.sym)}.json"
        if f.exists():
            for row in json.loads(f.read_bytes()):
                legacy.append({**row, "_isin": r.isin, "_sym": r.sym, "_system": "legacy"})
    integrated = _integrated(store, net, window, sym_map, log)
    df = pd.DataFrame(legacy + integrated)
    store.record("financial_discovery", monthly_windows=len(wins), failed_windows=failed[:20],
                 per_company_lookups=len(missing), filings=len(df))
    log(f"  discovered {len(df):,} result filings for candidate companies")
    if failed:
        log(f"  {len(failed)} monthly windows failed; rerun the command to retry them")
    return df


def _integrated(store: Store, net: Net, window: Window, sym_map: dict, log) -> list[dict]:
    """SEBI integrated filings (Mar-2025 quarter onwards): monthly windows, paged."""
    if window.end < INTEGRATED_FROM:
        return []
    folder = store.dir("financials", "integrated")
    out = []
    wins = list(months(INTEGRATED_FROM, window.end))
    current = date.today().replace(day=1)
    for a, b in wins:
        f = folder / f"{a:%Y-%m}.json"
        if not f.exists() or a >= current:
            rows, seen = [], set()
            for page in range(1, 120):
                payload = json.loads(net.api("integrated-filing-results", index="equities",
                                             type="Integrated Filing- Financials", page=page, size=100,
                                             from_date=a.strftime("%d-%m-%Y"), to_date=b.strftime("%d-%m-%Y")))
                batch = _rows(payload)
                fresh = [x for x in batch if json.dumps(x, sort_keys=True) not in seen]
                for x in fresh:
                    seen.add(json.dumps(x, sort_keys=True))
                rows.extend(fresh)
                total = payload.get("totalCount") if isinstance(payload, dict) else None
                if not batch or not fresh or len(batch) < 100 or (isinstance(total, int) and len(rows) >= total):
                    break
            store.write_atomic(f, json.dumps(rows).encode())
        for row in json.loads(f.read_bytes()):
            sym = sym_map.get(str(row.get("symbol") or row.get("sm_symbol") or "").upper())
            if sym:
                out.append({**row, "_sym": sym, "_system": "integrated"})
    log(f"  integrated filings (2025+): {len(out):,} for candidate companies")
    return out


# ------------------------------------------------------------------------ 3. selection
def _record(row: dict):
    from packages.data_layer.exchange_financials import NSEIntegratedFinancialsProvider
    raw = {k: v for k, v in row.items() if not str(k).startswith("_")}
    if raw.get("toDate") and not raw.get("quarterEndDate"):
        raw["quarterEndDate"] = raw["toDate"]
    if str(raw.get("consolidated", "")).strip().lower() in ("non-consolidated", "non consolidated", "standalone"):
        raw["consolidatedStandalone"] = "standalone"
    provider = object.__new__(NSEIntegratedFinancialsProvider)
    return provider._parse_row(raw), raw


def select(store: Store, window: Window, cands: pd.DataFrame, found: pd.DataFrame, log=print) -> list[dict]:
    """Choose the XBRL documents Module A needs; returns job dicts."""
    need = {}
    for r in cands.itertuples():   # one company can appear under two ISINs (e.g. after a split)
        first = r.first_month.to_timestamp().date() - timedelta(days=820)   # 8 quarters + filing lag
        lo, hi = max(first, window.financial_start), r.last_month.to_timestamp(how="end").date()
        if r.sym in need:
            lo, hi = min(lo, need[r.sym][0]), max(hi, need[r.sym][1])
        need[r.sym] = (lo, hi)
    by_quarter = defaultdict(list)
    placeholders = 0
    for row in found.to_dict("records"):
        record, raw = _record(row)
        if record is None:
            continue
        lo, hi = need.get(row["_sym"], (None, None))
        if lo is None or not lo <= record.period_end <= hi:
            continue
        uri = record.xbrl_uri or ""
        if not uri or uri.rstrip("/").endswith("/-") or not uri.lower().split("?")[0].endswith(".xml"):
            placeholders += 1
            continue
        by_quarter[(row["_sym"], record.period_end)].append((record, raw, row))
    jobs = []
    for (sym, period), items in by_quarter.items():
        scopes = {rec.statement_scope for rec, _, _ in items}
        scope = "consolidated" if "consolidated" in scopes else "standalone"
        for rec, raw, row in items:
            if rec.statement_scope == scope:
                jobs.append(dict(sym=sym, isin=row.get("_isin"), url=rec.xbrl_uri, row=raw,
                                 period_end=str(rec.period_end), scope=scope))
    uniq = {j["url"]: j for j in jobs}
    log(f"  {len(uniq):,} XBRL statements needed ({placeholders:,} older filings have no XBRL and are skipped)")
    store.record("financial_selection", documents=len(uniq), no_xbrl_placeholders=placeholders,
                 quarters=len(by_quarter))
    return list(uniq.values())


# ------------------------------------------------------------------------ 4. download + parse
def _doc_path(store: Store, url: str):
    h = hashlib.sha1(url.encode()).hexdigest()
    return store.root / "financials" / "xbrl" / h[:2] / f"{h}.xml.gz"


def download(store: Store, net: Net, jobs: list[dict], workers: int = 8, log=print) -> dict:
    todo = [j for j in jobs if not _doc_path(store, j["url"]).exists()
            and not _doc_path(store, j["url"]).with_suffix(".missing").exists()]
    log(f"  XBRL download: {len(jobs):,} statements ({len(todo):,} new) from NSE's archive")
    prog = Progress("XBRL files", len(todo), log, every=10) if todo else None
    failed = []

    def one(job):
        p = _doc_path(store, job["url"])
        try:
            body = net.get(job["url"], accept="application/xml,text/xml,*/*")
        except NotFound:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.with_suffix(".missing").touch()
            return
        if b"<" not in body[:200]:
            raise ValueError("not xml")
        store.write_atomic(p, gzip.compress(body, 6))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, j): j for j in todo}
        for fut in as_completed(futures):
            try:
                fut.result()
            except HostBlocked:
                for f in futures:
                    f.cancel()
                raise
            except Exception as exc:  # noqa: BLE001 - retried next run
                failed.append(str(exc)[:120])
            prog.tick()
    have = sum(_doc_path(store, j["url"]).exists() for j in jobs)
    info = dict(needed=len(jobs), downloaded=have, failed=len(failed), failed_examples=failed[:10])
    store.record("financial_download", **info)
    return info


def _parse_one(args):
    path, job = args
    try:
        from packages.data_layer.exchange_financials import filings_from_exchange_record
        record, _ = _record(job["row"])
        xml = gzip.decompress(open(path, "rb").read())
        if b"<!DOCTYPE" in xml[:2000].upper() or b"<!ENTITY" in xml[:4000].upper():
            return [], "entities_forbidden"
        out = []
        for f in filings_from_exchange_record(record, xml):
            d = asdict(f)
            out.append(dict(sym=job["sym"], isin=job.get("isin"), period_end=d["period_end"],
                            filed_at=d["filed_at"], filing_kind=d["filing_kind"],
                            consolidated=d["statement_scope"] == "consolidated", version=d["version"],
                            submission=d.get("submission_type"), url=job["url"],
                            **{k: (float(d[k]) if d[k] is not None else None) for k in FACT_KEYS}))
        return out, None
    except Exception as exc:  # noqa: BLE001 - recorded per document
        return [], type(exc).__name__ + ": " + str(exc)[:80]


def parse(store: Store, jobs: list[dict], log=print) -> dict:
    items = [(str(_doc_path(store, j["url"])), j) for j in jobs if _doc_path(store, j["url"]).exists()]
    log(f"  parsing {len(items):,} XBRL statements")
    prog = Progress("parse XBRL", len(items), log, every=10)
    rows, errors = [], defaultdict(int)
    workers = max(1, min(8, (os.cpu_count() or 2) - 1))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for out, err in pool.map(_parse_one, items, chunksize=32):
            rows.extend(out)
            if err:
                errors[err.split(":")[0]] += 1
            prog.tick()
    df = pd.DataFrame(rows)
    if not df.empty:
        df["period_end"] = pd.to_datetime(df.period_end)
        df["filed_at"] = pd.to_datetime(df.filed_at)
        df = df.sort_values(["sym", "period_end", "filed_at"]).reset_index(drop=True)
    df.to_parquet(store.root / "financials.parquet", index=False)
    usable = df.dropna(subset=["sales", "pat"]) if not df.empty else df
    by_year = usable.groupby(usable.period_end.dt.year).sym.nunique().to_dict() if not usable.empty else {}
    info = dict(statements=len(items), rows=len(df), usable_rows=len(usable),
                companies=int(usable.sym.nunique()) if not usable.empty else 0,
                companies_by_year={int(k): int(v) for k, v in by_year.items()}, parse_errors=dict(errors))
    store.record("financials", **info)
    log(f"  financial rows: {info['usable_rows']:,} usable for {info['companies']:,} companies; "
        f"companies by year: {info['companies_by_year']}")
    return info


def run(store: Store, net: Net, window: Window, workers: int = 8, log=print) -> dict:
    log("Fundamentals (PMS Setup 1, Module A): NSE result filings and XBRL statements")
    cands = candidates(store, window, log)
    found = discover(store, net, window, cands, log)
    jobs = select(store, window, cands, found, log)
    download(store, net, jobs, workers, log)
    return parse(store, jobs, log)
