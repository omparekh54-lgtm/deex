"""Benchmarks/VIX, shareholding & pledge, index membership, sectors, listing master, surveillance."""
from __future__ import annotations

import io
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import pandas as pd

from .net import Net, NotFound
from .store import INDEX_NAMES, Progress, Store, Window

ARCHIVE = "https://nsearchives.nseindia.com"
SHAREHOLDING_CSV = ("https://raw.githubusercontent.com/yurukatsu/nse-historical-membership/"
                    "main/shareholding_history/data/parsed/_flat.csv")
MEMBERSHIP_CSV = ("https://raw.githubusercontent.com/aditya-jha/nse-historical-membership/"
                  "main/index_history/data/index_membership_history.csv")
# Current NSE index constituent lists carry an official 'Industry' (sector) column.
SECTOR_LISTS = ["ind_niftytotalmarket_list.csv", "ind_nifty500list.csv", "ind_niftymicrocap250_list.csv",
                "ind_niftysmallcap250list.csv", "ind_niftymidcap150list.csv"]
# Sector index membership is used for companies no longer in any current list (e.g. delisted).
SECTOR_FROM_INDEX = {
    "Nifty Bank": "Financial Services", "Nifty Private Bank": "Financial Services",
    "Nifty PSU Bank": "Financial Services", "Nifty Financial Services": "Financial Services",
    "Nifty IT": "Information Technology", "Nifty FMCG": "Fast Moving Consumer Goods",
    "Nifty Pharma": "Healthcare", "Nifty Healthcare": "Healthcare", "Nifty Auto": "Automobile and Auto Components",
    "Nifty Metal": "Metals & Mining", "Nifty Realty": "Realty", "Nifty Energy": "Oil Gas & Consumable Fuels",
    "Nifty Oil & Gas": "Oil Gas & Consumable Fuels", "Nifty Media": "Media Entertainment & Publication",
    "Nifty Consumer Durables": "Consumer Durables",
}


def sessions(store: Store) -> list[date]:
    df = pd.read_parquet(store.root / "prices.parquet", columns=["date"])
    return sorted(d.date() for d in df["date"].drop_duplicates())


# ---------------------------------------------------------------------------- indices
def _parse_index_day(body: bytes, day: date) -> list[dict]:
    d = pd.read_csv(io.BytesIO(body))
    d.columns = [c.strip() for c in d.columns]
    rows = []
    for _, r in d.iterrows():
        code = INDEX_NAMES.get(str(r.get("Index Name", "")).strip())
        if not code:
            continue
        num = lambda k: pd.to_numeric(str(r.get(k, "")).replace(",", "").strip(), errors="coerce")  # noqa: E731
        rows.append(dict(date=day, code=code, open=num("Open Index Value"), high=num("High Index Value"),
                         low=num("Low Index Value"), close=num("Closing Index Value")))
    return rows


def fetch_indices(store: Store, net: Net, window: Window, workers: int = 8, log=print) -> dict:
    days = [d for d in sessions(store) if d <= window.end]
    raw = store.dir("indices", "raw")
    todo = [d for d in days if not (raw / f"{d:%Y%m%d}.csv").exists()]
    log(f"Indices: Nifty 50/500/MidSmallcap 400/Midcap 150/Smallcap 250 + India VIX, {len(days)} sessions "
        f"({len(todo)} to download)")
    missing = []
    if todo:
        prog = Progress("index files", len(todo), log)

        def one(day: date):
            url = f"{ARCHIVE}/content/indices/ind_close_all_{day:%d%m%Y}.csv"
            try:
                store.write_atomic(raw / f"{day:%Y%m%d}.csv", net.get(url))
            except NotFound:
                missing.append(str(day))

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for fut in as_completed([pool.submit(one, d) for d in todo]):
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001
                    missing.append(str(exc)[:120])
                prog.tick()
    rows = []
    for f in sorted(raw.glob("*.csv")):
        day = datetime.strptime(f.stem, "%Y%m%d").date()
        try:
            rows.extend(_parse_index_day(f.read_bytes(), day))
        except (ValueError, pd.errors.ParserError):
            missing.append(f"unparseable {f.name}")
    df = pd.DataFrame(rows).drop_duplicates(["date", "code"], keep="last").sort_values(["code", "date"])
    df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(store.root / "indices.parquet", index=False)
    cover = {c: int(n) for c, n in df.groupby("code").close.count().items()}
    info = dict(sessions=len(days), rows=len(df), per_index=cover, missing=len(missing), missing_examples=missing[:20])
    store.record("indices", **info)
    log(f"  index rows per code: {cover}")
    return info


# ---------------------------------------------------------------------------- reference
def fetch_reference(store: Store, net: Net, log=print) -> dict:
    ref = store.dir("reference")
    info = {}
    log("Reference: shareholding/pledge history, index membership history, sectors, listing master")
    for name, url in (("shareholding_flat.csv", SHAREHOLDING_CSV), ("index_membership.csv", MEMBERSHIP_CSV)):
        dst = ref / name
        if not dst.exists():
            store.write_atomic(dst, net.get(url))
        info[name] = dst.stat().st_size
    # Shareholding -> compact point-in-time table keyed by ISIN.
    sh = pd.read_csv(ref / "shareholding_flat.csv", low_memory=False,
                     usecols=lambda c: c in {"ticker", "period", "filing_date", "submission_date", "broadcast_date",
                                             "revised", "promoter_pct", "pledge_pct", "pledge_pct_of_promoter",
                                             "total_shares", "isin", "record_id"})
    for c in ("filing_date", "submission_date", "broadcast_date"):
        sh[c] = pd.to_datetime(sh[c], errors="coerce", dayfirst=True, format="mixed")
    sh["period_end"] = pd.to_datetime(sh["period"].astype(str) + "-01", errors="coerce") + pd.offsets.MonthEnd(0)
    sh["filed_at"] = sh["broadcast_date"].fillna(sh["submission_date"]).fillna(sh["filing_date"])
    # Missing filing time: assume the regulatory deadline (21 days after quarter end), never earlier.
    sh["filed_at"] = sh["filed_at"].fillna(sh["period_end"] + pd.Timedelta(days=21))
    sh = sh.dropna(subset=["period_end", "total_shares"]).rename(columns={"ticker": "sym"})
    sh = sh[["sym", "isin", "period_end", "filed_at", "promoter_pct", "pledge_pct", "pledge_pct_of_promoter",
             "total_shares", "revised"]]
    sh.to_parquet(store.root / "shareholding.parquet", index=False)
    info["shareholding_rows"] = len(sh)
    info["shareholding_symbols"] = int(sh.sym.nunique())
    mem = pd.read_csv(ref / "index_membership.csv", low_memory=False)
    mem["valid_from"] = pd.to_datetime(mem["valid_from"], errors="coerce")
    mem["valid_to"] = pd.to_datetime(mem["valid_to"], errors="coerce")
    mem.to_parquet(store.root / "membership.parquet", index=False)
    info["membership_rows"] = len(mem)
    # Sectors: official current lists, then historical sector-index membership as a fallback.
    sectors = []
    for name in SECTOR_LISTS:
        dst = ref / name
        try:
            if not dst.exists():
                store.write_atomic(dst, net.get(f"{ARCHIVE}/content/indices/{name}"))
            d = pd.read_csv(dst)
            d.columns = [c.strip() for c in d.columns]
            for _, r in d.iterrows():
                sectors.append(dict(sym=str(r.get("Symbol", "")).strip(), isin=str(r.get("ISIN Code", "")).strip(),
                                    sector=str(r.get("Industry", "")).strip(), source="NSE_INDEX_LIST_CURRENT"))
        except Exception as exc:  # noqa: BLE001
            log(f"  sector list {name} unavailable: {exc}")
    for idx, sector in SECTOR_FROM_INDEX.items():
        for s in mem.loc[mem.index_name == idx, "symbol"].dropna().unique():
            sectors.append(dict(sym=str(s), isin=None, sector=sector, source="SECTOR_INDEX_MEMBERSHIP"))
    sec = pd.DataFrame(sectors)
    if not sec.empty:
        sec = sec[sec.sector.astype(bool) & (sec.sector != "nan")].drop_duplicates(["sym"], keep="first")
    sec.to_parquet(store.root / "sectors.parquet", index=False)
    info["sector_symbols"] = int(len(sec))
    # Listing master (names, listing dates, ISIN) for all currently listed equities.
    try:
        dst = ref / "EQUITY_L.csv"
        if not dst.exists():
            store.write_atomic(dst, net.get(f"{ARCHIVE}/content/equities/EQUITY_L.csv"))
        info["equity_master_rows"] = int(len(pd.read_csv(dst)))
    except Exception as exc:  # noqa: BLE001
        info["equity_master_error"] = str(exc)[:200]
    store.record("reference", **info)
    log(f"  shareholding rows {info['shareholding_rows']:,} ({info['shareholding_symbols']:,} symbols), "
        f"membership rows {info['membership_rows']:,}, sectors for {info['sector_symbols']:,} symbols")
    return info


def fill_sectors(store: Store, net: Net, symbols: list[str], log=print) -> int:
    """Fetch NSE's current industry for listed symbols that no list covered (bounded, polite)."""
    path = store.root / "sectors.parquet"
    sec = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=["sym", "isin", "sector", "source"])
    cache = store.dir("reference", "quotes")
    need = [s for s in symbols if s not in set(sec.sym)]
    if not need:
        return 0
    log(f"  looking up current NSE industry for {len(need)} more symbols")
    prog = Progress("industry lookups", len(need), log)
    added = []
    for sym in need:
        f = cache / f"{re.sub(r'[^A-Za-z0-9_&-]', '_', sym)}.json"
        try:
            if not f.exists():
                store.write_atomic(f, net.api("quote-equity", symbol=sym))
            info = json.loads(f.read_bytes()).get("industryInfo") or {}
            sector = info.get("sector") or info.get("macro")
            if sector:
                added.append(dict(sym=sym, isin=None, sector=sector, source="NSE_QUOTE_CURRENT"))
        except NotFound:
            store.write_atomic(f, b"{}")
        except (ValueError, json.JSONDecodeError):
            pass
        prog.tick()
    if added:
        sec = pd.concat([sec, pd.DataFrame(added)], ignore_index=True).drop_duplicates(["sym"], keep="first")
        sec.to_parquet(path, index=False)
    return len(added)


# ---------------------------------------------------------------------------- surveillance
def _norm(c: str) -> str:
    return re.sub(r"[^a-z0-9]", "", c.lower())


def _parse_reg_ind(body: bytes, day: date) -> list[dict]:
    d = pd.read_csv(io.BytesIO(body), low_memory=False)
    cols = {_norm(c): c for c in d.columns}
    sym = cols.get("symbol")
    gsm = cols.get("gsm")
    lt = next((v for k, v in cols.items() if "longterm" in k and "asm" in k), None)
    st = next((v for k, v in cols.items() if "shortterm" in k and "asm" in k), None)
    if not sym:
        return []
    out = []
    for _, r in d.iterrows():
        vals = {}
        for name, col in (("GSM", gsm), ("ASM_LT", lt), ("ASM_ST", st)):
            if col is None:
                continue
            v = pd.to_numeric(r[col], errors="coerce")
            if pd.notna(v) and int(v) != 100:   # 100 = not applicable; GSM 0 is an active stage
                vals[name] = int(v)
        if vals:
            out.append(dict(date=day, sym=str(r[sym]).strip(), **vals))
    return out


def fetch_surveillance(store: Store, net: Net, window: Window, workers: int = 6, log=print) -> dict:
    """NSE's daily ASM/GSM indicator file. Standardised history exists from 23 Jan 2024 only."""
    first = max(date(2024, 1, 23), window.price_start)
    days = [d for d in sessions(store) if first <= d <= window.end]
    raw = store.dir("surveillance", "raw")
    todo = [d for d in days if not (raw / f"{d:%Y%m%d}.csv").exists() and not (raw / f"{d:%Y%m%d}.none").exists()]
    log(f"Surveillance (ASM/GSM): {len(days)} sessions since {first} ({len(todo)} to download)")
    if todo:
        prog = Progress("surveillance files", len(todo), log)

        def one(day: date):
            for kind in ("REG1_IND", "REG_IND"):
                try:
                    store.write_atomic(raw / f"{day:%Y%m%d}.csv",
                                       net.get(f"{ARCHIVE}/content/cm/{kind}{day:%d%m%y}.csv"))
                    return
                except NotFound:
                    continue
            (raw / f"{day:%Y%m%d}.none").touch()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for fut in as_completed([pool.submit(one, d) for d in todo]):
                try:
                    fut.result()
                except Exception:  # noqa: BLE001 - retried next run
                    pass
                prog.tick()
    rows, covered = [], []
    for f in sorted(raw.glob("*.csv")):
        day = datetime.strptime(f.stem, "%Y%m%d").date()
        try:
            rows.extend(_parse_reg_ind(f.read_bytes(), day))
            covered.append(day)
        except (ValueError, pd.errors.ParserError):
            pass
    df = pd.DataFrame(rows, columns=["date", "sym", "GSM", "ASM_LT", "ASM_ST"])
    df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(store.root / "surveillance.parquet", index=False)
    pd.DataFrame({"date": pd.to_datetime(covered)}).to_parquet(store.root / "surveillance_days.parquet", index=False)
    info = dict(first=str(first), sessions=len(days), files=len(covered), flagged_rows=len(df))
    store.record("surveillance", **info)
    return info
