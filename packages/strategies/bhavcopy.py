"""NSE cash-market bhavcopy loader and corporate-action-adjusted daily panel.

Data source order for each trading day:
  1. A local copy (env ``DEEX_BHAVCOPY_DIR`` pointing at a folder with ``legacy/YYYY`` and ``udiff/YYYY``).
  2. The on-disk download cache (env ``DEEX_BHAVCOPY_CACHE``, default ``/tmp/deex_bhavcopy``).
  3. The public mirror of NSE's archive (github.com/Harikrish0406/nse-equity-bhavcopy-archive).
  4. NSE's own archive (for days newer than the mirror; NSE may refuse cloud servers).

Bhavcopies are unadjusted and survivorship-free. Splits and bonuses are back-adjusted from
the opening gap (see ``adjust``). Weekend special sessions are dropped.
"""
from __future__ import annotations

import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

MIRROR = os.getenv("DEEX_BHAVCOPY_MIRROR",
                   "https://raw.githubusercontent.com/Harikrish0406/nse-equity-bhavcopy-archive/main")
NSE_ARCHIVE = "https://nsearchives.nseindia.com/content/cm"
UDIFF_FROM = date(2024, 1, 1)
KEEP_SERIES = {"EQ", "BE", "BZ"}
_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


class BhavcopyUnavailable(RuntimeError):
    pass


def relpath(day: date) -> str:
    if day < UDIFF_FROM:
        return f"legacy/{day.year}/cm{day.day:02d}{_MONTHS[day.month - 1]}{day.year}bhav.csv.zip"
    return f"udiff/{day.year}/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"


def cache_dir() -> Path:
    path = Path(os.getenv("DEEX_BHAVCOPY_CACHE", "/tmp/deex_bhavcopy"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def _client():
    import httpx
    return httpx.Client(timeout=30, follow_redirects=False,
                        headers={"User-Agent": "Mozilla/5.0 (deex research)", "Accept": "*/*"})


def _no_session_days(client) -> set[date]:
    """Weekdays the mirror recorded as non-trading (NSE returned 404)."""
    local = cache_dir() / "_no_session.txt"
    text = None
    if local.exists():
        text = local.read_text()
    else:
        try:
            r = client.get(f"{MIRROR}/_no_session.txt")
            if r.status_code == 200:
                text = r.text
                local.write_text(text)
        except Exception:
            text = None
    out = set()
    for line in (text or "").split():
        try:
            out.add(date.fromisoformat(line.strip()[:10]))
        except ValueError:
            continue
    return out


def _fetch(day: date, client, no_session: set[date]) -> Path | None:
    """Return a local zip path for ``day`` or None for a confirmed non-trading day."""
    rel = relpath(day)
    local_root = os.getenv("DEEX_BHAVCOPY_DIR")
    if local_root and (Path(local_root) / rel).exists():
        return Path(local_root) / rel
    cached = cache_dir() / rel
    if cached.exists():
        return cached
    if (cache_dir() / "holidays" / day.isoformat()).exists() or day in no_session:
        return None
    urls = [f"{MIRROR}/raw/{rel}"]
    if day >= UDIFF_FROM:
        urls.append(f"{NSE_ARCHIVE}/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip")
    statuses = []
    for url in urls:
        for _ in range(3):
            try:
                r = client.get(url)
            except Exception as exc:  # network error: retry, then report
                statuses.append(type(exc).__name__)
                continue
            statuses.append(r.status_code)
            if r.status_code == 200 and r.content[:2] == b"PK":
                cached.parent.mkdir(parents=True, exist_ok=True)
                tmp = cached.with_suffix(".part")
                tmp.write_bytes(r.content)
                tmp.replace(cached)
                return cached
            if r.status_code == 404:
                break
            if r.status_code < 500 and r.status_code != 429:
                break
    if statuses and all(s == 404 for s in statuses):
        # 404 from every source. The mirror is complete up to its last update, so a weekday it
        # lacks and NSE also lacks is treated as a holiday.
        (cache_dir() / "holidays").mkdir(exist_ok=True)
        (cache_dir() / "holidays" / day.isoformat()).touch()
        return None
    raise BhavcopyUnavailable(
        f"NSE bhavcopy for {day.isoformat()} could not be downloaded (responses: {statuses}). "
        "Set DEEX_BHAVCOPY_DIR to a local archive or choose an earlier end date.")


def _parse(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path)
    d.columns = [c.strip() for c in d.columns]
    if "SERIES" in d.columns:   # legacy format
        d = d[d.SERIES.isin(KEEP_SERIES)]
        return pd.DataFrame({
            "date": pd.to_datetime(d.TIMESTAMP, format="mixed", dayfirst=True),
            "sym": d.SYMBOL.astype(str).str.strip(),
            "isin": d["ISIN"] if "ISIN" in d.columns else pd.Series(index=d.index, dtype=object),
            "series": d.SERIES, "open": d.OPEN, "high": d.HIGH, "low": d.LOW, "close": d.CLOSE,
            "prev": d.PREVCLOSE, "vol": d.TOTTRDQTY, "turnover": d.TOTTRDVAL})
    d = d[d.SctySrs.isin(KEEP_SERIES)]
    return pd.DataFrame({
        "date": pd.to_datetime(d.TradDt), "sym": d.TckrSymb.astype(str).str.strip(), "isin": d.ISIN,
        "series": d.SctySrs, "open": d.OpnPric, "high": d.HghPric, "low": d.LwPric, "close": d.ClsPric,
        "prev": d.PrvsClsgPric, "vol": d.TtlTradgVol, "turnover": d.TtlTrfVal})


def load_raw(start: date, end: date, progress=None, check=None, workers: int = 8) -> pd.DataFrame:
    """Raw (unadjusted) EQ/BE/BZ rows for every weekday in [start, end]."""
    days = [start + timedelta(n) for n in range((end - start).days + 1)]
    days = [d for d in days if d.weekday() < 5]
    if not days:
        raise BhavcopyUnavailable("No weekdays in the requested data window.")
    paths: list[Path | None] = [None] * len(days)
    with _client() as client:
        no_session = _no_session_days(client)

        def job(i):
            if check:
                check()
            paths[i] = _fetch(days[i], client, no_session)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for n, _ in enumerate(pool.map(job, range(len(days)))):
                if progress and n % 50 == 0:
                    progress(n / len(days), f"Loading NSE bhavcopies ({n}/{len(days)} days)")
    frames = []
    for p in paths:
        if p is None:
            continue
        if check and len(frames) % 100 == 0:
            check()
        try:
            frames.append(_parse(p))
        except (zipfile.BadZipFile, KeyError, ValueError) as exc:
            raise BhavcopyUnavailable(f"Unreadable bhavcopy file {p.name}: {exc}") from None
    if not frames:
        raise BhavcopyUnavailable("No bhavcopy files were found for the requested window.")
    return pd.concat(frames, ignore_index=True)


def adjust(raw: pd.DataFrame) -> pd.DataFrame:
    """Link symbols to ISINs and back-adjust splits/bonuses.

    Identical to the logic used for the published backtests:
    * a security id links symbol renames and ISIN changes (union-find);
    * an opening gap below 0.77x or above 1.3x the prior close is treated as a corporate action
      when it is within 7% of a standard ratio, or when below 0.6x (demerger / capital reduction).
    """
    df = raw[raw.date.dt.dayofweek < 5]
    df = df.sort_values("vol").drop_duplicates(["date", "sym"], keep="last")
    parent: dict[str, str] = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    pairs = df.dropna(subset=["isin"])[["sym", "isin"]].drop_duplicates()
    for s, i in pairs.itertuples(index=False):
        ra, rb = find("S:" + s), find("I:" + str(i))
        if ra != rb:
            parent[rb] = ra
    df = df.assign(sid=["S:" + s for s in df.sym])
    df["sid"] = df.sid.map(find)
    dup = df.duplicated(["date", "sid"], keep=False)
    df.loc[dup, "sid"] = "S:" + df.loc[dup, "sym"]
    df = df.drop_duplicates(["date", "sid"], keep="last")
    df = df.sort_values(["sid", "date"]).reset_index(drop=True)
    prev_close = df.groupby("sid").close.shift(1)
    r = (df.open / prev_close).values
    canon = np.array([1 / 2, 1 / 3, 1 / 4, 1 / 5, 1 / 10, 2 / 3, 3 / 4, 1 / 6, 2 / 5, 1 / 8, 1 / 20, 2, 5, 10])
    fac = np.ones(len(df))
    for i in np.where(np.isfinite(r) & ((r < 0.77) | (r > 1.3)))[0]:
        c = canon[np.argmin(np.abs(r[i] / canon - 1))]
        if abs(r[i] / c - 1) < 0.07:
            fac[i] = c
        elif r[i] < 0.6:
            fac[i] = r[i]
    df["f"] = fac
    df["adj"] = df.groupby("sid").f.transform(lambda x: x[::-1].cumprod()[::-1].shift(-1).fillna(1.0))
    for c in ["open", "high", "low", "close"]:
        df[c] = df[c] * df.adj
    df["vol"] = df.vol / df.adj
    last_sym = df.groupby("sid").sym.last()
    fund = df.groupby("sid")["isin"].agg(lambda x: x.astype(str).str.startswith("INF").any())
    df["fund"] = df.sid.map(fund)
    df["rights"] = df.sym.str.contains(r"-RE\d*$", regex=True)
    df["sym"] = df.sid.map(last_sym)
    # "adj" (back-adjustment factor) and "isin" let callers recover traded price levels and join
    # filings; the S1-S10 engines ignore them.
    return df[["date", "sid", "sym", "series", "open", "high", "low", "close", "vol", "turnover", "fund", "rights",
               "adj", "isin"]]


def load_panel(start: date, end: date, progress=None, check=None) -> pd.DataFrame:
    return adjust(load_raw(start, end, progress, check))
