"""Daily NSE cash-market bhavcopies (all stocks and ETFs, incl. delisted) -> one price table.

Source order, fastest and least intrusive first:
1. A sparse, shallow ``git clone`` of the public bhavcopy mirror (original NSE zips, one
   download, no NSE requests at all). Needs ``git`` on PATH.
2. The same mirror file-by-file over HTTPS if git is unavailable.
3. NSE's own archive only for trading days newer than the mirror (a few weeks at most).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .net import Net, NotFound
from .store import ETFS, Progress, Store, Window

MIRROR_REPO = "https://github.com/Harikrish0406/nse-equity-bhavcopy-archive"
MIRROR_RAW = "https://raw.githubusercontent.com/Harikrish0406/nse-equity-bhavcopy-archive/main"
NSE_ARCHIVE = "https://nsearchives.nseindia.com"
UDIFF_FROM = date(2024, 1, 1)   # the mirror keeps both formats Jan-Jul 2024
LEGACY_UNTIL = date(2024, 7, 5)
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
# Ordinary-equity series plus trade-for-trade/restricted series so held stocks never vanish.
KEEP_SERIES = {"EQ", "BE", "BZ", "SM", "ST", "SZ"}


def relpath(day: date) -> str:
    if day < UDIFF_FROM:
        return f"legacy/{day.year}/cm{day.day:02d}{MONTHS[day.month - 1]}{day.year}bhav.csv.zip"
    return f"udiff/{day.year}/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"


def nse_urls(day: date) -> list[str]:
    urls = []
    if day >= UDIFF_FROM:
        urls.append(f"{NSE_ARCHIVE}/content/cm/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip")
    if day <= LEGACY_UNTIL:
        urls.append(f"{NSE_ARCHIVE}/content/historical/EQUITIES/{day.year}/"
                    f"{MONTHS[day.month - 1]}/cm{day.day:02d}{MONTHS[day.month - 1]}{day.year}bhav.csv.zip")
    return urls


def _git_sparse(dest: Path, years: list[int], log) -> bool:
    if not shutil.which("git"):
        log("  git not found; downloading files individually instead (slower).")
        return False
    want = ["/_no_session.txt"] + [f"/raw/legacy/{y}/" for y in years if y <= 2024] + \
           [f"/raw/udiff/{y}/" for y in years if y >= 2024]
    try:
        if not (dest / ".git").exists():
            if dest.exists():
                shutil.rmtree(dest)
            subprocess.run(["git", "clone", "--quiet", "--depth", "1", "--filter=blob:none", "--no-checkout",
                            MIRROR_REPO, str(dest)], check=True, timeout=600)
            subprocess.run(["git", "-C", str(dest), "sparse-checkout", "init", "--no-cone"], check=True, timeout=60)
        else:
            subprocess.run(["git", "-C", str(dest), "fetch", "--quiet", "--depth", "1", "--filter=blob:none",
                            "origin", "main"], check=True, timeout=600)
        subprocess.run(["git", "-C", str(dest), "sparse-checkout", "set", "--no-cone", *want], check=True, timeout=60)
        subprocess.run(["git", "-C", str(dest), "checkout", "--quiet", "-B", "main", "origin/main"],
                       check=True, timeout=1800)
        return True
    except (subprocess.SubprocessError, OSError) as exc:
        log(f"  git mirror clone failed ({exc}); falling back to per-file download.")
        return False


def _valid_zip(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as z:
            return bool(z.namelist())
    except (zipfile.BadZipFile, OSError):
        return False


def download(store: Store, net: Net, window: Window, workers: int = 8, log=print) -> dict:
    """Place one bhavcopy zip per trading day under ``bhavcopy/``. Returns a summary."""
    mirror = store.root / "_mirror"
    target = store.dir("bhavcopy")
    end = window.end
    years = list(range(window.price_start.year, end.year + 1))
    log(f"Prices: NSE bhavcopies {window.price_start} -> {end} (all stocks and ETFs, incl. delisted)")
    used_git = _git_sparse(mirror, years, log)
    no_session: set[date] = set()
    ns_file = (mirror / "_no_session.txt") if used_git else store.path("bhavcopy", "_no_session.txt")
    if not used_git and not ns_file.exists():
        try:
            store.write_atomic(ns_file, net.get(MIRROR_RAW + "/_no_session.txt"))
        except Exception:  # noqa: BLE001 - optional optimisation only
            pass
    if ns_file.exists():
        for line in ns_file.read_text().split():
            try:
                no_session.add(date.fromisoformat(line.strip()[:10]))
            except ValueError:
                pass
    mirror_last = max(no_session) if no_session else date(2000, 1, 1)
    if used_git:
        for y in years:
            for kind in ("legacy", "udiff"):
                src = mirror / "raw" / kind / str(y)
                if src.is_dir():
                    for f in src.iterdir():
                        dst = target / kind / str(y) / f.name
                        if not dst.exists():
                            dst.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                os.link(f, dst)      # no extra disk space on the same drive
                            except OSError:
                                shutil.copy2(f, dst)
                        mirror_last = max(mirror_last, _day_from_name(f.name) or mirror_last)
    holidays = store.dir("bhavcopy", "_holidays")
    days = [window.price_start + timedelta(n) for n in range((end - window.price_start).days + 1)]
    todo = [d for d in days if not (target / relpath(d)).exists() and d not in no_session
            and not (holidays / d.isoformat()).exists() and not (d <= mirror_last and d.weekday() >= 5)]
    # Weekends after the mirror are skipped unless NSE published a special session; those
    # are rare and a 404 is cheap, but we do not probe them to keep requests minimal.
    todo = [d for d in todo if d.weekday() < 5]
    failed: list[str] = []
    if todo:
        log(f"  {len(todo)} trading days not in the mirror; fetching them individually")
        prog = Progress("bhavcopy days", len(todo), log)

        def fetch(day: date):
            urls = ([] if day > mirror_last else [f"{MIRROR_RAW}/raw/{relpath(day)}"]) + nse_urls(day)
            for url in urls:
                try:
                    body = net.get(url)
                except NotFound:
                    continue
                if body[:2] == b"PK":
                    name = relpath(day) if "BhavCopy_NSE_CM" in url or "/raw/" in url else \
                        f"legacy/{day.year}/cm{day.day:02d}{MONTHS[day.month - 1]}{day.year}bhav.csv.zip"
                    store.write_atomic(target / name, body)
                    return
            if day < date.today() - timedelta(days=1):
                (holidays / day.isoformat()).touch()   # every source said 404: not a session

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch, d): d for d in todo}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001 - recorded, retried next run
                    failed.append(f"{futures[fut]}: {exc}"[:200])
                prog.tick()
    files = sorted(p for p in target.rglob("*.zip"))
    bad = [p for p in files if not _valid_zip(p)]
    for p in bad:
        p.unlink()
    summary = dict(source="git mirror" if used_git else "https mirror", mirror_last=str(mirror_last),
                   files=len(files) - len(bad), failed_days=failed[:50], failed=len(failed))
    store.record("prices_download", **summary)
    return summary


def _day_from_name(name: str) -> date | None:
    try:
        if name.startswith("BhavCopy_NSE_CM"):
            s = name.split("_")[6]
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        if name.startswith("cm"):
            return date(int(name[7:11]), MONTHS.index(name[4:7]) + 1, int(name[2:4]))
    except (ValueError, IndexError):
        return None
    return None


def parse_zip(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path, low_memory=False)
    d.columns = [c.strip() for c in d.columns]
    if "SERIES" in d.columns:
        d = d[d.SERIES.astype(str).str.strip().isin(KEEP_SERIES)]
        out = pd.DataFrame({
            "date": pd.to_datetime(d.TIMESTAMP, format="mixed", dayfirst=True),
            "sym": d.SYMBOL.astype(str).str.strip(),
            "isin": d["ISIN"].astype(str).str.strip() if "ISIN" in d.columns else None,
            "series": d.SERIES.astype(str).str.strip(),
            "open": d.OPEN, "high": d.HIGH, "low": d.LOW, "close": d.CLOSE, "prev": d.PREVCLOSE,
            "vol": d.TOTTRDQTY, "turnover": d.TOTTRDVAL})
    else:
        d = d[d.SctySrs.astype(str).str.strip().isin(KEEP_SERIES)]
        out = pd.DataFrame({
            "date": pd.to_datetime(d.TradDt), "sym": d.TckrSymb.astype(str).str.strip(),
            "isin": d.ISIN.astype(str).str.strip(), "series": d.SctySrs.astype(str).str.strip(),
            "open": d.OpnPric, "high": d.HghPric, "low": d.LwPric, "close": d.ClsPric,
            "prev": d.PrvsClsgPric, "vol": d.TtlTradgVol, "turnover": d.TtlTrfVal})
    return out


def build_table(store: Store, window: Window, log=print, workers: int = 4) -> dict:
    """Parse every zip once into ``prices.parquet`` (raw, unadjusted, all series kept)."""
    out = store.root / "prices.parquet"
    files = sorted(store.root.joinpath("bhavcopy").rglob("*.zip"))
    keep = []
    for f in files:
        d = _day_from_name(f.name)
        if d and window.price_start <= d <= window.end:
            keep.append((d, f))
    # Jan-Jul 2024 has both formats; prefer UDiFF to avoid duplicate days.
    by_day: dict[date, Path] = {}
    for d, f in sorted(keep, key=lambda x: (x[0], "udiff" in str(x[1]))):
        by_day[d] = f
    if out.exists() and store.manifest().get("steps", {}).get("prices_table", {}).get("days") == len(by_day):
        log(f"Prices table already built ({len(by_day)} sessions).")
        return store.manifest()["steps"]["prices_table"]
    log(f"Building price table from {len(by_day)} daily files")
    prog = Progress("parse bhavcopies", len(by_day), log)
    frames = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for frame in pool.map(parse_zip, by_day.values()):
            frames.append(frame)
            prog.tick()
    df = pd.concat(frames, ignore_index=True)
    df = df[df.date.dt.dayofweek < 5]   # weekend special sessions are excluded from daily bars
    for c in ("open", "high", "low", "close", "prev", "vol", "turnover"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["isin"] = df["isin"].replace({"nan": None, "None": None, "": None})
    df = df.sort_values(["date", "sym", "series"]).reset_index(drop=True)
    df.to_parquet(out, index=False)
    info = dict(days=len(by_day), rows=len(df), symbols=int(df.sym.nunique()),
                isins=int(df["isin"].nunique()), first=str(df.date.min().date()), last=str(df.date.max().date()),
                etfs_present=sorted(set(df.sym) & set(ETFS)))
    store.record("prices_table", **info)
    log(f"  {info['rows']:,} rows, {info['symbols']:,} symbols, {info['first']} -> {info['last']}")
    return info
