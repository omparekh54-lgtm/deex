"""Data folder layout, windows and a small JSON manifest of what each step produced."""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# The S10 ETF basket plus the regime/benchmark funds used by the S1-S10 engines.
ETFS = ["NIFTYBEES", "JUNIORBEES", "MID150BEES", "BANKBEES", "PSUBNKBEES", "ITBEES", "PHARMABEES",
        "CPSEETF", "GOLDBEES", "SILVERBEES", "LIQUIDBEES"]
# Official index names in NSE's daily index file -> DEEX codes. Older files used CNX names.
INDEX_NAMES = {
    "Nifty 50": "NIFTY50", "CNX Nifty": "NIFTY50",
    "Nifty 500": "NIFTY500", "CNX 500": "NIFTY500",
    "Nifty MidSmallcap 400": "NIFTYMIDSMALLCAP400",
    "Nifty Midcap 150": "NIFTYMIDCAP150", "Nifty Smallcap 250": "NIFTYSMALLCAP250",
    "India VIX": "INDIAVIX",
}


@dataclass(frozen=True)
class Window:
    """Backtest window plus warm-up buffers for price indicators and financial history."""
    start: date
    end: date
    price_warmup_years: int = 2
    financial_warmup_years: int = 2

    @property
    def price_start(self) -> date:
        return _years_before(self.start, self.price_warmup_years)

    @property
    def financial_start(self) -> date:
        # Eight contiguous quarters are needed before the first evaluated day.
        return _years_before(self.start, self.financial_warmup_years) - timedelta(days=100)


def _years_before(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(year=day.year - years, day=28)


def months(start: date, end: date):
    """Yield (first_day, last_day) calendar-month windows covering [start, end]."""
    cur = date(start.year, start.month, 1)
    while cur <= end:
        nxt = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
        yield max(cur, start), min(nxt - timedelta(days=1), end)
        cur = nxt


class Store:
    def __init__(self, root: str | os.PathLike):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def path(self, *parts) -> Path:
        p = self.root.joinpath(*parts)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def dir(self, *parts) -> Path:
        p = self.root.joinpath(*parts)
        p.mkdir(parents=True, exist_ok=True)
        return p

    @staticmethod
    def write_atomic(path: Path, data: bytes):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + f".part{threading.get_ident()}")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    # ---------------------------------------------------------------- manifest
    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.json"

    def manifest(self) -> dict:
        try:
            return json.loads(self.manifest_path.read_text())
        except (OSError, ValueError):
            return {}

    def record(self, step: str, **info):
        with self._lock:
            m = self.manifest()
            m.setdefault("steps", {})[step] = {
                **info, "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            self.write_atomic(self.manifest_path, json.dumps(m, indent=2, default=str).encode())

    def set_window(self, window: Window):
        with self._lock:
            m = self.manifest()
            m["window"] = {"start": str(window.start), "end": str(window.end),
                           "price_start": str(window.price_start),
                           "financial_start": str(window.financial_start)}
            self.write_atomic(self.manifest_path, json.dumps(m, indent=2, default=str).encode())


class Progress:
    """Throttled console progress for long loops."""

    def __init__(self, label: str, total: int, log=print, every: float = 5.0):
        self.label, self.total, self.log, self.every = label, max(total, 1), log, every
        self.done = 0
        self.t0 = self.last = time.monotonic()
        self.lock = threading.Lock()

    def tick(self, n: int = 1):
        with self.lock:
            self.done += n
            now = time.monotonic()
            if now - self.last >= self.every or self.done >= self.total:
                self.last = now
                rate = self.done / max(now - self.t0, 1e-6)
                eta = (self.total - self.done) / rate if rate > 0 else 0
                self.log(f"  {self.label}: {self.done}/{self.total} "
                         f"({100 * self.done / self.total:.0f}%, {rate:.1f}/s, ~{eta / 60:.1f} min left)")
