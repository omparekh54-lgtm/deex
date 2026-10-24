"""Corporate actions (dividends, splits, bonuses) and board meetings (results dates) by month."""
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd

from .net import Net, NotFound
from .store import Progress, Store, Window, months

ENDPOINTS = {
    "actions": "corporates-corporateActions",
    "board_meetings": "corporate-board-meetings",
}
RESULT_WORDS = re.compile(r"financial result|results|quarterly|audited|unaudited", re.I)


def _rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for k in ("data", "rows"):
            if isinstance(payload.get(k), list):
                return [r for r in payload[k] if isinstance(r, dict)]
    return []


def fetch_events(store: Store, net: Net, window: Window, workers: int = 2, log=print) -> dict:
    start = window.price_start
    info = {}
    for kind, path in ENDPOINTS.items():
        folder = store.dir("events", kind)
        windows = list(months(start, window.end))
        current = date.today().replace(day=1)
        todo = [(a, b) for a, b in windows
                if not (folder / f"{a:%Y-%m}.json").exists() or a >= current]  # refresh the live month
        log(f"Events: {kind.replace('_', ' ')} for {len(windows)} months ({len(todo)} to download)")
        prog = Progress(kind, len(todo), log) if todo else None
        failed = []

        def one(win):
            a, b = win
            try:
                body = net.api(path, index="equities", from_date=a.strftime("%d-%m-%Y"),
                               to_date=b.strftime("%d-%m-%Y"))
                json.loads(body)   # validate before caching
            except NotFound:
                body = b"[]"
            store.write_atomic(folder / f"{a:%Y-%m}.json", body)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(one, w): w for w in todo}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001 - retried on the next run
                    failed.append(f"{futures[fut][0]:%Y-%m}: {str(exc)[:100]}")
                prog.tick()
        rows = []
        for f in sorted(folder.glob("*.json")):
            try:
                rows.extend(_rows(json.loads(f.read_bytes())))
            except ValueError:
                failed.append(f"bad json {f.name}")
        table = _actions_table(rows) if kind == "actions" else _board_table(rows)
        table.to_parquet(store.root / f"{kind}.parquet", index=False)
        info[kind] = dict(months=len(windows), rows=len(table), failed=len(failed), failed_examples=failed[:10])
        if failed:
            log(f"  {len(failed)} months failed for {kind}; rerun the command to retry them")
    store.record("events", **info)
    return info


def _date(v):
    return pd.to_datetime(v, errors="coerce", dayfirst=True, format="mixed")


def _actions_table(rows: list[dict]) -> pd.DataFrame:
    out = []
    for r in rows:
        subject = str(r.get("subject") or r.get("purpose") or "")
        amount = None
        kind = "OTHER"
        if re.search(r"dividend", subject, re.I):
            kind = "DIVIDEND"
            m = re.search(r"(?:Rs\.?|Re\.?|INR)\s*([0-9]+(?:\.[0-9]+)?)", subject, re.I)
            amount = float(m.group(1)) if m else None
        elif re.search(r"split|sub-division|sub division", subject, re.I):
            kind = "SPLIT"
        elif re.search(r"bonus", subject, re.I):
            kind = "BONUS"
        out.append(dict(sym=str(r.get("symbol", "")).strip(), series=r.get("series"), isin=r.get("isin"),
                        kind=kind, subject=subject, cash_per_share=amount,
                        ex_date=_date(r.get("exDate")), record_date=_date(r.get("recDate")),
                        announced_at=_date(r.get("caBroadcastDate") or r.get("broadcastDate"))))
    df = pd.DataFrame(out, columns=["sym", "series", "isin", "kind", "subject", "cash_per_share",
                                    "ex_date", "record_date", "announced_at"])
    return df.drop_duplicates(["sym", "ex_date", "subject"]).sort_values(["ex_date", "sym"]).reset_index(drop=True)


def _board_table(rows: list[dict]) -> pd.DataFrame:
    out = []
    for r in rows:
        purpose = f"{r.get('bm_purpose') or ''} {r.get('bm_desc') or ''}"
        out.append(dict(sym=str(r.get("bm_symbol", "")).strip(), meeting_date=_date(r.get("bm_date")),
                        announced_at=_date(r.get("bm_timestamp")), purpose=purpose.strip()[:300],
                        results=bool(RESULT_WORDS.search(purpose))))
    df = pd.DataFrame(out, columns=["sym", "meeting_date", "announced_at", "purpose", "results"])
    return df.dropna(subset=["meeting_date"]).drop_duplicates(["sym", "meeting_date", "purpose"]) \
        .sort_values(["meeting_date", "sym"]).reset_index(drop=True)
