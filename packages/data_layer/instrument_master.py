from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Iterable

import httpx

from .models import Instrument


DEFAULT_URLS = (
    "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
)


@dataclass(frozen=True)
class InstrumentMasterSnapshot:
    instruments: list[Instrument]
    source_url: str
    sha256: str


def _exchange(value: str) -> str:
    raw = (value or "").upper()
    if raw.startswith("NSE"):
        return "NSE"
    if raw.startswith("BSE"):
        return "BSE"
    if raw.startswith("NFO"):
        return "NFO"
    if raw.startswith("BFO"):
        return "BFO"
    if raw.startswith("MCX"):
        return "MCX"
    return raw


def normalize_angel_instruments(rows: Iterable[dict]) -> list[Instrument]:
    result: list[Instrument] = []
    for row in rows:
        exchange = _exchange(str(row.get("exch_seg", "")))
        trading_symbol = str(row.get("symbol") or "").strip()
        name = str(row.get("name") or "").strip()
        token = str(row.get("token") or "").strip()
        if not exchange or not trading_symbol or not token:
            continue

        # PMS v1 is cash-equity focused. Keep EQ securities from NSE/BSE here;
        # derivatives can be ingested later without changing the data model.
        if exchange not in {"NSE", "BSE"}:
            continue
        if exchange == "NSE" and not trading_symbol.endswith("-EQ"):
            continue

        symbol = trading_symbol[:-3] if trading_symbol.endswith("-EQ") else name or trading_symbol
        stable_key = f"{exchange}:{symbol}"
        result.append(
            Instrument(
                instrument_id=stable_key,
                exchange=exchange,
                symbol=symbol,
                trading_symbol=trading_symbol,
                token=token,
                name=name or None,
                metadata={
                    "instrumenttype": row.get("instrumenttype"),
                    "lotsize": row.get("lotsize"),
                    "tick_size": row.get("tick_size"),
                },
            )
        )
    return result


class AngelInstrumentMasterLoader:
    def __init__(
        self,
        urls: tuple[str, ...] = DEFAULT_URLS,
        attempts_per_url: int = 3,
        timeout_seconds: float = 90.0,
    ):
        self.urls = urls
        self.attempts_per_url = attempts_per_url
        self.timeout_seconds = timeout_seconds

    def download(self) -> InstrumentMasterSnapshot:
        errors: list[str] = []
        headers = {"User-Agent": "design-experience-pms/1.0"}
        for url in self.urls:
            for attempt in range(1, self.attempts_per_url + 1):
                try:
                    with httpx.Client(timeout=self.timeout_seconds, follow_redirects=True) as client:
                        response = client.get(url, headers=headers)
                        response.raise_for_status()
                        body = response.content
                    rows = json.loads(body)
                    if not isinstance(rows, list) or len(rows) < 100:
                        raise ValueError("instrument master payload is unexpectedly small")
                    instruments = normalize_angel_instruments(rows)
                    if not instruments:
                        raise ValueError("no cash-market instruments found")
                    return InstrumentMasterSnapshot(
                        instruments=instruments,
                        source_url=url,
                        sha256=hashlib.sha256(body).hexdigest(),
                    )
                except Exception as exc:  # network/provider boundary
                    errors.append(f"{url} attempt {attempt}: {exc}")
                    if attempt < self.attempts_per_url:
                        time.sleep(min(2 ** (attempt - 1), 4))
        raise RuntimeError("Angel instrument master failed: " + " | ".join(errors))
