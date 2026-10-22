from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Iterable

from .models import Candle, FinancialFiling, Instrument, SurveillanceState


class InMemoryStorage:
    """Deterministic storage used by tests, local development and dry runs."""

    def __init__(self) -> None:
        self.instruments: dict[str, Instrument] = {}
        self.candles: dict[tuple[str, str, str], Candle] = {}
        self.filings: dict[tuple, FinancialFiling] = {}
        self.surveillance: list[SurveillanceState] = []

    def upsert_instruments(self, instruments: Iterable[Instrument]) -> int:
        count = 0
        for instrument in instruments:
            self.instruments[instrument.instrument_id] = instrument
            count += 1
        return count

    def upsert_candles(self, candles: Iterable[Candle]) -> int:
        count = 0
        for candle in candles:
            key = (
                candle.instrument_id,
                candle.timestamp.date().isoformat(),
                candle.source,
            )
            self.candles[key] = candle
            count += 1
        return count

    def append_filings(self, filings: Iterable[FinancialFiling]) -> int:
        count = 0
        for filing in filings:
            key = (
                filing.instrument_id,
                filing.period_end,
                filing.filing_kind,
                filing.statement_scope,
                filing.filed_at,
                filing.version,
                filing.source,
            )
            self.filings[key] = filing
            count += 1
        return count

    def record_surveillance(self, states: Iterable[SurveillanceState]) -> int:
        rows = list(states)
        self.surveillance.extend(rows)
        return len(rows)

    def latest_filing_as_of(
        self,
        instrument_id: str,
        as_of: datetime,
    ) -> FinancialFiling | None:
        candidates = [
            filing
            for filing in self.filings.values()
            if filing.instrument_id == instrument_id and filing.filed_at <= as_of
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: (row.filed_at, row.version))
