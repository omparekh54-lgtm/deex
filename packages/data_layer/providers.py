from __future__ import annotations

from datetime import date, datetime
from typing import Protocol, Iterable

from .models import Candle, FinancialFiling, Instrument, Quote, SurveillanceState


class InstrumentProvider(Protocol):
    def instruments(self) -> Iterable[Instrument]: ...


class MarketDataProvider(Protocol):
    name: str

    def daily_candles(
        self,
        instrument: Instrument,
        start: date,
        end: date,
    ) -> list[Candle]: ...

    def latest_quote(self, instrument: Instrument) -> Quote: ...


class FundamentalDataProvider(Protocol):
    name: str

    def filings(self, instrument: Instrument, as_of: datetime) -> list[FinancialFiling]: ...

    def surveillance_state(
        self,
        instrument: Instrument,
        as_of: datetime,
    ) -> SurveillanceState: ...


class StorageBackend(Protocol):
    def upsert_instruments(self, instruments: Iterable[Instrument]) -> int: ...

    def upsert_candles(self, candles: Iterable[Candle]) -> int: ...

    def append_filings(self, filings: Iterable[FinancialFiling]) -> int: ...

    def record_surveillance(self, states: Iterable[SurveillanceState]) -> int: ...
