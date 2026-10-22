from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

from .instrument_master import AngelInstrumentMasterLoader
from .models import DataQualityIssue, Instrument
from .providers import MarketDataProvider, StorageBackend
from .validation import validate_candles


@dataclass(frozen=True)
class IngestionResult:
    dataset: str
    attempted: int
    written: int
    issues: list[DataQualityIssue]


class DataIngestionService:
    def __init__(self, storage: StorageBackend):
        self.storage = storage

    def refresh_angel_instruments(
        self,
        loader: AngelInstrumentMasterLoader,
    ) -> IngestionResult:
        snapshot = loader.download()
        written = self.storage.upsert_instruments(snapshot.instruments)
        return IngestionResult(
            dataset="instruments",
            attempted=len(snapshot.instruments),
            written=written,
            issues=[],
        )

    def ingest_daily_prices(
        self,
        provider: MarketDataProvider,
        instruments: Iterable[Instrument],
        start: date,
        end: date,
        reject_on_error: bool = True,
    ) -> IngestionResult:
        all_candles = []
        issues: list[DataQualityIssue] = []
        for instrument in instruments:
            candles = provider.daily_candles(instrument, start, end)
            candle_issues = validate_candles(candles)
            issues.extend(candle_issues)
            if reject_on_error and any(issue.severity == "ERROR" for issue in candle_issues):
                continue
            all_candles.extend(candles)

        written = self.storage.upsert_candles(all_candles)
        return IngestionResult(
            dataset="daily_prices",
            attempted=len(all_candles),
            written=written,
            issues=issues,
        )
