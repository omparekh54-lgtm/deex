from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .models import Candle, Instrument, Quote

IST = ZoneInfo("Asia/Kolkata")


class AngelSession(Protocol):
    def getCandleData(self, params: dict[str, Any]) -> dict[str, Any]: ...
    def ltpData(self, exchange: str, tradingsymbol: str, symboltoken: str) -> dict[str, Any]: ...


class AngelOneError(RuntimeError):
    pass


class AngelOneMarketDataProvider:
    """Normalized Angel One SmartAPI adapter.

    The provider deliberately receives an authenticated SmartAPI session rather
    than credentials. Authentication/session refresh belongs to deployment code.
    This keeps credentials out of strategy logic and makes the adapter testable.
    """

    name = "angel_one"
    MAX_DAILY_DAYS = 2000

    def __init__(self, session: AngelSession):
        self.session = session

    @staticmethod
    def _require_token(instrument: Instrument) -> str:
        if not instrument.token:
            raise AngelOneError(f"Missing Angel One token for {instrument.trading_symbol}")
        return instrument.token

    @staticmethod
    def _fmt_day(value: date, end: bool = False) -> str:
        clock = time(15, 30) if end else time(9, 15)
        return datetime.combine(value, clock).strftime("%Y-%m-%d %H:%M")

    def _request_daily_chunk(
        self,
        instrument: Instrument,
        start: date,
        end: date,
    ) -> list[Candle]:
        token = self._require_token(instrument)
        params = {
            "exchange": instrument.exchange,
            "symboltoken": token,
            "interval": "ONE_DAY",
            "fromdate": self._fmt_day(start),
            "todate": self._fmt_day(end, end=True),
        }
        response = self.session.getCandleData(params)
        if not response or response.get("status") is not True:
            raise AngelOneError(
                f"Angel historical request failed: {response.get('errorcode') if response else 'empty'} "
                f"{response.get('message') if response else ''}".strip()
            )

        rows = response.get("data") or []
        candles: list[Candle] = []
        for row in rows:
            if len(row) < 6:
                continue
            timestamp = datetime.fromisoformat(str(row[0]))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=IST)
            candles.append(
                Candle(
                    instrument_id=instrument.instrument_id,
                    timestamp=timestamp,
                    open=Decimal(str(row[1])),
                    high=Decimal(str(row[2])),
                    low=Decimal(str(row[3])),
                    close=Decimal(str(row[4])),
                    volume=int(row[5] or 0),
                    source=self.name,
                )
            )
        return candles

    def daily_candles(
        self,
        instrument: Instrument,
        start: date,
        end: date,
    ) -> list[Candle]:
        if end < start:
            raise ValueError("end must be on or after start")

        result: list[Candle] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(end, cursor + timedelta(days=self.MAX_DAILY_DAYS - 1))
            result.extend(self._request_daily_chunk(instrument, cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)

        # De-duplicate defensive retries/chunk-boundary anomalies.
        by_ts = {c.timestamp: c for c in result}
        return [by_ts[key] for key in sorted(by_ts)]

    def latest_quote(self, instrument: Instrument) -> Quote:
        token = self._require_token(instrument)
        response = self.session.ltpData(
            instrument.exchange,
            instrument.trading_symbol,
            token,
        )
        if not response or response.get("status") is not True:
            raise AngelOneError(
                f"Angel quote request failed: {response.get('message') if response else 'empty response'}"
            )
        data = response.get("data") or {}
        ltp = data.get("ltp")
        if ltp is None:
            raise AngelOneError("Angel quote response did not contain ltp")
        return Quote(
            instrument_id=instrument.instrument_id,
            timestamp=datetime.now(IST),
            last_price=Decimal(str(ltp)),
            open=Decimal(str(data["open"])) if data.get("open") is not None else None,
            high=Decimal(str(data["high"])) if data.get("high") is not None else None,
            low=Decimal(str(data["low"])) if data.get("low") is not None else None,
            close=Decimal(str(data["close"])) if data.get("close") is not None else None,
            source=self.name,
        )
