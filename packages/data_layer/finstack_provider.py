from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .models import Candle, Instrument, Quote

IST = ZoneInfo("Asia/Kolkata")


class FinstackUnavailable(RuntimeError):
    pass


def _india_module():
    try:
        from finstack import india
    except ImportError as exc:
        raise FinstackUnavailable(
            "Finstack is not installed. The PMS now uses Finstack/open-source sources as its primary market-data layer."
        ) from exc
    return india


def _find_column(columns, *candidates: str) -> str | None:
    normalized = {str(c).strip().lower().replace("_", " "): str(c) for c in columns}
    for candidate in candidates:
        key = candidate.lower().replace("_", " ")
        if key in normalized:
            return normalized[key]
    return None


class FinstackNSEMarketProvider:
    """Primary free/open-source NSE market provider for the PMS.

    Finstack performs source routing/fallbacks. Official NSE/BSE data remains the
    authority for hard eligibility/reference rules such as filings and surveillance.
    """

    name = "finstack_nse"

    def daily_candles(
        self,
        instrument: Instrument,
        start: date,
        end: date,
    ) -> list[Candle]:
        if instrument.exchange != "NSE":
            raise ValueError("FinstackNSEMarketProvider currently supports NSE instruments only")
        india = _india_module()
        df = india.nse_history(
            instrument.symbol,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        if df is None or df.empty:
            return []

        open_col = _find_column(df.columns, "open", "open price")
        high_col = _find_column(df.columns, "high", "high price")
        low_col = _find_column(df.columns, "low", "low price")
        close_col = _find_column(df.columns, "close", "close price", "last")
        volume_col = _find_column(df.columns, "volume", "total traded quantity", "tottrdqty")
        if not all((open_col, high_col, low_col, close_col, volume_col)):
            raise ValueError(f"Finstack returned unsupported OHLCV columns: {list(df.columns)}")

        result: list[Candle] = []
        for idx, row in df.iterrows():
            raw_ts = row.get("Date") if "Date" in df.columns else idx
            ts = (
                datetime.fromisoformat(str(raw_ts))
                if isinstance(raw_ts, str)
                else raw_ts.to_pydatetime()
                if hasattr(raw_ts, "to_pydatetime")
                else datetime.combine(raw_ts, datetime.min.time())
            )
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            result.append(
                Candle(
                    instrument_id=instrument.instrument_id,
                    timestamp=ts,
                    open=Decimal(str(row[open_col])),
                    high=Decimal(str(row[high_col])),
                    low=Decimal(str(row[low_col])),
                    close=Decimal(str(row[close_col])),
                    volume=int(float(row[volume_col])),
                    source=self.name,
                )
            )
        return sorted(result, key=lambda candle: candle.timestamp)

    def latest_quote(self, instrument: Instrument) -> Quote:
        if instrument.exchange != "NSE":
            raise ValueError("FinstackNSEMarketProvider currently supports NSE instruments only")
        india = _india_module()
        data = india.nse_quote(instrument.symbol)
        ltp = data.get("lastPrice") or data.get("last_price") or data.get("ltp") or data.get("last")
        if ltp is None:
            raise ValueError("Finstack NSE quote does not contain an identifiable last price")
        return Quote(
            instrument_id=instrument.instrument_id,
            timestamp=datetime.now(IST),
            last_price=Decimal(str(ltp).replace(",", "")),
            source=self.name,
        )
