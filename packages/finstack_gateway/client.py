from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol, Any


class MarketDataProvider(Protocol):
    def daily_prices(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]: ...
    def latest_quote(self, symbol: str) -> dict[str, Any]: ...


class FundamentalDataProvider(Protocol):
    def latest_financials(self, symbol: str, as_of: datetime) -> dict[str, Any]: ...
    def surveillance_state(self, symbol: str, as_of: datetime) -> dict[str, Any]: ...


@dataclass
class FinstackGateway:
    """Boundary between PMS logic and external data providers.

    The strategy must consume normalized, point-in-time records through this
    interface. Provider-specific Angel One / NSE logic belongs behind this
    boundary, never inside strategy modules.
    """

    market: MarketDataProvider | None = None
    fundamentals: FundamentalDataProvider | None = None

    def get_stock_snapshot(self, symbol: str, as_of: datetime) -> dict[str, Any]:
        if self.fundamentals is None:
            raise RuntimeError("Fundamental provider is not configured")
        return {
            "symbol": symbol,
            "as_of": as_of.isoformat(),
            "financials": self.fundamentals.latest_financials(symbol, as_of),
            "surveillance": self.fundamentals.surveillance_state(symbol, as_of),
        }
