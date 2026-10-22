from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, text

from packages.pms.brokers import PaperBroker
from packages.pms.config import load_strategy_config
from packages.pms.costs import estimate_delivery_costs


@dataclass(frozen=True)
class PaperBar:
    instrument_id: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float


class PaperExecutionService:
    """Execute persisted fake-money orders against observed market bars.

    PAPER is reserved for formal forward validation. PAPER_SANDBOX powers the
    interactive app module and is intentionally excluded from readiness gates.
    """

    def __init__(
        self,
        engine: Engine,
        broker: PaperBroker | None = None,
        cfg: dict | None = None,
        *,
        account_mode: str = "PAPER",
    ):
        if account_mode.upper() != "PAPER_SANDBOX":
            raise PermissionError(
                "Formal PAPER requires FormalPaperExecution and durable trade checkpoints"
            )
        self.engine = engine
        self.account_mode = account_mode.upper()
        self.broker = broker or PaperBroker(engine, account_mode=self.account_mode)
        if self.broker.account_mode != self.account_mode:
            raise ValueError("Paper broker/account mode mismatch")
        self.cfg = cfg or load_strategy_config()

    def _instrument_pk(self, instrument_id: str) -> int:
        exchange, symbol = instrument_id.split(":", 1)
        trading_symbol = f"{symbol}-EQ" if exchange == "NSE" else symbol
        with self.engine.connect() as conn:
            value = conn.execute(
                text(
                    "select id from instruments where exchange=:e and trading_symbol=:s order by valid_from desc limit 1"
                ),
                {"e": exchange, "s": trading_symbol},
            ).scalar_one()
        return int(value)

    def process_bar(self, bar: PaperBar) -> list[dict]:
        from packages.pms.trade_engine import validate_ohlc

        validate_ohlc(bar.open, bar.high, bar.low, bar.close)
        pk = self._instrument_pk(bar.instrument_id)
        with self.engine.connect() as conn:
            orders = [
                dict(r)
                for r in conn.execute(
                    text("""
                        select * from broker_orders
                        where broker=:broker and mode=:mode and instrument_id=:id and status='OPEN' and created_at<:opened
                        order by created_at
                        """),
                    {
                        "broker": self.broker.broker_name,
                        "mode": self.account_mode,
                        "id": pk,
                        "opened": datetime.combine(
                            bar.trade_date, time(9, 15), tzinfo=ZoneInfo("Asia/Kolkata")
                        ),
                    },
                )
                .mappings()
                .all()
            ]
        fills = []
        for order in orders:
            side = str(order["side"]).upper()
            kind = str(order["order_type"]).upper()
            trigger = (
                float(order["trigger_price"])
                if order["trigger_price"] is not None
                else None
            )
            limit = (
                float(order["limit_price"])
                if order["limit_price"] is not None
                else None
            )
            fill_price = None
            if kind == "MARKET":
                fill_price = bar.open
            elif kind in {"STOP", "STOP_MARKET", "SL-M"} and trigger is not None:
                if side == "BUY" and bar.high >= trigger:
                    fill_price = max(bar.open, trigger)
                if side == "SELL" and bar.low <= trigger:
                    fill_price = min(bar.open, trigger)
            elif kind == "LIMIT" and limit is not None:
                if side == "BUY" and bar.low <= limit:
                    fill_price = min(bar.open, limit)
                if side == "SELL" and bar.high >= limit:
                    fill_price = max(bar.open, limit)
            if fill_price is None:
                continue
            qty = int(order["quantity"])
            costs = estimate_delivery_costs(
                trade_date=bar.trade_date,
                side=side,
                price=fill_price,
                quantity=qty,
                cfg=self.cfg["backtest"],
            )
            fill = self.broker.fill_order(
                str(order["id"]), fill_price, qty, costs.total
            )
            fill.update({"side": side, "reason": kind, "costs": costs.total})
            fills.append(fill)
        return fills

    def process_date(self, trade_date: date) -> list[dict]:
        """Process every open paper order using that date's observed daily bar."""
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text("""
                    select distinct i.exchange,
                           coalesce(i.nse_symbol, replace(i.trading_symbol,'-EQ','')) as symbol,
                           p.open,p.high,p.low,p.close
                    from broker_orders o
                    join instruments i on i.id=o.instrument_id
                    join daily_prices p on p.instrument_id=i.id and p.trade_date=:d
                    where o.broker=:broker and o.mode=:mode and o.status='OPEN'
                    """),
                    {
                        "d": trade_date,
                        "broker": self.broker.broker_name,
                        "mode": self.account_mode,
                    },
                )
                .mappings()
                .all()
            )
        fills: list[dict] = []
        for row in rows:
            fills.extend(
                self.process_bar(
                    PaperBar(
                        instrument_id=f"{row['exchange']}:{row['symbol']}",
                        trade_date=trade_date,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                    )
                )
            )
        return fills
