from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Iterable

from sqlalchemy import Engine, text
from packages.pms.ledger import ledger_positions


@dataclass(frozen=True)
class PositionSnapshot:
    instrument_id: int
    quantity: int
    average_price: float
    last_price: float
    market_value: float
    unrealized_pnl: float


@dataclass(frozen=True)
class PortfolioSnapshot:
    snapshot_date: date
    account_mode: str
    cash: float
    market_value: float
    realized_pnl: float
    unrealized_pnl: float
    nav: float
    open_risk: float
    portfolio_heat_pct: float
    positions: tuple[PositionSnapshot, ...]


class PortfolioAccountingService:
    def __init__(self, engine: Engine, initial_capital: float = 10_000_000):
        self.engine = engine
        self.initial_capital = initial_capital

    def snapshot(self, on_date: date, account_mode: str = "PAPER") -> PortfolioSnapshot:
        with self.engine.connect() as conn:
            cash_delta = float(
                conn.execute(
                    text(
                        "select coalesce(sum(cash_delta),0) from portfolio_ledger where account_mode=:m and (event_time at time zone 'Asia/Kolkata')::date<=:d"
                    ),
                    {"m": account_mode, "d": on_date},
                ).scalar_one()
            )
            realized = float(
                conn.execute(
                    text(
                        "select coalesce(sum(realized_pnl_delta),0) from portfolio_ledger where account_mode=:m and (event_time at time zone 'Asia/Kolkata')::date<=:d"
                    ),
                    {"m": account_mode, "d": on_date},
                ).scalar_one()
            )
            fills = (
                conn.execute(
                    text(
                        """select instrument_id,quantity,price,fees from portfolio_ledger
                where account_mode=:m and event_type='FILL' and (event_time at time zone 'Asia/Kolkata')::date<=:d
                order by event_time,id"""
                    ),
                    {"m": account_mode, "d": on_date},
                )
                .mappings()
                .all()
            )
            lots = []
            for pk, lot in ledger_positions(fills).items():
                mark = conn.execute(
                    text(
                        """select close from daily_prices where instrument_id=:id and trade_date<=:d
                    order by trade_date desc,case when source='ANGEL_ONE' then 0 when source like 'NSE%' then 1 else 2 end,source limit 1"""
                    ),
                    {"id": pk, "d": on_date},
                ).scalar_one_or_none()
                if mark is None:
                    raise ValueError("PAPER holding has no observed price mark")
                lots.append(
                    {
                        "instrument_id": pk,
                        "quantity": lot["quantity"],
                        "avg_price": lot["average_price"],
                        "last_price": mark,
                    }
                )
        positions = []
        market_value = 0.0
        unrealized = 0.0
        for r in lots:
            qty = int(r["quantity"])
            avg = float(r["avg_price"] or 0)
            last = float(r["last_price"] or avg)
            mv = qty * last
            upnl = qty * (last - avg)
            market_value += mv
            unrealized += upnl
            positions.append(
                PositionSnapshot(int(r["instrument_id"]), qty, avg, last, mv, upnl)
            )
        cash = self.initial_capital + cash_delta
        nav = cash + market_value
        open_risk = 0.0
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text(
                        "select entry,stop,remaining_shares from trades where state not in ('CLOSED','CANCELLED') and entry is not null and stop is not null and exists(select 1 from broker_orders o where o.strategy_trade_id=trades.id and o.mode=:m)"
                    ),
                    {"m": account_mode},
                )
                .mappings()
                .all()
            )
            for r in rows:
                open_risk += max(
                    0.0,
                    (float(r["entry"]) - float(r["stop"])) * int(r["remaining_shares"]),
                )
        heat = (open_risk / nav * 100.0) if nav > 0 else 0.0
        return PortfolioSnapshot(
            on_date,
            account_mode,
            round(cash, 2),
            round(market_value, 2),
            round(realized, 2),
            round(unrealized, 2),
            round(nav, 2),
            round(open_risk, 2),
            round(heat, 4),
            tuple(positions),
        )

    def persist_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        import json

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                insert into portfolio_snapshots(snapshot_date,account_mode,cash,market_value,realized_pnl,unrealized_pnl,nav,open_risk,portfolio_heat_pct,positions)
                values (:d,:m,:cash,:mv,:rp,:up,:nav,:risk,:heat,cast(:positions as jsonb))
                on conflict (snapshot_date,account_mode) do update set cash=excluded.cash,market_value=excluded.market_value,
                  realized_pnl=excluded.realized_pnl,unrealized_pnl=excluded.unrealized_pnl,nav=excluded.nav,
                  open_risk=excluded.open_risk,portfolio_heat_pct=excluded.portfolio_heat_pct,positions=excluded.positions,created_at=now()
            """),
                {
                    "d": snapshot.snapshot_date,
                    "m": snapshot.account_mode,
                    "cash": snapshot.cash,
                    "mv": snapshot.market_value,
                    "rp": snapshot.realized_pnl,
                    "up": snapshot.unrealized_pnl,
                    "nav": snapshot.nav,
                    "risk": snapshot.open_risk,
                    "heat": snapshot.portfolio_heat_pct,
                    "positions": json.dumps([asdict(p) for p in snapshot.positions]),
                },
            )
