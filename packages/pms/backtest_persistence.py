from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import Engine, text

from packages.pms.backtest import BacktestResult


class BacktestPersistence:
    def __init__(self, engine: Engine):
        self.engine = engine

    def create_run(self, run_id: UUID, *, strategy_version: str, start, end, initial_capital: float, config: dict) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                insert into backtest_runs(id,strategy_version,started_on,ended_on,initial_capital,config,status)
                values(:id,:v,:s,:e,:c,cast(:cfg as jsonb),'RUNNING')
            """), {"id": str(run_id), "v": strategy_version, "s": start, "e": end, "c": initial_capital, "cfg": json.dumps(config)})

    def finalize_run(self, run_id: UUID, result: BacktestResult) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                update backtest_runs set ending_cash=:cash, realized_gross_pnl=:gross,
                  total_costs=:costs, realized_net_pnl=:net, return_pct=:ret,
                  status='COMPLETED', completed_at=now() where id=:id
            """), {"id": str(run_id), "cash": result.ending_cash, "gross": result.realized_gross_pnl, "costs": result.total_costs, "net": result.realized_net_pnl, "ret": result.return_pct})

    def persist_result(self, run_id: UUID, result: BacktestResult) -> None:
        with self.engine.begin() as conn:
            for event in result.events:
                conn.execute(text("""
                    insert into backtest_events(run_id,event_time,event_type,payload)
                    values(:run_id,:t,:type,cast(:payload as jsonb))
                """), {"run_id": str(run_id), "t": datetime.combine(event.trade_date, datetime.min.time()), "type": event.event_type.value, "payload": json.dumps({"instrument_id": event.instrument_id, **event.payload})})
            for fill in result.fills:
                conn.execute(text("""
                    insert into backtest_fills(run_id,instrument_id,trade_date,side,price,quantity,reason,costs)
                    select :run_id,id,:d,:side,:p,:q,:reason,cast(:costs as jsonb)
                    from instruments where (exchange || ':' || coalesce(nse_symbol, replace(trading_symbol,'-EQ',''))) = :instrument
                    order by id desc limit 1
                """), {"run_id": str(run_id), "d": fill.trade_date, "side": fill.side, "p": fill.price, "q": fill.quantity, "reason": fill.reason, "instrument": fill.instrument_id, "costs": json.dumps(asdict(fill.costs))})
