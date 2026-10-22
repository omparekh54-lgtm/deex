from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from dataclasses import asdict
from datetime import date
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine

from packages.pms.backtest_persistence import BacktestPersistence
from packages.pms.config import load_strategy_config
from packages.pms.database_backtest import DatabaseBacktestStrategy
from packages.pms.backtest_matrix import metrics
from packages.pms.matrix_replay import ResearchDataGap
from packages.pms.walk_forward import ReplayPeriod


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run isolated historical PMS replay with fresh A/B/C/E decisions")
    parser.add_argument("--symbols", required=True, help="Comma-separated canonical IDs, e.g. NSE:ABC,NSE:XYZ")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)

    from dotenv import load_dotenv
    from apps.api.main import _render_safe_database_url

    load_dotenv()
    database_url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL or PMS_DATABASE_URL is required")

    engine = create_engine(_render_safe_database_url(database_url), pool_pre_ping=True,
                           connect_args={"options": "-c search_path=pms,public"})
    try:
        symbols = [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        cfg = load_strategy_config(args.config) if args.config else load_strategy_config()
        if args.capital is not None:
            cfg["backtest"]["initial_capital"] = args.capital

        strategy = DatabaseBacktestStrategy(engine, symbols, cfg=cfg)
        try:
            result = strategy.run(start, end)
        except ResearchDataGap as exc:
            print(json.dumps({"status": "BLOCKED", "blockers": str(exc).split(";")}, indent=2))
            return 2
        performance = metrics(result, ReplayPeriod("CUSTOM", start, end, "EXPLORATORY"))
        cfg["backtest"]["matrix_research_only"] = True
        cfg["research_execution"] = strategy.diagnostics

        run_id = uuid4()
        if not args.no_persist:
            persistence = BacktestPersistence(engine)
            persistence.create_run(
                run_id,
                strategy_version=cfg["version"],
                start=start,
                end=end,
                initial_capital=result.initial_capital,
                config=cfg,
            )
            persistence.persist_result(run_id, result)
            persistence.finalize_run(run_id, result)

        print(json.dumps({
            "run_id": None if args.no_persist else str(run_id),
            "status": "COMPLETED" if result.closed_trades else "NO_TRADES",
            "diagnostics": strategy.diagnostics,
            "metrics": performance,
            "initial_capital": result.initial_capital,
            "ending_cash": result.ending_cash,
            "realized_gross_pnl": result.realized_gross_pnl,
            "total_costs": result.total_costs,
            "realized_net_pnl": result.realized_net_pnl,
            "return_pct": result.return_pct,
            "closed_trades": result.closed_trades,
            "fills": [asdict(fill) for fill in result.fills],
        }, default=str, indent=2))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
