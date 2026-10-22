"""Replay a local trade checkpoint with daily bars; never connects to a broker."""

from __future__ import annotations
import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from packages.pms.config import load_strategy_config
from packages.pms.trade_engine import (
    TradePosition,
    TradeState,
    activate_entry,
    process_trade_session,
    reentry_allowed,
)


def demo():
    position = TradePosition(
        "NSE:DEMO",
        date(2026, 10, 16),
        100,
        95,
        1_000_000,
        entry_sessions=(date(2026, 10, 19), date(2026, 10, 21), date(2026, 10, 22)),
    )
    bars = [
        dict(
            date="2026-10-19",
            open=103,
            high=107,
            low=102,
            close=106,
            ema9=104,
            previous_ema9=103,
            previous_ema21=100,
            fundamental_eligible=True,
        ),
        dict(
            date="2026-10-21",
            open=106,
            high=107,
            low=103.5,
            close=104,
            ema9=105,
            previous_ema9=105,
            previous_ema21=101,
            fundamental_eligible=True,
        ),
        dict(
            date="2026-10-22",
            open=106,
            high=107,
            low=104,
            close=104,
            ema9=105,
            previous_ema9=105,
            previous_ema21=105,
            fundamental_eligible=True,
        ),
        dict(
            date="2026-10-23",
            open=105,
            high=106,
            low=104,
            close=105,
            ema9=105,
            previous_ema9=105,
            previous_ema21=105,
            fundamental_eligible=True,
        ),
    ]
    return {
        "mode": "SYNTHETIC_SIMULATION_ONLY",
        "position": position.checkpoint(),
        "bars": bars,
    }


def replay(payload, cfg=None):
    cfg = cfg or load_strategy_config()
    position = TradePosition.restore(payload["position"])
    if position.state == TradeState.SIGNAL_CREATED:
        activate_entry(position)
    if len(position.entry_sessions) != int(cfg["technical"]["entry_window_days"]):
        raise ValueError("Supply the complete three-session exchange calendar")
    for row in payload["bars"]:
        process_trade_session(
            position,
            date.fromisoformat(row["date"]),
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["ema9"],
            row["previous_ema9"],
            row["previous_ema21"],
            cfg,
            fundamental_eligible=row["fundamental_eligible"],
        )
    return {
        "mode": payload.get("mode", "LOCAL_REPLAY_ONLY"),
        "broker_orders_submitted": 0,
        "position": position.checkpoint(),
        "reentry_allowed": reentry_allowed(position),
        "pnl_basis": "gross; use the event-driven backtester for transaction costs",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--demo",
        action="store_true",
        help="Synthetic gap-entry, partial-exit and Leg2-exit example",
    )
    group.add_argument(
        "--input", type=Path, help="JSON with position checkpoint and daily bars"
    )
    args = parser.parse_args()
    payload = (
        demo() if args.demo else json.loads(args.input.read_text(encoding="utf-8"))
    )
    print(json.dumps(replay(payload), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
