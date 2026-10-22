"""Compatibility API for callers of the former stored-selection backtester."""
from __future__ import annotations

from copy import deepcopy
from datetime import date

from packages.pms.backtest_matrix import Case
from packages.pms.config import load_strategy_config
from packages.pms.replay_service import prepared_replay
from packages.pms.research import ResearchVariant


class DatabaseBacktestStrategy:
    """Runs the isolated replay; saved C/E decisions are never consulted."""

    def __init__(self, engine, symbols, variant=None, cfg=None):
        self.engine, self.symbols = engine, symbols
        self.variant = variant or ResearchVariant("FULL_A_B_C_E")
        self.cfg = deepcopy(cfg if cfg is not None else load_strategy_config())
        self.diagnostics = None

    def run(self, start: date, end: date, initial_capital=None):
        cfg = deepcopy(self.cfg)
        if initial_capital is not None:
            cfg["backtest"]["initial_capital"] = initial_capital
        case = Case(self.variant.name, self.variant)
        self.diagnostics = None
        with prepared_replay(self.engine, self.symbols, start, end, [case]) as session:
            result, self.diagnostics = session.run(cfg, case)
        return result
