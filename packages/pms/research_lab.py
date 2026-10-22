from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Iterable

from packages.pms.backtest_matrix import Case, metrics, round_trips
from packages.pms.config import load_strategy_config
from packages.pms.replay_service import prepared_replay
from packages.pms.research import DEFAULT_VARIANTS, ResearchVariant, run_robustness_grid


class ResearchLab:
    def __init__(self, engine, symbols, start, end, initial_capital=None, cfg=None):
        self.engine, self.symbols = engine, symbols
        self.start, self.end, self.initial_capital = start, end, initial_capital
        self.base_config = deepcopy(cfg if cfg is not None else load_strategy_config())

    def _run(self, session, cfg, variant):
        config = deepcopy(cfg)
        if self.initial_capital is not None:
            config["backtest"]["initial_capital"] = self.initial_capital
        result, diagnostics = session.run(config, Case(variant.name, variant))
        payload = metrics(result, session.period)
        closed, _ = round_trips(result)
        winners = [t["pnl"] for t in closed if t["pnl"] > 0]
        losers = [t["pnl"] for t in closed if t["pnl"] < 0]
        # Retain old field names as aliases, computed from complete position cycles.
        payload.update(
            variant=variant.name,
            status="COMPLETED" if closed else "NO_TRADES",
            diagnostics=diagnostics,
            gross_pnl=result.realized_gross_pnl,
            costs=result.total_costs,
            net_realized_pnl=result.realized_net_pnl,
            closed_trades_engine=result.closed_trades,
            trades=len(closed),
            expectancy=payload["expectancy_rupees"],
            avg_winner=sum(winners) / len(winners) if winners else 0.0,
            avg_loser=sum(losers) / len(losers) if losers else 0.0,
            avg_holding_days=payload["average_holding_days"],
        )
        return payload

    def run_variant(self, cfg: dict, variant: ResearchVariant) -> dict:
        case = Case(variant.name, variant)
        with prepared_replay(self.engine, self.symbols, self.start, self.end, [case]) as session:
            return self._run(session, cfg, variant)

    def _development_only(self):
        if self.end >= date(2025, 1, 1):
            raise ValueError("Variant comparisons and robustness must end by 2024-12-31; use walk-forward for reserved holdout evaluation")

    def run_all_variants(self, variants: Iterable[ResearchVariant] = DEFAULT_VARIANTS):
        self._development_only()
        variants = tuple(variants)
        cases = [Case(v.name, v) for v in variants]
        with prepared_replay(self.engine, self.symbols, self.start, self.end, cases) as session:
            return {v.name: self._run(session, self.base_config, v) for v in variants}

    def run_robustness(self, variant=None):
        self._development_only()
        variant = variant or ResearchVariant("FULL_A_B_C_E")
        with prepared_replay(self.engine, self.symbols, self.start, self.end,
                             [Case(variant.name, variant)]) as session:
            return run_robustness_grid(
                self.base_config, lambda cfg, v: self._run(session, cfg, v), variant
            )
