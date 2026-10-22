from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Engine

from packages.pms.fundamental_history import FinancialHistoryMetrics, calculate_financial_history
from packages.pms.module_a import FundamentalDecision, FundamentalSnapshot, evaluate_fundamentals

from .queries import PointInTimeQueries


@dataclass(frozen=True)
class ModuleAHardFilterContext:
    market_cap_cr: float
    avg_traded_value_cr: float
    promoter_pledge_pct: float


@dataclass(frozen=True)
class ModuleAHistoryEvaluation:
    instrument_id: str
    as_of: datetime
    metrics: FinancialHistoryMetrics
    snapshot: FundamentalSnapshot
    decision: FundamentalDecision
    history: tuple[dict[str, Any], ...]


class ModuleAHistoryService:
    """Point-in-time bridge from exchange filings to deterministic Module A logic."""

    def __init__(self, engine: Engine, cfg: dict):
        self.queries = PointInTimeQueries(engine)
        self.cfg = cfg

    def evaluate(
        self,
        instrument_id: str,
        as_of: datetime,
        hard_filters: ModuleAHardFilterContext,
    ) -> ModuleAHistoryEvaluation:
        history = self.queries.financial_history_as_of(
            instrument_id,
            as_of,
            prefer_consolidated=True,
            limit_periods=20,
        )
        if not history:
            raise ValueError(f"no financial history available for {instrument_id} as of {as_of.isoformat()}")

        from packages.pms.module_a_pipeline import ModuleAPipeline
        # Compatibility adapter: caller-supplied hard-filter values are not
        # trusted for eligibility. All production inputs come from the warehouse.
        snapshot, decision = ModuleAPipeline(self.queries.engine, self.cfg).evaluate(instrument_id, as_of)
        fcfg = self.cfg['fundamental']
        metrics = calculate_financial_history(history, as_of=as_of,
            consistency_metrics=tuple(fcfg.get('consistency_growth_metrics',['sales','pat'])),
            min_ocf_to_pat=float(fcfg['min_ocf_to_pat']),
            max_cash_flow_age_days=int(fcfg.get('max_cash_flow_age_days',550)),
            min_core_profit_growth=float(fcfg['min_pat_yoy_pct']))
        return ModuleAHistoryEvaluation(
            instrument_id=instrument_id,
            as_of=as_of,
            metrics=metrics,
            snapshot=snapshot,
            decision=decision,
            history=tuple(history),
        )
