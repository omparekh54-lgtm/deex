from __future__ import annotations

from dataclasses import dataclass, field, replace
from itertools import product
from math import sqrt
from statistics import mean, pstdev
from typing import Callable, Iterable


@dataclass(frozen=True)
class TradeOutcome:
    pnl: float
    capital_at_entry: float
    holding_days: int = 0

    @property
    def return_pct(self) -> float:
        return (self.pnl / self.capital_at_entry * 100.0) if self.capital_at_entry else 0.0


@dataclass(frozen=True)
class PerformanceMetrics:
    total_return_pct: float
    cagr_pct: float | None
    max_drawdown_pct: float
    sharpe: float | None
    sortino: float | None
    calmar: float | None
    win_rate_pct: float
    profit_factor: float | None
    expectancy: float
    avg_winner: float
    avg_loser: float
    trades: int
    avg_holding_days: float


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, (value / peak - 1.0) * 100.0)
    return abs(worst)


def calculate_performance(
    *,
    initial_capital: float,
    ending_equity: float,
    equity_curve: list[float],
    trades: Iterable[TradeOutcome],
    years: float | None = None,
) -> PerformanceMetrics:
    rows = list(trades)
    returns = [t.return_pct for t in rows]
    wins = [t.pnl for t in rows if t.pnl > 0]
    losses = [t.pnl for t in rows if t.pnl < 0]
    total_return = (ending_equity / initial_capital - 1.0) * 100.0 if initial_capital > 0 else 0.0
    cagr = None
    if years and years > 0 and initial_capital > 0 and ending_equity > 0:
        cagr = ((ending_equity / initial_capital) ** (1.0 / years) - 1.0) * 100.0

    std = pstdev(returns) if len(returns) > 1 else 0.0
    sharpe = mean(returns) / std * sqrt(len(returns)) if std > 0 else None
    downside = [r for r in returns if r < 0]
    downside_std = pstdev(downside) if len(downside) > 1 else 0.0
    sortino = mean(returns) / downside_std * sqrt(len(returns)) if downside_std > 0 else None
    dd = max_drawdown(equity_curve)
    calmar = cagr / dd if cagr is not None and dd > 0 else None
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else (None if gross_profit == 0 else float("inf"))

    return PerformanceMetrics(
        total_return_pct=round(total_return, 4),
        cagr_pct=round(cagr, 4) if cagr is not None else None,
        max_drawdown_pct=round(dd, 4),
        sharpe=round(sharpe, 4) if sharpe is not None else None,
        sortino=round(sortino, 4) if sortino is not None else None,
        calmar=round(calmar, 4) if calmar is not None else None,
        win_rate_pct=round((len(wins) / len(rows) * 100.0) if rows else 0.0, 4),
        profit_factor=round(pf, 4) if pf is not None and pf != float("inf") else pf,
        expectancy=round(mean([t.pnl for t in rows]), 2) if rows else 0.0,
        avg_winner=round(mean(wins), 2) if wins else 0.0,
        avg_loser=round(mean(losses), 2) if losses else 0.0,
        trades=len(rows),
        avg_holding_days=round(mean([t.holding_days for t in rows]), 2) if rows else 0.0,
    )


@dataclass(frozen=True)
class ResearchVariant:
    name: str
    use_module_a: bool = True
    use_module_c: bool = True
    use_module_e: bool = True
    setup_2: bool = False
    reentry: bool = True


DEFAULT_VARIANTS = (
    ResearchVariant("B_ONLY", use_module_a=False, use_module_c=False, use_module_e=False),
    ResearchVariant("B_PLUS_A", use_module_a=True, use_module_c=False, use_module_e=False),
    ResearchVariant("B_PLUS_A_PLUS_C", use_module_a=True, use_module_c=True, use_module_e=False),
    ResearchVariant("FULL_A_B_C_E", use_module_a=True, use_module_c=True, use_module_e=True),
    ResearchVariant("FULL_NO_REENTRY", use_module_a=True, use_module_c=True, use_module_e=True, reentry=False),
    ResearchVariant("SETUP2_BACKTEST", use_module_a=True, use_module_c=True, use_module_e=True, setup_2=True),
)


@dataclass(frozen=True)
class RobustnessPoint:
    parameters: dict[str, float]
    score: float
    metrics: dict


Runner = Callable[[dict, ResearchVariant], dict]


def run_variants(base_config: dict, runner: Runner, variants: Iterable[ResearchVariant] = DEFAULT_VARIANTS) -> dict[str, dict]:
    return {variant.name: runner(base_config, variant) for variant in variants}


def run_robustness_grid(
    base_config: dict,
    runner: Runner,
    variant: ResearchVariant,
    *,
    adr_values: tuple[float, ...] = (2.5, 3.0, 4.0),
    rally_values: tuple[float, ...] = (12.0, 15.0, 18.0),
    risk_values: tuple[float, ...] = (0.25, 0.5, 0.75),
    volume_values: tuple[float, ...] = (1.25, 1.5, 2.0),
) -> list[RobustnessPoint]:
    results: list[RobustnessPoint] = []
    for adr, rally, risk, volume in product(adr_values, rally_values, risk_values, volume_values):
        cfg = {
            **base_config,
            "technical": {**base_config["technical"], "min_adr_20d_pct": adr, "rally_min_pct": rally, "rally_min_volume_ratio": volume},
            "risk": {**base_config["risk"], "risk_per_trade_pct": risk},
        }
        metrics = runner(cfg, variant)
        # Robustness score rewards return and profit factor while penalizing drawdown.
        total_return = float(metrics.get("total_return_pct", 0.0))
        drawdown = float(metrics.get("max_drawdown_pct", 0.0))
        profit_factor = float(metrics.get("profit_factor") or 0.0)
        score = total_return + min(profit_factor, 3.0) * 10.0 - drawdown
        results.append(RobustnessPoint(
            parameters={"adr": adr, "rally": rally, "risk": risk, "volume_ratio": volume},
            score=round(score, 4),
            metrics=metrics,
        ))
    return sorted(results, key=lambda x: x.score, reverse=True)
