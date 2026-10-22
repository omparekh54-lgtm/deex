from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

from packages.pms.research import ResearchVariant
from packages.pms.research_lab import ResearchLab


@dataclass(frozen=True)
class StressPeriod:
    name: str
    start: date
    end: date
    description: str = ""


DEFAULT_STRESS_PERIODS: tuple[StressPeriod, ...] = (
    StressPeriod("2018_2019_WEAK_MARKET", date(2018, 1, 1), date(2019, 12, 31), "Extended weak/broadly difficult Indian equity environment"),
    StressPeriod("COVID_CRASH_2020", date(2020, 2, 1), date(2020, 5, 31), "COVID crash and first rebound"),
    StressPeriod("2022_RISK_OFF", date(2022, 1, 1), date(2022, 12, 31), "2022 risk-off / volatile market environment"),
)


@dataclass(frozen=True)
class StressResult:
    period: StressPeriod
    metrics: dict


class StressTestRunner:
    def __init__(self, engine, symbols: list[str], initial_capital: float | None = None):
        self.engine = engine
        self.symbols = symbols
        self.initial_capital = initial_capital

    def run(
        self,
        *,
        variant: ResearchVariant | None = None,
        periods: Iterable[StressPeriod] = DEFAULT_STRESS_PERIODS,
    ) -> list[StressResult]:
        variant = variant or ResearchVariant("FULL_A_B_C_E")
        results: list[StressResult] = []
        for period in periods:
            lab = ResearchLab(
                self.engine,
                self.symbols,
                period.start,
                period.end,
                initial_capital=self.initial_capital,
            )
            metrics = lab.run_variant(lab.base_config, variant)
            results.append(StressResult(period=period, metrics=metrics))
        return results


def summarize_stress_results(results: Iterable[StressResult]) -> dict:
    rows = list(results)
    if not rows:
        return {"periods": 0, "positive_periods": 0, "worst_drawdown_pct": 0.0, "worst_return_pct": 0.0}
    returns = [float(x.metrics.get("total_return_pct", 0.0)) for x in rows]
    drawdowns = [float(x.metrics.get("max_drawdown_pct", 0.0)) for x in rows]
    return {
        "periods": len(rows),
        "positive_periods": sum(1 for x in returns if x > 0),
        "worst_drawdown_pct": max(drawdowns) if drawdowns else 0.0,
        "worst_return_pct": min(returns) if returns else 0.0,
        "average_return_pct": round(sum(returns) / len(returns), 4),
    }
