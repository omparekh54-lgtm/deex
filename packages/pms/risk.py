from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import math


@dataclass(frozen=True)
class PositionSize:
    shares: int
    stop: float
    rupee_risk: float
    position_value: float
    position_pct: float


def compute_stop(
    entry: float,
    signal_low: float,
    raw_stop_buffer_pct: float,
    max_stop_risk_pct: float,
) -> float:
    values = (entry, signal_low, raw_stop_buffer_pct, max_stop_risk_pct)
    if not all(math.isfinite(float(v)) for v in values):
        raise ValueError("Risk inputs must be finite")
    if (
        entry <= 0
        or signal_low <= 0
        or not 0 <= raw_stop_buffer_pct < 100
        or not 0 < max_stop_risk_pct < 100
    ):
        raise ValueError("Invalid price or stop percentage")
    raw = Decimal(str(signal_low)) * (1 - Decimal(str(raw_stop_buffer_pct)) / 100)
    capped_floor = Decimal(str(entry)) * (1 - Decimal(str(max_stop_risk_pct)) / 100)
    # Round the protective level up so precision never widens the risk cap.
    return float(
        max(raw, capped_floor).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
    )


def size_position(
    capital: float,
    entry: float,
    signal_low: float,
    risk_per_trade_pct: float,
    max_position_pct: float,
    raw_stop_buffer_pct: float,
    max_stop_risk_pct: float,
) -> PositionSize:
    if not all(
        math.isfinite(float(v))
        for v in (capital, entry, signal_low, risk_per_trade_pct, max_position_pct)
    ):
        raise ValueError("Sizing inputs must be finite")
    if not 0 <= risk_per_trade_pct <= 100 or not 0 <= max_position_pct <= 100:
        raise ValueError("Risk and position percentages must be between 0 and 100")
    if capital <= 0 or entry <= 0 or signal_low <= 0:
        raise ValueError("capital, entry and signal_low must be positive")

    stop = compute_stop(entry, signal_low, raw_stop_buffer_pct, max_stop_risk_pct)
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        raise ValueError("stop must be below entry")

    risk_budget = Decimal(str(capital)) * Decimal(str(risk_per_trade_pct)) / 100
    precise_risk = Decimal(str(entry)) - Decimal(str(stop))
    by_risk = int(risk_budget // precise_risk)
    by_cap = int(
        (Decimal(str(capital)) * Decimal(str(max_position_pct)) / 100)
        // Decimal(str(entry))
    )
    shares = max(0, min(by_risk, by_cap))

    value = shares * entry
    rupee_risk = shares * risk_per_share
    return PositionSize(
        shares=shares,
        stop=stop,
        rupee_risk=round(rupee_risk, 2),
        position_value=round(value, 2),
        position_pct=round((value / capital) * 100, 4),
    )
