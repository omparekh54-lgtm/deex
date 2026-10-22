from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
import math


class RallyState(str, Enum):
    BELOW_21EMA = "BELOW_21EMA"
    RALLY_TRACKING = "RALLY_TRACKING"
    RALLY_VALID = "RALLY_VALID"
    RALLY_INVALID = "RALLY_INVALID"
    SIGNAL_CREATED = "SIGNAL_CREATED"


@dataclass(frozen=True)
class Bar:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    ema21: float


@dataclass
class RallyTracker:
    state: RallyState = RallyState.BELOW_21EMA
    start_date: str | None = None
    base: float | None = None
    highest_high: float | None = None
    highest_high_date: str | None = None
    trading_days: int = 0
    cumulative_volume: float = 0.0
    peak_trading_days: int = 0
    baseline_volume_50d: float = 0.0
    peak_cumulative_volume: float = 0.0
    signal_date: str | None = None
    signal_high: float | None = None
    signal_low: float | None = None
    last_date: str | None = None

    def rally_pct(self) -> float:
        return (
            (self.highest_high / self.base - 1) * 100
            if self.base and self.highest_high
            else 0.0
        )

    def velocity(self) -> float:
        return self.rally_pct() / max(self.peak_trading_days, 1)

    def volume_ratio(self) -> float:
        return (
            self.peak_cumulative_volume
            / max(self.peak_trading_days, 1)
            / self.baseline_volume_50d
            if self.baseline_volume_50d > 0
            else 0.0
        )


def _qualify(tracker: RallyTracker, cfg: dict):
    t = cfg["technical"]
    if tracker.peak_trading_days > int(t["rally_max_days"]):
        tracker.state = RallyState.RALLY_INVALID
    else:
        valid = (
            tracker.rally_pct() + 1e-9 >= t["rally_min_pct"]
            and tracker.velocity() + 1e-9 >= t["rally_min_velocity_pct_per_day"]
            and tracker.volume_ratio() + 1e-9 >= t["rally_min_volume_ratio"]
        )
        tracker.state = RallyState.RALLY_VALID if valid else RallyState.RALLY_TRACKING


def advance_rally(
    tracker: RallyTracker,
    bar: Bar,
    previous_close: float,
    previous_ema21: float,
    baseline_volume_50d: float,
    cfg: dict,
) -> RallyTracker:
    """Track one rally causally, using a frozen pre-rally volume baseline.

    Duration, velocity and rally-average volume cover start through highest-high
    day (inclusive). A later higher high recomputes validity; a flat tail does
    not lengthen the measured rally. First strict close below EMA21 ends it.
    """
    numbers = [
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.ema21,
        previous_close,
        previous_ema21,
    ]
    if (
        not all(math.isfinite(v) for v in numbers)
        or min(bar.open, bar.high, bar.low, bar.close, bar.ema21) <= 0
        or bar.volume < 0
    ):
        raise ValueError("invalid/non-finite rally bar")
    if not bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high:
        raise ValueError("invalid OHLC range")
    if tracker.last_date is not None and bar.date <= tracker.last_date:
        raise ValueError("rally bars must have unique increasing dates")
    tracker.last_date = bar.date
    if tracker.state == RallyState.SIGNAL_CREATED:
        return tracker
    if tracker.state == RallyState.BELOW_21EMA:
        if previous_close <= previous_ema21 and bar.close > bar.ema21:
            tracker.start_date = bar.date
            tracker.base = bar.low
            tracker.highest_high = bar.high
            tracker.highest_high_date = bar.date
            tracker.trading_days = tracker.peak_trading_days = 1
            tracker.cumulative_volume = tracker.peak_cumulative_volume = bar.volume
            tracker.baseline_volume_50d = float(baseline_volume_50d)
            if (
                not math.isfinite(tracker.baseline_volume_50d)
                or tracker.baseline_volume_50d <= 0
            ):
                tracker.state = RallyState.RALLY_INVALID
            else:
                _qualify(tracker, cfg)
        return tracker
    if bar.close < bar.ema21:
        if tracker.state == RallyState.RALLY_VALID:
            tracker.state = RallyState.SIGNAL_CREATED
            tracker.signal_date = bar.date
            tracker.signal_high = bar.high
            tracker.signal_low = bar.low
        else:
            _reset(tracker, bar.date)
        return tracker
    if tracker.state == RallyState.RALLY_INVALID:
        return tracker
    tracker.trading_days += 1
    tracker.cumulative_volume += bar.volume
    if tracker.highest_high is None or bar.high > tracker.highest_high:
        tracker.highest_high = bar.high
        tracker.highest_high_date = bar.date
        tracker.peak_trading_days = tracker.trading_days
        tracker.peak_cumulative_volume = tracker.cumulative_volume
        _qualify(tracker, cfg)
    return tracker


def _reset(tracker: RallyTracker, last_date: str | None = None):
    empty = RallyTracker(last_date=last_date)
    for field in fields(tracker):
        setattr(tracker, field.name, getattr(empty, field.name))
