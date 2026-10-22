"""Deterministic long-only trade lifecycle. No broker orders are submitted here."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from enum import Enum

from packages.pms.risk import size_position

ENGINE_VERSION = "TRADE_ENGINE_v1.1"


class TradeState(str, Enum):
    SIGNAL_CREATED = "SIGNAL_CREATED"
    PENDING_ENTRY = "PENDING_ENTRY"
    FILLED = "FILLED"
    LEG1_ARMED = "LEG1_ARMED"
    PENDING_LEG1_FULL_EXIT = "PENDING_LEG1_FULL_EXIT"
    PENDING_LEG1_PARTIAL_EXIT = "PENDING_LEG1_PARTIAL_EXIT"
    LEG2_ACTIVE = "LEG2_ACTIVE"
    PENDING_LEG2_EXIT = "PENDING_LEG2_EXIT"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


PENDING_EXITS = {
    TradeState.PENDING_LEG1_FULL_EXIT,
    TradeState.PENDING_LEG1_PARTIAL_EXIT,
    TradeState.PENDING_LEG2_EXIT,
}
OPEN_STATES = {TradeState.FILLED, TradeState.LEG1_ARMED, TradeState.LEG2_ACTIVE}


def _positive(**values):
    if any(not math.isfinite(float(v)) or float(v) <= 0 for v in values.values()):
        raise ValueError("Prices and capital must be finite and positive")


def validate_ohlc(open_, high, low, close):
    _positive(open=open_, high=high, low=low, close=close)
    if not low <= min(open_, close) <= max(open_, close) <= high:
        raise ValueError("Invalid OHLC range")


@dataclass
class TradePosition:
    instrument_id: str
    signal_date: date
    signal_high: float
    signal_low: float
    capital: float
    state: TradeState = TradeState.SIGNAL_CREATED
    entry: float | None = None
    stop: float | None = None
    shares: int = 0
    remaining_shares: int = 0
    filled_on: date | None = None
    leg1_armed: bool = False
    entry_bars_seen: int = 0
    realized_pnl: float = 0.0
    exit_reason: str | None = None
    reentry_signal_date: date | None = None
    reentry_signal_high: float | None = None
    reentry_signal_low: float | None = None
    history: list[dict] = field(default_factory=list)
    entry_sessions: tuple[date, ...] = ()
    pending_exit_signal_date: date | None = None
    reentry_created: bool = False
    processed_inputs: dict = field(default_factory=dict)
    last_phase_dates: dict = field(default_factory=dict)

    def __post_init__(self):
        _positive(high=self.signal_high, low=self.signal_low, capital=self.capital)
        if self.signal_low >= self.signal_high:
            raise ValueError("Signal high must exceed signal low")
        if (
            any(
                not isinstance(v, int) or isinstance(v, bool) or v < 0
                for v in (self.shares, self.remaining_shares, self.entry_bars_seen)
            )
            or self.remaining_shares > self.shares
        ):
            raise ValueError("Invalid position quantities")
        if not math.isfinite(self.realized_pnl):
            raise ValueError("Invalid realized PnL")
        if self.entry is not None:
            _positive(entry=self.entry)
        if self.stop is not None:
            _positive(stop=self.stop)
        if self.entry_sessions and (
            tuple(sorted(set(self.entry_sessions))) != self.entry_sessions
            or self.entry_sessions[0] <= self.signal_date
        ):
            raise ValueError(
                "Entry sessions must be unique, ordered and after the signal"
            )

    def log(self, event: str, **payload):
        self.history.append({"event": event, **payload})

    def checkpoint(self):
        """JSON-compatible state for a caller's transactional event store."""
        result = asdict(self)
        result["engine_version"] = ENGINE_VERSION
        result["state"] = self.state.value
        for key in (
            "signal_date",
            "filled_on",
            "reentry_signal_date",
            "pending_exit_signal_date",
        ):
            result[key] = result[key].isoformat() if result[key] else None
        result["entry_sessions"] = [d.isoformat() for d in self.entry_sessions]
        return result

    @classmethod
    def restore(cls, payload):
        from copy import deepcopy

        result = deepcopy(payload)
        if result.pop("engine_version", ENGINE_VERSION) != ENGINE_VERSION:
            raise ValueError("Checkpoint requires a different trade-engine version")
        result["state"] = TradeState(result["state"])
        for key in (
            "signal_date",
            "filled_on",
            "reentry_signal_date",
            "pending_exit_signal_date",
        ):
            if result.get(key):
                result[key] = date.fromisoformat(result[key])
        result["entry_sessions"] = tuple(
            date.fromisoformat(d) for d in result.get("entry_sessions", [])
        )
        return cls(**result)


def _accept(position, phase, bar_date, **payload):
    """Idempotent replay, with conflicting or out-of-order inputs rejected."""
    key = phase + ":" + bar_date.isoformat()
    if key in position.processed_inputs:
        if position.processed_inputs[key] != payload:
            raise ValueError("Conflicting replay of " + key)
        return False
    previous = max(position.last_phase_dates.values(), default=None)
    if previous and bar_date.isoformat() < previous:
        raise ValueError("Out-of-order " + phase + " bar")
    position.processed_inputs[key] = payload
    position.last_phase_dates[phase] = bar_date.isoformat()
    return True


def activate_entry(position):
    if position.state != TradeState.SIGNAL_CREATED:
        raise ValueError("entry can only be activated from SIGNAL_CREATED")
    position.state = TradeState.PENDING_ENTRY
    position.entry_bars_seen = 0
    position.log("ENTRY_ACTIVATED", buy_stop=position.signal_high)
    return position


def cancel_entry(position, reason, bar_date=None):
    if position.state in {TradeState.SIGNAL_CREATED, TradeState.PENDING_ENTRY}:
        position.state = TradeState.CANCELLED
        position.exit_reason = reason
        position.log(
            "ENTRY_CANCELLED", date=str(bar_date) if bar_date else None, reason=reason
        )
    return position


def cancel_if_fundamental_ineligible(position):
    return cancel_entry(position, "module_a_ineligible")


def process_entry_bar(
    position,
    bar_date,
    open_,
    high,
    cfg,
    *,
    fundamental_eligible=True,
    entry_share_limit=None,
):
    _positive(open=open_, high=high)
    if high < open_:
        raise ValueError("High cannot be below open")
    if bar_date < position.signal_date:
        raise ValueError("Entry bar predates signal")
    if bar_date == position.signal_date:
        return position
    if position.state != TradeState.PENDING_ENTRY:
        return position
    if not fundamental_eligible:
        return cancel_entry(position, "module_a_ineligible", bar_date)
    window = int(cfg["technical"]["entry_window_days"])
    if window < 1:
        raise ValueError("Entry window must be positive")
    if position.entry_sessions:
        if len(position.entry_sessions) > window:
            raise ValueError("Invalid entry calendar")
        if bar_date > position.entry_sessions[-1]:
            if len(position.entry_sessions) < window:
                return cancel_entry(position, "entry_calendar_unavailable", bar_date)
            return cancel_entry(position, "entry_window_expired", bar_date)
        if bar_date not in position.entry_sessions:
            return position
        age = position.entry_sessions.index(bar_date) + 1
    else:
        # Low-level research callers must supply every trading session. The
        # production adapter supplies the explicit exchange calendar instead.
        age = position.entry_bars_seen + 1
    sizing = None
    if age <= window and high >= position.signal_high:
        sizing = size_position(
            position.capital,
            max(open_, position.signal_high),
            position.signal_low,
            **{
                k: float(cfg["risk"][k])
                for k in (
                    "risk_per_trade_pct",
                    "max_position_pct",
                    "raw_stop_buffer_pct",
                    "max_stop_risk_pct",
                )
            },
        )
    if not _accept(position, "entry", bar_date, open=open_, high=high):
        return position
    position.entry_bars_seen = age
    if age > window:
        return cancel_entry(position, "entry_window_expired", bar_date)
    if sizing is None:
        return position
    if entry_share_limit is not None:
        if (
            not isinstance(entry_share_limit, int)
            or isinstance(entry_share_limit, bool)
            or entry_share_limit < 0
        ):
            raise ValueError("Invalid entry share limit")
        from dataclasses import replace

        sizing = replace(sizing, shares=min(sizing.shares, entry_share_limit))
    if sizing.shares < 1:
        return cancel_entry(position, "position_size_zero", bar_date)
    position.entry = max(open_, position.signal_high)
    position.stop = sizing.stop
    position.shares = position.remaining_shares = sizing.shares
    position.filled_on = bar_date
    position.state = TradeState.FILLED
    position.log(
        "FILLED",
        date=str(bar_date),
        entry=position.entry,
        stop=sizing.stop,
        shares=sizing.shares,
    )
    return position


def _close(position, bar_date, price, reason):
    qty = position.remaining_shares
    pnl = (price - position.entry) * qty
    position.realized_pnl += pnl
    position.remaining_shares = 0
    position.state = TradeState.CLOSED
    position.exit_reason = reason
    position.log(
        "CLOSED", date=str(bar_date), price=price, shares=qty, reason=reason, pnl=pnl
    )
    return position


def process_next_open(position, bar_date, open_):
    _positive(open=open_)
    if position.state not in PENDING_EXITS:
        return position
    if (
        position.entry is None
        or position.stop is None
        or position.pending_exit_signal_date is None
    ):
        raise ValueError("Pending exit lacks fill/stop/signal date")
    if bar_date < position.pending_exit_signal_date:
        raise ValueError("Open predates exit signal")
    if bar_date == position.pending_exit_signal_date:
        return position
    if not _accept(position, "open", bar_date, open=open_):
        return position
    # A gap through a protective stop takes precedence over an EMA exit and
    # must not create an EMA re-entry opportunity.
    if open_ <= position.stop:
        reason = "breakeven_stop" if position.stop == position.entry else "stop"
        return _close(position, bar_date, open_, reason)
    if position.state == TradeState.PENDING_LEG1_FULL_EXIT:
        return _close(position, bar_date, open_, "leg1_full_exit")
    if position.state == TradeState.PENDING_LEG2_EXIT:
        return _close(position, bar_date, open_, "leg2_ema21")
    qty = (position.remaining_shares + 1) // 2  # round half-sale up for odd lots
    pnl = (open_ - position.entry) * qty
    position.realized_pnl += pnl
    position.remaining_shares -= qty
    position.log("LEG1_EXECUTED", date=str(bar_date), price=open_, shares=qty, pnl=pnl)
    if position.remaining_shares == 0:
        position.state = TradeState.CLOSED
        position.exit_reason = "leg1_partial_completed"
    elif open_ <= position.entry:
        return _close(position, bar_date, open_, "breakeven_gap")
    else:
        position.stop = position.entry
        position.state = TradeState.LEG2_ACTIVE
        position.pending_exit_signal_date = None
    return position


def process_protective_stop(position, bar_date, open_, low):
    """Price-only protection remains available when EMA history is incomplete."""
    _positive(open=open_, low=low)
    if low > open_:
        raise ValueError("Low exceeds open")
    if position.state not in OPEN_STATES:
        return position
    if position.entry is None or position.stop is None or position.filled_on is None:
        raise ValueError("Filled trade lacks entry/stop/date")
    if bar_date < position.filled_on:
        raise ValueError("Stop bar predates fill")
    if low <= position.stop:
        # If entry triggers intraday, the earlier open cannot be used as a
        # fictitious stop fill. Daily-bar ambiguity uses a conservative stop.
        price = min(open_, position.stop)
        if bar_date == position.filled_on and open_ < position.entry:
            price = position.stop
        return _close(
            position,
            bar_date,
            price,
            "breakeven_stop" if position.state == TradeState.LEG2_ACTIVE else "stop",
        )
    return position


def process_position_bar(
    position, bar_date, open_, high, low, close, ema9, previous_ema9, previous_ema21
):
    validate_ohlc(open_, high, low, close)
    _positive(ema9=ema9, previous_ema9=previous_ema9, previous_ema21=previous_ema21)
    if position.state not in OPEN_STATES:
        return position
    if position.entry is None or position.stop is None or position.filled_on is None:
        raise ValueError("Filled trade missing entry/stop/date")
    if bar_date < position.filled_on:
        raise ValueError("Position bar predates fill")
    if not _accept(
        position,
        "close",
        bar_date,
        open=open_,
        high=high,
        low=low,
        close=close,
        ema9=ema9,
        previous_ema9=previous_ema9,
        previous_ema21=previous_ema21,
    ):
        return position
    process_protective_stop(position, bar_date, open_, low)
    if position.state == TradeState.CLOSED:
        return position
    if position.state == TradeState.FILLED and close > ema9:
        position.leg1_armed = True
        position.state = TradeState.LEG1_ARMED
        position.log("LEG1_ARMED", date=str(bar_date))
        return position
    reason = None
    if position.state == TradeState.LEG1_ARMED and close < previous_ema9:
        if close <= position.entry:
            position.state = TradeState.PENDING_LEG1_FULL_EXIT
            reason = "leg1_full_exit"
        else:
            position.state = TradeState.PENDING_LEG1_PARTIAL_EXIT
            reason = "leg1_partial_exit"
    elif position.state == TradeState.LEG2_ACTIVE and close < previous_ema21:
        position.reentry_signal_date = bar_date
        position.reentry_signal_high = high
        position.reentry_signal_low = low
        position.state = TradeState.PENDING_LEG2_EXIT
        reason = "leg2_ema21"
    if reason:
        position.pending_exit_signal_date = bar_date
        position.log(
            "EXIT_SIGNAL", date=str(bar_date), reason=reason, execution="next_open"
        )
    return position


def reentry_allowed(position):
    return (
        position.state == TradeState.CLOSED
        and position.exit_reason == "leg2_ema21"
        and not position.reentry_created
        and position.reentry_signal_date is not None
        and position.reentry_signal_high is not None
        and position.reentry_signal_low is not None
    )


def create_reentry(position, capital, *, fundamental_eligible=True, entry_sessions=()):
    if not reentry_allowed(position):
        raise ValueError("Re-entry requires an unused Leg2 EMA21 exit")
    if not fundamental_eligible:
        raise ValueError("Module A eligibility required for re-entry")
    result = TradePosition(
        position.instrument_id,
        position.reentry_signal_date,
        position.reentry_signal_high,
        position.reentry_signal_low,
        capital,
        entry_sessions=tuple(entry_sessions),
    )
    position.reentry_created = True
    position.log("REENTRY_CREATED", signal_date=str(result.signal_date))
    return result


def position_from_module_b(result, capital, calendar, cfg):
    """Create an unactivated proposal; Module C still allocates capital/slots."""
    if result.research_only or not result.eligible_universe or not result.signal:
        raise ValueError("A production-eligible Module B signal is required")
    signal = result.signal
    if signal.get("setup") != "SETUP_1" or signal.get("status") not in {
        "ACTIVE",
        "SIGNAL_CREATED",
    }:
        raise ValueError("Only a live production Setup 1 signal is supported")
    signal_date = date.fromisoformat(signal["signal_date"])
    window = int(cfg["technical"]["entry_window_days"])
    sessions = calendar.forward(signal_date + timedelta(days=1), window)
    if (
        len(sessions) != window
        or result.as_of < signal_date
        or result.as_of >= sessions[-1]
    ):
        raise ValueError("Entry window expired or exchange calendar unavailable")
    return TradePosition(
        result.instrument_id,
        signal_date,
        signal["signal_high"],
        signal["signal_low"],
        capital,
        entry_sessions=tuple(sessions),
    )


def process_trade_session(
    position,
    bar_date,
    open_,
    high,
    low,
    close,
    ema9,
    previous_ema9,
    previous_ema21,
    cfg,
    *,
    fundamental_eligible,
):
    """Canonical session order: pending next-open exits, entry, stop, close rule.

    Daily-bar fills that touch both entry and stop assume stop after entry.
    A loss of fundamental eligibility cancels unfilled entries only.
    """
    validate_ohlc(open_, high, low, close)
    if not isinstance(fundamental_eligible, bool):
        raise ValueError("Fundamental eligibility must be explicit true/false")
    _positive(ema9=ema9, previous_ema9=previous_ema9, previous_ema21=previous_ema21)
    process_next_open(position, bar_date, open_)
    if not fundamental_eligible:
        cancel_if_fundamental_ineligible(position)
    process_entry_bar(
        position, bar_date, open_, high, cfg, fundamental_eligible=fundamental_eligible
    )
    process_position_bar(
        position, bar_date, open_, high, low, close, ema9, previous_ema9, previous_ema21
    )
    expire_entry_at_close(position, bar_date, cfg)
    return position


def expire_entry_at_close(position, bar_date, cfg):
    window = int(cfg["technical"]["entry_window_days"])
    expired = (
        (
            len(position.entry_sessions) == window
            and bar_date >= position.entry_sessions[-1]
        )
        if position.entry_sessions
        else position.entry_bars_seen >= window
    )
    if expired:
        cancel_entry(position, "entry_window_expired", bar_date)
    return position
