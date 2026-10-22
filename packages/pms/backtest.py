from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Callable, Iterable

from packages.pms.config import load_strategy_config
from packages.pms.costs import CostBreakdown, estimate_delivery_costs
from packages.pms.trade_engine import (
    TradePosition,
    TradeState,
    OPEN_STATES,
    PENDING_EXITS,
    activate_entry,
    cancel_entry,
    create_reentry,
    expire_entry_at_close,
    process_entry_bar,
    process_next_open,
    process_position_bar,
    reentry_allowed,
    validate_ohlc,
)


class EventType(str, Enum):
    CORPORATE_ACTION = "CORPORATE_ACTION"
    MARKET_OPEN = "MARKET_OPEN"
    BAR = "BAR"
    SIGNAL = "SIGNAL"
    ENTRY_FILL = "ENTRY_FILL"
    EXIT_FILL = "EXIT_FILL"
    POSITION_STATE = "POSITION_STATE"
    REENTRY_SIGNAL = "REENTRY_SIGNAL"
    MARKET_CLOSE = "MARKET_CLOSE"


@dataclass(frozen=True)
class MarketBar:
    instrument_id: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    ema9: float
    previous_ema9: float
    previous_ema21: float


@dataclass(frozen=True)
class Signal:
    instrument_id: str
    signal_date: date
    signal_high: float
    signal_low: float
    score: float = 0.0
    stop_risk_pct: float = 0.0
    adtv: float = 0.0
    reentry: bool = False


@dataclass(frozen=True)
class Fill:
    instrument_id: str
    trade_date: date
    side: str
    price: float
    quantity: int
    reason: str
    costs: CostBreakdown


@dataclass(frozen=True)
class BacktestEvent:
    event_type: EventType
    trade_date: date
    instrument_id: str | None = None
    payload: dict = field(default_factory=dict)


@dataclass
class BacktestResult:
    initial_capital: float
    ending_cash: float
    realized_gross_pnl: float
    total_costs: float
    realized_net_pnl: float
    return_pct: float
    fills: list[Fill]
    events: list[BacktestEvent]
    closed_trades: int
    equity_curve: list[tuple[date, float]] = field(default_factory=list)
    ending_equity: float = 0.0
    unrealized_pnl: float = 0.0


SignalProvider = Callable[[date], Iterable[Signal]]
EligibilityProvider = Callable[[str, date], bool]
CapacityProvider = Callable[[date], int]


class EventDrivenBacktester:
    """Research harness. Eligibility/capacity callbacks must be knowable at open.

    Signals are supplied after close. Dates from all bars are exchange sessions
    unless an explicit, complete trading_sessions calendar is supplied. This
    harness does not replace Module C's sector/ranking/portfolio-heat allocator.
    """

    def __init__(self, initial_capital=None, cfg=None):
        self.cfg = cfg or load_strategy_config()
        value = (
            self.cfg["backtest"]["initial_capital"]
            if initial_capital is None
            else initial_capital
        )
        if not math.isfinite(float(value)) or value <= 0:
            raise ValueError("Invalid initial capital")
        if not self.cfg["backtest"].get("same_day_stop_after_entry", True):
            raise ValueError(
                "Daily-bar engine requires conservative same-day stop handling"
            )
        self.initial_capital = self.cash = float(value)
        self.positions = {}
        self.fills = []
        self.events = []
        self.total_costs = self.realized_gross_pnl = 0.0
        self.closed_trade_count = 0
        self.equity_curve = []
        self.last_prices = {}
        self.open_marks = {}
        self.seen_signals = set()
        self._ran = False
        self.entry_guard = None
        self.entry_timing_guard = None
        self.signal_ranks = {}
        self.pending_rank_provider = None

    def _active_count(self):
        return sum(
            p.state not in {TradeState.CLOSED, TradeState.CANCELLED}
            for p in self.positions.values()
        )

    def _trim_pending(self, day, capacity):
        def rank(p):
            values = (
                self.pending_rank_provider(p, day)
                if self.pending_rank_provider else None
            )
            score, risk, adtv = values or self.signal_ranks.get(
                (p.instrument_id, p.signal_date), (0.0, 0.0, 0.0)
            )
            if not all(math.isfinite(float(v)) for v in (score, risk, adtv)):
                raise ValueError("Invalid pending-order rank")
            return (-score, risk, -adtv, p.signal_date, p.instrument_id)

        pending = sorted(
            (p for p in self.positions.values() if p.state == TradeState.PENDING_ENTRY),
            key=rank,
        )
        for p in reversed(pending):
            if self._active_count() <= capacity:
                break
            cancel_entry(p, "regime_capacity", day)
            self.events.append(
                BacktestEvent(
                    EventType.POSITION_STATE, day, p.instrument_id,
                    {"to": "CANCELLED", "reason": "regime_capacity"},
                )
            )

    def _costs(self, d, side, price, qty):
        return estimate_delivery_costs(
            trade_date=d, side=side, price=price, quantity=qty, cfg=self.cfg["backtest"]
        )

    def _record_fill(self, *, d, instrument_id, side, price, qty, reason):
        if qty <= 0:
            return
        costs = self._costs(d, side, price, qty)
        self.total_costs += costs.total
        self.cash += (
            -price * qty - costs.total if side == "BUY" else price * qty - costs.total
        )
        self.fills.append(Fill(instrument_id, d, side, price, qty, reason, costs))
        self.events.append(
            BacktestEvent(
                EventType.ENTRY_FILL if side == "BUY" else EventType.EXIT_FILL,
                d,
                instrument_id,
                {
                    "reason": reason,
                    "price": price,
                    "quantity": qty,
                    "costs": costs.total,
                },
            )
        )

    def _entry_sessions(self, signal_date):
        n = int(self.cfg["technical"]["entry_window_days"])
        return tuple(d for d in self.sessions if d > signal_date)[:n]

    def _apply_open_execution(self, position, bar, eligibility_provider, allow_reentry):
        before_state = position.state
        before_remaining, before_pnl = position.remaining_shares, position.realized_pnl
        process_next_open(position, bar.trade_date, bar.open)
        if position.state == before_state:
            return None
        sold = before_remaining - position.remaining_shares
        if sold:
            self._record_fill(
                d=bar.trade_date,
                instrument_id=position.instrument_id,
                side="SELL",
                price=bar.open,
                qty=sold,
                reason=position.exit_reason or "leg1_partial_exit",
            )
        self.realized_gross_pnl += position.realized_pnl - before_pnl
        self.events.append(
            BacktestEvent(
                EventType.POSITION_STATE,
                bar.trade_date,
                position.instrument_id,
                {"from": before_state.value, "to": position.state.value},
            )
        )
        if position.state == TradeState.CLOSED:
            self.closed_trade_count += 1
        if (
            allow_reentry
            and reentry_allowed(position)
            and eligibility_provider(position.instrument_id, bar.trade_date)
        ):
            replacement = create_reentry(
                position,
                self._equity_at_open(),
                entry_sessions=self._entry_sessions(position.reentry_signal_date),
            )
            activate_entry(replacement)
            self.seen_signals.add((replacement.instrument_id, replacement.signal_date))
            self.events.append(
                BacktestEvent(
                    EventType.REENTRY_SIGNAL,
                    bar.trade_date,
                    position.instrument_id,
                    {
                        "signal_date": str(replacement.signal_date),
                        "signal_high": replacement.signal_high,
                        "signal_low": replacement.signal_low,
                    },
                )
            )
            return replacement
        return None

    def _equity_at_open(self):
        return self.cash + sum(
            p.remaining_shares
            * self.open_marks.get(i, self.last_prices.get(i, p.entry or 0))
            for i, p in self.positions.items()
        )

    def _apply_entry(self, position, bar):
        if self.entry_timing_guard is not None:
            self.entry_timing_guard(position, bar)
        # Recompute the risk budget from portfolio equity, not leftover cash.
        position.capital = self._equity_at_open()
        process_entry_bar(position, bar.trade_date, bar.open, bar.high, self.cfg)
        if position.state != TradeState.FILLED:
            return
        price = float(position.entry)
        lo, hi = 0, position.shares
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if (
                price * mid + self._costs(bar.trade_date, "BUY", price, mid).total
                <= self.cash
            ):
                lo = mid
            else:
                hi = mid - 1
        qty = lo
        if self.entry_guard is not None and qty:
            allowed = self.entry_guard(position, bar, qty)
            if (
                isinstance(allowed, bool)
                or not isinstance(allowed, int)
                or not 0 <= allowed <= qty
            ):
                raise ValueError("Invalid research entry guard quantity")
            qty = allowed
        if qty < 1:
            position.state = TradeState.PENDING_ENTRY
            position.entry = position.stop = position.filled_on = None
            position.shares = position.remaining_shares = 0
            position.history.pop()  # no executed fill occurred
            reason = (
                "research_portfolio_guard"
                if lo > 0 and self.entry_guard is not None
                else "insufficient_cash"
            )
            cancel_entry(position, reason, bar.trade_date)
            if reason == "research_portfolio_guard":
                self.events.append(
                    BacktestEvent(
                        EventType.POSITION_STATE,
                        bar.trade_date,
                        position.instrument_id,
                        {"to": "CANCELLED", "reason": reason},
                    )
                )
            return
        position.shares = position.remaining_shares = qty
        position.history[-1]["shares"] = qty
        self._record_fill(
            d=bar.trade_date,
            instrument_id=position.instrument_id,
            side="BUY",
            price=price,
            qty=qty,
            reason="entry",
        )

        self.events[-1].payload.update(
            initial_stop=position.stop, initial_risk=(price - position.stop) * qty
        )
        if self.cfg["backtest"].get("research_leg1_always_armed", False):
            position.leg1_armed = True
            position.state = TradeState.LEG1_ARMED

    def _apply_bar(self, position, bar):
        before_state = position.state
        before_remaining, before_pnl = position.remaining_shares, position.realized_pnl
        process_position_bar(
            position,
            bar.trade_date,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.ema9,
            bar.previous_ema9,
            bar.previous_ema21,
        )
        if position.state == TradeState.CLOSED and before_remaining > 0:
            self._record_fill(
                d=bar.trade_date,
                instrument_id=position.instrument_id,
                side="SELL",
                price=float(position.history[-1]["price"]),
                qty=before_remaining,
                reason=position.exit_reason or "exit",
            )
            self.closed_trade_count += 1
        self.realized_gross_pnl += position.realized_pnl - before_pnl
        if position.state != before_state:
            self.events.append(
                BacktestEvent(
                    EventType.POSITION_STATE,
                    bar.trade_date,
                    position.instrument_id,
                    {"from": before_state.value, "to": position.state.value},
                )
            )

    def run(
        self,
        bars,
        signal_provider,
        eligibility_provider=None,
        capacity_provider=None,
        *,
        allow_reentry=True,
        trading_sessions=None,
        signal_eligibility_provider=None,
        close_capacity_provider=None,
        bar_provider=None,
        session_bounds=None,
        on_session_open=None,
        on_session_close=None,
        entry_guard=None,
        allow_live_signals=False,
        auto_reentry=True,
        pending_rank_provider=None,
        entry_timing_guard=None,
    ):
        if self._ran:
            raise ValueError("Use a fresh backtester for each run")
        eligibility_provider = eligibility_provider or (lambda _i, _d: True)
        capacity_provider = capacity_provider or (
            lambda _d: int(self.cfg["risk"]["max_open_trades"])
        )
        signal_eligibility_provider = (
            signal_eligibility_provider or eligibility_provider
        )
        close_capacity_provider = close_capacity_provider or capacity_provider
        bars_by_date = {}
        for bar in bars:
            validate_ohlc(bar.open, bar.high, bar.low, bar.close)
            if any(
                not math.isfinite(float(v)) or v <= 0
                for v in (bar.ema9, bar.previous_ema9, bar.previous_ema21)
            ):
                raise ValueError("Invalid EMA values")
            daily = bars_by_date.setdefault(bar.trade_date, {})
            if bar.instrument_id in daily and daily[bar.instrument_id] != bar:
                raise ValueError("Conflicting duplicate market bar")
            daily[bar.instrument_id] = bar
        self.sessions = sorted(
            set(trading_sessions) if trading_sessions is not None else bars_by_date
        )
        if not set(bars_by_date).issubset(self.sessions):
            raise ValueError("Market bar outside exchange calendar")
        self.entry_guard = entry_guard
        self.entry_timing_guard = entry_timing_guard
        self.pending_rank_provider = pending_rank_provider
        if bar_provider is not None and (
            not trading_sessions or session_bounds is None
        ):
            raise ValueError("Lazy research bars require explicit sessions and bounds")
        self._ran = True
        run_days = (
            [d for d in self.sessions if session_bounds[0] <= d <= session_bounds[1]]
            if bar_provider
            else (
                day
                for day in self.sessions
                if bars_by_date and min(bars_by_date) <= day <= max(bars_by_date)
            )
        )
        for d in run_days:
            daily = bars_by_date.get(d, {})
            if bar_provider is not None:
                daily = {}
                for bar in bar_provider(d):
                    validate_ohlc(bar.open, bar.high, bar.low, bar.close)
                    if (
                        bar.trade_date != d
                        or bar.instrument_id in daily
                        or any(
                            not math.isfinite(float(v)) or v <= 0
                            for v in (bar.ema9, bar.previous_ema9, bar.previous_ema21)
                        )
                    ):
                        raise ValueError("Invalid lazy research bar")
                    daily[bar.instrument_id] = bar
            if on_session_open is not None:
                on_session_open(d, self, daily)
            self.open_marks = {i: b.open for i, b in daily.items()}
            self.events.append(BacktestEvent(EventType.MARKET_OPEN, d))
            capacity = max(
                0,
                min(
                    int(self.cfg["risk"]["max_open_trades"]), int(capacity_provider(d))
                ),
            )
            for instrument, position in list(self.positions.items()):
                bar = daily.get(instrument)
                if bar and position.state in PENDING_EXITS:
                    replacement = self._apply_open_execution(
                        position, bar, eligibility_provider, allow_reentry and auto_reentry
                    )
                    if replacement is not None:
                        self.positions[instrument] = replacement
            # Filled holdings survive capacity reductions; lowest-ranked pending
            # orders are cancelled using the last available after-close ranking.
            for i, p in self.positions.items():
                if p.state == TradeState.PENDING_ENTRY and not eligibility_provider(i, d):
                    cancel_entry(p, "module_a_ineligible", d)
            self._trim_pending(d, capacity)
            for instrument, position in list(self.positions.items()):
                bar = daily.get(instrument)
                if not bar:
                    continue
                if position.state == TradeState.PENDING_ENTRY:
                    self._apply_entry(position, bar)
                if position.state in OPEN_STATES:
                    self._apply_bar(position, bar)
            for p in self.positions.values():
                expire_entry_at_close(p, d, self.cfg)
            self.events.append(BacktestEvent(EventType.MARKET_CLOSE, d))
            self.last_prices.update({i: b.close for i, b in daily.items()})
            if on_session_close is not None:
                on_session_close(d, self)
            close_capacity = max(
                0,
                min(
                    int(self.cfg["risk"]["max_open_trades"]),
                    int(close_capacity_provider(d)),
                ),
            )
            for i, p in self.positions.items():
                if p.state == TradeState.PENDING_ENTRY and not signal_eligibility_provider(i, d):
                    cancel_entry(p, "module_a_ineligible", d)
            for signal in signal_provider(d):
                if allow_live_signals and (
                    signal.signal_date > d
                    or d not in self.sessions
                    or d
                    > (
                        self._entry_sessions(signal.signal_date)
                        or (signal.signal_date,)
                    )[-1]
                ):
                    raise ValueError("Future or expired live research signal")
                if not allow_live_signals and signal.signal_date != d:
                    raise ValueError(
                        "Signal provider must emit newly formed close-date signals only"
                    )
                if (signal.instrument_id, signal.signal_date) in self.seen_signals:
                    continue
                existing = self.positions.get(signal.instrument_id)
                if existing and existing.state not in {
                    TradeState.CLOSED,
                    TradeState.CANCELLED,
                }:
                    continue
                if (
                    self._active_count() >= close_capacity
                    or not signal_eligibility_provider(signal.instrument_id, d)
                ):
                    continue
                if signal.reentry:
                    if (
                        not allow_reentry or not existing or not reentry_allowed(existing)
                        or signal.signal_date != existing.reentry_signal_date
                        or signal.signal_high != existing.reentry_signal_high
                        or signal.signal_low != existing.reentry_signal_low
                    ):
                        raise ValueError("Invalid allocated re-entry")
                    p = create_reentry(
                        existing, max(self._equity_at_open(), 0),
                        entry_sessions=self._entry_sessions(signal.signal_date),
                    )
                else:
                    p = TradePosition(
                        signal.instrument_id,
                        signal.signal_date,
                        signal.signal_high,
                        signal.signal_low,
                        max(self._equity_at_open(), 0),
                        entry_sessions=self._entry_sessions(signal.signal_date),
                    )
                self.signal_ranks[(signal.instrument_id, signal.signal_date)] = (
                    signal.score, signal.stop_risk_pct, signal.adtv
                )
                activate_entry(p)
                self.positions[signal.instrument_id] = p
                self.seen_signals.add((signal.instrument_id, signal.signal_date))
                self.events.append(
                    BacktestEvent(
                        EventType.SIGNAL,
                        d,
                        signal.instrument_id,
                        {
                            "signal_high": signal.signal_high,
                            "signal_low": signal.signal_low,
                        },
                    )
                )
            self._trim_pending(d, close_capacity)
            self.last_prices.update({i: b.close for i, b in daily.items()})
            equity = self.cash + sum(
                p.remaining_shares * self.last_prices.get(i, p.entry or 0)
                for i, p in self.positions.items()
            )
            self.equity_curve.append((d, round(equity, 2)))
        net = self.realized_gross_pnl - self.total_costs
        ending_equity = (
            self.equity_curve[-1][1] if self.equity_curve else self.initial_capital
        )
        unrealized = sum(
            p.remaining_shares
            * (self.last_prices.get(i, p.entry or 0) - (p.entry or 0))
            for i, p in self.positions.items()
        )
        return BacktestResult(
            self.initial_capital,
            round(self.cash, 2),
            round(self.realized_gross_pnl, 2),
            round(self.total_costs, 2),
            round(net, 2),
            round((ending_equity / self.initial_capital - 1) * 100, 4),
            self.fills,
            self.events,
            self.closed_trade_count,
            self.equity_curve,
            ending_equity,
            round(unrealized, 2),
        )
