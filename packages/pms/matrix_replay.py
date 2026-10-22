"""Isolated historical A/B/C/E portfolio replay. Never reads live account state."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace

from sqlalchemy import text

from packages.data_layer.queries import PointInTimeQueries
from packages.data_layer.features import compute_features, FeatureFrame
from packages.data_layer.reference_data import CorporateAction, split_adjustment_factor
from packages.pms.backtest import (
    EventDrivenBacktester,
    MarketBar,
    Signal,
    BacktestEvent,
    EventType,
)
from packages.pms.execution_timing import intraday_trigger, intraday_information_changed
from packages.pms.backtest_matrix import fingerprint, round_trips
from packages.pms.market_calendar import MarketCalendar
from packages.pms.module_a_context import IST, end_of_day
from packages.pms.module_a_data import CONTEXT_SQL, context_params, normalize_context
from packages.pms.module_a_pipeline import ModuleAPipeline
from packages.pms.module_b_pipeline import ModuleBPipeline
from packages.pms.module_b_setup2 import EarningsAnchoredSetup2
from packages.pms.module_c import AllocationState
from packages.pms.module_c_pipeline import ModuleCPipeline
from packages.pms.module_e import RegimeTracker
from packages.pms.module_e_data import COVERAGE_SQL, classify_liquidity
from packages.pms.module_e_pipeline import ModuleEPipeline
from packages.pms.risk import size_position
from packages.pms.trade_engine import (
    TradeState,
    OPEN_STATES,
    PENDING_EXITS,
    reentry_allowed,
)


class ResearchDataGap(ValueError):
    """A run cannot produce comparable evidence from incomplete inputs."""


class HistoricalA(ModuleAPipeline):
    def __init__(self, engine, cfg, symbols):
        super().__init__(engine, cfg)
        self.symbols = symbols
        self.financials = PointInTimeQueries(engine, historical=True)
        self.inputs.contexts = self.contexts

    def contexts(self, stamp, instrument_pk=None):
        # An inactive company today can still belong to the historical universe.
        sql = CONTEXT_SQL.replace(
            "i.active=true",
            "i.listed_from<=cast(:market_date as date) and (i.delisted_on is null or i.delisted_on>cast(:market_date as date))",
        )
        with self.engine.connect() as conn:
            rows = (
                conn.execute(text(sql), context_params(stamp, self.cfg, instrument_pk))
                .mappings()
                .all()
            )
        return [normalize_context(dict(r), stamp, self.cfg) for r in rows]

    def scan(self, stamp, *, persist=False, symbols=None):
        if persist:
            raise ValueError("Historical matrix A must remain read-only")
        return super().scan(stamp, persist=False, symbols=self.symbols)


class ResearchB(ModuleBPipeline):
    def __init__(self, engine, cfg, historical_a, calendar):
        super().__init__(engine, cfg)
        self.module_a = historical_a
        self.prices = PointInTimeQueries(engine, historical=True)
        self._calendar = lambda _: calendar
        self.candle_cache = {}

    def _candles(self, instrument, day, lookback_days=500):
        key = instrument, day, lookback_days
        if key not in self.candle_cache:
            try:
                self.candle_cache[key] = super()._candles(
                    instrument, day, lookback_days
                )
            except (ValueError, KeyError) as exc:
                raise ResearchDataGap(
                    "historical_price_or_reference_input_invalid"
                ) from exc
        return self.candle_cache[key]

    def _signal_cancelled(self, instrument, signal):
        # Stored production cancellations belong to another account/run.
        return False

    def features(self, candles):
        result = compute_features(candles)
        window = int(self.cfg["backtest"].get("research_adr_window", 20))
        if window not in (20, 50):
            raise ValueError("Matrix ADR window must be 20 or 50")
        if window == 50 and not result.data.empty:
            frame = result.data.copy()
            frame["adr20_pct"] = (
                (frame.high / frame.low).rolling(50, min_periods=50).mean() - 1
            ) * 100
            return FeatureFrame(frame)
        return result

    def evaluate(self, instrument_id, as_of, *, require_module_a=True):
        result = super().evaluate(
            instrument_id, as_of, require_module_a=require_module_a
        )
        cap = self.cfg["backtest"].get("research_adr_upper_cap")
        if result.eligible_universe and cap is not None:
            adr = float(
                self.features(self._candles(instrument_id, as_of))
                .data.iloc[-1]
                .adr20_pct
            )
            if not math.isfinite(adr) or adr > float(cap):
                return replace(
                    result,
                    eligible_universe=False,
                    signal=None,
                    watchlist=None,
                    rejected_reasons=("research_adr_upper_cap",),
                )
        return result


class ResearchE(ModuleEPipeline):
    def __init__(self, engine, cfg, replay):
        super().__init__(engine, cfg)
        self.replay, self.tracker = replay, RegimeTracker()

    def _coverage(self, expected):
        sql = COVERAGE_SQL.replace("i.active and", "i.listed_from is not null and")
        with self.engine.connect() as conn:
            rows = [
                dict(r)
                for r in conn.execute(text(sql), {"d": expected}).mappings().all()
            ]
        return classify_liquidity(
            rows,
            expected,
            float(self.cfg["regime"]["breadth_min_turnover_cr"]) * 10000000,
        )

    def _previous_tracker(self, expected, calendar):
        previous = max((d for d in calendar.sessions if d < expected), default=None)
        if self.tracker.last_session != previous:
            self.tracker = RegimeTracker()
        return self.tracker, "isolated_matrix_history"

    def _strategy_feedback(self, stamp):
        closed, _ = round_trips(
            SimpleNamespace(
                fills=self.replay.broker.fills, events=self.replay.broker.events
            )
        )
        recent = closed[-15:]
        bad = len(recent) >= 10 and any(t["r_multiple"] is None for t in recent)
        return dict(
            closed_trades=len(recent),
            expectancy_r=(
                sum(t["r_multiple"] for t in recent) / len(recent)
                if len(recent) >= 10 and not bad
                else None
            ),
            reasons=["matrix_initial_risk_unavailable"] if bad else [],
            pnl_basis="isolated matrix cycles after costs",
        )


class ResearchC(ModuleCPipeline):
    def __init__(self, engine, cfg, replay):
        super().__init__(engine, cfg)
        self.replay, self.a, self.b = replay, replay.a, replay.b

    def _evaluate_signal(self, instrument, day):
        if self.replay.case.variant.setup_2:
            return self.replay.setup2.evaluate(instrument, day, require_module_a=True)
        return super()._evaluate_signal(instrument, day)

    def _validate_signal(self, result):
        if self.replay.case.variant.setup_2 and result.research_only:
            if result.signal and (
                result.signal.get("setup") != "SETUP_2"
                or result.signal.get("status") not in {"ACTIVE", "SIGNAL_CREATED"}
            ):
                raise ValueError("unsupported_or_inactive_signal")
            return
        super()._validate_signal(result)

    def _regime_capacity(self, expected):
        return self.replay.capacity, ()

    def _portfolio_state(self, expected, as_of, sectors):
        return self.replay.allocation_state(sectors)


class MatrixReplay:
    def __init__(self, engine, symbols, cfg, case, start, end):
        self.engine, self.symbols, self.cfg, self.case = (
            engine,
            tuple(symbols),
            cfg,
            case,
        )
        self.start, self.end = start, end
        self.calendar = MarketCalendar.from_database(engine, end_of_day(end))
        self.calendar.sessions = {
            d for d in self.calendar.sessions if d <= end + timedelta(days=150)
        }
        self.a = HistoricalA(engine, cfg, list(symbols))
        self.b = ResearchB(engine, cfg, self.a, self.calendar)
        self.setup2 = EarningsAnchoredSetup2(self.b)
        self.broker = EventDrivenBacktester(cfg=cfg)
        self.c = ResearchC(engine, cfg, self)
        self.e = ResearchE(engine, cfg, self)
        self.capacity = int(cfg["risk"]["max_open_trades"])
        self.last_analysis, self.metadata = {}, {}
        self.admissions = {}
        self.rejections = Counter()
        self.regimes, self.decisions = [], []
        self.actions = self._actions()
        with engine.connect() as conn:
            self.listings = {
                r["instrument_id"]: (r["listed_from"], r["delisted_on"])
                for r in conn.execute(
                    text(
                        "select exchange||':'||coalesce(nse_symbol,replace(trading_symbol,'-EQ','')) instrument_id,listed_from,delisted_on from instruments where exchange='NSE'"
                    )
                )
                .mappings()
                .all()
            }

    def member(self, instrument, day):
        listed, delisted = self.listings[instrument]
        return (
            listed is not None
            and listed <= day
            and (delisted is None or day < delisted)
        )

    def _actions(self):
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text(
                        """select i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) instrument_id,
            c.action_type,c.ex_date,c.ratio_from,c.ratio_to,c.cash_amount,c.announced_at,c.source
            from corporate_actions c join instruments i on i.id=c.instrument_id
            where c.ex_date between :s and :e order by c.ex_date,c.id"""
                    ),
                    {"s": self.start, "e": self.end},
                )
                .mappings()
                .all()
            )
        result = {}
        for row in rows:
            action = CorporateAction(**dict(row))
            key = action.instrument_id, action.action_type.upper(), action.ex_date
            if key in result and result[key] != action:
                raise ResearchDataGap("conflicting_corporate_actions")
            result[key] = action
        return tuple(result.values())

    def bars(self, day):
        self.b.candle_cache.clear()  # Bound memory to one simulated day.
        output = []
        for instrument in self.symbols:
            if not self.member(instrument, day):
                position = self.broker.positions.get(instrument)
                if position and position.remaining_shares:
                    raise ResearchDataGap("held_delisting_exit_policy_unavailable")
                continue
            candles = self.b._candles(instrument, day)
            if not candles or candles[-1].timestamp.date() != day:
                held = self.broker.positions.get(instrument)
                if held and held.remaining_shares:
                    raise ResearchDataGap(
                        "held_stock_missing_session_or_delisting_exit_policy"
                    )
                continue
            frame = self.b.features(candles).data
            if len(frame) < 22:
                continue  # New listings cannot signal until one year; no fake EMA.
            row, previous = frame.iloc[-1], frame.iloc[-2]
            output.append(
                MarketBar(
                    instrument,
                    day,
                    *[float(row[k]) for k in ("open", "high", "low", "close", "ema9")],
                    float(previous.ema9),
                    float(previous.ema21),
                )
            )
        return output

    def on_open(self, day, broker, bars):
        seen = set()
        for action in self.actions:
            if action.ex_date != day:
                continue
            position = broker.positions.get(action.instrument_id)
            if not position or position.state in {
                TradeState.CLOSED,
                TradeState.CANCELLED,
            }:
                continue
            if not action.announced_at or action.announced_at > datetime.combine(
                day, time(9, 15), tzinfo=IST
            ):
                raise ResearchDataGap("corporate_action_not_knowable_at_open")
            kind = action.action_type.upper()
            if kind in {"SPLIT", "BONUS"}:
                if action.instrument_id in seen:
                    raise ResearchDataGap(
                        "multiple_same_day_capital_actions_require_review"
                    )
                seen.add(action.instrument_id)
                factor = float(split_adjustment_factor(action))
                for field in ("shares", "remaining_shares"):
                    qty = getattr(position, field) / factor
                    if abs(qty - round(qty)) > 1e-6:
                        raise ResearchDataGap("fractional_corporate_action_entitlement")
                    setattr(position, field, int(round(qty)))
                for field in (
                    "entry",
                    "stop",
                    "signal_high",
                    "signal_low",
                    "reentry_signal_high",
                    "reentry_signal_low",
                ):
                    value = getattr(position, field)
                    if value is not None:
                        setattr(position, field, value * factor)
                if action.instrument_id in broker.last_prices:
                    broker.last_prices[action.instrument_id] *= factor
                broker.events.append(
                    BacktestEvent(
                        EventType.CORPORATE_ACTION,
                        day,
                        action.instrument_id,
                        {"price_factor": factor, "kind": kind},
                    )
                )
            elif kind == "DIVIDEND":
                if (
                    action.cash_amount is None
                    or not math.isfinite(float(action.cash_amount))
                    or action.cash_amount < 0
                ):
                    raise ResearchDataGap("invalid_dividend")
                income = position.remaining_shares * float(action.cash_amount)
                broker.cash += income
                broker.realized_gross_pnl += income
                position.realized_pnl += income
                broker.events.append(
                    BacktestEvent(
                        EventType.CORPORATE_ACTION,
                        day,
                        action.instrument_id,
                        {"cash_income": income, "kind": kind},
                    )
                )
            else:
                raise ResearchDataGap("unsupported_held_corporate_action")

    def nav(self):
        b = self.broker
        return b.cash + sum(
            p.remaining_shares * b.last_prices.get(i, p.entry or 0)
            for i, p in b.positions.items()
        )

    def allocation_state(self, sectors):
        counts, held, risk, reserved, pending, opened = Counter(), set(), 0.0, 0.0, 0, 0
        nav = self.nav()
        blockers = []
        for instrument, p in self.broker.positions.items():
            if p.state in {TradeState.CLOSED, TradeState.CANCELLED}:
                continue
            info = self.last_analysis.get(instrument)
            if not info:
                blockers.append("matrix_position_sector_unavailable")
                continue
            counts[info["sector"]] += 1
            held.add(info["instrument_pk"])
            if p.state == TradeState.PENDING_ENTRY:
                pending += 1
                size = size_position(
                    nav,
                    p.signal_high,
                    p.signal_low,
                    **{
                        k: self.cfg["risk"][k]
                        for k in (
                            "risk_per_trade_pct",
                            "max_position_pct",
                            "raw_stop_buffer_pct",
                            "max_stop_risk_pct",
                        )
                    },
                )
                admission = self.admissions.get((instrument, p.signal_date))
                shares = min(size.shares, admission["shares"]) if admission else size.shares
                risk += shares * (p.signal_high - size.stop)
                reserved += shares * p.signal_high + info.get("estimated_entry_costs", 0)
            else:
                opened += 1
                risk += max(0, (p.entry - p.stop) * p.remaining_shares)
        return (
            AllocationState(
                opened,
                pending,
                risk / nav * 100,
                max(0, self.broker.cash - reserved),
                dict(counts),
                nav,
                tuple(blockers),
            ),
            held,
        )

    def on_close(self, day, broker):
        if self.case.variant.use_module_e:
            _, decision = self.e.evaluate(day, persist=False)
            if not decision.data_ready:
                raise ResearchDataGap("matrix_regime_inputs_incomplete")
            self.capacity = decision.max_open_trades
            self.regimes.append(
                {
                    "date": day,
                    "band": decision.band,
                    "capacity": decision.max_open_trades,
                    "score": decision.score,
                }
            )

    def eligibility(self, instrument, day, *, at_open=True):
        if not self.case.variant.use_module_a:
            return True
        stamp = (
            datetime.combine(day, time(9, 15), tzinfo=IST)
            if at_open
            else end_of_day(day)
        )
        _, decision = self.a.evaluate(instrument, stamp)
        return decision.eligible

    def signals(self, day):
        if self.case.variant.use_module_c:
            reentries = {}
            if self.case.variant.reentry:
                for instrument, p in self.broker.positions.items():
                    if reentry_allowed(p):
                        reentries[instrument] = {
                            **self.metadata.get(instrument, {}),
                            "setup": "SETUP_1",
                            "status": "ACTIVE",
                            "signal_date": p.reentry_signal_date.isoformat(),
                            "signal_high": p.reentry_signal_high,
                            "signal_low": p.reentry_signal_low,
                        }
            priority = (
                (lambda c: int(fingerprint([self.case.seed, day, c.instrument_id]), 16))
                if self.case.seed is not None
                else None
            )
            report = self.c.generate(
                day,
                persist=False,
                reentry_signals=reentries,
                selection_priority=priority,
            )
            if report.blockers:
                raise ResearchDataGap("matrix_ranking_or_account_inputs_incomplete")
            self.rejections.update(report.rejection_counts)
            self.rejections.update(
                "A:" + reason
                for row in report.results
                for reason in row.get("module_a_reasons", [])
            )
            self.decisions.append(
                {
                    "date": day,
                    "universe": report.universe_size,
                    "qualified": report.qualified,
                    "selected": report.selected,
                }
            )
            for row in report.results:
                if row.get("sector"):
                    self.last_analysis[row["instrument_id"]] = row
            for row in report.order_sheet:
                info = row["signal"]
                self.metadata[row["instrument_id"]] = info
                self.admissions[(row["instrument_id"], date.fromisoformat(info["signal_date"]))] = {
                    "day": day, "shares": int(row["shares"]),
                }
                yield Signal(
                    row["instrument_id"],
                    date.fromisoformat(info["signal_date"]),
                    float(info["signal_high"]),
                    float(info["signal_low"]),
                    float(row["score"]),
                    (float(row["buy_stop"]) - float(row["stop"])) / float(row["buy_stop"]) * 100,
                    float(row["stats"]["adtv"]),
                    row["instrument_id"] in reentries,
                )
        else:
            for instrument in self.symbols:
                if not self.member(instrument, day):
                    continue
                evaluator = self.setup2 if self.case.variant.setup_2 else self.b
                outcome = evaluator.evaluate(
                    instrument, day, require_module_a=self.case.variant.use_module_a
                )
                self.rejections.update(outcome.rejected_reasons)
                if outcome.eligible_universe and outcome.signal:
                    info = outcome.signal
                    yield Signal(
                        instrument,
                        date.fromisoformat(str(info["signal_date"])),
                        float(info["signal_high"]),
                        float(info["signal_low"]),
                    )

    def pending_rank(self, position, day):
        info = self.last_analysis.get(position.instrument_id, {})
        if (
            info.get("stats") and info.get("score") is not None
            and info.get("buy_stop") and info.get("stop") is not None
        ):
            return (
                float(info["score"]),
                (float(info["buy_stop"]) - float(info["stop"])) / float(info["buy_stop"]) * 100,
                float(info["stats"]["adtv"]),
            )
        return None

    def entry_timing_guard(self, position, bar):
        if (not self.case.variant.use_module_a
                or bar.trade_date <= position.signal_date
                or (position.entry_sessions and bar.trade_date not in position.entry_sessions)
                or not intraday_trigger(bar.open, bar.high, position.signal_high)):
            return
        with self.engine.connect() as conn:
            instrument_pk = self.b.prices._db_instrument_id(conn, position.instrument_id)
            if intraday_information_changed(conn, instrument_pk, bar.trade_date):
                raise ResearchDataGap("intraday_filing_trigger_order_unknown")

    def entry_guard(self, position, bar, quantity):
        if not self.case.variant.use_module_c:
            return quantity
        admission = self.admissions.get((position.instrument_id, position.signal_date))
        if not admission or admission["day"] >= bar.trade_date:
            return 0
        quantity = min(quantity, admission["shares"])
        info = self.last_analysis.get(position.instrument_id)
        if (
            not info
            or not info.get("stats")
            or float(info.get("score", 0)) < float(self.cfg["ranking"]["minimum_score"])
        ):
            return 0
        window = self.calendar.forward(
            bar.trade_date,
            int(self.cfg["technical"]["earnings_blackout_trading_days"]) + 1,
        )
        if len(window) != int(
            self.cfg["technical"]["earnings_blackout_trading_days"]
        ) + 1 or self.b.reference.earnings_within_trading_window(
            position.instrument_id,
            bar.trade_date,
            window,
            as_of=datetime.combine(bar.trade_date, time(9, 15), tzinfo=IST),
        ):
            return 0
        risk, sector_count = 0.0, 0
        for instrument, p in self.broker.positions.items():
            if instrument == position.instrument_id or p.state in {
                TradeState.CLOSED,
                TradeState.CANCELLED,
            }:
                continue
            other = self.last_analysis.get(instrument)
            if not other:
                return 0
            sector_count += (
                other["sector"].strip().casefold() == info["sector"].strip().casefold()
            )
            risk += (
                max(0, (p.entry - p.stop) * p.remaining_shares)
                if p.remaining_shares
                else float(other.get("rupee_risk", 0))
            )
        if sector_count >= self.cfg["risk"]["max_positions_per_sector"]:
            return 0
        budget = max(
            0,
            position.capital * self.cfg["risk"]["max_portfolio_heat_pct"] / 100 - risk,
        )
        by_heat = int(budget / (position.entry - position.stop))
        by_liquidity = int(float(info["stats"]["adtv"]) * 0.02 / position.entry)
        return max(0, min(quantity, by_heat, by_liquidity))

    def run(self):
        if self.case.variant.use_module_e:
            # Independent warmup; no trade or live PAPER feedback is imported.
            for day in sorted(d for d in self.calendar.sessions if d < self.start)[-5:]:
                self.on_close(day, self.broker)
        result = self.broker.run(
            [],
            self.signals,
            eligibility_provider=lambda i, d: self.eligibility(i, d),
            signal_eligibility_provider=lambda i, d: self.eligibility(
                i, d, at_open=False
            ),
            capacity_provider=lambda d: self.capacity,
            close_capacity_provider=lambda d: self.capacity,
            allow_reentry=self.case.variant.reentry,
            trading_sessions=sorted(self.calendar.sessions),
            bar_provider=self.bars,
            session_bounds=(self.start, self.end),
            on_session_open=self.on_open,
            on_session_close=self.on_close,
            entry_guard=self.entry_guard,
            allow_live_signals=True,
            auto_reentry=not self.case.variant.use_module_c,
            pending_rank_provider=self.pending_rank,
            entry_timing_guard=self.entry_timing_guard,
        )
        return result, dict(
            rejections=dict(self.rejections),
            regime=[r for r in self.regimes if r["date"] >= self.start],
            regime_summary=dict(
                Counter(
                    str(r["capacity"]) for r in self.regimes if r["date"] >= self.start
                )
            ),
            allocation=self.decisions,
            execution_policy="ranked pending cancellation; fresh C re-entry after exit; ambiguous intraday filings block replay",
            replay="isolated A/B/C/E and simulated account; no stored selected lists or live holdings",
            dividend_assumption="credited on ex-date, before personal taxes; payment-date reinvestment timing requires separate audit",
            limitations=[
                "Point-in-time source vintages and missing delisted securities require independent data integrity audit.",
                "Daily OHLC cannot establish intraday event ordering; same-day stops are conservative.",
            ],
        )
