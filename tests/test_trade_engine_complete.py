from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from packages.pms.config import load_strategy_config
from packages.pms.risk import compute_stop, size_position
from packages.pms.trade_engine import (
    TradePosition,
    TradeState,
    activate_entry,
    cancel_if_fundamental_ineligible,
    process_entry_bar,
    process_next_open,
    process_position_bar,
    create_reentry,
    reentry_allowed,
    position_from_module_b,
    process_trade_session,
)
from packages.pms.market_calendar import MarketCalendar
from packages.pms.module_b_pipeline import ModuleBResult
from packages.pms.backtest import EventDrivenBacktester, MarketBar, Signal

CFG = load_strategy_config()
D = date(2026, 10, 16)
SESSIONS = (date(2026, 10, 19), date(2026, 10, 21), date(2026, 10, 22))


def pending(**kw):
    p = TradePosition("NSE:ABC", D, 100, 95, 1_000_000, **kw)
    return activate_entry(p)


def filled():
    p = pending(entry_sessions=SESSIONS)
    process_entry_bar(p, SESSIONS[0], 100, 103, CFG)
    return p


def bar(
    p, d, *, open_=103, high=106, low=101, close=104, ema9=102, prev9=102, prev21=99
):
    return process_position_bar(p, d, open_, high, low, close, ema9, prev9, prev21)


def leg2():
    p = filled()
    bar(p, SESSIONS[0])
    bar(p, SESSIONS[1], close=102, prev9=103)
    process_next_open(p, SESSIONS[2], 105)
    assert p.state == TradeState.LEG2_ACTIVE
    return p


def test_calendar_window_can_fill_on_third_session_despite_missing_bars():
    p = pending(entry_sessions=SESSIONS)
    process_entry_bar(p, SESSIONS[2], 102, 105, CFG)
    assert p.state == TradeState.FILLED and p.entry_bars_seen == 3 and p.entry == 102


def test_expired_calendar_window_cannot_fill_when_only_one_bar_was_seen():
    p = pending(entry_sessions=SESSIONS)
    process_entry_bar(p, SESSIONS[-1] + timedelta(days=1), 110, 120, CFG)
    assert p.state == TradeState.CANCELLED and p.exit_reason == "entry_window_expired"


def test_signal_day_and_holiday_do_not_consume_window():
    p = pending(entry_sessions=SESSIONS)
    for d in (D, date(2026, 10, 20)):
        process_entry_bar(p, d, 100, 110, CFG)
    assert p.state == TradeState.PENDING_ENTRY and p.entry_bars_seen == 0


def test_duplicate_pending_bar_is_idempotent_and_conflicting_replay_rejected():
    p = pending()
    process_entry_bar(p, SESSIONS[0], 98, 99, CFG)
    checkpoint = p.checkpoint()
    process_entry_bar(p, SESSIONS[0], 98, 99, CFG)
    assert p.checkpoint() == checkpoint
    with pytest.raises(ValueError, match="Conflicting replay"):
        process_entry_bar(p, SESSIONS[0], 98, 99.5, CFG)


def test_no_future_or_out_of_order_fill_processing():
    p = filled()
    bar(p, SESSIONS[1])
    with pytest.raises(ValueError, match="Out-of-order"):
        bar(p, SESSIONS[0])


def test_pending_entry_loses_a_membership_before_trigger():
    p = pending()
    process_entry_bar(p, SESSIONS[0], 110, 120, CFG, fundamental_eligible=False)
    assert p.state == TradeState.CANCELLED and p.shares == 0 and p.entry is None


def test_fundamental_loss_does_not_force_exit_of_filled_trade():
    p = filled()
    cancel_if_fundamental_ineligible(p)
    assert p.state == TradeState.FILLED and p.remaining_shares > 0


def test_gap_fill_recalculates_stop_risk_and_cap():
    p = pending()
    process_entry_bar(p, SESSIONS[0], 120, 125, CFG)
    assert p.entry == 120 and p.stop == pytest.approx(120 * 0.945)
    assert p.shares * (p.entry - p.stop) <= 5000 and p.shares * p.entry <= 100000


def test_untouched_buy_stop_does_not_fill():
    p = pending()
    process_entry_bar(p, SESSIONS[0], 98, 99.99, CFG)
    assert p.entry is None
    process_entry_bar(p, SESSIONS[1], 98, 100, CFG)
    assert p.entry == 100


@pytest.mark.parametrize("qty", [1, 3, 4])
def test_partial_sale_odd_lot_rounding_and_share_conservation(qty):
    p = filled()
    p.shares = p.remaining_shares = qty
    bar(p, SESSIONS[0])
    bar(p, SESSIONS[1], close=102, prev9=103)
    process_next_open(p, SESSIONS[2], 105)
    sold = sum(e.get("shares", 0) for e in p.history if e["event"] == "LEG1_EXECUTED")
    assert sold == (qty + 1) // 2
    assert sold + p.remaining_shares == qty
    assert p.realized_pnl == sold * 5
    assert p.state == (TradeState.CLOSED if qty == 1 else TradeState.LEG2_ACTIVE)


def test_entry_day_stop_uses_stop_not_pre_entry_open():
    p = pending()
    process_trade_session(
        p, SESSIONS[0], 90, 102, 89, 101, 100, 99, 98, CFG, fundamental_eligible=True
    )
    assert p.state == TradeState.CLOSED and p.history[-1]["price"] == 94.525
    assert not reentry_allowed(p)


def test_protective_stop_gap_exits_at_open():
    p = filled()
    bar(p, SESSIONS[1], open_=90, high=96, low=89, close=94)
    assert p.state == TradeState.CLOSED and p.history[-1]["price"] == 90
    assert p.realized_pnl == (90 - 100) * p.shares


def test_exact_stop_touch_exits_all():
    p = filled()
    bar(p, SESSIONS[0], low=p.stop)
    assert p.state == TradeState.CLOSED and p.remaining_shares == 0


def test_leg1_not_armed_until_strict_above_and_no_same_bar_ema_exit():
    p = filled()
    bar(p, SESSIONS[0], close=102, ema9=102, prev9=105)
    assert p.state == TradeState.FILLED
    bar(p, SESSIONS[1], close=103, ema9=102, prev9=104)
    assert p.state == TradeState.LEG1_ARMED


@pytest.mark.parametrize("close", [99, 100])
def test_leg1_loss_or_breakeven_schedules_full_exit_next_open(close):
    p = filled()
    bar(p, SESSIONS[0])
    bar(p, SESSIONS[1], low=98, close=close, prev9=103)
    assert p.state == TradeState.PENDING_LEG1_FULL_EXIT
    before = p.checkpoint()
    process_next_open(p, SESSIONS[1], 102)
    assert p.checkpoint() == before
    process_next_open(p, SESSIONS[2], 99)
    assert p.exit_reason == "leg1_full_exit" and not reentry_allowed(p)


def test_leg1_profit_signal_does_not_sell_until_next_open():
    p = filled()
    bar(p, SESSIONS[0])
    qty = p.remaining_shares
    bar(p, SESSIONS[1], close=102, prev9=103)
    assert p.state == TradeState.PENDING_LEG1_PARTIAL_EXIT and p.remaining_shares == qty
    process_next_open(p, SESSIONS[2], 105)
    assert p.stop == p.entry and 0 < p.remaining_shares < qty


def test_partial_exit_gap_below_entry_closes_remainder_without_reentry():
    p = filled()
    bar(p, SESSIONS[0])
    bar(p, SESSIONS[1], close=102, prev9=103)
    process_next_open(p, SESSIONS[2], 99)
    assert p.state == TradeState.CLOSED and p.exit_reason == "breakeven_gap"
    assert p.realized_pnl == -p.shares and not reentry_allowed(p)


def test_leg2_breakeven_stop_priority_over_ema_exit():
    p = leg2()
    bar(p, SESSIONS[2], low=100, close=101, prev21=104)
    assert p.exit_reason == "breakeven_stop" and not reentry_allowed(p)


def test_ema21_signal_reentry_uses_exit_candle_once_and_keeps_original_window():
    p = leg2()
    bar(p, SESSIONS[2], low=101, close=102, prev21=103)
    assert p.state == TradeState.PENDING_LEG2_EXIT
    exit_day = date(2026, 10, 23)
    process_next_open(p, exit_day, 103)
    assert reentry_allowed(p)
    r = create_reentry(
        p, 1_000_000, entry_sessions=(exit_day, date(2026, 10, 26), date(2026, 10, 27))
    )
    assert r.signal_date == SESSIONS[2] and r.signal_high == 106 and r.signal_low == 101
    assert not reentry_allowed(p)
    with pytest.raises(ValueError):
        create_reentry(p, 1_000_000)


def test_gap_through_leg2_stop_overrides_pending_ema_reentry():
    p = leg2()
    bar(p, SESSIONS[2], low=101, close=102, prev21=103)
    process_next_open(p, date(2026, 10, 23), 95)
    assert p.exit_reason == "breakeven_stop" and not reentry_allowed(p)


def test_reentry_requires_current_a_membership():
    p = leg2()
    bar(p, SESSIONS[2], low=101, close=102, prev21=103)
    process_next_open(p, date(2026, 10, 23), 103)
    with pytest.raises(ValueError, match="Module A"):
        create_reentry(p, 1_000_000, fundamental_eligible=False)
    assert reentry_allowed(p)


def test_json_checkpoint_roundtrip_keeps_event_replay_idempotent():
    p = filled()
    bar(p, SESSIONS[0])
    q = TradePosition.restore(json.loads(json.dumps(p.checkpoint(), allow_nan=False)))
    assert q.checkpoint() == p.checkpoint()
    bar(q, SESSIONS[0])
    assert q.checkpoint() == p.checkpoint()


@pytest.mark.parametrize(
    "changes",
    [
        {"capital": float("nan")},
        {"capital": 0},
        {"signal_high": 90},
        {"signal_low": float("inf")},
        {"shares": -1},
    ],
)
def test_invalid_position_data_rejected(changes):
    values = dict(
        instrument_id="NSE:A",
        signal_date=D,
        signal_high=100,
        signal_low=95,
        capital=1e6,
    )
    with pytest.raises(ValueError):
        TradePosition(**{**values, **changes})


@pytest.mark.parametrize(
    "changes",
    [
        {"entry": float("inf")},
        {"risk_per_trade_pct": -1},
        {"max_position_pct": float("nan")},
        {"max_stop_risk_pct": 100},
    ],
)
def test_invalid_risk_inputs_rejected(changes):
    values = dict(
        capital=1e6,
        entry=100,
        signal_low=95,
        risk_per_trade_pct=0.5,
        max_position_pct=10,
        raw_stop_buffer_pct=0.5,
        max_stop_risk_pct=5.5,
    )
    with pytest.raises(ValueError):
        size_position(**{**values, **changes})


def test_canonical_session_cancels_at_third_close_and_replay_is_harmless():
    p = pending(entry_sessions=SESSIONS)
    for d in SESSIONS:
        process_trade_session(
            p, d, 98, 99, 97, 98, 99, 99, 99, CFG, fundamental_eligible=True
        )
    assert p.state == TradeState.CANCELLED and p.exit_reason == "entry_window_expired"
    before = p.checkpoint()
    process_trade_session(
        p, SESSIONS[-1], 98, 99, 97, 98, 99, 99, 99, CFG, fundamental_eligible=True
    )
    assert p.checkpoint() == before


def test_invalid_bar_cannot_change_trade_state():
    p = filled()
    before = p.checkpoint()
    with pytest.raises(ValueError):
        bar(p, SESSIONS[0], low=107)
    assert p.checkpoint() == before


def b_result(**kw):
    signal = dict(
        signal_date=D.isoformat(),
        signal_high=100,
        signal_low=95,
        setup="SETUP_1",
        status="SIGNAL_CREATED",
    )
    return ModuleBResult("NSE:A", D, True, (), signal, **kw)


def test_module_b_adapter_uses_holiday_aware_sessions_and_requires_activation():
    calendar = MarketCalendar([D], {2026: {date(2026, 10, 20)}})
    p = position_from_module_b(b_result(), 1e6, calendar, CFG)
    assert p.state == TradeState.SIGNAL_CREATED and p.entry_sessions == SESSIONS


def test_module_b_adapter_rejects_research_expired_and_unknown_calendar():
    calendar = MarketCalendar([D], {2026: {date(2026, 10, 20)}})
    for result, c in [
        (b_result(research_only=True), calendar),
        (replace(b_result(), as_of=SESSIONS[-1]), calendar),
        (b_result(), MarketCalendar([D])),
        (
            replace(b_result(), signal={**b_result().signal, "setup": "SETUP_2"}),
            calendar,
        ),
    ]:
        with pytest.raises(ValueError):
            position_from_module_b(result, 1e6, c, CFG)


def market(d, **kw):
    values = dict(
        instrument_id="NSE:A",
        trade_date=d,
        open=100,
        high=102,
        low=96,
        close=101,
        ema9=100,
        previous_ema9=100,
        previous_ema21=99,
    )
    return MarketBar(**{**values, **kw})


def signals(d):
    return [Signal("NSE:A", D, 100, 95)] if d == D else []


def test_backtest_rechecks_eligibility_before_pending_fill():
    engine = EventDrivenBacktester(1e6)
    r = engine.run(
        [market(D), market(SESSIONS[0])],
        signals,
        eligibility_provider=lambda i, d: d == D,
    )
    assert (
        not r.fills and engine.positions["NSE:A"].exit_reason == "module_a_ineligible"
    )


def test_backtest_cash_and_share_ledger_same_day_stop():
    engine = EventDrivenBacktester(1e6)
    r = engine.run(
        [market(D), market(SESSIONS[0], open=90, high=102, low=89, close=101)], signals
    )
    buy, sell = r.fills
    assert (
        sell.price == 94.525 and buy.quantity == sell.quantity and r.closed_trades == 1
    )
    assert r.ending_cash == pytest.approx(
        1e6 + r.realized_gross_pnl - r.total_costs, abs=0.02
    )
    assert engine.positions["NSE:A"].remaining_shares == 0


def test_backtest_pending_expires_on_calendar_even_without_stock_bars():
    engine = EventDrivenBacktester(1e6)
    bars = [market(D)] + [market(d, instrument_id="NSE:OTHER") for d in SESSIONS]
    engine.run(bars, signals, trading_sessions=[D, *SESSIONS])
    assert engine.positions["NSE:A"].exit_reason == "entry_window_expired"


def test_backtest_capacity_zero_cancels_pending_before_trigger():
    engine = EventDrivenBacktester(1e6)
    r = engine.run(
        [market(D), market(SESSIONS[0])],
        signals,
        capacity_provider=lambda d: 7 if d == D else 0,
    )
    assert not r.fills and engine.positions["NSE:A"].exit_reason == "regime_capacity"


def test_backtest_counts_costs_in_affordable_quantity_and_never_ghost_holds():
    cfg = deepcopy(CFG)
    cfg["risk"]["max_position_pct"] = 100
    cfg["risk"]["risk_per_trade_pct"] = 100
    cfg["backtest"]["brokerage_flat_per_order"] = 100
    engine = EventDrivenBacktester(150, cfg)
    r = engine.run([market(D), market(SESSIONS[0])], signals)
    assert not r.fills and engine.positions["NSE:A"].remaining_shares == 0
    assert engine.positions["NSE:A"].entry is None and r.ending_cash == 150


def test_missing_bar_mark_uses_last_known_close_not_zero():
    engine = EventDrivenBacktester(1e6)
    bars = [
        market(D),
        market(SESSIONS[0]),
        market(SESSIONS[1], instrument_id="NSE:OTHER"),
    ]
    r = engine.run(bars, signals)
    assert r.equity_curve[-1][1] == r.equity_curve[-2][1]


def test_backtest_duplicate_conflicts_and_repeat_runs_rejected():
    with pytest.raises(ValueError):
        EventDrivenBacktester(1e6).run([market(D), market(D, close=100)], lambda d: [])
    engine = EventDrivenBacktester(1e6)
    engine.run([market(D)], lambda d: [])
    with pytest.raises(ValueError):
        engine.run([market(D)], lambda d: [])


def test_open_and_close_gates_are_distinct_without_future_knowledge():
    engine = EventDrivenBacktester(1e6)
    r = engine.run(
        [market(D), market(SESSIONS[0])],
        signals,
        eligibility_provider=lambda i, d: True,
        signal_eligibility_provider=lambda i, d: True,
        capacity_provider=lambda d: 0,
        close_capacity_provider=lambda d: 7,
    )
    assert not r.fills  # Close approval cannot override next-open defensive regime.


def test_restore_does_not_alias_original_checkpoint_lists():
    p = filled()
    payload = p.checkpoint()
    q = TradePosition.restore(payload)
    q.log("TEST")
    assert len(q.history) == len(payload["history"]) + 1


def test_explicit_eligibility_and_bad_candle_reject_before_mutation():
    p = pending(entry_sessions=SESSIONS)
    before = p.checkpoint()
    with pytest.raises(ValueError):
        process_trade_session(
            p,
            SESSIONS[0],
            100,
            103,
            96,
            102,
            101,
            100,
            99,
            CFG,
            fundamental_eligible="false",
        )
    assert p.checkpoint() == before


def test_demo_completes_gap_partial_exit_and_leg2_exit_without_orders():
    from scripts.replay_trade_engine import demo, replay

    result = replay(demo())
    assert result["broker_orders_submitted"] == 0 and result["reentry_allowed"]
    assert result["position"]["state"] == "CLOSED"
    assert result["position"]["realized_pnl"] > 0



def test_fractional_stop_rounding_does_not_widen_five_point_five_percent_cap():
    entry = 103.123
    stop = compute_stop(entry, 50, 0.5, 5.5)
    assert stop >= entry * 0.945
    result = size_position(1000000, entry, 50, 0.5, 10, 0.5, 5.5)
    assert result.shares * (entry - result.stop) <= 5000
    assert result.shares * entry <= 100000


def test_open_portfolio_return_uses_equity_and_exposes_unrealized_pnl():
    result = EventDrivenBacktester(1e6).run(
        [market(D), market(SESSIONS[0], close=102)], signals
    )
    assert result.ending_equity == result.equity_curve[-1][1]
    assert result.unrealized_pnl > 0
    assert result.ending_equity == pytest.approx(
        1e6 + result.realized_net_pnl + result.unrealized_pnl, abs=0.02
    )
    assert result.return_pct == round((result.ending_equity / 1e6 - 1) * 100, 4)


def test_zero_size_cancels_without_an_entry_or_holdings():
    p = TradePosition("NSE:A", D, 100, 95, 50)
    activate_entry(p)
    process_entry_bar(p, SESSIONS[0], 100, 102, CFG)
    assert (
        p.exit_reason == "position_size_zero"
        and p.remaining_shares == 0
        and p.entry is None
    )


def test_simulation_resumes_checkpoint_with_identical_final_history():
    from scripts.replay_trade_engine import demo, replay

    payload = demo()
    expected = replay(payload)
    first = replay({**payload, "bars": payload["bars"][:2]})
    resumed = replay(
        {**payload, "position": first["position"], "bars": payload["bars"][2:]}
    )
    assert resumed == expected
