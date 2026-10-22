from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta
import json
from types import SimpleNamespace

import pytest

from packages.pms.backtest import (
    BacktestEvent,
    BacktestResult,
    EventDrivenBacktester,
    EventType,
    Fill,
    MarketBar,
    Signal,
)
from packages.pms.backtest_matrix import (
    Cell,
    Case,
    MatrixRunner,
    Period,
    core_cases,
    matrix_plan,
    metrics,
    round_trips,
)
from packages.pms.config import load_strategy_config
from packages.pms.costs import CostBreakdown
from packages.pms.matrix_replay import (
    MatrixReplay,
    ResearchDataGap,
    ResearchE,
    ResearchB,
    HistoricalA,
)
from packages.pms.module_c import (
    AllocationState,
    Candidate,
    RankingInputs,
    allocate_candidates,
)
from packages.pms.research import ResearchVariant
from packages.pms.trade_engine import TradePosition, TradeState, activate_entry

DAY = date(2020, 1, 2)
PERIOD = Period("TEST", DAY, DAY + timedelta(days=3))
CFG = load_strategy_config()


def costs(total=0):
    return CostBreakdown(0, 0, 0, 0, 0, 0, 0, 0, 0, total)


def result(fills=(), events=(), curve=None):
    curve = (
        curve if curve is not None else [(DAY, 10000), (DAY + timedelta(days=1), 10000)]
    )
    return BacktestResult(
        10000,
        10000,
        0,
        sum(f.costs.total for f in fills),
        0,
        0,
        list(fills),
        list(events),
        0,
        curve,
        curve[-1][1] if curve else 10000,
        0,
    )


def completed_result():
    fills = [
        Fill("NSE:TEST", DAY, "BUY", 100, 10, "entry", costs(10)),
        Fill("NSE:TEST", DAY + timedelta(days=1), "SELL", 110, 5, "partial", costs(4)),
        Fill("NSE:TEST", DAY + timedelta(days=2), "SELL", 120, 5, "exit", costs(6)),
    ]
    event = BacktestEvent(EventType.ENTRY_FILL, DAY, "NSE:TEST", {"initial_risk": 100})
    return result(
        fills,
        [event],
        [
            (DAY, 9990),
            (DAY + timedelta(days=1), 10036),
            (DAY + timedelta(days=2), 10130),
        ],
    )


def test_predeclared_design_covers_rules_without_holdout_or_duplicate_cells():
    before = deepcopy(CFG)
    cells = matrix_plan(CFG)
    assert len(cells) == 128
    assert len({c.key for c in cells}) == len(cells)
    assert max(c.period.end for c in cells) == date(2024, 12, 31)
    assert {c.case.family for c in cells} == {"CORE", "ADR", "EXIT", "RANDOM", "COST"}
    assert {c.case.variant.reentry for c in cells if c.case.variant.setup_2} == {
        True,
        False,
    }
    assert len(matrix_plan(CFG, quick=True)) == 110
    assert CFG == before


def test_research_config_isolated_and_friction_does_not_change_statutory_fees():
    cells = matrix_plan(CFG)
    case = next(c.case for c in cells if c.case.family == "COST")
    cfg = case.config(CFG)
    assert cfg["backtest"]["impact_bps"] == 2 * CFG["backtest"]["impact_bps"]
    assert cfg["backtest"]["delivery_stt_pct"] == CFG["backtest"]["delivery_stt_pct"]
    cfg["risk"]["risk_per_trade_pct"] = 3
    assert CFG["risk"]["risk_per_trade_pct"] == 0.5


@pytest.mark.parametrize(
    "start,end", [(date(2025, 1, 1), date(2025, 1, 2)), (DAY, DAY - timedelta(days=1))]
)
def test_holdout_and_reversed_periods_rejected(start, end):
    with pytest.raises(ValueError):
        Period("X", start, end)


def test_partials_count_as_one_closed_cycle_and_allocate_entry_cost_once():
    trades, active = round_trips(completed_result())
    assert len(trades) == 1 and not active
    assert trades[0]["pnl"] == 130
    assert trades[0]["r_multiple"] == 1.3
    m = metrics(completed_result(), PERIOD)
    assert m["closed_trades"] == 1 and m["fills"] == 3
    assert m["profit_factor"] is None
    json.dumps(m, allow_nan=False)


def test_splits_and_dividends_do_not_manufacture_exit_lots():
    fills = [
        Fill("NSE:X", DAY, "BUY", 100, 10, "entry", costs(10)),
        Fill("NSE:X", DAY + timedelta(days=3), "SELL", 55, 20, "exit", costs(5)),
    ]
    events = [
        BacktestEvent(EventType.ENTRY_FILL, DAY, "NSE:X", {"initial_risk": 100}),
        BacktestEvent(
            EventType.CORPORATE_ACTION,
            DAY + timedelta(days=1),
            "NSE:X",
            {"price_factor": 0.5},
        ),
        BacktestEvent(
            EventType.CORPORATE_ACTION,
            DAY + timedelta(days=2),
            "NSE:X",
            {"cash_income": 20},
        ),
    ]
    trades, active = round_trips(result(fills, events))
    assert not active and trades[0]["pnl"] == 105


def test_open_positions_are_marked_and_not_fabricated_as_closed_trades():
    r = result(
        [Fill("NSE:X", DAY, "BUY", 100, 10, "entry", costs(1))],
        curve=[(DAY, 9999), (DAY + timedelta(days=1), 10099)],
    )
    m = metrics(r, PERIOD)
    assert m["closed_trades"] == 0 and m["open_positions"] == 1
    assert m["expectancy_r"] is None and m["win_rate_pct"] is None
    assert m["total_return_pct"] == pytest.approx(0.99)


def test_initial_capital_seeds_drawdown_and_ratios_use_daily_nav():
    m = metrics(result(curve=[(DAY, 9000), (DAY + timedelta(days=1), 9500)]), PERIOD)
    assert m["max_drawdown_pct"] == pytest.approx(10)
    assert m["sharpe"] is not None and m["ratio_basis"].startswith("daily NAV")


@pytest.mark.parametrize("curve", [[], [(DAY, float("nan"))], [(DAY, -1)], [(DAY, 0)]])
def test_invalid_or_empty_nav_never_becomes_zero_risk_success(curve):
    with pytest.raises(ValueError):
        metrics(result(curve=curve), PERIOD)


def test_sale_without_inventory_is_rejected():
    with pytest.raises(ValueError):
        round_trips(result([Fill("NSE:X", DAY, "SELL", 100, 1, "exit", costs())]))


def runner(
    tmp_path,
    *,
    gate=lambda c, cfg: [],
    execute=lambda c, cfg: (completed_result(), {}),
    dataset=None,
    cells=None,
):
    return MatrixRunner(
        CFG,
        cells or [Cell(PERIOD, core_cases()[0]), Cell(PERIOD, core_cases()[1])],
        dataset or {"fingerprint": "fixture"},
        tmp_path / "matrix.json",
        executor=execute,
        gate=gate,
        code_version="fixture-code",
    )


def test_resume_skips_only_verified_completed_cells(tmp_path):
    calls = []
    execute = lambda c, cfg: (calls.append(c.key) or completed_result(), {})
    first = runner(tmp_path, execute=execute).run(max_cells=1)
    assert first["summary"]["status"] == "INCOMPLETE"
    second = runner(tmp_path, execute=execute).run()
    assert len(calls) == 2 and second["summary"]["status"] == "COMPLETED"
    assert second["summary"]["acceptance"] == "NOT_CERTIFIED"


def test_blocked_cells_have_no_performance_and_retry_after_repair(tmp_path):
    def never(*args):
        pytest.fail("Blocked cell must not execute")

    report = runner(
        tmp_path, gate=lambda c, cfg: ["missing_prices"], execute=never
    ).run()
    assert all(r["status"] == "BLOCKED" and "metrics" not in r for r in report["cells"])
    report = runner(tmp_path).run()
    assert report["summary"]["status"] == "COMPLETED"


def test_no_trades_is_not_a_performance_pass(tmp_path):
    report = runner(tmp_path, execute=lambda c, cfg: (result(), {})).run()
    assert report["summary"]["counts"]["NO_TRADES"] == 2
    assert report["summary"]["performance_pass"] is None
    assert not report["cells"][0]["trade_frequency_sufficient"]


def test_changed_data_and_tampered_checkpoint_rejected(tmp_path):
    runner(tmp_path).run()
    with pytest.raises(ValueError, match="changed"):
        runner(tmp_path, dataset={"fingerprint": "changed"})
    path = tmp_path / "matrix.json"
    report = json.loads(path.read_text())
    report["cells"][0]["metrics"]["total_return_pct"] = 999
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="checksum"):
        runner(tmp_path)


def test_failed_executor_does_not_publish_exception_credentials(tmp_path):
    def fail(c, cfg):
        raise RuntimeError("postgresql://private:password@host")

    report = runner(tmp_path, execute=fail).run()
    assert report["cells"][0]["status"] == "FAILED"
    assert "password" not in json.dumps(report)


def test_runtime_data_gap_is_blocked_not_failed_or_zero_return(tmp_path):
    def fail(c, cfg):
        raise ResearchDataGap("missing_historical_breadth")

    report = runner(tmp_path, execute=fail).run()
    assert report["cells"][0]["status"] == "BLOCKED"
    assert report["cells"][0]["blockers"] == ["missing_historical_breadth"]


def test_interruption_leaves_running_cell_retriable(tmp_path):
    def interrupt(c, cfg):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        runner(tmp_path, execute=interrupt).run()
    assert (
        json.loads((tmp_path / "matrix.json").read_text())["cells"][0]["status"]
        == "RUNNING"
    )
    assert runner(tmp_path).run()["summary"]["status"] == "COMPLETED"


def test_seeded_random_selection_keeps_same_hard_filters():
    ranking = RankingInputs(100, 100, 100, 100, 100, 100)
    candidates = [
        Candidate(
            f"NSE:{s}",
            s,
            True,
            True,
            1,
            4,
            10000,
            10000000,
            False,
            100,
            5,
            ranking,
            500,
            0,
        )
        for s in ("X", "Y")
    ]
    state = AllocationState(0, 0, 0, 100000, {}, 100000)
    normal = allocate_candidates(candidates, state, 1, CFG)
    random = allocate_candidates(
        candidates,
        state,
        1,
        CFG,
        priority=lambda c: 0 if c.instrument_id.endswith("Y") else 1,
    )
    assert normal.selected[0].instrument_id == "NSE:X"
    assert random.selected[0].instrument_id == "NSE:Y"
    rejected = Candidate(
        "NSE:BAD",
        "X",
        False,
        True,
        1,
        4,
        10000,
        10000000,
        False,
        100,
        5,
        ranking,
        500,
        0,
    )
    assert not allocate_candidates(
        [rejected], state, 1, CFG, priority=lambda c: 0
    ).selected


def bars_and_signal():
    days = [DAY + timedelta(days=i) for i in range(4)]
    bars = [
        MarketBar("NSE:X", days[0], 100, 102, 99, 100, 99, 99, 99),
        MarketBar("NSE:X", days[1], 101, 104, 100, 103, 100, 99, 99),
        MarketBar("NSE:X", days[2], 103, 104, 90, 95, 100, 100, 99),
        MarketBar("NSE:X", days[3], 95, 98, 94, 96, 99, 100, 99),
    ]
    return days, bars, lambda d: [Signal("NSE:X", DAY, 101, 98)] if d == DAY else []


def test_lazy_session_replay_matches_eager_engine_and_never_requests_future_bar():
    days, bars, signals = bars_and_signal()
    eager = EventDrivenBacktester(initial_capital=100000).run(
        bars, signals, trading_sessions=days
    )
    calls = []

    def lazy(d):
        calls.append(d)
        return [b for b in bars if b.trade_date == d]

    replay = EventDrivenBacktester(initial_capital=100000).run(
        [],
        signals,
        trading_sessions=days,
        bar_provider=lazy,
        session_bounds=(days[0], days[-1]),
    )
    assert calls == days
    assert eager.fills == replay.fills and eager.equity_curve == replay.equity_curve
    assert replay.events[3].event_type in set(EventType)


def test_entry_gap_guard_can_resize_or_reject_without_cash_or_heat_violation():
    days, bars, signals = bars_and_signal()
    r = EventDrivenBacktester(initial_capital=100000).run(
        bars, signals, trading_sessions=days, entry_guard=lambda p, b, qty: min(qty, 3)
    )
    assert r.fills[0].quantity == 3
    assert (
        next(e for e in r.events if e.event_type == EventType.ENTRY_FILL).payload[
            "initial_risk"
        ]
        > 0
    )
    blocked = EventDrivenBacktester(initial_capital=100000).run(
        bars, signals, trading_sessions=days, entry_guard=lambda *a: 0
    )
    assert blocked.fills == []


def test_invalid_guard_quantity_or_lazy_bar_rejected():
    days, bars, signals = bars_and_signal()
    with pytest.raises(ValueError):
        EventDrivenBacktester().run(bars, signals, entry_guard=lambda *a: -1)
    with pytest.raises(ValueError):
        EventDrivenBacktester().run(
            [],
            signals,
            trading_sessions=days,
            bar_provider=lambda d: [bars[-1]],
            session_bounds=(days[0], days[-1]),
        )


def test_live_signals_admitted_only_during_remaining_window():
    days, bars, _ = bars_and_signal()
    signal = lambda d: [Signal("NSE:X", DAY, 101, 98)] if d == days[1] else []
    r = EventDrivenBacktester().run(
        bars, signal, trading_sessions=days, allow_live_signals=True
    )
    assert r.fills[0].trade_date >= days[2]
    with pytest.raises(ValueError):
        EventDrivenBacktester().run(
            bars,
            lambda d: (
                [Signal("NSE:X", DAY + timedelta(days=1), 101, 98)] if d == DAY else []
            ),
            allow_live_signals=True,
        )


def test_e_feedback_uses_only_this_simulated_account_and_net_costs():
    e = object.__new__(ResearchE)
    e.replay = SimpleNamespace(
        broker=SimpleNamespace(
            fills=completed_result().fills, events=completed_result().events
        )
    )
    e.engine = SimpleNamespace(connect=lambda: pytest.fail("No live account query"))
    feedback = e._strategy_feedback(None)
    assert feedback["closed_trades"] == 1 and feedback["expectancy_r"] is None


def test_adr_window_50_uses_fifty_observations_without_mutating_base_features():
    import pandas as pd

    b = object.__new__(ResearchB)
    b.cfg = deepcopy(CFG)
    b.cfg["backtest"]["research_adr_window"] = 50
    from packages.data_layer.models import Candle
    from datetime import datetime, timezone
    from decimal import Decimal

    candles = [
        Candle(
            "NSE:X",
            datetime(2020, 1, 1, tzinfo=timezone.utc) + timedelta(days=i),
            Decimal(100),
            Decimal(104 if i < 30 else 110),
            Decimal(100),
            Decimal(102),
            100000,
            "NSE",
            turnover=Decimal(10000000),
        )
        for i in range(50)
    ]
    frame = b.features(candles).data
    assert frame.iloc[-1].adr20_pct == pytest.approx(6.4)
    assert frame.iloc[-2].adr20_pct != frame.iloc[-2].adr20_pct


def test_corporate_action_requires_exact_entitlement_and_preserves_value():
    from packages.data_layer.reference_data import CorporateAction
    from datetime import datetime

    replay = object.__new__(MatrixReplay)
    p = TradePosition(
        "NSE:X",
        DAY,
        100,
        95,
        100000,
        state=TradeState.FILLED,
        entry=100,
        stop=95,
        shares=10,
        remaining_shares=10,
        filled_on=DAY,
    )
    broker = SimpleNamespace(
        positions={"NSE:X": p},
        last_prices={"NSE:X": 100},
        events=[],
        cash=1000,
        realized_gross_pnl=0,
    )
    replay.actions = [
        CorporateAction(
            "NSE:X",
            "SPLIT",
            DAY,
            1,
            2,
            announced_at=datetime.combine(
                DAY,
                time_for_test(),
                tzinfo=__import__("zoneinfo").ZoneInfo("Asia/Kolkata"),
            ),
        )
    ]
    replay.on_open(DAY, broker, {})
    assert (
        p.remaining_shares == 20
        and p.stop == 47.5
        and broker.last_prices["NSE:X"] * p.remaining_shares == 1000
    )
    p.shares = p.remaining_shares = 1
    replay.actions = [
        CorporateAction(
            "NSE:X", "SPLIT", DAY, 2, 1, announced_at=replay.actions[0].announced_at
        )
    ]
    with pytest.raises(ResearchDataGap, match="fractional"):
        replay.on_open(DAY, broker, {})


def time_for_test():
    from datetime import time

    return time(8)


def test_historical_b_does_not_import_production_signal_cancellations():
    b = object.__new__(ResearchB)
    b.engine = SimpleNamespace(
        connect=lambda: pytest.fail("No production signal-state query")
    )
    assert b._signal_cancelled("NSE:X", {"signal_date": "2020-01-02"}) is False


def test_always_armed_research_exit_is_behaviorally_distinct_from_default():
    days = [DAY + timedelta(days=i) for i in range(3)]
    bars = [
        MarketBar("NSE:X", days[0], 100, 101, 99, 100, 102, 102, 102),
        MarketBar("NSE:X", days[1], 101, 102, 99, 100, 102, 102, 102),
        MarketBar("NSE:X", days[2], 100, 102, 99, 101, 102, 102, 102),
    ]
    signals = lambda d: [Signal("NSE:X", DAY, 101, 98)] if d == DAY else []
    normal = EventDrivenBacktester(initial_capital=100000).run(
        bars, signals, trading_sessions=days
    )
    cfg = deepcopy(CFG)
    cfg["backtest"]["research_leg1_always_armed"] = True
    always = EventDrivenBacktester(initial_capital=100000, cfg=cfg).run(
        bars, signals, trading_sessions=days
    )
    assert len(normal.fills) == 1 and len(always.fills) == 2
    assert (
        always.fills[-1].reason == "leg1_full_exit"
        and always.fills[-1].trade_date == days[2]
    )


def test_upper_adr_cap_filters_real_research_evaluation_path(monkeypatch):
    import pandas as pd
    from packages.pms.module_b_pipeline import ModuleBPipeline, ModuleBResult

    b = object.__new__(ResearchB)
    b.cfg = deepcopy(CFG)
    b.cfg["backtest"]["research_adr_upper_cap"] = 8
    monkeypatch.setattr(
        ModuleBPipeline,
        "evaluate",
        lambda *a, **kw: ModuleBResult("NSE:X", DAY, True, (), None),
    )
    b._candles = lambda *a: []
    b.features = lambda *a: SimpleNamespace(data=pd.DataFrame({"adr20_pct": [9]}))
    assert b.evaluate("NSE:X", DAY).rejected_reasons == ("research_adr_upper_cap",)
    b.cfg["backtest"]["research_adr_upper_cap"] = None
    assert b.evaluate("NSE:X", DAY).eligible_universe


@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_completed_cell_requires_unchanged_full_trade_journal(tmp_path, damage):
    import hashlib

    path = tmp_path / "cell.json"
    payload = b'{"fixture":"trade journal"}'
    path.write_bytes(payload)

    def execute(c, cfg):
        return completed_result(), dict(
            evidence_file="cell.json",
            evidence_sha256=hashlib.sha256(payload).hexdigest(),
        )

    runner(tmp_path, execute=execute).run()
    if damage == "missing":
        path.unlink()
    else:
        path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="trade journal"):
        runner(tmp_path)
