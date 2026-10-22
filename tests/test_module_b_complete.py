from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from packages.data_layer.models import Candle
from packages.data_layer.reference_data import CorporateAction, ReferenceDataQueries
from packages.pms.config import load_strategy_config
from packages.pms.market_calendar import MarketCalendar
from packages.pms.module_a_context import IST
from packages.pms.module_b import Bar, RallyState, RallyTracker, advance_rally
from packages.pms.module_b_pipeline import ModuleBPipeline, ModuleBResult
from packages.pms.module_b_setup2 import EarningsAnchoredSetup2

CFG = load_strategy_config()
D = date(2026, 10, 16)


def step(tracker, day=1, high=115, close=110, volume=150, baseline=100, low=100):
    return advance_rally(
        tracker,
        Bar(
            (date(2026, 1, 1) + timedelta(days=day)).isoformat(),
            105,
            high,
            low,
            close,
            volume,
            105,
        ),
        100 if day == 1 else 110,
        105,
        baseline,
        CFG,
    )


def test_exact_thresholds_qualify_on_crossing_day():
    t = step(RallyTracker())
    assert t.state == RallyState.RALLY_VALID
    assert t.rally_pct() == pytest.approx(15)
    assert t.volume_ratio() == 1.5


def test_frozen_baseline_and_peak_duration_survive_long_flat_tail():
    t = step(RallyTracker())
    for day in range(2, 40):
        step(t, day, high=114, volume=10, baseline=10000)
    assert t.state == RallyState.RALLY_VALID
    assert t.trading_days == 39 and t.peak_trading_days == 1
    assert t.baseline_volume_50d == 100
    assert t.volume_ratio() == 1.5
    step(t, 40, high=114, close=104)
    assert t.state == RallyState.SIGNAL_CREATED


def test_later_peak_recomputes_volume_validity_instead_of_sticky_pass():
    t = step(RallyTracker())
    step(t, 2, high=116, volume=0, baseline=1)
    assert t.state == RallyState.RALLY_TRACKING
    step(t, 3, close=104)
    assert t.state == RallyState.BELOW_21EMA
    assert t.start_date is None and t.signal_date is None


def test_peak_after_thirty_sessions_invalidates_rally():
    t = step(RallyTracker())
    for day in range(2, 31):
        step(t, day, high=114)
    step(t, 31, high=150)
    assert t.state == RallyState.RALLY_INVALID
    step(t, 32, close=104)
    assert t.state == RallyState.BELOW_21EMA


def test_equal_ema_is_not_signal_and_first_strict_below_is_signal():
    t = step(RallyTracker())
    step(t, 2, close=105)
    assert t.state == RallyState.RALLY_VALID
    step(t, 3, close=104)
    assert t.signal_date == "2026-01-04"
    assert t.signal_high == 115 and t.signal_low == 100


@pytest.mark.parametrize("baseline", [0, -1, float("nan"), float("inf")])
def test_invalid_baseline_cannot_qualify(baseline):
    assert step(RallyTracker(), baseline=baseline).state == RallyState.RALLY_INVALID


@pytest.mark.parametrize(
    "kwargs",
    [{"volume": float("nan")}, {"close": float("inf")}, {"high": 104}, {"volume": -1}],
)
def test_invalid_rally_bars_rejected(kwargs):
    with pytest.raises(ValueError):
        step(RallyTracker(), **kwargs)


def test_duplicate_or_backward_bar_rejected():
    t = step(RallyTracker())
    with pytest.raises(ValueError):
        step(t)


def test_calendar_skips_holidays_and_weekends_and_supports_special_sessions():
    c = MarketCalendar(
        [date(2026, 10, 16)], {2026: {date(2026, 10, 20)}}, [date(2026, 10, 18)]
    )
    assert c.forward(date(2026, 10, 17), 3) == [
        date(2026, 10, 18),
        date(2026, 10, 19),
        date(2026, 10, 21),
    ]
    assert c.forward(date(2027, 1, 1), 1) == []


def test_calendar_actual_sessions_override_weekday_assumptions():
    c = MarketCalendar([date(2026, 10, 16), date(2026, 10, 21)], {2026: set()})
    assert c.forward(date(2026, 10, 19), 1) == [date(2026, 10, 21)]
    assert c.age(date(2026, 10, 16), date(2026, 10, 21)) == 1


@pytest.fixture
def pipeline(monkeypatch):
    p = ModuleBPipeline(None)
    sessions = list(pd.bdate_range(end=D, periods=270).date)
    calendar = MarketCalendar(sessions, {2026: {date(2026, 10, 20)}})
    monkeypatch.setattr(p, "_calendar", lambda _: calendar)
    monkeypatch.setattr(p, "_stamp", lambda _: datetime(2026, 10, 18, 18, tzinfo=IST))
    decision = SimpleNamespace(eligible=True, failed_rules=())
    monkeypatch.setattr(p.module_a, "evaluate", lambda *args: (None, decision))
    candles = [
        Candle(
            "NSE:ABC",
            datetime.combine(d, datetime.min.time()),
            Decimal("100"),
            Decimal("105"),
            Decimal("95"),
            Decimal("100"),
            200000,
            "test",
        )
        for d in sessions
    ]
    monkeypatch.setattr(p, "_candles", lambda *args: candles)
    monkeypatch.setattr(p.reference, "is_index_member", lambda *args: False)
    monkeypatch.setattr(
        p.reference, "earnings_within_trading_window", lambda *args, **kwargs: False
    )
    monkeypatch.setattr(p, "_signal_cancelled", lambda *args: False)
    return p


def test_complete_universe_can_pass_without_a_setup(pipeline):
    result = pipeline.evaluate("NSE:ABC", D)
    assert (
        result.eligible_universe and result.signal is None and not result.research_only
    )


def test_a_gate_runs_before_price_or_reference_queries(pipeline, monkeypatch):
    monkeypatch.setattr(
        pipeline.module_a,
        "evaluate",
        lambda *args: (
            None,
            SimpleNamespace(eligible=False, failed_rules=("leverage_policy_missing",)),
        ),
    )
    monkeypatch.setattr(
        pipeline, "_candles", lambda *args: pytest.fail("Must not read technical data")
    )
    result = pipeline.evaluate("NSE:ABC", D)
    assert result.rejected_reasons == ("module_a_ineligible",)
    assert result.module_a_reasons == ("leverage_policy_missing",)


def test_research_bypass_is_marked_and_cannot_persist(pipeline):
    result = pipeline.evaluate("NSE:ABC", D, require_module_a=False)
    assert result.research_only
    with pytest.raises(PermissionError):
        pipeline.persist_results([result], pipeline._stamp(D))


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("close", 30, "price"),
        ("avg_volume20", 100000, "avg_volume_20d"),
        ("worst_volume20", 75000, "worst_volume_20d"),
        ("avg_turnover20", 9999999, "turnover_low"),
        ("avg_turnover20", 5000000001, "turnover_high"),
        ("adr20_pct", 2.99, "adr"),
        ("high_52w", 201, "distance_52w_high"),
        ("adr20_pct", float("nan"), "indicators_unavailable"),
    ],
)
def test_universe_boundaries(pipeline, monkeypatch, field, value, reason):
    import packages.pms.module_b_pipeline as module

    original = module.compute_features

    def altered(candles):
        result = original(candles)
        result.data.loc[result.data.index[-1], field] = value
        return result

    monkeypatch.setattr(module, "compute_features", altered)
    assert reason in pipeline.evaluate("NSE:ABC", D).rejected_reasons


@pytest.mark.parametrize("turnover", [10000000, 5000000000])
def test_turnover_endpoints_are_inclusive(pipeline, monkeypatch, turnover):
    import packages.pms.module_b_pipeline as module

    original = module.compute_features

    def altered(candles):
        result = original(candles)
        result.data.loc[result.data.index[-1], "avg_turnover20"] = turnover
        result.data.loc[result.data.index[-1], "adr20_pct"] = 3
        result.data.loc[result.data.index[-1], "high_52w"] = 200
        return result

    monkeypatch.setattr(module, "compute_features", altered)
    assert pipeline.evaluate("NSE:ABC", D).eligible_universe


def test_missing_stock_session_and_stale_prices_block(pipeline, monkeypatch):
    candles = pipeline._candles("NSE:ABC", D)
    monkeypatch.setattr(pipeline, "_candles", lambda *args: candles[:-1])
    reasons = pipeline.evaluate("NSE:ABC", D).rejected_reasons
    assert (
        "price_history_stale" in reasons and "price_history_missing_sessions" in reasons
    )


def test_nifty_member_and_known_earnings_block(pipeline, monkeypatch):
    monkeypatch.setattr(pipeline.reference, "is_index_member", lambda *args: True)
    monkeypatch.setattr(
        pipeline.reference,
        "earnings_within_trading_window",
        lambda *args, **kwargs: True,
    )
    assert set(pipeline.evaluate("NSE:ABC", D).rejected_reasons) == {
        "nifty50_excluded",
        "earnings_blackout",
    }


def test_missing_future_market_calendar_fails_closed(pipeline, monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "_calendar",
        lambda _: MarketCalendar(
            [c.timestamp.date() for c in pipeline._candles("", D)]
        ),
    )
    assert (
        "earnings_calendar_market_dates_unavailable"
        in pipeline.evaluate("NSE:ABC", D).rejected_reasons
    )


def test_weekend_blackout_counts_next_three_sessions(pipeline, monkeypatch):
    captured = []
    monkeypatch.setattr(
        pipeline.reference,
        "earnings_within_trading_window",
        lambda *a, **kw: captured.extend(a[2]) or False,
    )
    pipeline.evaluate("NSE:ABC", date(2026, 10, 18))
    assert captured == [
        date(2026, 10, 18),
        date(2026, 10, 19),
        date(2026, 10, 21),
        date(2026, 10, 22),
    ]


def test_after_close_entry_window_includes_next_three_sessions(pipeline, monkeypatch):
    sessions = [
        date(2026, 10, 16),
        date(2026, 10, 19),
        date(2026, 10, 21),
        date(2026, 10, 22),
    ]
    monkeypatch.setattr(pipeline, "_calendar", lambda _: MarketCalendar(sessions))
    signal = {"signal_date": D.isoformat()}
    assert pipeline._live_window(signal, D)["days_left"] == 3
    assert pipeline._live_window(signal, D)["expires_on"] == "2026-10-22"
    assert pipeline._live_window(signal, sessions[2])["days_left"] == 1
    assert pipeline._live_window(signal, sessions[3]) is None
    assert pipeline._live_window(signal, D - timedelta(days=1)) is None


def setup_frame():
    frame = pd.DataFrame(
        {
            "open": 100.0,
            "high": 102.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 100.0,
            "ema21": 100.0,
        },
        index=pd.bdate_range("2026-01-01", periods=55),
    )
    frame.iloc[50] = [105, 115, 100, 110, 150, 105]
    frame.iloc[51] = [109, 114, 100, 110, 150, 105]
    frame.iloc[52] = [105, 114, 100, 104, 150, 105]
    return frame


def test_setup1_watchlist_signal_and_no_future_leakage(pipeline):
    frame = setup_frame()
    signal, watchlist = pipeline._setup1_state(frame, frame.index[51].date())
    assert signal is None and watchlist["state"] == "RALLY_VALID"
    signal, watchlist = pipeline._setup1_state(frame, frame.index[52].date())
    assert signal["signal_date"] == frame.index[52].date().isoformat()
    assert watchlist is None and signal["baseline_volume_50d"] == 100
    assert signal["rally_duration_sessions"] == 1


def test_calendar_cache_is_point_in_time(monkeypatch):
    p = ModuleBPipeline(None)
    seen = []
    monkeypatch.setattr(
        MarketCalendar,
        "from_database",
        lambda engine, stamp: seen.append(stamp.date()) or MarketCalendar(),
    )
    p._calendar(date(2026, 1, 1))
    p._calendar(date(2026, 1, 2))
    p._calendar(date(2026, 1, 1))
    assert seen == [date(2026, 1, 1), date(2026, 1, 2)]


def raw_pipeline(monkeypatch, rows, actions=()):
    p = ModuleBPipeline(None)
    monkeypatch.setattr(p, "_calendar", lambda _: MarketCalendar([D]))
    monkeypatch.setattr(p.prices, "daily_prices", lambda *a, **kw: rows)
    monkeypatch.setattr(
        p.reference, "corporate_actions_between", lambda *a: list(actions)
    )
    return p


def raw_row(**kw):
    return dict(
        trade_date=D,
        open=100,
        high=110,
        low=90,
        close=100,
        volume=100000,
        source="nse",
        **kw,
    )


@pytest.mark.parametrize("volume", [float("nan"), float("inf"), -1, 1.5])
def test_bad_volume_rejected_before_integer_conversion(monkeypatch, volume):
    row = raw_row()
    row["volume"] = volume
    with pytest.raises(ValueError):
        raw_pipeline(monkeypatch, [row])._candles("NSE:ABC", D)


def test_source_selection_is_deterministic_and_skips_non_sessions(monkeypatch):
    a = raw_row()
    b = {**a, "source": "yfinance", "close": 101}
    holiday = {**a, "trade_date": date(2026, 10, 17)}
    for rows in ([a, b, holiday], [b, holiday, a]):
        candles = raw_pipeline(monkeypatch, rows)._candles("NSE:ABC", D)
        assert len(candles) == 1 and candles[0].source == "nse"


def test_yahoo_with_actions_requires_verified_adjustment_basis(monkeypatch):
    row = {**raw_row(), "source": "yfinance"}
    action = CorporateAction("NSE:ABC", "SPLIT", D, 1, 2)
    p = raw_pipeline(monkeypatch, [row], [action])
    with pytest.raises(ValueError, match="adjustment_basis_unverified"):
        p._candles("NSE:ABC", D)
    p.cfg["technical"]["adjusted_price_sources"] = ["yfinance"]
    assert p._candles("NSE:ABC", D)[0].adjusted


def test_conflicting_corporate_actions_block(monkeypatch):
    actions = [CorporateAction("NSE:ABC", "SPLIT", D, 1, n) for n in [2, 3]]
    with pytest.raises(ValueError, match="conflicting_corporate_actions"):
        raw_pipeline(monkeypatch, [raw_row()], actions)._candles("NSE:ABC", D)


def test_web_api_cannot_bypass_a(monkeypatch):
    from app.main import app
    monkeypatch.delenv("PMS_DATABASE_URL", raising=False)
    client=TestClient(app)
    token=client.get('/api/status').json()['csrf_token']
    result = client.post("/api/runs", headers={'X-CSRF-Token':token},json={
        "start": "2022-01-03", "end": "2024-12-31"
    })
    assert result.status_code == 409
    assert "historical dataset" in result.json()["detail"]


def test_known_weekend_earnings_and_announcement_time():
    import sqlite3

    engine = create_engine(
        "sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES}
    )
    with engine.begin() as c:
        c.execute(
            text(
                "create table instruments(id integer,exchange text,trading_symbol text,metadata text)"
            )
        )
        c.execute(text("insert into instruments values(1,'NSE','ABC-EQ','{}')"))
        c.execute(
            text(
                "create table earnings_calendar(instrument_id integer,event_date date,announced_at timestamp)"
            )
        )
        c.execute(
            text("insert into earnings_calendar values(1,:d,:t)"),
            {"d": date(2026, 10, 18), "t": datetime(2026, 10, 15, 12)},
        )
    reference = ReferenceDataQueries(engine)
    dates = [D, date(2026, 10, 19), date(2026, 10, 21), date(2026, 10, 22)]
    assert reference.earnings_within_trading_window(
        "NSE:ABC", D, dates, as_of=datetime(2026, 10, 16)
    )
    assert not reference.earnings_within_trading_window(
        "NSE:ABC", D, dates, as_of=datetime(2026, 10, 14)
    )


def reaction_frame():
    frame = pd.DataFrame(
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 100.0,
            "ema21": 99.0,
        },
        index=pd.bdate_range("2026-01-01", periods=100),
    )
    frame.iloc[25, frame.columns.get_loc("close")] = 110
    frame.iloc[26] = [110, 127, 109, 120, 200, 115]
    frame.iloc[27] = [120, 125, 110, 112, 100, 115]
    return frame


def test_setup2_uses_filing_timestamp_for_pre_result_close(pipeline):
    frame = reaction_frame()
    variant = EarningsAnchoredSetup2(pipeline)
    filing_day = frame.index[25].date()
    before_close = variant._from_filing(
        frame,
        datetime.combine(filing_day, datetime.min.time(), tzinfo=IST).replace(hour=14),
        frame.index[27].date(),
    )
    after_close = variant._from_filing(
        frame,
        datetime.combine(filing_day, datetime.min.time(), tzinfo=IST).replace(hour=18),
        frame.index[27].date(),
    )
    assert before_close["pre_result_close"] == 100
    assert after_close["pre_result_close"] == 110
    assert before_close["reaction_volume_ratio"] == 2
    assert after_close["signal_date"] == frame.index[27].date().isoformat()


def test_setup2_reaction_volume_and_early_below_disqualify(pipeline):
    frame = reaction_frame()
    variant = EarningsAnchoredSetup2(pipeline)
    filed = datetime.combine(
        frame.index[25].date(), datetime.min.time(), tzinfo=IST
    ).replace(hour=18)
    frame.loc[frame.index[26], "volume"] = 199
    assert variant._from_filing(frame, filed, frame.index[-1].date()) is None
    frame.loc[frame.index[26], ["volume", "high", "close", "ema21"]] = [
        200,
        112,
        110,
        115,
    ]
    assert variant._from_filing(frame, filed, frame.index[-1].date()) is None


def test_setup2_sixty_session_expiry(pipeline):
    frame = reaction_frame()
    variant = EarningsAnchoredSetup2(pipeline)
    filed = datetime.combine(
        frame.index[25].date(), datetime.min.time(), tzinfo=IST
    ).replace(hour=18)
    frame.loc[frame.index[27] :, "close"] = 120
    frame.loc[frame.index[86], "ema21"] = 125  # session 61 cannot create signal
    assert variant._from_filing(frame, filed, frame.index[-1].date()) is None
    frame.loc[frame.index[85], "ema21"] = 125  # session 60 is still valid
    assert (
        variant._from_filing(frame, filed, frame.index[-1].date())["signal_date"]
        == frame.index[85].date().isoformat()
    )


def test_unknown_special_session_fails_closed_until_confirmed():
    special = date(2026, 11, 8)
    c = MarketCalendar([D], {2026: {special}}, unverified_sessions=[special])
    assert c.forward(date(2026, 11, 6), 3) == []
    c = MarketCalendar(
        [D],
        {2026: {special}},
        special_sessions=[special],
        unverified_sessions=[special],
    )
    assert c.forward(date(2026, 11, 6), 3) == [
        date(2026, 11, 6),
        special,
        date(2026, 11, 9),
    ]


def test_holiday_parser_requires_complete_year_and_tracks_special_session():
    from packages.pms.market_calendar import parse_holiday_calendar

    with pytest.raises(ValueError):
        parse_holiday_calendar({"CM": []})
    payload = {
        "CM": [
            {"tradingDate": f"{day:02d}-Jan-2026", "description": "holiday"}
            for day in range(1, 9)
        ]
    }
    payload["CM"][0]["description"] = "Diwali*"
    out = parse_holiday_calendar(payload)
    assert len(out["holiday_years"]["2026"]) == 8
    assert out["unverified_special_session_dates"] == ["2026-01-01"]


def test_setup2_cannot_read_future_signal_bar(pipeline):
    frame = reaction_frame()
    filed = datetime.combine(
        frame.index[25].date(), datetime.min.time(), tzinfo=IST
    ).replace(hour=18)
    assert (
        EarningsAnchoredSetup2(pipeline)._from_filing(
            frame, filed, frame.index[26].date()
        )
        is None
    )


def test_setup2_requires_stage3_trigger_and_never_production_persistable(
    pipeline, monkeypatch
):
    candles = pipeline._candles("", D)
    filed = datetime(2026, 8, 1, 18, tzinfo=IST)
    monkeypatch.setattr(
        pipeline.prices,
        "financial_history_as_of",
        lambda *a, **kw: [{"filed_at": filed}],
    )
    monkeypatch.setattr(
        pipeline.module_a,
        "evaluate",
        lambda *a: (None, SimpleNamespace(eligible=True, triggers=())),
    )
    variant = EarningsAnchoredSetup2(pipeline)
    monkeypatch.setattr(
        variant, "_from_filing", lambda *a: pytest.fail("No Stage 3 trigger")
    )
    result = variant.evaluate("NSE:ABC", D)
    assert result.research_only and result.signal is None
    with pytest.raises(PermissionError):
        pipeline.persist_results([result], pipeline._stamp(D))


def test_bulk_scan_reports_blocked_a_without_technical_bypass(pipeline, monkeypatch):
    rows = [
        {
            "instrument_id": "NSE:ABC",
            "decision": {"eligible": False, "failed_rules": ["missing_financials"]},
        }
    ]
    monkeypatch.setattr(
        pipeline.module_a, "scan", lambda *a, **kw: {"eligible": 0, "results": rows}
    )
    monkeypatch.setattr(
        pipeline, "evaluate", lambda *a, **kw: pytest.fail("A rejected the stock")
    )
    result = pipeline.scan(D)
    assert (
        result["evaluated"] == 1
        and result["module_a_eligible"] == 0
        and result["signals"] == 0
    )
    assert result["results"][0]["diagnostics"]["technical_evaluation_skipped"]


def test_mixed_persistence_dates_rejected_before_database(pipeline):
    rows = [
        ModuleBResult("NSE:ABC", d, False, ("module_a_ineligible",), None)
        for d in [D, D - timedelta(days=1)]
    ]
    with pytest.raises(ValueError, match="one as_of"):
        pipeline.persist_results(rows, pipeline._stamp(D))


def test_current_intraday_candle_is_not_used(monkeypatch):
    p = raw_pipeline(monkeypatch, [raw_row()])
    monkeypatch.setattr(p, "_stamp", lambda _: datetime(2026, 10, 16, 14, tzinfo=IST))
    assert p._candles("NSE:ABC", D) == []


def test_reported_turnover_is_preferred_over_close_times_volume(pipeline, monkeypatch):
    from dataclasses import replace

    candles = [
        replace(c, turnover=Decimal("6000000000")) for c in pipeline._candles("", D)
    ]
    monkeypatch.setattr(pipeline, "_candles", lambda *a: candles)
    assert "turnover_high" in pipeline.evaluate("NSE:ABC", D).rejected_reasons
