"""Run one library strategy and return a report in the same shape as the PMS Setup 1 report."""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from packages.strategies import signals as S
from packages.strategies.bhavcopy import load_panel
from packages.strategies.catalog import CATALOG
from packages.strategies.engine import BUY_COST, SELL_COST, rebalance_sim, simulate
from packages.strategies.market import Market

ENGINE_VERSION = "deex-strategy-library-1.0"
WARMUP_DAYS = 560   # about 380 sessions: 252-day momentum, 52-week highs and the regime dial need history


def _noop(*_a, **_k):
    return None


def run_backtest(mkt: Market, strategy_id: str, start, end, capital: float, check=None):
    """Simulate one strategy on prepared matrices. Returns (equity, trades, fills, open positions)."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    reg = mkt.regime()
    if strategy_id == "S1":
        return rebalance_sim(S.s1_momentum(mkt, start), mkt.O, mkt.C, start, end, capital, mkt.sym, check)
    if strategy_id == "S6":
        return rebalance_sim(S.s6_lowvol(mkt, start), mkt.O, mkt.C, start, end, capital, mkt.sym, check)
    if strategy_id == "S10":
        w, O, C = S.s10_etf(mkt, start)
        return rebalance_sim(w, O, C, start, end, capital, None, check)
    from dataclasses import replace
    if strategy_id == "S7":
        sig, ex = S.s7_donchian(mkt, start)
        return simulate(mkt, sig, replace(S.S7P, capital=capital), start, end, reg, ex, check)
    if strategy_id == "S8":
        sig, ex = S.s8_stage2(mkt, start)
        return simulate(mkt, sig, replace(S.S8P, capital=capital), start, end, reg, ex, check)
    fn = {"S2": S.s2_base_breakout, "S3": S.s3_vcp, "S4": S.s4_episodic_pivot,
          "S5": S.s5_leader_pullback, "S9": S.s9_mom_dip}[strategy_id]
    return simulate(mkt, fn(mkt, start), replace(S.HOUSE, capital=capital), start, end, reg, None, check)


def _metrics(eq: pd.DataFrame, trades, fills, open_pos, capital, start, end):
    values = [float(capital), *eq.equity.astype(float).tolist()]
    rets = np.diff(values) / np.array(values[:-1])
    years = max(1, (end - start).days + 1) / 365.25
    peak, dd = values[0], 0.0
    for v in values:
        peak = max(peak, v)
        dd = max(dd, (1 - v / peak) * 100)
    sd = float(np.std(rets)) if len(rets) > 1 else 0.0
    downside = math.sqrt(float(np.mean(np.minimum(rets, 0) ** 2))) if len(rets) else 0.0
    cagr = ((values[-1] / values[0]) ** (1 / years) - 1) * 100
    win = [t["pnl"] for t in trades if t["pnl"] > 0]
    lose = [t["pnl"] for t in trades if t["pnl"] < 0]
    rmult = [t["r_multiple"] for t in trades if t.get("r_multiple") is not None]
    return dict(
        total_return_pct=(values[-1] / values[0] - 1) * 100, cagr_pct=cagr, max_drawdown_pct=dd,
        sharpe=float(np.mean(rets)) / sd * math.sqrt(252) if sd else None,
        sortino=float(np.mean(rets)) / downside * math.sqrt(252) if downside else None,
        calmar=cagr / dd if dd else None, risk_free_rate=0,
        ratio_basis="daily NAV returns; 252 sessions/year; zero risk-free assumption",
        profit_factor=sum(win) / abs(sum(lose)) if lose else None,
        closed_trades=len(trades), fills=len(fills), open_positions=len(open_pos),
        win_rate_pct=len(win) / len(trades) * 100 if trades else None,
        expectancy_rupees=float(np.mean([t["pnl"] for t in trades])) if trades else None,
        expectancy_r=float(np.mean(rmult)) if rmult and len(rmult) == len(trades) else None,
        average_holding_days=float(np.mean([t["holding_days"] for t in trades])) if trades else None,
        trades_per_year=len(trades) / years, initial_capital=values[0], ending_equity=values[-1],
        average_exposure_pct=float(eq.exposure.mean() * 100),
        total_costs=float(sum(f["costs"]["total"] for f in fills)))


def _benchmarks(mkt: Market, start, end):
    out = {}
    for sym in ("NIFTYBEES", "MID150BEES", "GOLDBEES"):
        s = mkt.funds_px[sym].reindex(mkt.dates).ffill()
        s = s[(s.index >= start) & (s.index <= end)].dropna()
        if len(s) > 20:
            yrs = max(1, (end - start).days + 1) / 365.25
            out[sym] = dict(cagr_pct=((s.iloc[-1] / s.iloc[0]) ** (1 / yrs) - 1) * 100,
                            max_drawdown_pct=float((1 - s / s.cummax()).max() * 100), basis="price only, no dividends")
    return out


def build_report(mkt, strategy_id, start, end, capital, eq, trades, fills, open_pos, data_note):
    from packages.pms.backtest_matrix import fingerprint, plain
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    meta = CATALOG[strategy_id]
    peak = float(capital)
    curve = []
    for day, v in eq.equity.items():
        peak = max(peak, v)
        curve.append({"date": day.date().isoformat(), "equity": float(v), "drawdown_pct": (1 - v / peak) * 100})
    traded = sorted({f["instrument_id"] for f in fills})
    adtv = mkt.adtv20
    sid_of = {v: k for k, v in mkt.sym.items()}
    stocks = []
    for name in traded:
        sid = sid_of.get(name)
        val = float(adtv[sid].loc[:end].dropna().iloc[-1]) if sid in adtv.columns and adtv[sid].loc[:end].notna().any() else 0.0
        stocks.append({"instrument_id": name, "sector": "Not classified", "adtv": val})
    audit = meta["audit"]
    limitations = [
        f"Independent audit verdict: {audit['verdict']}. {audit['note']} "
        f"Audited CAGR {audit['recent']}% (Oct 2021-Sep 2026) and {audit['holdout']}% (Jan 2011-Sep 2021 holdout); "
        "Nifty 50 ETF price-only 7.2% and 11.0%.",
        "Price-only strategy from the strategy library. PMS Modules A (fundamentals) and C, sector caps and "
        "historical ASM/GSM lists are not applied. Trade-for-trade (BE/BZ) series are excluded from entries.",
        "Data: NSE bhavcopies including delisted stocks. Splits and bonuses are inferred from opening gaps, "
        "not an official corporate-action record. Dividends are not added.",
        f"Costs: {BUY_COST*100:.3f}% per buy and {SELL_COST*100:.3f}% per sell including 0.15% assumed slippage. "
        "Taxes are not modelled. Daily bars cannot show intraday order; same-day entry and stop assumes the stop hit.",
        f"Regime dial benchmark: {mkt.regime_benchmark}. Idle cash earns no interest.",
        "Open positions are marked at the final close, not force-closed.",
        "Historical simulation only. Not certified, not investment advice. No broker orders are placed.",
        data_note,
    ]
    payload = {
        "title": "DEEX Strategy Library Backtest Report", "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "COMPLETED" if trades or open_pos else "NO_TRADES", "demo": False,
        "acceptance": "EXPLORATORY_NOT_CERTIFIED", "strategy_id": strategy_id,
        "mode": "LIBRARY_" + strategy_id, "mode_label": f"{strategy_id} {meta['name']} ({meta['family']})",
        "parameters": {"strategy": strategy_id, "start": str(start.date()), "end": str(end.date()),
                       "capital": capital, "symbols": traded},
        "metrics": _metrics(eq, trades, fills, open_pos, capital, start, end),
        "benchmarks": _benchmarks(mkt, start, end),
        "equity_curve": curve, "trades": trades, "open_positions": open_pos, "fills": fills,
        "diagnostics": {"limitations": limitations, "audit": audit, "rules": meta["rules"],
                        "universe": {"selection": "All NSE EQ stocks passing the strategy's filters on each date. "
                                     "Listed below: every instrument the strategy traded, with its last 20-day median traded value.",
                                     "stocks": stocks}},
        "configuration": {"strategy": strategy_id, "rules": meta["rules"], "buy_cost": BUY_COST, "sell_cost": SELL_COST},
        "engine_source_commit": ENGINE_VERSION, "rules_source": "DEEX 10-strategy specification v1",
        "execution_enabled": False,
    }
    payload = plain(payload)
    payload["report_fingerprint"] = fingerprint(payload)
    return payload


def run_strategy(strategy_id: str, start: date, end: date, capital: float, progress=_noop, check=_noop, panel=None):
    if strategy_id not in CATALOG:
        raise ValueError("Unknown strategy")
    data_start = start - timedelta(days=WARMUP_DAYS)
    if panel is None:
        progress(5, "Loading NSE bhavcopies (first run downloads and caches them)")
        panel = load_panel(data_start, end, lambda f, m: progress(5 + int(35 * f), m), check)
        note = "Bhavcopy source: local archive, download cache or the public mirror of NSE's archive."
    else:
        note = "Bhavcopy source: panel supplied by caller."
    check()
    progress(45, "Building price matrices and indicators")
    mkt = Market(panel)
    progress(55, "Computing the regime dial")
    mkt.regime()
    check()
    progress(65, "Generating signals and replaying the portfolio")
    eq, trades, fills, open_pos = run_backtest(mkt, strategy_id, start, end, capital, check)
    if eq.empty:
        raise ValueError("No trading sessions in the selected period.")
    progress(95, "Building report")
    return build_report(mkt, strategy_id, start, end, capital, eq, trades, fills, open_pos, note)
