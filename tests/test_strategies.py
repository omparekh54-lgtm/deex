"""Strategy library (S1-S10): selection API, engine behaviour and isolation from PMS Setup 1."""
import time
from datetime import date

import numpy as np
import pandas as pd
import pytest

from packages.strategies import runner
from packages.strategies.catalog import STRATEGY_IDS, public_catalog
from packages.strategies.engine import BUY_COST, SELL_COST
from packages.strategies.market import Market
from packages.strategies.runner import run_backtest, run_strategy

ETFS = ["NIFTYBEES", "JUNIORBEES", "MID150BEES", "GOLDBEES", "BANKBEES", "SILVERBEES"]


def synthetic_panel(seed=3, n=60, start="2019-01-01", end="2023-12-29"):
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(start, end)
    rows = []
    for k in range(n + len(ETFS)):
        fund = k >= n
        sym = ETFS[k - n] if fund else f"STK{k:03d}"
        drift = rng.normal(0.0006, 0.0008)
        vol = 0.012 if fund else rng.uniform(0.015, 0.03)
        r = rng.normal(drift, vol, len(days))
        # occasional gap-up thrusts on high volume so event strategies trigger
        shock = rng.random(len(days)) < 0.004
        r[shock] += 0.09
        close = 100 * np.exp(np.cumsum(r))
        open_ = close / np.exp(r) * np.where(shock, 1.05, 1 + rng.normal(0, 0.003, len(days)))
        high = np.maximum(open_, close) * (1 + rng.uniform(0, 0.02, len(days)))
        low = np.minimum(open_, close) * (1 - rng.uniform(0, 0.02, len(days)))
        volume = rng.uniform(8e5, 1.2e6, len(days)) * np.where(shock, 4, 1)
        rows.append(pd.DataFrame({"date": days, "sid": "S:" + sym, "sym": sym, "series": "EQ", "open": open_,
                                  "high": high, "low": low, "close": close, "vol": volume,
                                  "turnover": volume * close, "fund": fund, "rights": False}))
    return pd.concat(rows, ignore_index=True)


@pytest.fixture(scope="module")
def panel():
    return synthetic_panel()


@pytest.mark.parametrize("sid", STRATEGY_IDS)
def test_every_strategy_runs_and_accounts_consistently(panel, sid):
    report = run_strategy(sid, date(2021, 1, 4), date(2023, 12, 29), 1_000_000, panel=panel)
    m = report["metrics"]
    assert report["equity_curve"] and all(np.isfinite(r["equity"]) and r["equity"] > 0 for r in report["equity_curve"])
    costs = sum(f["costs"]["total"] for f in report["fills"])
    assert m["total_costs"] == pytest.approx(costs)
    for f in report["fills"]:
        rate = BUY_COST if f["side"] == "BUY" else SELL_COST
        assert f["costs"]["total"] == pytest.approx(f["price"] * f["quantity"] * rate)
        assert "2021-01-04" <= f["trade_date"] <= "2023-12-29"
    bought = {}
    for f in report["fills"]:
        bought[f["instrument_id"]] = bought.get(f["instrument_id"], 0) + (f["quantity"] if f["side"] == "BUY" else -f["quantity"])
    held = {p["instrument"]: p["shares"] for p in report["open_positions"]}
    assert {k: v for k, v in bought.items() if v} == held


@pytest.mark.parametrize("sid", ["S1", "S3", "S4", "S7", "S8", "S10"])
def test_no_look_ahead_future_prices_do_not_change_past_equity(panel, sid):
    cut = pd.Timestamp("2022-06-30")
    altered = panel.copy()
    later = altered.date > cut
    for c in ("open", "high", "low", "close"):
        altered.loc[later, c] *= 1.5
    s, e = pd.Timestamp("2021-01-04"), pd.Timestamp("2023-12-29")
    a = run_backtest(Market(panel), sid, s, e, 1e6)[0].equity
    b = run_backtest(Market(altered), sid, s, e, 1e6)[0].equity
    pd.testing.assert_series_equal(a[a.index <= cut], b[b.index <= cut])
