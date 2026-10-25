"""Strategy library (S1-S10): selection API, engine behaviour and isolation from PMS Setup 1."""
import time
from datetime import date

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main, security
from app.main import app
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


@pytest.fixture(autouse=True)
def clear():
    with main.LOCK:
        main.SESSIONS.clear(); main.JOBS.clear(); security.LIMITER.reset()
    yield


@pytest.fixture
def client():
    with TestClient(app) as c:
        c.headers["X-CSRF-Token"] = c.get("/api/status").json()["csrf_token"]
        yield c


def wait(client, job_id):
    for _ in range(1500):
        value = client.get("/api/runs/" + job_id).json()
        if value["status"] not in ("RUNNING", "QUEUED"):
            return value
        time.sleep(.02)
    pytest.fail("job timed out")


def test_catalog_lists_original_strategy_first_then_ten():
    items = public_catalog()
    assert items[0]["id"] == "PMS_SETUP1" and items[0]["default"]
    assert [i["id"] for i in items[1:]] == [f"S{k}" for k in range(1, 11)]
    assert all(i["audit"]["verdict"] for i in items[1:])


def test_strategies_endpoint_and_original_contract_unchanged(client):
    data = client.get("/api/strategies").json()["strategies"]
    assert len(data) == 11
    assert client.get("/api/status").json()["strategy"] == "FULL_PMS_SETUP1"
    # The original endpoint still rejects any strategy field and still requires the PMS database.
    assert client.post("/api/runs", json={"start": "2022-01-03", "end": "2024-12-31", "strategy": "S1"}).status_code == 422
    assert client.post("/api/runs", json={"start": "2022-01-03", "end": "2024-12-31"}).status_code == 409


@pytest.mark.parametrize("payload", [
    {"strategy": "S11", "start": "2022-01-03", "end": "2024-12-31"},
    {"strategy": "PMS_SETUP1", "start": "2022-01-03", "end": "2024-12-31"},
    {"strategy": "S1", "start": "2010-01-04", "end": "2012-12-31"},
    {"strategy": "S1", "start": "2024-12-31", "end": "2022-01-03"},
    {"strategy": "S1", "start": "2012-01-02", "end": "2024-12-31"},
    {"strategy": "S1", "start": "2022-01-03", "end": "2024-12-31", "symbols": ["DIXON"]},
    {"strategy": "S1", "start": "2022-01-03", "end": "2024-12-31", "capital": 5000},
])
def test_strategy_run_validation(client, payload):
    assert client.post("/api/strategy-runs", json=payload).status_code == 422
    assert not main.JOBS


def test_api_run_report_and_downloads(client, monkeypatch, panel):
    monkeypatch.setattr(runner, "load_panel", lambda *a, **k: panel)
    job = client.post("/api/strategy-runs", json={"strategy": "S1", "start": "2021-01-04", "end": "2023-12-29",
                                                  "capital": 1000000})
    assert job.status_code == 202
    result = wait(client, job.json()["id"])
    assert result["status"] in ("COMPLETED", "NO_TRADES"), result
    report = result["report"]
    assert report["strategy_id"] == "S1" and report["execution_enabled"] is False
    assert report["metrics"]["initial_capital"] == 1000000
    assert report["diagnostics"]["audit"]["verdict"]
    for kind in ("json", "csv", "html"):
        assert client.get(f"/api/runs/{job.json()['id']}/download/{kind}").status_code == 200
    # another session cannot read the report
    with TestClient(app) as other:
        other.get("/api/status")
        assert other.get("/api/runs/" + job.json()["id"]).status_code == 404


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
