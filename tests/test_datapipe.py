"""Offline tests for the fast data pipeline: parsers, pacing/back-off and filing selection."""
from __future__ import annotations

import gzip
import time
import zipfile
from datetime import date

import httpx
import pandas as pd
import pytest

from packages.datapipe import events, financials, net, prices, reference
from packages.datapipe.store import Store, Window, months


def test_window_buffers_and_months():
    w = Window(date(2016, 10, 24), date(2026, 10, 23))
    assert w.price_start == date(2014, 10, 24)
    assert w.financial_start < date(2014, 10, 24)
    m = list(months(date(2024, 1, 15), date(2024, 3, 2)))
    assert m == [(date(2024, 1, 15), date(2024, 1, 31)), (date(2024, 2, 1), date(2024, 2, 29)),
                 (date(2024, 3, 1), date(2024, 3, 2))]


def test_bhavcopy_paths_and_parsers(tmp_path):
    assert prices.relpath(date(2017, 1, 2)) == "legacy/2017/cm02JAN2017bhav.csv.zip"
    assert prices.relpath(date(2025, 6, 2)).endswith("BhavCopy_NSE_CM_0_0_0_20250602_F_0000.csv.zip")
    assert prices._day_from_name("cm02JAN2017bhav.csv.zip") == date(2017, 1, 2)
    assert prices._day_from_name("BhavCopy_NSE_CM_0_0_0_20250602_F_0000.csv.zip") == date(2025, 6, 2)
    legacy = ("SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,\n"
              "ABC,EQ,10,11,9,10.5,10.5,10,1000,10500,02-JAN-2017,5,INE000A01010,\n"
              "ABC,N1,10,11,9,10.5,10.5,10,1000,10500,02-JAN-2017,5,INE000A01011,\n")
    p = tmp_path / "cm02JAN2017bhav.csv.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("cm02JAN2017bhav.csv", legacy)
    df = prices.parse_zip(p)
    assert list(df.sym) == ["ABC"] and df["isin"].iloc[0] == "INE000A01010" and df.turnover.iloc[0] == 10500


def test_index_and_surveillance_parsers():
    body = ("Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value\n"
            "CNX Nifty,02-01-2017,8210,8212,8133,8179\nIndia VIX,02-01-2017,-,-,-,15.2\nNifty Bank,02-01-2017,1,1,1,1\n")
    rows = reference._parse_index_day(body.encode(), date(2017, 1, 2))
    codes = {r["code"]: r for r in rows}
    assert set(codes) == {"NIFTY50", "INDIAVIX"} and codes["NIFTY50"]["close"] == 8179
    assert pd.isna(codes["INDIAVIX"]["open"]) and codes["INDIAVIX"]["close"] == 15.2
    reg = ("Symbol,Status,Series,GSM,Long_Term_Additional_Surveillance_Measure (Long Term ASM),"
           "Short_Term_Additional_Surveillance_Measure (Short Term ASM)\nAAA,A,EQ,100,100,100\nBBB,A,EQ,0,100,100\n"
           "CCC,A,EQ,100,2,100\n")
    flagged = reference._parse_reg_ind(reg.encode(), date(2024, 2, 1))
    assert {r["sym"] for r in flagged} == {"BBB", "CCC"}      # GSM 0 is an active stage; 100 is inactive


def test_action_and_board_tables():
    a = events._actions_table([
        {"symbol": "X", "subject": "Interim Dividend - Rs 2.50 Per Share", "exDate": "05-Feb-2020"},
        {"symbol": "Y", "subject": "Face Value Split (Sub-Division) - From Rs 10/- To Re 1/-", "exDate": "06-Feb-2020"},
        {"symbol": "Z", "subject": "Bonus 1:1", "exDate": "07-Feb-2020"}])
    assert list(a.kind) == ["DIVIDEND", "SPLIT", "BONUS"] and a.cash_per_share.iloc[0] == 2.5
    b = events._board_table([{"bm_symbol": "X", "bm_date": "10-Feb-2020", "bm_purpose": "Financial Results",
                              "bm_timestamp": "01-Feb-2020 10:00:00"},
                             {"bm_symbol": "X", "bm_date": "11-Feb-2020", "bm_purpose": "Fund Raising"}])
    assert list(b.results) == [True, False]


class _Transport(httpx.BaseTransport):
    def __init__(self, codes):
        self.codes, self.calls = list(codes), 0

    def handle_request(self, request):
        self.calls += 1
        code = self.codes.pop(0) if self.codes else 200
        return httpx.Response(code, content=b"PKdata", request=request)


def _net_with(transport, monkeypatch, policy):
    monkeypatch.setitem(net.POLICIES, "example.test", policy)
    n = net.Net(log=lambda *_: None)
    n._local.client = httpx.Client(transport=transport)
    n._local.cookie_version = n._cookie_version
    return n


def test_backoff_slows_down_then_recovers(monkeypatch):
    t = _Transport([429, 200])
    n = _net_with(t, monkeypatch, net.HostPolicy(max_concurrency=1, start_rps=50, max_rps=50, cooldown_s=0.05))
    assert n.get("https://example.test/a") == b"PKdata"
    g = n.gates["example.test"]
    assert t.calls == 2 and g.blocks == 1 and g.rps == 25      # halved after the refusal


def test_circuit_breaker_stops_a_refusing_host(monkeypatch):
    t = _Transport([403] * 10)
    n = _net_with(t, monkeypatch, net.HostPolicy(max_concurrency=1, start_rps=50, max_rps=50, cooldown_s=0.01,
                                                 max_strikes=3))
    with pytest.raises(net.HostBlocked):
        n.get("https://example.test/a", retries=10)
    assert t.calls == 3


def test_pacing_respects_rate(monkeypatch):
    t = _Transport([200] * 6)
    n = _net_with(t, monkeypatch, net.HostPolicy(max_concurrency=1, start_rps=20, max_rps=20))
    t0 = time.monotonic()
    for _ in range(6):
        n.get("https://example.test/x")
    assert time.monotonic() - t0 >= 5 / 20 - 0.02


XBRL = """<?xml version="1.0"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:in-bse-fin="http://www.bseindia.com/xbrl/fin"
 xmlns:iso4217="http://www.xbrl.org/2003/iso4217">
 <xbrli:context id="Q"><xbrli:entity><xbrli:identifier scheme="x">INE000A01010</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:startDate>2019-04-01</xbrli:startDate><xbrli:endDate>2019-06-30</xbrli:endDate></xbrli:period></xbrli:context>
 <xbrli:unit id="INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>
 <in-bse-fin:RevenueFromOperations contextRef="Q" unitRef="INR" decimals="0">5000000000</in-bse-fin:RevenueFromOperations>
 <in-bse-fin:ProfitLossForPeriod contextRef="Q" unitRef="INR" decimals="0">600000000</in-bse-fin:ProfitLossForPeriod>
</xbrli:xbrl>"""


def test_filing_selection_prefers_consolidated_and_parses_xbrl(tmp_path):
    store = Store(tmp_path)
    cands = pd.DataFrame({"isin": ["INE000A01010"], "first_month": [pd.Period("2020-06", "M")],
                          "last_month": [pd.Period("2021-06", "M")], "sym": ["ABC"], "symbols": [["ABC"]],
                          "financial_sector": [False]})
    base = {"symbol": "ABC", "toDate": "30-Jun-2019", "broadCastDate": "10-Aug-2019 17:00:00", "period": "Quarterly"}
    found = pd.DataFrame([
        {**base, "consolidated": "Non-Consolidated", "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/S.xml",
         "_sym": "ABC", "_isin": "INE000A01010"},
        {**base, "consolidated": "Consolidated", "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/C.xml",
         "_sym": "ABC", "_isin": "INE000A01010"},
        {**base, "toDate": "30-Sep-2019", "consolidated": "Consolidated",
         "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/-", "_sym": "ABC", "_isin": "INE000A01010"}])
    jobs = financials.select(store, Window(date(2020, 6, 1), date(2021, 6, 30)), cands, found, log=lambda *_: None)
    assert [j["url"].rsplit("/", 1)[-1] for j in jobs] == ["C.xml"]       # consolidated chosen, '-' skipped
    path = financials._doc_path(store, jobs[0]["url"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(XBRL.encode()))
    rows, err = financials._parse_one((str(path), jobs[0]))
    assert err is None and rows[0]["consolidated"] and rows[0]["sales"] == 500.0 and rows[0]["pat"] == 60.0
