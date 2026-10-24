"""PMS Setup 1 (Modules A + B + C + E, re-entry on) replayed from the local data folder.

This mirrors ``packages.pms.matrix_replay.MatrixReplay`` decision for decision, but reads the
parquet tables written by ``scripts/fetch_all_data.py`` instead of the Design Experience
PostgreSQL schema. The rules are not re-implemented: every decision calls the same pure
functions the PMS pipeline uses:

* Module A: ``module_a_pipeline.evaluate_inputs`` -> ``fundamental_history`` + ``module_a``
* Module B: ``module_b.advance_rally`` with the Module B universe filters
* Module C: ``module_c_inputs.measure_features/ranking_inputs`` + ``module_c.allocate_candidates``
* Module E: ``module_e.RegimeTracker`` fed by official index levels and computed breadth
* Execution: ``backtest.EventDrivenBacktester`` (the shared trade engine)

Differences forced by public data are listed in the report's ``limitations``.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from statistics import median
from types import SimpleNamespace

import numpy as np
import pandas as pd

from packages.pms.backtest import EventDrivenBacktester, MarketBar, Signal
from packages.pms.backtest_matrix import fingerprint, metrics, plain, round_trips
from packages.pms.config import load_strategy_config
from packages.pms.costs import estimate_delivery_costs
from packages.pms.module_a_context import IST, end_of_day
from packages.pms.module_a_pipeline import evaluate_inputs
from packages.pms.module_b import Bar, RallyState, RallyTracker, advance_rally
from packages.pms.module_c import AllocationState, Candidate, allocate_candidates, rank_candidate
from packages.pms.module_c_inputs import measure_features, ranking_inputs
from packages.pms.module_e import RegimeInputs, RegimeTracker
from packages.pms.risk import size_position
from packages.pms.trade_engine import TradeState, reentry_allowed
from packages.strategies.lake import load_panel

WARMUP_DAYS = 600
SURVEILLANCE_FROM = date(2024, 1, 23)


def _percentile_map(values: dict) -> dict:
    """Same tie-aware percentile ranking as Module C."""
    if not values:
        return {}
    if len(values) == 1:
        return {next(iter(values)): 50.0}
    ordered = sorted(values.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    out, i, n = {}, 0, len(ordered)
    while i < n:
        j = i
        while j + 1 < n and ordered[j + 1][1] == ordered[i][1]:
            j += 1
        score = 100 * (1 - (i + j) / 2 / (n - 1))
        for k in range(i, j + 1):
            out[ordered[k][0]] = round(score, 4)
        i = j + 1
    return out


class LakePMS:
    def __init__(self, root, start: date, end: date, capital: float = 1e7, *, cfg=None,
                 strict_surveillance=False, log=print):
        from pathlib import Path
        self.root, self.start, self.end = Path(root), start, end
        self.cfg = cfg or load_strategy_config()
        self.cfg["backtest"]["initial_capital"] = capital
        self.cfg["backtest"]["matrix_research_only"] = True
        self.strict_surveillance = strict_surveillance
        self.log = log
        self.notes = Counter()
        self._load()

    # ======================================================================== data
    def _load(self):
        t = self.cfg["technical"]
        self.log("PMS: loading prices")
        panel = load_panel(self.root, self.start - timedelta(days=WARMUP_DAYS), self.end)
        panel = panel[~panel.fund & ~panel.rights]
        self.sessions = sorted(d.date() for d in panel.date.drop_duplicates())
        self.pos = {d: i for i, d in enumerate(self.sessions)}
        # Keep securities that could ever meet Module B's turnover floor.
        tv20 = panel.sort_values("date").groupby("sid").turnover.transform(lambda s: s.rolling(20, min_periods=20).mean())
        keep = set(panel.loc[tv20 >= float(t["min_avg_turnover_20d_cr"]) * 1e7, "sid"])
        panel = panel[panel.sid.isin(keep)]
        last_sym = panel.sort_values("date").groupby("sid").sym.last()
        dup = last_sym[last_sym.duplicated(keep=False)]
        self.iid = {sid: "NSE:" + s + (f"~{sid[-6:]}" if sid in dup.index else "") for sid, s in last_sym.items()}
        self.sid_of = {v: k for k, v in self.iid.items()}
        self.symbols = panel.groupby("sid").sym.agg(lambda s: set(s))
        self.isins = panel.dropna(subset=["isin"]).groupby("sid")["isin"].agg(lambda s: set(s))
        self.log(f"PMS: computing features and Module B rally states for {len(keep):,} securities")
        self.F, self.signals, self.watch = {}, {}, {}
        self.daily_candidates = defaultdict(list)
        nS = len(self.sessions)
        sids = sorted(keep)
        self.col = {sid: k for k, sid in enumerate(sids)}
        W = {k: np.full((nS, len(sids)), np.nan, dtype=np.float32)
             for k in ("close", "dma50", "ema21", "high", "low", "high_52w", "low_52w", "avg_turnover20")}
        W["eq"] = np.zeros((nS, len(sids)), dtype=bool)
        W["watch"] = np.zeros((nS, len(sids)), dtype=bool)
        W["nrows"] = np.zeros((nS, len(sids)), dtype=np.int32)
        for sid, g in panel.groupby("sid", sort=False):
            g = g.sort_values("date").drop_duplicates("date", keep="last")
            f = self._features(g)
            self.F[sid] = f
            self.signals[sid], self.watch[sid] = self._rally(f)
            k, rows = self.col[sid], f.session_pos.values
            for name in ("close", "dma50", "ema21", "high", "low", "high_52w", "low_52w", "avg_turnover20"):
                W[name][rows, k] = f[name].values
            W["eq"][rows, k] = (f.series.values == "EQ")
            W["nrows"][rows, k] = np.arange(1, len(f) + 1)
            wd = [self.pos[d] for d in self.watch[sid]]
            W["watch"][wd, k] = True
            for p in self._setup_positions(sid, f):
                self.daily_candidates[p].append(sid)
        self.W = W
        self._load_reference()

    def _features(self, g: pd.DataFrame) -> pd.DataFrame:
        f = pd.DataFrame({"open": g.open.values, "high": g.high.values, "low": g.low.values,
                          "close": g.close.values, "volume": g.vol.values, "turnover": g.turnover.values,
                          "raw_close": (g.close / g.adj).values, "raw_volume": (g.vol * g.adj).values,
                          "series": g.series.values}, index=pd.DatetimeIndex(g.date.values))
        c = f.close
        f["ema9"] = c.ewm(span=9, adjust=False, min_periods=9).mean()
        f["ema21"] = c.ewm(span=21, adjust=False, min_periods=21).mean()
        f["dma50"] = c.rolling(50, min_periods=50).mean()
        pc = c.shift(1)
        tr = pd.concat([f.high - f.low, (f.high - pc).abs(), (f.low - pc).abs()], axis=1).max(axis=1)
        f["atr14"] = tr.rolling(14, min_periods=14).mean()
        f["adr20_pct"] = 100.0 * ((f.high / f.low).rolling(20, min_periods=20).mean() - 1.0)
        f["avg_volume20"] = f.raw_volume.rolling(20, min_periods=20).mean()
        f["worst_volume20"] = f.raw_volume.rolling(20, min_periods=20).min()
        f["avg_turnover20"] = f.turnover.rolling(20, min_periods=20).mean()
        f["avg_turnover63"] = f.turnover.rolling(63, min_periods=1).mean()
        f["n63"] = f.turnover.rolling(63, min_periods=1).count()
        f["high_52w"] = f.high.rolling(252, min_periods=1).max()
        f["low_52w"] = f.low.rolling(252, min_periods=1).min()
        f["vol_base50"] = f.volume.shift(1).rolling(50, min_periods=50).mean()
        f["session_pos"] = [self.pos[d.date()] for d in f.index]
        return f

    def _setup_positions(self, sid, f: pd.DataFrame):
        """Session positions where Module B's price/volume filters pass and a setup is live or watched.

        Only these (sid, day) pairs can produce Module C rows, so the daily loop skips the rest. Earnings
        blackout and Nifty 50 exclusion are checked later per pair (they need reference data)."""
        t = self.cfg["technical"]
        n = int(t["min_history_days"])
        sp = f.session_pos.values
        idx = np.arange(len(f))
        complete = np.zeros(len(f), dtype=bool)
        if len(f) >= n:
            complete[n - 1:] = (sp[n - 1:] - sp[:len(f) - n + 1]) == n - 1
        dist = (f.high_52w - f.close) / f.high_52w * 100
        tv = f.avg_turnover20 / 1e7
        ok = (complete & (f.series.values == "EQ") & (f.raw_close.values > float(t["min_price"]))
              & (f.avg_volume20.values > float(t["min_avg_volume_20d"]))
              & (f.worst_volume20.values > float(t["min_worst_day_volume_20d"]))
              & (tv.values >= float(t["min_avg_turnover_20d_cr"])) & (tv.values <= float(t["max_avg_turnover_20d_cr"]))
              & (f.adr20_pct.values >= float(t["min_adr_20d_pct"]))
              & (dist.values <= float(t["max_distance_from_52w_high_pct"])))
        setup = np.zeros(len(f), dtype=bool)
        where = {p: i for i, p in zip(idx, sp)}
        for sdate, _ in self.signals[sid]:
            p0 = self.pos[sdate]
            for p in range(p0, p0 + int(t["entry_window_days"])):
                if p in where:
                    setup[where[p]] = True
        for d in self.watch[sid]:
            setup[where[self.pos[d]]] = True
        return sp[ok & setup]

    def _rally(self, f: pd.DataFrame):
        """Module B Setup 1 state for every session, from one forward pass of the rally tracker."""
        signals, watch = [], {}
        tracker = RallyTracker()
        o, h, l, c, v = (f[k].values for k in ("open", "high", "low", "close", "volume"))
        e21, base = f.ema21.values, f.vol_base50.values
        days = [d.date() for d in f.index]
        for i in range(50, len(f)):
            if not (math.isfinite(e21[i]) and math.isfinite(e21[i - 1])):
                continue
            tracker = advance_rally(tracker, Bar(str(days[i]), o[i], h[i], l[i], c[i], v[i], e21[i]),
                                    c[i - 1], e21[i - 1], base[i], self.cfg)
            if tracker.state == RallyState.SIGNAL_CREATED:
                signals.append((days[i], self._payload(tracker) | {
                    "signal_date": tracker.signal_date, "signal_high": tracker.signal_high,
                    "signal_low": tracker.signal_low, "setup": "SETUP_1"}))
                tracker = RallyTracker(last_date=str(days[i]))
            elif tracker.state == RallyState.RALLY_VALID:
                watch[days[i]] = self._payload(tracker)
        return signals, watch

    @staticmethod
    def _payload(t):
        return {"rally_start": t.start_date, "rally_pct": round(t.rally_pct(), 4), "velocity": round(t.velocity(), 4),
                "volume_ratio": round(t.volume_ratio(), 4), "rally_duration_sessions": t.peak_trading_days,
                "state": t.state.value}

    def _load_reference(self):
        r = self.root
        fin = pd.read_parquet(r / "financials.parquet") if (r / "financials.parquet").exists() else pd.DataFrame()
        self.fin = defaultdict(list)
        if not fin.empty:
            facts = ["sales", "operating_profit", "pat", "diluted_eps", "pbt", "other_income", "exceptional_items",
                     "operating_cash_flow", "debt", "equity", "interest_expense"]
            for row in fin.itertuples(index=False):
                self.fin[row.sym].append(dict(
                    period_end=row.period_end.date(), filed_at=row.filed_at.to_pydatetime(),
                    filing_kind=row.filing_kind, consolidated=bool(row.consolidated), source="NSE",
                    revision_number=int(row.version or 1),
                    facts={k: getattr(row, k) for k in facts if getattr(row, k) is not None
                           and not (isinstance(getattr(row, k), float) and math.isnan(getattr(row, k)))}))
        sh = pd.read_parquet(r / "shareholding.parquet")
        sh["pledge"] = sh.pledge_pct_of_promoter.where(sh.pledge_pct_of_promoter.notna(),
                                                       np.where(sh.promoter_pct.fillna(0) == 0, 0.0, np.nan))
        self.sh = {isin: g.sort_values("filed_at") for isin, g in sh.dropna(subset=["isin"]).groupby("isin")}
        sec = pd.read_parquet(r / "sectors.parquet") if (r / "sectors.parquet").exists() else pd.DataFrame(columns=["sym", "sector"])
        self.sector_by_sym = dict(zip(sec.sym, sec.sector))
        idx = pd.read_parquet(r / "indices.parquet")
        self.index = {c: g.set_index(g.date.dt.date).close.sort_index() for c, g in idx.groupby("code")}
        mem = pd.read_parquet(r / "membership.parquet")
        n50 = mem[mem.index_name == "Nifty 50"]
        self.nifty50 = defaultdict(list)
        for row in n50.itertuples():
            self.nifty50[row.symbol].append((row.valid_from.date() if pd.notna(row.valid_from) else date.min,
                                             row.valid_to.date() if pd.notna(row.valid_to) else date.max))
        bm = pd.read_parquet(r / "board_meetings.parquet") if (r / "board_meetings.parquet").exists() else pd.DataFrame()
        self.results_dates = defaultdict(list)
        if not bm.empty:
            for row in bm[bm.results].itertuples():
                known = row.announced_at if pd.notna(row.announced_at) else row.meeting_date - pd.Timedelta(days=5)
                self.results_dates[row.sym].append((row.meeting_date.date(), known.to_pydatetime()))
        self.has_board_data = not bm.empty
        act = pd.read_parquet(r / "actions.parquet") if (r / "actions.parquet").exists() else pd.DataFrame()
        self.dividends = defaultdict(list)
        if not act.empty:
            for row in act[(act.kind == "DIVIDEND") & act.cash_per_share.notna()].itertuples():
                if pd.notna(row.ex_date):
                    self.dividends[row.ex_date.date()].append((row.sym, float(row.cash_per_share)))
        sv_path = r / "surveillance.parquet"
        sv = pd.read_parquet(sv_path) if sv_path.exists() else pd.DataFrame(columns=["date", "sym"])
        self.surv = defaultdict(set)
        for d, s in zip(sv.date, sv.sym):
            self.surv[d.date()].add(s)
        days = pd.read_parquet(r / "surveillance_days.parquet") if (r / "surveillance_days.parquet").exists() else pd.DataFrame({"date": []})
        self.surv_days = sorted(d.date() for d in pd.to_datetime(days.date))

    # ======================================================================== helpers
    def forward(self, day: date, n: int) -> list[date]:
        i = self.pos.get(day)
        if i is None:
            i = next((k for k, d in enumerate(self.sessions) if d > day), len(self.sessions)) - 1
        return self.sessions[i + 1:i + 1 + n]

    def row(self, sid, day):
        f = self.F[sid]
        p = self.pos.get(day)
        if p is None:
            return None, -1
        sp = f.session_pos.values
        i = int(np.searchsorted(sp, p, side="right")) - 1
        if i < 0 or sp[i] != p:
            return None, -1
        return f.iloc[i], i

    def sector(self, sid):
        for s in sorted(self.symbols[sid], key=lambda x: x != self.iid[sid][4:].split("~")[0]):
            if s in self.sector_by_sym:
                return self.sector_by_sym[s]
        return None

    def results_within(self, sid, dates, as_of: datetime) -> bool:
        target = set(dates)
        if not target:
            return True
        for s in self.symbols[sid]:
            for meeting, known in self.results_dates.get(s, ()):
                if meeting in target and known.replace(tzinfo=IST) <= as_of:
                    return True
        return False

    def is_nifty50(self, sid, day):
        return any(a <= day < b for s in self.symbols[sid] for a, b in self.nifty50.get(s, ()))

    # ======================================================================== Module A
    def module_a(self, sid, stamp: datetime):
        key = (sid, stamp)
        if key in self._a_cache:
            return self._a_cache[key]
        market_date = stamp.date() if stamp.time() >= time(15, 30) else stamp.date() - timedelta(days=1)
        f = self.F[sid]
        k = self._bisect(market_date)                 # last exchange session on/before market_date
        j = int(np.searchsorted(f.session_pos.values, k, side="right")) - 1 if k >= 0 else -1
        r = f.iloc[j] if j >= 0 else None             # this security's last traded close
        ctx = dict(sector=self.sector(sid), industry=None, suspended=None, open_data_errors=0, asm_gsm=False)
        first_day = f.index[0].date()
        ctx["listing_known"] = first_day <= stamp.date()
        if r is not None:
            ctx.update(price_date=r.name.date(), close=float(r.raw_close),
                       avg_traded_value_cr=float(r.avg_turnover63) / 1e7, liquidity_sessions=int(r.n63))
        else:
            ctx.update(price_date=None, liquidity_sessions=0)
        best = None
        for isin in self.isins.get(sid, ()):
            g = self.sh.get(isin)
            if g is None:
                continue
            g = g[(g.filed_at <= pd.Timestamp(stamp.replace(tzinfo=None))) & (g.period_end <= pd.Timestamp(market_date))]
            if len(g) and (best is None or g.period_end.iloc[-1] > best.period_end):
                best = g.iloc[-1]
        if best is not None:
            ctx.update(shareholding_period=best.period_end.date(),
                       promoter_pledge_pct=None if pd.isna(best.pledge) else float(best.pledge),
                       total_shares=float(best.total_shares))
        else:
            ctx.update(shareholding_period=None, promoter_pledge_pct=None, total_shares=None)
        close, shares = ctx.get("close"), ctx.get("total_shares")
        ctx["market_cap_cr"] = close * shares / 1e7 if close is not None and shares else None
        if stamp.date() >= SURVEILLANCE_FROM:
            recent = [d for d in self.surv_days if d <= market_date][-1:]
            ctx["surveillance_known"] = bool(recent) and (market_date - recent[0]).days <= 7
            ctx["asm_gsm"] = bool(recent) and any(s in self.surv[recent[0]] for s in self.symbols[sid])
        else:
            ctx["surveillance_known"] = not self.strict_surveillance
        ctx["instrument_id"] = self.iid[sid]
        history = []
        for s in self.symbols[sid]:
            history.extend(self.fin.get(s, ()))
        snapshot, decision = evaluate_inputs(history, ctx, stamp, self.cfg)
        self._a_cache[key] = (snapshot, decision)
        return snapshot, decision

    def _bisect(self, day):
        import bisect
        return bisect.bisect_right(self.sessions, day) - 1

    # ======================================================================== Module B
    def module_b(self, sid, day):
        """Universe filters + live Setup 1 signal / watchlist after the close of ``day``."""
        t = self.cfg["technical"]
        r, i = self.row(sid, day)
        if r is None or r.series != "EQ":
            return None, None, ("no_bar_or_not_eq",)
        f = self.F[sid]
        n = int(t["min_history_days"])
        if i + 1 < n or f.session_pos.iloc[i] - f.session_pos.iloc[i - n + 1] != n - 1:
            return None, None, ("price_history_missing_sessions",)
        rej = []
        if not all(math.isfinite(float(r[k])) for k in ("avg_volume20", "worst_volume20", "avg_turnover20", "adr20_pct")):
            return None, None, ("indicators_unavailable",)
        if r.raw_close <= float(t["min_price"]):
            rej.append("price")
        if r.avg_volume20 <= float(t["min_avg_volume_20d"]):
            rej.append("avg_volume_20d")
        if r.worst_volume20 <= float(t["min_worst_day_volume_20d"]):
            rej.append("worst_volume_20d")
        cr = r.avg_turnover20 / 1e7
        if cr < float(t["min_avg_turnover_20d_cr"]):
            rej.append("turnover_low")
        if cr > float(t["max_avg_turnover_20d_cr"]):
            rej.append("turnover_high")
        if r.adr20_pct < float(t["min_adr_20d_pct"]):
            rej.append("adr")
        if (r.high_52w - r.close) / r.high_52w * 100 > float(t["max_distance_from_52w_high_pct"]):
            rej.append("distance_52w_high")
        if self.is_nifty50(sid, day):
            rej.append("nifty50_excluded")
        bn = int(t["earnings_blackout_trading_days"])
        fut = self.forward(day, bn)
        if len(fut) < bn:
            rej.append("earnings_calendar_market_dates_unavailable")
        elif self.results_within(sid, [day, *fut], end_of_day(day)):
            rej.append("earnings_blackout")
        cap = self.cfg["backtest"].get("research_adr_upper_cap")
        if cap is not None and r.adr20_pct > float(cap):
            rej.append("research_adr_upper_cap")
        if rej:
            return None, None, tuple(rej)
        signal = None
        days = int(t["entry_window_days"])
        for sdate, payload in reversed(self.signals[sid]):
            if sdate <= day:
                age = self.pos[day] - self.pos[sdate]
                if age < days:
                    signal = {**payload, "signal_date": str(sdate), "age_trading_sessions": age,
                              "status": "SIGNAL_CREATED" if age == 0 else "ACTIVE"}
                break
        return signal, self.watch[sid].get(day), ()

    # ======================================================================== Module E
    def _index_on(self, code, day, n):
        s = self.index.get(code)
        if s is None:
            return None
        s = s[s.index <= day]
        need = [d for d in self.sessions if d <= day][-n:]
        if len(need) < n or not set(need).issubset(s.index):
            return None
        return s.reindex(need)

    def breadth(self, day):
        """Module E breadth over the liquid EQ universe (vectorised verified_breadth)."""
        W, p = self.W, self.pos[day]
        if p < 11:
            return None
        floor = float(self.cfg["regime"]["breadth_min_turnover_cr"]) * 1e7
        n_ok = W["nrows"][p] >= 260
        # 20 consecutive sessions present (rows counter advanced by exactly 19 over the last 19 sessions)
        consec = (W["nrows"][p] - W["nrows"][p - 19]) == 19 if p >= 19 else np.zeros_like(n_ok)
        live = (n_ok & consec & W["eq"][p] & (W["avg_turnover20"][p] >= floor)
                & np.isfinite(W["dma50"][p]) & np.isfinite(W["ema21"][p]))
        n = int(live.sum())
        if n == 0:
            return None
        c = W["close"][:, live]
        above50 = int((c[p] > W["dma50"][p, live]).sum())
        above21 = int((c[p] > W["ema21"][p, live]).sum())
        d = np.diff(c[p - 10:p + 1], axis=0)
        adv, dec = int(np.nansum(d > 0)), int(np.nansum(d < 0))
        net = []
        for off in range(6):
            hi = int((W["high"][p - off, live] >= W["high_52w"][p - off, live]).sum())
            lo = int((W["low"][p - off, live] <= W["low_52w"][p - off, live]).sum())
            net.append((hi - lo) / n * 100)
            if off == 0:
                highs, lows = hi / n * 100, lo / n * 100
        valid = int(W["watch"][p, live].sum())
        return dict(breadth_above_50dma_pct=above50 / n * 100, breadth_above_21ema_pct=above21 / n * 100,
                    advance_decline_10d=adv / dec if dec else 2.0 if adv else 1.0,
                    new_highs_pct=highs, new_lows_pct=lows, net_highs_trend_pp=net[0] - sum(net[1:]) / 5,
                    valid_rallies_pct=valid / n * 100)

    def module_e(self, day):
        mid = self._index_on("NIFTYMIDSMALLCAP400", day, 205)
        if mid is None:
            mid = self._index_on("NIFTY500", day, 205)
            if mid is not None:
                self.notes["regime_used_nifty500_fallback_days"] += 1
        vix = self._index_on("INDIAVIX", day, 6)
        breadth = self.breadth(day)
        if mid is None or vix is None or breadth is None:
            self.notes["regime_inputs_unavailable_days"] += 1
            return None
        dma200 = mid.rolling(200).mean()
        closed, _ = round_trips(SimpleNamespace(fills=self.broker.fills, events=self.broker.events))
        recent = closed[-15:]
        bad = len(recent) >= 10 and any(t["r_multiple"] is None for t in recent)
        exp = (sum(t["r_multiple"] for t in recent) / len(recent)) if len(recent) >= 10 and not bad else None
        inputs = RegimeInputs(
            midcap_close=float(mid.iloc[-1]), midcap_dma50=float(mid.iloc[-50:].mean()),
            midcap_dma200=float(dma200.iloc[-1]),
            midcap_dma200_slope_pct=(float(dma200.iloc[-1]) / float(dma200.iloc[-6]) - 1) * 100,
            india_vix=float(vix.iloc[-1]), india_vix_5d_change_pct=(float(vix.iloc[-1]) / float(vix.iloc[-6]) - 1) * 100,
            strategy_expectancy_r=exp, closed_trades=len(recent), **breadth)
        prev = max((d for d in self.sessions if d < day), default=None)
        if self.tracker.last_session != prev:
            self.tracker = RegimeTracker()
        return self.tracker.update(inputs, self.cfg, session=day, require_full_window=True)

    def on_close(self, day, broker):
        decision = self.module_e(day)
        if decision is None or not decision.data_ready:
            self.capacity = 0
            self.regimes.append({"date": day, "band": "UNAVAILABLE", "capacity": 0, "score": None})
            return
        self.capacity = decision.max_open_trades
        self.regimes.append({"date": day, "band": decision.band, "capacity": decision.max_open_trades,
                             "score": decision.score})

    # ======================================================================== Module C
    def bench_returns(self, day):
        s = self._index_on("NIFTY500", day, 127)
        if s is None:
            return {}
        return {n: (float(s.iloc[-1]) / float(s.iloc[-1 - n]) - 1) * 100 for n in (63, 126)}

    def allocation_state(self):
        counts, held, risk, reserved, pending, opened = Counter(), set(), 0.0, 0.0, 0, 0
        nav = self.nav()
        for instrument, p in self.broker.positions.items():
            if p.state in {TradeState.CLOSED, TradeState.CANCELLED}:
                continue
            info = self.last_analysis.get(instrument)
            if not info:
                continue
            counts[info["sector"]] += 1
            held.add(instrument)
            if p.state == TradeState.PENDING_ENTRY:
                pending += 1
                size = size_position(nav, p.signal_high, p.signal_low, **{
                    k: self.cfg["risk"][k] for k in ("risk_per_trade_pct", "max_position_pct",
                                                     "raw_stop_buffer_pct", "max_stop_risk_pct")})
                adm = self.admissions.get((instrument, p.signal_date))
                shares = min(size.shares, adm["shares"]) if adm else size.shares
                risk += shares * (p.signal_high - size.stop)
                reserved += shares * p.signal_high + info.get("estimated_entry_costs", 0)
            else:
                opened += 1
                risk += max(0, (p.entry - p.stop) * p.remaining_shares)
        return AllocationState(opened, pending, risk / nav * 100, max(0, self.broker.cash - reserved),
                               dict(counts), nav, ()), held

    def nav(self):
        b = self.broker
        return b.cash + sum(p.remaining_shares * b.last_prices.get(i, p.entry or 0) for i, p in b.positions.items())

    def generate(self, day):
        """After-close Module C order sheet for the next session (as ModuleCPipeline.generate)."""
        stamp = end_of_day(day)
        bench = self.bench_returns(day)
        reentries = {}
        for instrument, p in self.broker.positions.items():
            if reentry_allowed(p):
                reentries[instrument] = {**self.metadata.get(instrument, {}), "setup": "SETUP_1", "status": "ACTIVE",
                                         "signal_date": p.reentry_signal_date.isoformat(),
                                         "signal_high": p.reentry_signal_high, "signal_low": p.reentry_signal_low}
        ready = []
        todays = set(self.daily_candidates.get(self.pos[day], ())) | {self.sid_of[i] for i in reentries}
        for sid in sorted(todays):
            instrument = self.iid[sid]
            signal, watch, rej = self.module_b(sid, day)
            if rej:
                continue
            if instrument in reentries:
                signal, watch = dict(reentries[instrument]), None
            if not signal and not watch:
                continue
            snap, dec = self.module_a(sid, stamp)
            if not dec.eligible:
                self.rejections.update("A:" + x for x in (dec.failed_rules or ()))
                continue
            sector = snap.sector
            if not sector:
                self.rejections["sector_unavailable_as_of"] += 1
                continue
            if not bench:
                self.rejections["nifty500_history_unavailable"] += 1
                continue
            info = dict(signal or watch)
            f = self.F[sid]
            r, i = self.row(sid, day)
            frame = f.iloc[:i + 1]
            sdate = date.fromisoformat(info["signal_date"]) if signal else day
            try:
                stats = measure_features(frame, sdate, bench)
            except ValueError as exc:
                self.rejections[str(exc)] += 1
                continue
            row = {"instrument_id": instrument, "sector": sector, "kind": "SIGNAL" if signal else "WATCHLIST",
                   "signal": info, "stats": stats}
            self.last_analysis[instrument] = {**self.last_analysis.get(instrument, {}), **row}
            ready.append({"sid": sid, "row": row, "stats": stats, "signal": info, "decision": dec})
        if not ready:
            return []
        state, held = self.allocation_state()
        rel = {r["row"]["instrument_id"]: (r["stats"]["relative_63"] + r["stats"]["relative_126"]) / 2 for r in ready}
        rel_rank = _percentile_map(rel)
        by_sector = defaultdict(list)
        for r in ready:
            by_sector[r["row"]["sector"]].append(rel[r["row"]["instrument_id"]])
        sec_rank = _percentile_map({s: median(v) for s, v in by_sector.items()})
        candidates, rows = [], {}
        risk_keys = ("risk_per_trade_pct", "max_position_pct", "raw_stop_buffer_pct", "max_stop_risk_pct")
        for v in ready:
            row, stats, signal = v["row"], v["stats"], v["signal"]
            inst = row["instrument_id"]
            try:
                inputs = ranking_inputs(signal, stats, v["decision"].score, bool(v["decision"].triggers),
                                        rel_rank[inst], sec_rank[row["sector"]], self.cfg)
                score = rank_candidate(inputs, self.cfg)
                row.update(score=score.score, qualified=score.qualified)
                self.last_analysis[inst].update(score=score.score)
                if row["kind"] == "WATCHLIST":
                    continue
                if not float(signal["signal_high"]) > float(signal["signal_low"]) > 0:
                    # A circuit-locked candle (high == low) cannot define a buy-stop above its stop;
                    # the shared trade engine refuses it, so reject it here with an explicit reason.
                    raise ValueError("invalid_signal_range")
                sdate = date.fromisoformat(signal["signal_date"])
                age = self.pos[day] - self.pos[sdate] + 1
                expiry = self.forward(sdate, int(self.cfg["technical"]["entry_window_days"]))
                if len(expiry) != int(self.cfg["technical"]["entry_window_days"]):
                    raise ValueError("entry_calendar_unavailable")
                pos = size_position(state.nav or 0, float(signal["signal_high"]), float(signal["signal_low"]),
                                    **{k: float(self.cfg["risk"][k]) for k in risk_keys})
                entry = float(signal["signal_high"])
                risk = pos.shares * (entry - pos.stop)
                costs = estimate_delivery_costs(trade_date=expiry[min(max(age - 1, 0), len(expiry) - 1)], side="BUY",
                                                price=entry, quantity=pos.shares, cfg=self.cfg["backtest"]).total \
                    if pos.shares else 0.0
                nxt = self.forward(day, 1)
                order_day = nxt[0] if nxt else day
                bn = int(self.cfg["technical"]["earnings_blackout_trading_days"])
                dates = [order_day, *self.forward(order_day, bn)] if nxt else []
                clear = len(dates) == bn + 1 and not self.results_within(v["sid"], dates, stamp)
                cand = Candidate(inst, row["sector"], True, clear, age, stats["adr20"], entry * pos.shares,
                                 stats["adtv"], inst in held, pos.shares, (entry - pos.stop) / entry * 100,
                                 inputs, risk, costs)
                candidates.append(cand)
                row.update(buy_stop=entry, stop=pos.stop, shares=pos.shares, rupee_risk=risk,
                           estimated_entry_costs=costs)
                self.last_analysis[inst].update(row)
                rows[inst] = row
            except (ValueError, KeyError) as exc:
                self.rejections[str(exc)] += 1
        if not candidates:
            return []
        alloc = allocate_candidates(candidates, state, self.capacity, self.cfg)
        for inst, reasons in alloc.rejected.items():
            self.rejections.update(reasons)
        self.decisions.append({"date": day, "ready": len(ready), "candidates": len(candidates),
                               "selected": len(alloc.selected)})
        out = []
        for c in alloc.selected:
            row = rows[c.instrument_id]
            info = row["signal"]
            self.metadata[c.instrument_id] = info
            self.admissions[(c.instrument_id, date.fromisoformat(info["signal_date"]))] = {"day": day,
                                                                                         "shares": int(row["shares"])}
            out.append(Signal(c.instrument_id, date.fromisoformat(info["signal_date"]), float(info["signal_high"]),
                              float(info["signal_low"]), float(row["score"]),
                              (float(row["buy_stop"]) - float(row["stop"])) / float(row["buy_stop"]) * 100,
                              float(row["stats"]["adtv"]), c.instrument_id in reentries))
        return out

    # ======================================================================== broker hooks
    def bars(self, day):
        out = []
        for instrument, p in self.broker.positions.items():
            if p.state in {TradeState.CLOSED, TradeState.CANCELLED}:
                continue
            sid = self.sid_of[instrument]
            r, i = self.row(sid, day)
            if r is None or i < 22:
                continue
            prev = self.F[sid].iloc[i - 1]
            vals = [float(r[k]) for k in ("open", "high", "low", "close", "ema9")]
            if not all(math.isfinite(x) and x > 0 for x in vals + [float(prev.ema9), float(prev.ema21)]):
                continue
            out.append(MarketBar(instrument, day, *vals, float(prev.ema9), float(prev.ema21)))
        return out

    def on_open(self, day, broker, bars):
        for sym, cash in self.dividends.get(day, ()):
            for instrument, p in broker.positions.items():
                if p.state in {TradeState.CLOSED, TradeState.CANCELLED} or not p.remaining_shares:
                    continue
                sid = self.sid_of[instrument]
                if sym not in self.symbols[sid]:
                    continue
                r, _ = self.row(sid, day)
                adj = float(r.close / r.raw_close) if r is not None and r.raw_close else 1.0
                income = p.remaining_shares * cash * adj   # shares are in back-adjusted units
                broker.cash += income
                broker.realized_gross_pnl += income
                p.realized_pnl += income
                self.notes["dividends_credited"] += 1

    def eligibility(self, instrument, day, *, at_open=True):
        stamp = datetime.combine(day, time(9, 15), tzinfo=IST) if at_open else end_of_day(day)
        return self.module_a(self.sid_of[instrument], stamp)[1].eligible

    def entry_guard(self, position, bar, quantity):
        adm = self.admissions.get((position.instrument_id, position.signal_date))
        if not adm or adm["day"] >= bar.trade_date:
            return 0
        quantity = min(quantity, adm["shares"])
        info = self.last_analysis.get(position.instrument_id)
        if not info or not info.get("stats") or float(info.get("score", 0)) < float(self.cfg["ranking"]["minimum_score"]):
            return 0
        bn = int(self.cfg["technical"]["earnings_blackout_trading_days"])
        window = [bar.trade_date, *self.forward(bar.trade_date, bn)]
        if len(window) != bn + 1 or self.results_within(self.sid_of[position.instrument_id], window,
                                                        datetime.combine(bar.trade_date, time(9, 15), tzinfo=IST)):
            return 0
        risk, sector_count = 0.0, 0
        for instrument, p in self.broker.positions.items():
            if instrument == position.instrument_id or p.state in {TradeState.CLOSED, TradeState.CANCELLED}:
                continue
            other = self.last_analysis.get(instrument)
            if not other:
                return 0
            sector_count += other["sector"].strip().casefold() == info["sector"].strip().casefold()
            risk += max(0, (p.entry - p.stop) * p.remaining_shares) if p.remaining_shares else float(other.get("rupee_risk", 0))
        if sector_count >= self.cfg["risk"]["max_positions_per_sector"]:
            return 0
        budget = max(0, position.capital * self.cfg["risk"]["max_portfolio_heat_pct"] / 100 - risk)
        by_heat = int(budget / (position.entry - position.stop))
        by_liq = int(float(info["stats"]["adtv"]) * 0.02 / position.entry)
        return max(0, min(quantity, by_heat, by_liq))

    def pending_rank(self, position, day):
        info = self.last_analysis.get(position.instrument_id, {})
        if info.get("stats") and info.get("score") is not None and info.get("buy_stop") and info.get("stop") is not None:
            return (float(info["score"]), (float(info["buy_stop"]) - float(info["stop"])) / float(info["buy_stop"]) * 100,
                    float(info["stats"]["adtv"]))
        return None

    # ======================================================================== run
    def run(self, progress=None):
        self.broker = EventDrivenBacktester(cfg=self.cfg)
        self.tracker = RegimeTracker()
        self.capacity = int(self.cfg["risk"]["max_open_trades"])
        self.last_analysis, self.metadata, self.admissions = {}, {}, {}
        self.rejections, self.regimes, self.decisions = Counter(), [], []
        self._a_cache = {}
        days = [d for d in self.sessions if self.start <= d <= self.end]
        if not days:
            raise ValueError("No sessions in the selected period")
        for d in [d for d in self.sessions if d < self.start][-5:]:
            self.on_close(d, self.broker)

        def close(day, broker):
            self.on_close(day, broker)
            if len(self._a_cache) > 200_000:
                self._a_cache.clear()
            if progress and day in days:
                k = days.index(day)
                if k % 20 == 0:
                    progress(k / len(days), f"Replayed {day}")

        result = self.broker.run(
            [], self.generate, eligibility_provider=lambda i, d: self.eligibility(i, d),
            signal_eligibility_provider=lambda i, d: self.eligibility(i, d, at_open=False),
            capacity_provider=lambda d: self.capacity, close_capacity_provider=lambda d: self.capacity,
            allow_reentry=True, trading_sessions=self.sessions, bar_provider=self.bars,
            session_bounds=(self.start, self.end), on_session_open=self.on_open, on_session_close=close,
            entry_guard=self.entry_guard, allow_live_signals=True, auto_reentry=False,
            pending_rank_provider=self.pending_rank)
        return result

    def report(self, result) -> dict:
        closed, active = round_trips(result)
        perf = metrics(result, SimpleNamespace(start=self.start, end=self.end))
        curve, peak = [], result.initial_capital
        for day, eq in result.equity_curve:
            peak = max(peak, eq)
            curve.append({"date": day.isoformat(), "equity": eq, "drawdown_pct": (1 - eq / peak) * 100})
        fin_cover = sum(1 for v in self.fin.values() if v)
        limitations = [
            "Replayed from public data in the local folder, not the Design Experience database. Decisions call the "
            "same Module A/B/C/E functions and trade engine as the PMS replay.",
            "Prices are NSE bhavcopies back-adjusted for splits/bonuses inferred from opening gaps; Rs 30 price, "
            "volume and market-cap tests use unadjusted levels. Cash dividends are credited on the ex-date when "
            "NSE's action text states an amount.",
            "Sector is the company's current NSE industry (or historical sectoral-index membership) applied to all "
            "dates; historical sector changes are not modelled.",
            ("Surveillance (ASM/GSM) is enforced from 23 Jan 2024, when NSE's daily file starts. Earlier dates are "
             "treated as clear." if not self.strict_surveillance else
             "Strict surveillance: dates before 23 Jan 2024 fail Module A (no historical ASM/GSM file)."),
            "Fundamentals come from NSE result XBRL. Older filings without XBRL are missing, so early years have "
            "fewer eligible companies; annual cash flow (OCF/PAT) is only in results XBRL from FY2019-20.",
            "Shareholding/pledge history is a public reconstruction of NSE SHP XBRL filings (aditya-jha/"
            "nse-historical-membership); Nifty 50 exclusion uses that project's membership intervals.",
            "Results-date blackout uses NSE board-meeting announcements; meetings without a timestamp are assumed "
            "known five days ahead.",
            "Intraday filing-order checks and held-stock delisting exits are not modelled; daily bars use "
            "conservative same-day stop handling.",
        ]
        payload = {
            "title": "DEEX PMS Setup 1 Backtest (local data)", "status": "COMPLETED" if closed else "NO_TRADES",
            "acceptance": "EXPLORATORY_NOT_CERTIFIED", "strategy_id": "PMS_SETUP1", "mode": "FULL_PMS_LOCAL",
            "mode_label": "PMS Setup 1 · A + B + C + E (local data)",
            "parameters": {"start": str(self.start), "end": str(self.end),
                           "capital": self.cfg["backtest"]["initial_capital"]},
            "metrics": perf, "equity_curve": curve, "trades": closed, "open_positions": list(active.values()),
            "fills": [asdict(f) for f in result.fills],
            "diagnostics": {"limitations": limitations, "rejections": dict(self.rejections.most_common(40)),
                            "regime_summary": dict(Counter(str(r["capacity"]) for r in self.regimes if r["date"] >= self.start)),
                            "allocation_days": len(self.decisions), "notes": dict(self.notes),
                            "securities": len(self.F), "companies_with_fundamentals": fin_cover},
            "configuration": self.cfg, "execution_enabled": False,
        }
        payload = plain(payload)
        payload["report_fingerprint"] = fingerprint(payload)
        return payload
