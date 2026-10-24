"""Portfolio simulators for the strategy library.

``simulate``      event-style swing engine (buy-stops / next-open entries, stops, exits, regime capacity)
``rebalance_sim`` periodic-rotation engine (target weights traded at the next open)

The trading logic is a line-for-line port of the code used for the published backtests; this module
only adds fill-level records (price, shares, costs, reason) and open-position reporting.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# Delivery costs per side as a fraction of traded value: STT 0.1% each side, stamp duty 0.015% on
# buys, exchange + SEBI fees, GST, DP charge, plus 0.15% assumed slippage / impact each side.
BUY_COST = 0.0010 + 0.00015 + 0.00004 + 0.00005 + 0.0015     # ~0.274%
SELL_COST = 0.0010 + 0.00004 + 0.00005 + 0.0002 + 0.0015     # ~0.279%


@dataclass
class P:
    """Trade-engine parameters."""
    risk: float = 0.0075
    pos_cap: float = 0.12
    max_pos: int = 10
    max_risk: float = 0.08
    liq_cap: float = 0.02
    window: int = 3
    use_regime: bool = True
    capital: float = 1e7
    trail: str | None = None
    trail_arm: bool = True
    partial_R: float | None = None
    partial_frac: float = 0.5
    trail2: str | None = None
    chandelier: float | None = None
    time_stop: int | None = None
    time_min: float = 0.0
    max_hold: int | None = None
    target: float | None = None
    be_after_R: float | None = None
    fixed_stop: float | None = None


def _fill(sym, day, side, price, qty, reason):
    cost = qty * price * (BUY_COST if side == "BUY" else SELL_COST)
    return {"instrument_id": sym, "trade_date": day.date().isoformat(), "side": side, "price": float(price),
            "quantity": int(qty), "reason": reason, "costs": {"total": float(cost)}}


def simulate(mkt, sig: pd.DataFrame, p: P, start, end, reg=None, exit_mat=None, check=None):
    dates = mkt.dates
    di = {x: i for i, x in enumerate(dates)}
    cols = mkt.C.columns
    ci = {c: i for i, c in enumerate(cols)}
    O, H, L, C = mkt.O.values, mkt.H.values, mkt.L.values, mkt.C.values
    Cf = mkt.Cf.values
    adtv = mkt.adtv20.values
    ma_needed = {x for x in (p.trail, p.trail2) if x}
    MA = {}
    for k in ma_needed:
        kind, n = k[:3], int(k[3:])
        MA[k] = (mkt.ema(n) if kind == "ema" else mkt.sma(n)).values
    ATR = mkt.atr.values if p.chandelier else None
    EX = exit_mat.reindex(index=dates, columns=cols).fillna(False).values.astype(bool) if exit_mat is not None else None
    frac = reg.frac.values if (reg is not None and p.use_regime) else np.ones(len(dates))

    sig = sig[(sig.date >= start - pd.Timedelta(days=5)) & (sig.date < end)].copy()
    sig["t"] = sig.date.map(di)
    sig["j"] = sig.sid.map(ci)
    sig = sig.dropna(subset=["t", "j"])
    sig["t"] = sig.t.astype(int)
    sig["j"] = sig.j.astype(int)
    sig = sig.sort_values(["t", "score"], ascending=[True, False])
    by_t = {t: g for t, g in sig.groupby("t")}

    t0 = int(np.searchsorted(dates, start))
    t1 = int(np.searchsorted(dates, end, side="right"))
    cash = p.capital
    pos, pend = {}, {}
    trades, eq_curve, fills = [], [], []
    exit_next = set()

    def equity(t):
        return cash + sum(q["sh"] * Cf[t, j] for j, q in pos.items())

    def close_pos(j, t, px, why, frac_sell=1.0):
        nonlocal cash
        q = pos[j]
        sh = q["sh"] if frac_sell >= 1 else int(q["sh"] * frac_sell)
        if sh <= 0:
            return
        cash += sh * px * (1 - SELL_COST)
        q["realized"] += sh * px * (1 - SELL_COST)
        q["sh"] -= sh
        fills.append(_fill(mkt.sym[cols[j]], dates[t], "SELL", px, sh, why))
        if q["sh"] == 0:
            pnl = q["realized"] - q["cost"]
            trades.append(dict(instrument=mkt.sym[cols[j]], entry_date=dates[q["t_in"]].date().isoformat(),
                               exit_date=dates[t].date().isoformat(), entry_price=float(q["entry"]),
                               exit_price=float(px), shares=int(q["shares0"]), pnl=float(pnl),
                               return_pct=float(pnl / q["cost"] * 100), r_multiple=float(pnl / q["risk_rs"]),
                               holding_days=int(t - q["t_in"]), exit_reason=why))
            del pos[j]

    for t in range(t0, t1):
        if check and t % 20 == 0:
            check()
        for j in list(exit_next):
            if j not in pos:
                exit_next.discard(j)
                continue
            if np.isnan(O[t, j]):
                if pos[j].get("missing", 0) < 5:
                    continue
                px = Cf[t - 1, j]
            else:
                px = O[t, j]
            close_pos(j, t, px, pos[j].get("pending_why", "rule"), pos[j].get("pending_frac", 1.0))
            if j in pos:
                pos[j].pop("pending_frac", None)
            exit_next.discard(j)

        eq_prev = equity(t - 1)
        for j in list(pend.keys()):
            o = pend[j]
            if t > o["exp"]:
                del pend[j]
                continue
            if np.isnan(O[t, j]):
                continue
            fill = None
            if o["mode"] == "open":
                fill = O[t, j]
            elif O[t, j] >= o["level"]:
                fill = O[t, j]
            elif H[t, j] >= o["level"]:
                fill = o["level"]
            if fill is None:
                continue
            del pend[j]
            stop = fill * (1 - p.fixed_stop) if p.fixed_stop else o["stop"]
            if stop >= fill * 0.995:
                continue
            stop = max(stop, fill * (1 - p.max_risk))
            rps = fill - stop
            sh = int(min(p.risk * eq_prev / rps, p.pos_cap * eq_prev / fill, p.liq_cap * adtv[t - 1, j] / fill,
                         cash / (fill * (1 + BUY_COST))))
            if sh <= 0:
                continue
            cost = sh * fill * (1 + BUY_COST)
            cash -= cost
            fills.append(_fill(mkt.sym[cols[j]], dates[t], "BUY", fill, sh, "entry"))
            pos[j] = dict(sh=sh, shares0=sh, entry=fill, stop=stop, t_in=t, cost=cost, realized=0.0,
                          risk_rs=sh * rps, r0=rps, armed=not p.trail_arm, partial_done=False, hc=fill)
            if L[t, j] <= stop:
                close_pos(j, t, stop, "stop")

        for j in list(pos.keys()):
            q = pos[j]
            if q["t_in"] == t or np.isnan(L[t, j]):
                continue
            if L[t, j] <= q["stop"]:
                px = min(O[t, j], q["stop"])
                close_pos(j, t, px, "stop" if q["stop"] < q["entry"] else "trail/BE stop")
                continue
            if p.target and H[t, j] >= q["entry"] * (1 + p.target):
                px = max(O[t, j], q["entry"] * (1 + p.target))
                close_pos(j, t, px, "target")

        for j, q in pos.items():
            c = C[t, j]
            if np.isnan(c):
                q["missing"] = q.get("missing", 0) + 1
                if q["missing"] >= 5:
                    exit_next.add(j)
                    q["pending_why"] = "suspended"
                continue
            q["hc"] = max(q["hc"], c)
            held = t - q["t_in"]
            why, fr = None, 1.0
            if p.be_after_R and c >= q["entry"] + p.be_after_R * q["r0"]:
                q["stop"] = max(q["stop"], q["entry"])
            if p.chandelier:
                q["stop"] = max(q["stop"], q["hc"] - p.chandelier * ATR[t, j])
            if p.partial_R and not q["partial_done"] and c >= q["entry"] + p.partial_R * q["r0"]:
                why, fr = "partial", p.partial_frac
                q["partial_done"] = True
                q["stop"] = max(q["stop"], q["entry"])
            ma = p.trail2 if (q["partial_done"] and p.trail2) else p.trail
            if ma:
                m = MA[ma][t, j]
                if not q["armed"] and c > m:
                    q["armed"] = True
                if q["armed"] and c < m:
                    why, fr = "trail " + ma, 1.0
            if EX is not None and EX[t, j]:
                why, fr = "signal exit", 1.0
            if p.time_stop and held >= p.time_stop and c < q["entry"] * (1 + p.time_min):
                why, fr = "time stop", 1.0
            if p.max_hold and held >= p.max_hold:
                why, fr = "max hold", 1.0
            if why:
                exit_next.add(j)
                q["pending_why"] = why
                q["pending_frac"] = fr

        cap = int(round(p.max_pos * frac[t]))
        if t in by_t:
            for r in by_t[t].itertuples():
                j = r.j
                if j in pos or j in pend:
                    continue
                if len(pos) + len(pend) >= cap:
                    break
                pend[j] = dict(level=r.level, stop=r.stop, exp=t + p.window, mode=r.mode, score=r.score)
        while pend and len(pos) + len(pend) > cap:
            jmin = min(pend, key=lambda k: pend[k]["score"])
            del pend[jmin]
        e = equity(t)
        eq_curve.append((dates[t], e, len(pos), sum(q["sh"] * Cf[t, j] for j, q in pos.items()) / e))

    last = t1 - 1
    open_pos = [dict(instrument=mkt.sym[cols[j]], entry_date=dates[q["t_in"]].date().isoformat(),
                     shares=int(q["sh"]), entry_price=float(q["entry"]), last_close=float(Cf[last, j]),
                     market_value=float(q["sh"] * Cf[last, j])) for j, q in pos.items()]
    eqs = pd.DataFrame(eq_curve, columns=["date", "equity", "npos", "exposure"]).set_index("date")
    return eqs, trades, fills, open_pos


def rebalance_sim(weights: pd.DataFrame, px_open: pd.DataFrame, px_close: pd.DataFrame, start, end,
                  capital=1e7, names=None, check=None):
    """Rotation engine: weights decided at the close of each row date, traded at the next open."""
    dates = px_close.index
    t0 = int(np.searchsorted(dates, start))
    t1 = int(np.searchsorted(dates, end, side="right"))
    Cf = px_close.ffill().values
    Of = px_open.values
    cols = list(px_close.columns)
    label = (lambda c: names.get(c, c)) if names is not None else (lambda c: c)
    sh = np.zeros(len(cols))
    cash = capital
    wmap = {dates.get_loc(k): v for k, v in weights.iterrows() if k in dates}
    curve, trades, fills, entry_px = [], [], [], {}
    basis, proceeds = {}, {}
    pending = None
    for t in range(t0, t1):
        if check and t % 20 == 0:
            check()
        if pending is not None:
            eq = cash + np.nansum(sh * np.where(np.isnan(Of[t]), Cf[t - 1], Of[t]))
            for k in range(len(cols)):
                tgt = pending.get(cols[k], 0.0)
                px = Of[t, k] if not np.isnan(Of[t, k]) else Cf[t - 1, k]
                if np.isnan(px):
                    continue
                want = int(tgt * eq / px)
                dlt = want - sh[k]
                if dlt == 0:
                    continue
                if np.isnan(Of[t, k]) and dlt > 0:
                    continue
                val = abs(dlt) * px
                if dlt > 0:
                    if val * (1 + BUY_COST) > cash:
                        dlt = int(cash / (px * (1 + BUY_COST)))
                        val = dlt * px
                    if dlt <= 0:
                        continue
                    cash -= val * (1 + BUY_COST)
                    if sh[k] == 0:
                        entry_px[k] = (px, dates[t], t)
                        basis[k], proceeds[k] = 0.0, 0.0
                    basis[k] = basis.get(k, 0.0) + val * (1 + BUY_COST)
                    fills.append(_fill(label(cols[k]), dates[t], "BUY", px, dlt, "rebalance buy"))
                else:
                    cash += val * (1 - SELL_COST)
                    proceeds[k] = proceeds.get(k, 0.0) + val * (1 - SELL_COST)
                    fills.append(_fill(label(cols[k]), dates[t], "SELL", px, -dlt,
                                       "rebalance exit" if want == 0 else "rebalance trim"))
                    if want == 0 and k in entry_px:
                        e0, dt0, ti = entry_px.pop(k)
                        pnl = proceeds.pop(k, 0.0) - basis.pop(k, 0.0)
                        trades.append(dict(instrument=label(cols[k]), entry_date=dt0.date().isoformat(),
                                           exit_date=dates[t].date().isoformat(), entry_price=float(e0),
                                           exit_price=float(px), pnl=float(pnl),
                                           return_pct=float((px / e0 * (1 - SELL_COST) / (1 + BUY_COST) - 1) * 100),
                                           r_multiple=None, holding_days=int(t - ti), exit_reason="rebalance exit"))
                sh[k] += dlt
            pending = None
        if t in wmap:
            pending = {c: w for c, w in wmap[t].items() if w > 0}
        e = cash + np.nansum(sh * Cf[t])
        curve.append((dates[t], e, int((sh > 0).sum()), np.nansum(sh * Cf[t]) / e))
    last = t1 - 1
    open_pos = [dict(instrument=label(cols[k]), entry_date=entry_px[k][1].date().isoformat() if k in entry_px else None,
                     shares=int(sh[k]), entry_price=float(entry_px[k][0]) if k in entry_px else None,
                     last_close=float(Cf[last, k]), market_value=float(sh[k] * Cf[last, k]))
                for k in range(len(cols)) if sh[k] > 0]
    eqs = pd.DataFrame(curve, columns=["date", "equity", "npos", "exposure"]).set_index("date")
    return eqs, trades, fills, open_pos
