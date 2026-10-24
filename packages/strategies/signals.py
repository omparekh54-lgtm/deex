"""Signal rules for the ten strategies (S1-S10). Signals use data up to and including the close of
``date``; orders execute on later sessions. Ported unchanged from the published backtests."""
from __future__ import annotations

import numpy as np
import pandas as pd

from packages.strategies.engine import P

# Shared "house" engine for S2, S3, S4, S5, S9: next-open entry, fixed 10% stop, 60-session exit.
HOUSE = P(risk=0.0075, pos_cap=0.12, max_pos=10, fixed_stop=0.10, max_risk=0.10, window=1, max_hold=60)
S7P = P(risk=0.0075, pos_cap=0.12, max_pos=10, max_risk=0.12, window=1)
S8P = P(risk=0.01, pos_cap=0.15, max_pos=10, max_risk=0.15, window=1)
ETFS = ["NIFTYBEES", "JUNIORBEES", "MID150BEES", "BANKBEES", "PSUBNKBEES", "ITBEES", "PHARMABEES",
        "CPSEETF", "GOLDBEES", "SILVERBEES"]


def _stack(df):
    try:
        return df.stack(future_stack=True)
    except TypeError:  # pragma: no cover - older pandas
        return df.stack(dropna=False)


def to_sig(mask, level, stop, score, start, mode="stop"):
    m = mask.fillna(False).astype(bool)
    m = m.loc[(m.index >= start - pd.Timedelta(days=10))]
    st = _stack(m)
    st = st[st]
    idx = st.index
    f = lambda x: _stack(x).reindex(idx).values if isinstance(x, pd.DataFrame) else np.full(len(idx), x)
    s = pd.DataFrame({"date": idx.get_level_values(0), "sid": idx.get_level_values(1),
                      "level": f(level), "stop": f(stop), "score": f(score), "mode": mode})
    return s.dropna(subset=["stop", "score"] + (["level"] if mode == "stop" else []))


def rsi(Cf, n):
    dlt = Cf.diff()
    up = dlt.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-dlt.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def common(mkt):
    if getattr(mkt, "_cm", None) is not None:
        return mkt._cm
    C, Cf = mkt.C, mkt.Cf
    r63, r126, r252 = Cf / Cf.shift(63) - 1, Cf / Cf.shift(126) - 1, Cf / Cf.shift(252) - 1
    U = mkt.univ
    rs = (0.4 * r63 + 0.3 * r126 + 0.3 * r252).where(U).rank(axis=1, pct=True) * 100
    s50, s150, s200 = mkt.sma(50), mkt.sma(150), mkt.sma(200)
    tt = (C > s50) & (s50 > s150) & (s150 > s200) & (s200 > s200.shift(20)) & \
         (C >= 1.25 * mkt.lo252) & (C >= 0.75 * mkt.hi252)
    mkt._cm = dict(rs=rs, tt=tt & U)
    return mkt._cm


def _rebalance_dates(mkt, start):
    reb = pd.Series(mkt.dates, index=mkt.dates).groupby(mkt.dates.to_period("M")).last()
    return reb[reb >= start - pd.Timedelta(days=35)]


def _rotation(mkt, score, start, top=20, buffer=40):
    frac_s = mkt.regime().frac
    W, held = {}, []
    for dt in _rebalance_dates(mkt, start):
        s = score.loc[dt].dropna().sort_values(ascending=False)
        rank = {k: i + 1 for i, k in enumerate(s.index)}
        keep = [h for h in held if rank.get(h, 9e9) <= buffer]
        new = [k for k in s.index if k not in keep][: max(top - len(keep), 0)]
        held = keep + new
        f = frac_s.loc[dt]
        gross = 1.0 if f >= 0.6 else (0.5 if f >= 0.3 else 0.0)
        W[dt] = {k: gross / top for k in held} if gross > 0 else {}
        if gross == 0:
            held = []
    return pd.DataFrame.from_dict(W, orient="index").fillna(0.0)


# ------------------------------------------------------------------ rotations
def s1_momentum(mkt, start, top=20, buffer=40):
    C, Cf = mkt.C, mkt.Cf
    U = mkt.univ & (mkt.adtv20 >= 1e8)
    r12_1 = Cf.shift(21) / Cf.shift(252) - 1
    vol = np.log(Cf).diff().rolling(252, min_periods=200).std() * np.sqrt(252)
    score = (r12_1 / vol).where(U & (C > mkt.sma(200)) & (C >= 0.75 * mkt.hi252))
    return _rotation(mkt, score, start, top, buffer)


def s6_lowvol(mkt, start, top=20, n_liquid=300):
    C, Cf = mkt.C, mkt.Cf
    U = mkt.univ & (mkt.adtv20.rank(axis=1, ascending=False) <= n_liquid)
    vol = np.log(Cf).diff().rolling(252, min_periods=200).std()
    r126 = Cf / Cf.shift(126) - 1
    score = (-vol).where(U & (C > mkt.sma(200)) & (r126 > 0))
    return _rotation(mkt, score, start, top)


def s10_etf(mkt, start, top=2):
    cols = [e for e in ETFS if e in mkt.etf["close"].columns]
    C = mkt.etf["close"][cols].reindex(mkt.dates)
    O = mkt.etf["open"][cols].reindex(mkt.dates)
    Cf = C.ffill()
    sc = (Cf / Cf.shift(63) + Cf / Cf.shift(126) + Cf / Cf.shift(252)) / 3 - 1
    ok = (Cf > Cf.rolling(200, min_periods=200).mean()) & (sc > 0)
    W = {}
    for dt in _rebalance_dates(mkt, start):
        s = sc.loc[dt].where(ok.loc[dt]).dropna().sort_values(ascending=False)
        W[dt] = {k: 1.0 / top for k in list(s.index[:top])}
    w = pd.DataFrame.from_dict(W, orient="index").fillna(0.0)
    return w, O, C


# ------------------------------------------------------------------ swing setups
def s2_base_breakout(mkt, start, L=25, depth=0.15):
    cm = common(mkt)
    C, H, Lo, V = mkt.C, mkt.H, mkt.L, mkt.V
    pivot = H.shift(1).rolling(L).max()
    base_low = Lo.shift(1).rolling(L).min()
    dep = 1 - base_low / pivot
    rng = (C - Lo) / (H - Lo).replace(0, np.nan)
    m = cm["tt"] & (cm["rs"] >= 70) & (dep <= depth) & (pivot >= 0.95 * mkt.hi252.shift(1)) & \
        (C > pivot) & (C.shift(1) <= pivot) & (V >= 1.5 * mkt.vol50) & (rng >= 0.6) & (C <= pivot * 1.06)
    stop = np.minimum(Lo, pivot) * 0.99
    score = cm["rs"] + 10 * (V / mkt.vol50).clip(upper=4)
    return to_sig(m, H * 1.001, stop, score, start).assign(mode="open")


def s3_vcp(mkt, start):
    cm = common(mkt)
    C, H, Lo, V = mkt.C, mkt.H, mkt.L, mkt.V
    h10, l10 = H.rolling(10).max(), Lo.rolling(10).min()
    h50, l50 = H.rolling(50).max(), Lo.rolling(50).min()
    tight = (h10 - l10) / (h50 - l50).replace(0, np.nan)
    m = cm["tt"] & (cm["rs"] >= 75) & (tight <= 0.35) & ((h10 / l10 - 1) <= 0.10) & \
        (C >= 0.95 * h10) & (V.rolling(10).mean() <= 0.85 * mkt.vol50) & (h10 >= 0.9 * mkt.hi252)
    return to_sig(m, h10 * 1.002, l10 * 0.995, cm["rs"] - 100 * tight, start).assign(mode="open")


def s4_episodic_pivot(mkt, start):
    C, O, H, Lo, V, Cf = mkt.C, mkt.O, mkt.H, mkt.L, mkt.V, mkt.Cf
    pc = Cf.shift(1)
    U = mkt.EQ & (C >= 30) & (mkt.adtv20.shift(1) >= 2e7) & (mkt.age >= 120)
    rng = (C - Lo) / (H - Lo).replace(0, np.nan)
    quiet = (pc / Cf.shift(61) - 1) <= 0.30
    m = U & (O >= 1.04 * pc) & (C >= 1.07 * pc) & (rng >= 0.6) & (V >= 3 * mkt.vol50.shift(1)) & quiet & \
        (C > mkt.sma(200))
    score = (V / mkt.vol50.shift(1)).clip(upper=20) + 100 * (C / pc - 1)
    return to_sig(m, H * 1.001, Lo * 0.995, score, start).assign(mode="open")


def s5_leader_pullback(mkt, start):
    cm = common(mkt)
    C, O, H, Lo, V = mkt.C, mkt.O, mkt.H, mkt.L, mkt.V
    s50 = mkt.sma(50)
    recent_high = H.rolling(40).max()
    depth = 1 - Lo / recent_high
    m = cm["tt"] & (cm["rs"] >= 80) & (recent_high >= 0.98 * mkt.hi252) & \
        (Lo <= s50 * 1.02) & (C >= s50) & (C > O) & (depth >= 0.08) & (depth <= 0.25) & \
        (V.rolling(5).mean() <= mkt.vol50) & (s50 > s50.shift(10))
    stop = np.minimum(Lo, s50) * 0.98
    return to_sig(m, H * 1.001, stop, cm["rs"] + 50 * (1 - depth), start).assign(mode="open")


def s7_donchian(mkt, start, rsmin=80, n_in=55, n_out=20):
    cm = common(mkt)
    C, Cf = mkt.C, mkt.Cf
    U = mkt.univ & (mkt.adtv20 >= 1e8)
    hh = Cf.shift(1).rolling(n_in).max()
    ll = Cf.shift(1).rolling(n_out).min()
    m = U & (C > hh) & (Cf.shift(1) <= hh.shift(1)) & (C > mkt.sma(200)) & (mkt.sma(50) > mkt.sma(200)) & \
        (cm["rs"].fillna(0) >= rsmin)
    return to_sig(m, None, C - 2.5 * mkt.atr, cm["rs"].fillna(0), start, mode="open"), C < ll


def s8_stage2(mkt, start):
    C, H, Lo, V, Cf = mkt.C, mkt.H, mkt.L, mkt.V, mkt.Cf
    wk = pd.Series(mkt.dates, index=mkt.dates).groupby(mkt.dates.to_period("W-FRI")).transform("max") == mkt.dates
    wk_end = pd.Series(wk.values, index=mkt.dates)
    wH, wL, wV = H.rolling(5).max(), Lo.rolling(5).min(), V.rolling(5).sum()
    hh26 = wH.shift(5).rolling(125).max()
    vavg10 = wV.shift(5).rolling(50).mean()
    ma30 = mkt.sma(150)
    mid = mkt.regime_series()
    mrs = (Cf / Cf.shift(130)).div(mid / mid.shift(130), axis=0) - 1
    U = mkt.univ & (mkt.adtv20 >= 7.5e7)
    m = U & (C > hh26) & (C > ma30) & (ma30 > ma30.shift(20)) & (wV >= 1.5 * vavg10) & (mrs > 0)
    m = m.mul(wk_end, axis=0).astype(bool)
    stop = wL.shift(5).rolling(40).min() * 0.99
    ex = (C < ma30).mul(wk_end, axis=0).astype(bool)
    return to_sig(m, None, stop, mrs * 100, start, mode="open"), ex


def s9_mom_dip(mkt, start, rsmin=90, thr=10):
    cm = common(mkt)
    C = mkt.C
    r2 = rsi(mkt.Cf, 2)
    m = cm["tt"] & (cm["rs"] >= rsmin) & (r2 < thr) & (C > mkt.sma(50))
    return to_sig(m, None, C * 0.9, cm["rs"], start, mode="open")
