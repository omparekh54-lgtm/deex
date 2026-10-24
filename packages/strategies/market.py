"""Price matrices, lazily computed indicators and the regime dial (Module E proxy)."""
from __future__ import annotations

from functools import cached_property

import numpy as np
import pandas as pd

BENCH_SYMBOLS = ["NIFTYBEES", "JUNIORBEES", "MID150BEES", "GOLDBEES", "LIQUIDBEES"]


class Market:
    """Wide date x security matrices built from an adjusted panel.

    Securities whose 20-day median traded value never reaches Rs 1 crore inside the loaded window
    are dropped after liquidity is measured. Every universe used by the strategies requires at least
    that much liquidity, so the drop does not change any signal; it only saves memory.
    """

    def __init__(self, panel: pd.DataFrame):
        p = panel
        self.funds_px = {s: p[p.sym == s].set_index("date").close for s in BENCH_SYMBOLS}
        etf = p[p.fund]
        self.etf = {k: etf.pivot_table(index="date", columns="sym", values=k) for k in ["open", "close", "turnover"]}
        s = p[~p.fund & ~p.rights]
        tv = s.pivot(index="date", columns="sid", values="turnover")
        adtv = tv.rolling(20, min_periods=15).median()
        keep = adtv.columns[(adtv.max() >= 1e7).values]
        s = s[s.sid.isin(keep)]
        piv = lambda c: s.pivot(index="date", columns="sid", values=c).reindex(index=tv.index, columns=keep)
        self.O, self.H, self.L, self.C = piv("open"), piv("high"), piv("low"), piv("close")
        self.V = piv("vol")
        self.TV = tv[keep]
        self.adtv20 = adtv[keep]
        self.EQ = piv("series") == "EQ"
        self.sym = s.groupby("sid").sym.last()
        self.dates = self.C.index
        self.Cf = self.C.ffill()
        self.regime_benchmark = "MID150BEES"
        self.reg = None
        self._ema, self._sma = {}, {}

    # ---- indicators (computed on first use)
    def ema(self, n):
        if n not in self._ema:
            self._ema[n] = self.Cf.ewm(span=n, adjust=False, min_periods=n).mean().where(self.C.notna())
        return self._ema[n]

    def sma(self, n):
        if n not in self._sma:
            self._sma[n] = self.Cf.rolling(n, min_periods=n).mean()
        return self._sma[n]

    @cached_property
    def atr(self):
        pc = self.Cf.shift(1)
        H, L = self.H, self.L
        tr = pd.concat([(H - L), (H - pc).abs(), (L - pc).abs()]).groupby(level=0).max()
        return tr.rolling(14, min_periods=10).mean()

    @cached_property
    def vol50(self):
        return self.V.rolling(50, min_periods=40).mean()

    @cached_property
    def hi252(self):
        return self.H.rolling(252, min_periods=200).max()

    @cached_property
    def lo252(self):
        return self.L.rolling(252, min_periods=200).min()

    @cached_property
    def age(self):
        return self.C.notna().cumsum()

    @cached_property
    def univ(self):
        """Base universe: EQ series, price >= Rs 30, median turnover >= Rs 5 cr, >= 250 sessions listed."""
        return self.EQ & (self.C >= 30) & (self.adtv20 >= 5e7) & (self.age >= 250)

    @cached_property
    def buniv(self):
        """Breadth universe for the regime dial: EQ, turnover >= Rs 1 cr, >= 200 sessions."""
        return self.EQ & (self.adtv20 >= 1e7) & (self.age >= 200)

    def regime_series(self):
        """Index for the regime dial: MID150BEES; JUNIORBEES when MID150BEES (listed 2019) is missing."""
        mid = self.funds_px["MID150BEES"].reindex(self.dates)
        first = mid.first_valid_index()
        if first is None or first > self.dates[0] + pd.Timedelta(days=10):
            self.regime_benchmark = "JUNIORBEES"
            return self.funds_px["JUNIORBEES"].reindex(self.dates).ffill()
        return mid.ffill()

    def regime(self):
        """Module E proxy: score 0-100 and capacity fraction (0, 0.3, 0.6, 0.85, 1)."""
        if self.reg is not None:
            return self.reg
        m = self.regime_series()
        nifty = self.funds_px["NIFTYBEES"].reindex(self.dates).ffill()
        sma50, sma200 = m.rolling(50).mean(), m.rolling(200).mean()
        trend = 10 * (m > sma50) + 10 * (m > sma200) + 10 * (sma200 > sma200.shift(20))
        B = self.buniv
        n = B.sum(axis=1).replace(0, np.nan)
        pct50 = ((self.C > self.sma(50)) & B).sum(axis=1) / n
        pct21 = ((self.C > self.ema(21)) & B).sum(axis=1) / n
        adv = ((self.Cf > self.Cf.shift(1)) & B).sum(axis=1).rolling(10).sum()
        dec = ((self.Cf < self.Cf.shift(1)) & B).sum(axis=1).rolling(10).sum()
        adr = adv / dec.replace(0, np.nan)
        breadth = (10 * ((pct50 - 0.3) / 0.4).clip(0, 1) + 8 * ((pct21 - 0.3) / 0.4).clip(0, 1)
                   + 7 * ((adr - 0.7) / 0.8).clip(0, 1))
        nh = ((self.H >= self.hi252) & B).sum(axis=1)
        nl = ((self.L <= self.lo252) & B).sum(axis=1)
        net = ((nh - nl) / n).rolling(5).mean()
        hl = 10 * ((net + 0.02) / 0.07).clip(0, 1) + 5 * (net > net.shift(10))
        rv = np.log(nifty).diff().rolling(20).std() * np.sqrt(252)
        rvp = rv.rolling(252, min_periods=120).rank(pct=True)
        vol = 10 * (1 - rvp).clip(0, 1) + 5 * (rv < rv.shift(5))
        score = (trend + breadth + hl + vol + 7.5).rolling(3).mean()
        band = pd.cut(score, [-1, 35, 50, 65, 80, 101], labels=False)
        out, cur = [], 2
        b = band.fillna(2).astype(int).values
        for i in range(len(b)):
            if b[i] < cur:
                cur = b[i]
            elif b[i] > cur and i > 0 and b[i - 1] > cur:
                cur = min(b[i], b[i - 1])
            out.append(cur)
        frac = pd.Series(np.array([0.0, 0.3, 0.6, 0.85, 1.0])[out], index=self.dates)
        frac[(m < sma200) & (pct50 < 0.30)] = 0.0
        self.reg = pd.DataFrame({"score": score, "band": out, "frac": frac, "pct50": pct50})
        return self.reg
