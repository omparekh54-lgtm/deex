"""Metadata for the ten selectable strategies, including the independent audit verdict for each."""
from __future__ import annotations

PMS_ID = "PMS_SETUP1"

# Audit figures: CAGR % net of costs on Rs 1 crore. "recent" = Oct 2021 - Sep 2026; "holdout" = Jan 2011 -
# Sep 2021 with frozen rules. Nifty 50 ETF (price only): 7.2% recent, 11.0% holdout.
CATALOG = {
    "S1": dict(
        name="Momentum Rotation 20", family="Monthly rotation",
        summary="Hold the 20 liquid stocks with the strongest risk-adjusted 12-month trend, refreshed monthly.",
        rules=["Universe: EQ stocks, price >= Rs 30, 20-day median traded value >= Rs 10 cr, >= 250 sessions listed",
               "Eligible: close above 200 DMA and within 25% of the 52-week high",
               "Score: 12-1 month return divided by 1-year volatility",
               "Month-end rebalance at the next open; top 20 equal weight; keep holdings ranked in the top 40",
               "Regime: fully invested at >= 60% capacity, half at 30%, cash at 0%"],
        audit=dict(verdict="Ready for forward paper testing", recent=25.3, holdout=22.2,
                   note="Only strategy with consistent evidence across both periods; drawdowns reached -39%.")),
    "S2": dict(
        name="52-Week-High Base Breakout", family="Swing (house engine)",
        summary="Leader breaks out of a tight 25-day base near its 52-week high on volume.",
        rules=["Trend template, RS rank >= 70", "Base depth <= 15%, pivot within 5% of 52-week high",
               "First close above pivot (<= 6% above), volume >= 1.5x 50-day average, close in top 40% of range",
               "House engine: next-open entry, 10% stop, exit after 60 sessions, 0.75% risk"],
        audit=dict(verdict="Reject in current form", recent=4.7, holdout=5.7,
                   note="Below Nifty in both periods.")),
    "S3": dict(
        name="Volatility Contraction (VCP)", family="Swing (house engine)",
        summary="Leader near highs whose range and volume contract before the next leg.",
        rules=["Trend template, RS rank >= 75",
               "10-day range <= 35% of 50-day range and <= 10%; close within 5% of 10-day high",
               "10-day volume <= 0.85x 50-day average; 10-day high >= 90% of 52-week high",
               "House engine: next-open entry, 10% stop, exit after 60 sessions"],
        audit=dict(verdict="Research candidate", recent=9.3, holdout=15.1,
                   note="Strong 2011-21, flat 2024-26; sensitive to the regime benchmark.")),
    "S4": dict(
        name="Episodic Pivot (Gap-and-Go)", family="Swing (house engine)",
        summary="Quiet stock gaps up hard on very high volume; ride the drift.",
        rules=["Open >= 4% above prior close, close >= 7% above, top 40% of range",
               "Volume >= 3x prior 50-day average; prior 60-session return <= 30%; close above 200 DMA",
               "House engine: next-open entry, 10% stop, exit after 60 sessions"],
        audit=dict(verdict="Reject in current form", recent=10.5, holdout=4.4,
                   note="Entering one day later cut 2021-26 CAGR to 3.8%; failed the holdout.")),
    "S5": dict(
        name="Leader Pullback to 50-DMA", family="Swing (house engine)",
        summary="Recent leader pulls back to a rising 50 DMA on light volume and closes up.",
        rules=["Trend template, RS rank >= 80, 40-day high >= 98% of 52-week high",
               "Low within 2% of 50 DMA, close at or above it, close above open; depth 8-25%",
               "5-day volume <= 50-day average; 50 DMA rising",
               "House engine: next-open entry, 10% stop, exit after 60 sessions"],
        audit=dict(verdict="Reject in current form", recent=9.4, holdout=6.0,
                   note="Depended on 2023; -35.7% drawdown.")),
    "S6": dict(
        name="Low-Volatility Trend Rotation", family="Monthly rotation",
        summary="Hold the 20 calmest of the 300 most liquid stocks that are still in uptrends.",
        rules=["Top 300 by 20-day median traded value; close above 200 DMA; positive 6-month return",
               "Lowest 1-year volatility ranks first; top 20 equal weight, monthly, keep while in top 40",
               "Regime: same exposure switch as S1"],
        audit=dict(verdict="Insufficient evidence", recent=4.8, holdout=12.4,
                   note="No excess return over Nifty across 2011-26.")),
    "S7": dict(
        name="Donchian 55/20 on RS Leaders", family="Trend following",
        summary="Buy a 55-day closing high in strong stocks; exit on a 20-day closing low.",
        rules=["Universe: median traded value >= Rs 10 cr, RS rank >= 80, close above 200 DMA, 50 DMA above 200 DMA",
               "Entry next open after the first close above the prior 55-session high close",
               "Stop: signal close - 2.5 x ATR(14), at most 12% below the fill",
               "Exit next open after a close below the prior 20-session low close; 0.75% risk, 12% cap"],
        audit=dict(verdict="Research candidate", recent=4.6, holdout=16.5,
                   note="Inconsistent between periods; 0.8% with an alternative regime benchmark.")),
    "S8": dict(
        name="Weekly Stage-2 Breakout", family="Weekly positional",
        summary="Breakout above a 25-week base and a rising 30-week average, on volume, outperforming midcaps.",
        rules=["Checked on the last session of each week; median traded value >= Rs 7.5 cr",
               "Close above prior 25-week high and rising 30-week (150-day) average; week volume >= 1.5x 10-week average",
               "26-week return above the midcap ETF's",
               "Stop: prior 8-week low x 0.99, at most 15% (in practice almost always 15%)",
               "Exit next open after a weekly close below the 30-week average; 1% risk, 15% cap"],
        audit=dict(verdict="Reject in current form", recent=10.8, holdout=8.9,
                   note="Behind Nifty in the holdout and over 2011-26; 5.4 years underwater.")),
    "S9": dict(
        name="Momentum-Leader Dip (RSI-2)", family="Swing (house engine)",
        summary="Buy the strongest 10% of stocks on a sharp 2-3 day dip.",
        rules=["Trend template, RS rank >= 90, RSI(2) below 10, close above 50 DMA",
               "House engine: next-open entry, 10% stop, exit after 60 sessions"],
        audit=dict(verdict="Insufficient evidence", recent=12.5, holdout=11.1,
                   note="Matched Nifty in the holdout.")),
    "S10": dict(
        name="ETF Dual-Momentum Allocation", family="Monthly ETF rotation",
        summary="Hold the two strongest of ten NSE ETFs (equity, sector, gold, silver) that are in uptrends.",
        rules=["ETFs: NIFTYBEES, JUNIORBEES, MID150BEES, BANKBEES, PSUBNKBEES, ITBEES, PHARMABEES, CPSEETF, GOLDBEES, SILVERBEES",
               "Score: average of 3, 6 and 12-month returns; eligible if score > 0 and above 200 DMA",
               "Month-end rebalance at next open; top 2 at 50% each; empty slot in cash"],
        audit=dict(verdict="Reject in current form", recent=23.2, holdout=6.8,
                   note="Buy-and-hold gold beat it 2021-26; ETF list chosen with hindsight.")),
}
STRATEGY_IDS = tuple(CATALOG)


def public_catalog():
    """Catalog for the UI: original PMS strategy first (default), then S1-S10."""
    items = [dict(id=PMS_ID, name="PMS Setup 1 (A + B + C + E)", family="Original PMS rules",
                  summary="Your original strategy: fundamentals, rally-pullback, ranking and regime, across 100 stocks.",
                  rules=[], audit=None, data="PMS historical database", default=True)]
    for key, v in CATALOG.items():
        items.append(dict(id=key, name=f"{key} {v['name']}", family=v["family"], summary=v["summary"],
                          rules=v["rules"], audit=v["audit"], data="NSE bhavcopy (all listed stocks)", default=False))
    return items
