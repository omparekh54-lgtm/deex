from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class BreadthSnapshot:
    trade_date: date
    universe_size: int
    pct_above_21ema: float
    pct_above_50dma: float
    advance_decline_ratio: float
    new_52w_highs: int
    new_52w_lows: int
    net_highs_pct: float


def compute_relative_strength(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute 3m/6m return percentiles from a close-price matrix.

    `prices` must have dates on rows and stable instrument IDs on columns.
    """
    ordered = prices.sort_index()
    if ordered.empty:
        return pd.DataFrame(columns=["return_3m", "return_6m", "rs_3m_pctile", "rs_6m_pctile"])

    latest = ordered.ffill().iloc[-1]
    ret_3m = latest / ordered.ffill().shift(63).iloc[-1] - 1
    ret_6m = latest / ordered.ffill().shift(126).iloc[-1] - 1
    result = pd.DataFrame({"return_3m": ret_3m, "return_6m": ret_6m})
    result["rs_3m_pctile"] = result["return_3m"].rank(pct=True) * 100.0
    result["rs_6m_pctile"] = result["return_6m"].rank(pct=True) * 100.0
    return result


def compute_breadth(latest_rows: pd.DataFrame, previous_close: pd.Series) -> BreadthSnapshot:
    """Build Module E breadth inputs from one row per liquid stock.

    Required columns: close, ema21, dma50, high_52w, low_52w. Index must be
    stable instrument IDs. `previous_close` uses the same index.
    """
    required = {"close", "ema21", "dma50", "high_52w", "low_52w", "trade_date"}
    missing = required - set(latest_rows.columns)
    if missing:
        raise ValueError(f"missing breadth columns: {', '.join(sorted(missing))}")
    if latest_rows.empty:
        raise ValueError("breadth universe is empty")

    valid = latest_rows.dropna(subset=["close", "ema21", "dma50"]).copy()
    if valid.empty:
        raise ValueError("breadth universe has no stocks with complete indicators")

    aligned_prev = previous_close.reindex(valid.index)
    advances = int((valid["close"] > aligned_prev).fillna(False).sum())
    declines = int((valid["close"] < aligned_prev).fillna(False).sum())
    ad_ratio = advances / declines if declines else float(advances)

    new_highs = int((valid["close"] >= valid["high_52w"]).sum())
    new_lows = int((valid["close"] <= valid["low_52w"]).sum())
    n = len(valid)
    trade_date_value = pd.Timestamp(valid["trade_date"].iloc[0]).date()
    return BreadthSnapshot(
        trade_date=trade_date_value,
        universe_size=n,
        pct_above_21ema=float((valid["close"] > valid["ema21"]).mean() * 100.0),
        pct_above_50dma=float((valid["close"] > valid["dma50"]).mean() * 100.0),
        advance_decline_ratio=float(ad_ratio),
        new_52w_highs=new_highs,
        new_52w_lows=new_lows,
        net_highs_pct=float((new_highs - new_lows) / n * 100.0),
    )
