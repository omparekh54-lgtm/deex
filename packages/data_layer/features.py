from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .models import Candle


@dataclass(frozen=True)
class FeatureFrame:
    data: pd.DataFrame


def candles_to_frame(candles: list[Candle]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": candle.timestamp,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": candle.volume,
            "turnover": (float(candle.turnover) if candle.turnover is not None
                         else float(candle.close) * candle.volume),
        }
        for candle in sorted(candles, key=lambda c: c.timestamp)
    ]
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    frame = pd.DataFrame(rows).set_index("timestamp")
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def compute_features(candles: list[Candle]) -> FeatureFrame:
    df = candles_to_frame(candles)
    if df.empty:
        return FeatureFrame(df)

    df["ema9"] = df["close"].ewm(span=9, adjust=False, min_periods=9).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False, min_periods=21).mean()
    df["dma50"] = df["close"].rolling(50, min_periods=50).mean()
    df["dma200"] = df["close"].rolling(200, min_periods=200).mean()

    previous_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = true_range.rolling(14, min_periods=14).mean()

    # PMS rule: ADR% = 100 * (20-day average of High/Low - 1).
    daily_range_ratio = df["high"] / df["low"]
    df["adr20_pct"] = 100.0 * (daily_range_ratio.rolling(20, min_periods=20).mean() - 1.0)

    df["avg_volume20"] = df["volume"].rolling(20, min_periods=20).mean()
    df["avg_volume50"] = df["volume"].rolling(50, min_periods=50).mean()
    df["worst_volume20"] = df["volume"].rolling(20, min_periods=20).min()

    # Prefer reported rupee turnover; close x volume is an explicit fallback
    # for feeds which do not supply the traded value.
    df["avg_turnover20"] = df["turnover"].rolling(20, min_periods=20).mean()
    df["high_52w"] = df["high"].rolling(252, min_periods=1).max()
    df["low_52w"] = df["low"].rolling(252, min_periods=1).min()
    df["distance_from_52w_high_pct"] = 100.0 * (df["close"] / df["high_52w"] - 1.0)

    df["ema21_slope_5d"] = df["ema21"].pct_change(5) * 100.0
    df["above_dma50"] = df["close"] > df["dma50"]
    df["above_dma200"] = df["close"] > df["dma200"]
    return FeatureFrame(df)
