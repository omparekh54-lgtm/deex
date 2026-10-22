from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd

from .reference_data import CorporateAction, split_adjustment_factor


@dataclass(frozen=True)
class AdjustmentResult:
    frame: pd.DataFrame
    cumulative_factor: float


def adjust_ohlcv_for_actions(frame: pd.DataFrame, actions: Iterable[CorporateAction]) -> AdjustmentResult:
    """Back-adjust OHLCV for split/bonus actions while preserving raw columns.

    Expected index: trade date/datetime. Pre-event prices are multiplied by the
    cumulative price factor and volume divided by the same factor.
    """
    if frame.empty:
        return AdjustmentResult(frame.copy(), 1.0)
    df = frame.copy().sort_index()
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            raise ValueError(f"missing required column {col}")
        df[f"raw_{col}"] = df[col]
    cumulative = 1.0
    relevant = sorted(
        [a for a in actions if a.action_type.upper() in {"SPLIT", "BONUS"}],
        key=lambda a: a.ex_date,
        reverse=True,
    )
    normalized_dates = pd.to_datetime(df.index).date
    for action in relevant:
        factor = split_adjustment_factor(action)
        cumulative *= factor
        mask = normalized_dates < action.ex_date
        for col in ("open", "high", "low", "close"):
            df.loc[mask, col] = df.loc[mask, col].astype(float) * factor
        if factor != 0:
            df.loc[mask, "volume"] = df.loc[mask, "volume"].astype(float) / factor
    df["adjustment_factor_applied"] = 1.0
    for idx in df.index:
        d = pd.Timestamp(idx).date()
        factor = 1.0
        for action in relevant:
            if d < action.ex_date:
                factor *= split_adjustment_factor(action)
        df.at[idx, "adjustment_factor_applied"] = factor
    return AdjustmentResult(df, cumulative)
