from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from .models import Candle, DataQualityIssue


def validate_candles(candles: list[Candle]) -> list[DataQualityIssue]:
    issues: list[DataQualityIssue] = []
    now = datetime.now(timezone.utc)
    grouped: dict[str, list[Candle]] = defaultdict(list)
    for candle in candles:
        grouped[candle.instrument_id].append(candle)
        if candle.low > candle.high:
            issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "ERROR", "LOW_GT_HIGH", "low is above high", now))
        if not (candle.low <= candle.open <= candle.high):
            issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "ERROR", "OPEN_OUTSIDE_RANGE", "open is outside low/high range", now))
        if not (candle.low <= candle.close <= candle.high):
            issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "ERROR", "CLOSE_OUTSIDE_RANGE", "close is outside low/high range", now))
        if candle.volume < 0:
            issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "ERROR", "NEGATIVE_VOLUME", "volume is negative", now))
        if min(candle.open, candle.high, candle.low, candle.close) <= 0:
            issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "ERROR", "NON_POSITIVE_PRICE", "OHLC contains a non-positive price", now))

    for instrument_id, rows in grouped.items():
        ordered = sorted(rows, key=lambda c: c.timestamp)
        seen = set()
        for candle in ordered:
            key = candle.timestamp
            if key in seen:
                issues.append(DataQualityIssue("daily_prices", instrument_id, "WARNING", "DUPLICATE_TIMESTAMP", f"duplicate candle at {key.isoformat()}", now))
            seen.add(key)
    return issues


def reconcile_candles(
    primary: list[Candle],
    secondary: list[Candle],
    close_tolerance_pct: float = 0.25,
    volume_tolerance_pct: float = 5.0,
) -> list[DataQualityIssue]:
    issues: list[DataQualityIssue] = []
    now = datetime.now(timezone.utc)
    secondary_by_key = {(c.instrument_id, c.timestamp.date()): c for c in secondary}
    for candle in primary:
        other = secondary_by_key.get((candle.instrument_id, candle.timestamp.date()))
        if other is None:
            continue
        if other.close:
            close_diff = abs(float(candle.close - other.close)) / float(other.close) * 100
            if close_diff > close_tolerance_pct:
                issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "WARNING", "CLOSE_SOURCE_MISMATCH", f"close differs by {close_diff:.3f}% between {candle.source} and {other.source}", now))
        if other.volume > 0:
            volume_diff = abs(candle.volume - other.volume) / other.volume * 100
            if volume_diff > volume_tolerance_pct:
                issues.append(DataQualityIssue("daily_prices", candle.instrument_id, "WARNING", "VOLUME_SOURCE_MISMATCH", f"volume differs by {volume_diff:.2f}% between {candle.source} and {other.source}", now))
    return issues
