"""Verified liquidity-universe breadth, independent of Module A eligibility."""

from __future__ import annotations
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import pandas as pd
from packages.data_layer.features import compute_features
from packages.pms.module_b_pipeline import ModuleBPipeline

COVERAGE_SQL = """
with calendar as (
 select trade_date,row_number() over(order by trade_date desc) session_rank
 from (select distinct trade_date from module_e_benchmark_bars where benchmark_code='NIFTY50'
       and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date<=:d order by trade_date desc limit 50) c
), prices as (
 select distinct on (p.instrument_id,p.trade_date) p.*,c.session_rank
 from daily_prices p join calendar c using(trade_date) where c.session_rank<=20
 order by p.instrument_id,p.trade_date,case when lower(p.source)='angel_one' then 0
 when lower(p.source) like 'nse%' then 1 else 2 end,lower(p.source)
), quality as (
 select instrument_id,count(*) sessions,max(trade_date) latest,
 avg(coalesce(turnover,close*volume)) adtv,
 count(*) filter(where open is null or high is null or low is null or close is null or volume is null
   or close<=0 or low<=0 or high<greatest(open,close) or low>least(open,close)
   or volume<0 or open<=0 or close::text in ('NaN','Infinity','-Infinity')
   or high::text in ('NaN','Infinity','-Infinity') or low::text in ('NaN','Infinity','-Infinity')
   or open::text in ('NaN','Infinity','-Infinity') or volume::text in ('NaN','Infinity','-Infinity')
   or coalesce(turnover,close*volume)<0 or coalesce(turnover,close*volume)::text in ('NaN','Infinity','-Infinity')) invalid_rows
 from prices group by instrument_id
)
select i.id,i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) instrument_id,
 i.listed_from,coalesce(q.sessions,0) sessions,q.latest,q.adtv,coalesce(q.invalid_rows,0) invalid_rows,
 (i.listed_from>(select min(trade_date) from calendar)) immature
from instruments i left join quality q on q.instrument_id=i.id
where i.exchange='NSE' and i.active and (i.listed_from is null or i.listed_from<=:d)
 and (i.delisted_on is null or i.delisted_on>:d) order by i.id
"""


def classify_liquidity(rows, expected, minimum_rupees=10000000):
    liquid = []
    issues = []
    immature = nonliquid = 0
    for row in rows:
        if row.get("immature"):
            immature += 1
            continue
        value = row.get("adtv")
        if (
            row["sessions"] != 20
            or row.get("latest") != expected
            or row.get("invalid_rows", 0)
            or value is None
            or not math.isfinite(float(value))
        ):
            issues.append(
                {
                    "instrument_id": row["instrument_id"],
                    "reason": "liquidity_20_session_coverage_unverified",
                    "observed_sessions": row["sessions"],
                    "latest": str(row.get("latest")),
                }
            )
            continue
        if float(value) >= minimum_rupees:
            liquid.append(row)
        else:
            nonliquid += 1
    quality = {
        "master_universe": len(rows),
        "liquid_universe": len(liquid),
        "verified_nonliquid": nonliquid,
        "immature_listings_excluded": immature,
        "unclassified": len(issues),
        "exceptions": issues,
        "turnover_threshold_rupees": minimum_rupees,
        "independent_of_module_a": True,
    }
    return liquid, quality


def verified_breadth(instruments, price_rows, actions, calendar, expected, cfg):
    """Equal-weight current liquid universe; daily A/D counts summed over ten sessions."""
    grouped = defaultdict(list)
    action_map = defaultdict(list)
    lookup = {row["instrument_id"]: row for row in instruments}
    for row in price_rows:
        grouped[int(row["instrument_id"])].append(row)
    for action in actions:
        action_map[action.instrument_id].append(action)
    pipeline = ModuleBPipeline(None, cfg)
    pipeline._calendar = lambda _: calendar
    pipeline.prices.daily_prices = lambda instrument, start, end, **kw: [
        r for r in grouped[lookup[instrument]["id"]] if start <= r["trade_date"] <= end
    ]
    pipeline.reference.corporate_actions_between = (
        lambda instrument, start, end: action_map[instrument]
    )
    sessions = sorted(d for d in calendar.sessions if d <= expected)
    if len(sessions) < 257:
        raise ValueError("breadth_exchange_history_incomplete")
    frames = []
    issues = []
    valid_rallies = 0
    for item in instruments:
        instrument = item["instrument_id"]
        try:
            candles = pipeline._candles(instrument, expected, lookback_days=500)
            frame = compute_features(candles).data
            if frame.empty:
                raise ValueError("breadth_price_history_missing_sessions")
            listed = item.get("listed_from")
            needed = [d for d in sessions[-257:] if listed is None or d >= listed]
            if len(needed) < 50 or not set(needed).issubset(set(frame.index.date)):
                raise ValueError("breadth_price_history_missing_sessions")
            if frame.empty or frame.index[-1].date() != expected:
                raise ValueError("breadth_price_history_stale")
            last = frame.iloc[-1]
            if any(
                not math.isfinite(float(last[k]))
                for k in ("dma50", "ema21", "high_52w", "low_52w")
            ):
                raise ValueError("breadth_indicators_unavailable")
            # Signals are not traded here. Only the common rally state is used.
            _, watchlist = pipeline._setup1_state(frame, expected)
            valid_rallies += int(watchlist is not None)
            frames.append(frame)
        except ValueError as exc:
            issues.append({"instrument_id": instrument, "reason": str(exc)})
    quality = {
        "measured_liquid_stocks": len(frames),
        "expected_liquid_stocks": len(instruments),
        "exceptions": issues,
        "valid_rallies": valid_rallies,
    }
    if issues or not frames:
        return None, quality
    count = len(frames)
    above50 = above21 = 0
    adv = dec = 0
    net_by_day = []
    for frame in frames:
        latest = frame.iloc[-1]
        above50 += int(latest["close"] > latest["dma50"])
        above21 += int(latest["close"] > latest["ema21"])
        for value in frame["close"].diff().iloc[-10:]:
            adv += int(value > 0)
            dec += int(value < 0)
    for offset in range(6):
        highs = lows = 0
        for frame in frames:
            row = frame.iloc[-1 - offset]
            highs += int(row["high"] >= row["high_52w"])
            lows += int(row["low"] <= row["low_52w"])
        net_by_day.append((highs - lows) / count * 100)
        if offset == 0:
            highs_today = highs / count * 100
            lows_today = lows / count * 100
    # Finite saturation is equivalent to a maximal A/D score when no declines
    # exist; a flat universe with neither advances nor declines is neutral 1.
    ratio = adv / dec if dec else 2.0 if adv else 1.0
    quality.update(
        advance_observations=adv,
        decline_observations=dec,
        zero_declines_saturated=bool(adv and not dec),
    )
    return {
        "breadth_above_50dma_pct": above50 / count * 100,
        "breadth_above_21ema_pct": above21 / count * 100,
        "advance_decline_10d": ratio,
        "new_highs_pct": highs_today,
        "new_lows_pct": lows_today,
        "net_highs_trend_pp": net_by_day[0] - sum(net_by_day[1:]) / 5,
        "valid_rallies_pct": valid_rallies / count * 100,
    }, quality
