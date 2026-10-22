"""Readiness measurements and reconciled PAPER-account input contracts."""

from __future__ import annotations
from collections import Counter
from datetime import date
import math

from packages.pms.module_c import AllocationState, RankingInputs
from packages.pms.module_a_context import as_ist
from packages.pms.costs import estimate_delivery_costs


def measure_features(frame, signal_date, benchmark_returns):
    if len(frame) < 252:
        raise ValueError("ranking_price_history_incomplete")
    latest = frame.iloc[-1]
    needed = ("ema21", "dma50", "atr14", "high_52w", "avg_turnover20", "adr20_pct")
    if any(not math.isfinite(float(latest[k])) for k in needed) or latest["atr14"] <= 0:
        raise ValueError("ranking_indicators_unavailable")
    signal = frame[frame.index.date == signal_date]
    if signal.empty:
        raise ValueError("signal_candle_unavailable")
    candle = signal.iloc[-1]
    if (
        not math.isfinite(float(candle["ema21"]))
        or not math.isfinite(float(candle["atr14"]))
        or candle["atr14"] <= 0
    ):
        raise ValueError("signal_indicators_unavailable")
    close = float(latest["close"])
    ret = {}
    for n in (63, 126):
        previous = float(frame.iloc[-n - 1]["close"])
        benchmark = benchmark_returns.get(n)
        if previous <= 0 or benchmark is None:
            raise ValueError("nifty500_history_unavailable")
        ret[n] = (close / previous / (1 + benchmark / 100) - 1) * 100
    high = float(candle["high"])
    low = float(candle["low"])
    close_location = (
        (float(candle["close"]) - low) / (high - low) if high > low else 0.5
    )
    return {
        "close": close,
        "adr20": float(latest["adr20_pct"]),
        "adtv": float(latest["avg_turnover20"]),
        "distance_52w": max(
            0, (float(latest["high_52w"]) - close) / float(latest["high_52w"]) * 100
        ),
        "relative_63": ret[63],
        "relative_126": ret[126],
        "pullback_depth_atr": max(
            0,
            (float(candle["ema21"]) - float(candle["close"])) / float(candle["atr14"]),
        ),
        "above_dma50": close > float(latest["dma50"]),
        "ema21_rising": float(latest["ema21"]) > float(frame.iloc[-6]["ema21"]),
        "signal_close_location": max(0, min(1, close_location)),
        "watchlist_distance_atr": abs(close - float(latest["ema21"]))
        / float(latest["atr14"]),
    }


def ranking_inputs(
    signal, stats, a_score, stage3, relative_percentile, sector_percentile, cfg
):
    r = cfg["ranking"]
    velocity = float(signal["velocity"])
    volume = float(signal["volume_ratio"])
    if any(not math.isfinite(v) or v < 0 for v in (velocity, volume, float(a_score))):
        raise ValueError("ranking_inputs_invalid")
    rally = 50 * (
        min(1, velocity / float(r.get("velocity_full_score", 2)))
        + min(1, volume / float(r.get("volume_ratio_full_score", 3)))
    )
    pullback = 25 * (
        max(
            0,
            1
            - stats["pullback_depth_atr"] / float(r.get("pullback_full_depth_atr", 1)),
        )
        + int(stats["above_dma50"])
        + int(stats["ema21_rising"])
        + stats["signal_close_location"]
    )
    fundamental = min(
        100, float(a_score) + (float(r.get("stage3_bonus", 10)) if stage3 else 0)
    )
    return RankingInputs(
        rally,
        pullback,
        fundamental,
        relative_percentile,
        max(0, 100 - 2 * stats["distance_52w"]),
        sector_percentile,
    )


def portfolio_state(snapshot, trades, orders, sectors, expected_date, as_of, cfg):
    """Reconcile an explicit PAPER snapshot with mode-linked trades and buy orders.

    Unknown stops/modes/history are blockers, never an assumed empty account.
    Snapshot cash is the cash ledger balance; pending buy cash is reserved here.
    """
    blockers = []
    held = set()
    counts = Counter()
    nav = 0.0
    cash = 0.0
    risk = 0.0
    opens = 0
    pending = 0
    if not snapshot:
        return (
            AllocationState(
                0, 0, 0, 0, {}, nav=0, blockers=("portfolio_snapshot_unavailable",)
            ),
            held,
        )
    if snapshot["snapshot_date"] != expected_date:
        blockers.append("portfolio_snapshot_stale")
    if as_ist(snapshot["created_at"]) > as_ist(as_of):
        blockers.append("portfolio_snapshot_not_knowable")
    try:
        cash = float(snapshot["cash"])
        nav = float(snapshot["nav"])
        if not math.isfinite(cash) or not math.isfinite(nav) or cash < 0 or nav <= 0:
            raise ValueError()
        if (
            snapshot.get("market_value") is not None
            and abs(nav - cash - float(snapshot["market_value"])) > 0.02
        ):
            blockers.append("portfolio_nav_unreconciled")
    except (ValueError, TypeError):
        return (
            AllocationState(
                0, 0, 0, 0, {}, nav=0, blockers=("portfolio_state_invalid",)
            ),
            held,
        )
    positions = {}
    if snapshot.get("market_value") is not None:
        marks = [row.get("market_value") for row in snapshot.get("positions") or []]
        if any(v is None or not math.isfinite(float(v)) for v in marks):
            blockers.append("portfolio_position_marks_unavailable")
        elif abs(sum(float(v) for v in marks) - float(snapshot["market_value"])) > 0.02:
            blockers.append("portfolio_market_value_unreconciled")
    for row in snapshot.get("positions") or []:
        pk = int(row["instrument_id"])
        qty = int(row["quantity"])
        if pk in positions or qty <= 0:
            blockers.append("portfolio_positions_invalid")
        positions[pk] = qty
        held.add(pk)
    quantities = Counter()
    trade_by_id = {}
    pending_ids = set()
    for row in trades:
        modes = set(row.get("account_modes") or [])
        if modes and "PAPER" not in modes:
            continue
        if modes != {"PAPER"}:
            blockers.append("trade_account_mode_unverified")
        if as_ist(row["updated_at"]) > as_ist(as_of):
            blockers.append("historical_account_state_unavailable")
        pk = int(row["instrument_id"])
        held.add(pk)
        trade_by_id[str(row["id"])] = row
        if row["state"] in {"SIGNAL_CREATED", "PENDING_ENTRY"}:
            pending += 1
            pending_ids.add(str(row["id"]))
        else:
            opens += 1
            quantities[pk] += int(row["remaining_shares"])
        sector = sectors.get(pk)
        if not sector:
            blockers.append("portfolio_sector_unavailable")
        else:
            counts[sector] += 1
        try:
            qty = int(
                row["shares"]
                if str(row["id"]) in pending_ids
                else row["remaining_shares"]
            )
            entry = float(row["entry"])
            stop = float(row["stop"])
            if qty <= 0 or not all(math.isfinite(v) and v > 0 for v in (entry, stop)):
                raise ValueError()
            risk += max(0, entry - stop) * qty
            if str(row["id"]) in pending_ids and stop >= entry:
                blockers.append("pending_stop_geometry_invalid")
        except (TypeError, ValueError):
            blockers.append("portfolio_stop_or_size_unavailable")
    if dict(quantities) != positions:
        blockers.append("portfolio_positions_unreconciled")
    reserved = 0.0
    matched_pending = set()
    order_trade_counts = Counter()
    for row in orders:
        if row["side"] != "BUY":
            continue
        if as_ist(row["updated_at"]) > as_ist(as_of):
            blockers.append("historical_account_state_unavailable")
        pk = int(row["instrument_id"])
        held.add(pk)
        qty = int(row["quantity"]) - int(row.get("filled_quantity") or 0)
        if qty <= 0:
            blockers.append("pending_order_quantity_invalid")
            continue
        trade = trade_by_id.get(str(row.get("strategy_trade_id")))
        if not trade or str(trade["id"]) not in pending_ids:
            blockers.append("pending_order_stop_or_trade_unverified")
            pending += 1
            sector = sectors.get(pk)
            if not sector:
                blockers.append("portfolio_sector_unavailable")
            else:
                counts[sector] += 1
        else:
            matched_pending.add(str(trade["id"]))
            order_trade_counts[str(trade["id"])] += 1
            if order_trade_counts[str(trade["id"])] > 1:
                blockers.append("multiple_pending_orders_for_trade")
        try:
            level = float(row.get("trigger_price") or row.get("limit_price"))
            if not math.isfinite(level) or level <= 0:
                raise ValueError()
            if (
                trade
                and str(trade["id"]) in pending_ids
                and (
                    abs(level - float(trade["entry"] or 0)) > 1e-8
                    or qty != int(trade["shares"])
                )
            ):
                blockers.append("pending_order_trade_mismatch")
            cost = estimate_delivery_costs(
                trade_date=expected_date,
                side="BUY",
                price=level,
                quantity=qty,
                cfg=cfg["backtest"],
            ).total
            reserved += level * qty + cost
        except (TypeError, ValueError):
            blockers.append("pending_order_price_unavailable")
    # Strategy proposals which have not reached the broker still reserve cash.
    for identity in pending_ids - matched_pending:
        row = trade_by_id[identity]
        try:
            level = float(row["entry"])
            qty = int(row["shares"])
            cost = estimate_delivery_costs(
                trade_date=expected_date,
                side="BUY",
                price=level,
                quantity=qty,
                cfg=cfg["backtest"],
            ).total
            reserved += level * qty + cost
        except (TypeError, ValueError):
            blockers.append("pending_reservation_unavailable")
    if reserved > cash:
        blockers.append("pending_cash_over_reserved")
    return (
        AllocationState(
            opens,
            pending,
            risk / nav * 100,
            max(0, cash - reserved),
            dict(counts),
            nav,
            tuple(dict.fromkeys(blockers)),
        ),
        held,
    )
