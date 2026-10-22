"""Formal forward PAPER lifecycle. No external broker is imported or called."""

from __future__ import annotations

from packages.pms.execution_timing import intraday_trigger, intraday_information_changed

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
import math
import os
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text
from packages.data_layer.features import compute_features
from packages.pms.config import load_strategy_config
from packages.pms.costs import estimate_delivery_costs
from packages.pms.market_calendar import MarketCalendar
from packages.pms.module_a_context import IST
from packages.pms.module_a_pipeline import ModuleAPipeline
from packages.pms.module_b_pipeline import ModuleBPipeline
from packages.pms.risk import size_position
from packages.pms.trade_engine import (
    TradePosition,
    TradeState,
    PENDING_EXITS,
    activate_entry,
    cancel_entry,
    process_entry_bar,
    process_next_open,
    process_position_bar,
    process_protective_stop,
    expire_entry_at_close,
    reentry_allowed,
    create_reentry,
    validate_ohlc,
)

VERSION = "PAPER_EXECUTION_v1.0"
ACCOUNT_LOCK = "pms_formal_paper_account"


def encode(value):
    return json.dumps(value, default=str, allow_nan=False, sort_keys=True)


def fingerprint(cfg):
    return hashlib.sha256(encode(cfg).encode()).hexdigest()


def identity(key):
    return str(uuid5(NAMESPACE_URL, "pms:formal-paper:" + key))


def sizing(cfg, nav, entry, low):
    return size_position(
        nav,
        entry,
        low,
        **{
            k: float(cfg["risk"][k])
            for k in (
                "risk_per_trade_pct",
                "max_position_pct",
                "raw_stop_buffer_pct",
                "max_stop_risk_pct",
            )
        },
    )


@dataclass
class Budget:
    nav: float
    cash: float
    risk: float
    slots: int
    sectors: dict = field(default_factory=dict)
    instruments: set = field(default_factory=set)

    def __post_init__(self):
        normalized = {}
        for key, value in self.sectors.items():
            name = key.strip().casefold()
            normalized[name] = normalized.get(name, 0) + value
        self.sectors = normalized

    def limit(self, cfg, *, instrument, sector, entry, stop, adtv, capacity, ceiling):
        sector = sector.strip().casefold() if isinstance(sector, str) else sector
        values = (self.nav, self.cash, self.risk, entry, stop, adtv)
        if (
            any(not math.isfinite(float(v)) for v in values)
            or self.nav <= 0
            or self.cash < 0
            or self.risk < 0
            or not 0 < stop < entry
            or adtv <= 0
            or not sector
        ):
            raise ValueError("invalid_account_or_entry_inputs")
        if (
            instrument in self.instruments
            or self.slots >= min(7, int(cfg["risk"]["max_open_trades"]), int(capacity))
            or self.sectors.get(sector, 0)
            >= int(cfg["risk"]["max_positions_per_sector"])
        ):
            return 0
        risk_left = max(
            0, self.nav * float(cfg["risk"]["max_portfolio_heat_pct"]) / 100 - self.risk
        )

        def floor(a, b):
            return int(Decimal(str(max(0, a))) // Decimal(str(b)))

        return max(
            0,
            min(
                int(ceiling),
                floor(risk_left, entry - stop),
                floor(adtv * 0.02, entry),
                floor(self.cash, entry),
                floor(self.nav * float(cfg["risk"]["max_position_pct"]) / 100, entry),
                floor(
                    self.nav * float(cfg["risk"]["risk_per_trade_pct"]) / 100,
                    entry - stop,
                ),
            ),
        )

    def reserve(self, instrument, sector, entry, stop, qty, fees):
        sector = sector.strip().casefold()
        self.cash -= entry * qty + fees
        self.risk += (entry - stop) * qty
        self.slots += 1
        self.sectors[sector] = self.sectors.get(sector, 0) + 1
        self.instruments.add(instrument)


def simulate_session(
    metadata,
    day,
    bar,
    indicators,
    cfg,
    budget,
    *,
    entry_ready,
    fundamental_eligible,
    capacity,
    adtv,
):
    """Pure transition; caller commits checkpoint, fills and ledger atomically.

    Budget excludes this pending order but reserves other pending orders. Same-day
    SELL proceeds and released slots/heat are deliberately not reused for entries.
    """
    meta = deepcopy(metadata)
    pos = TradePosition.restore(meta["checkpoint"])
    old_count = len(pos.history)
    validate_ohlc(*[bar[k] for k in ("open", "high", "low", "close")])
    digest = hashlib.sha256(
        encode({"bar": bar, "indicators": indicators}).encode()
    ).hexdigest()
    if meta.get("last_session") == str(day):
        if meta.get("bar_hash") != digest:
            raise ValueError("conflicting_observed_bar_replay")
        return meta, [], []
    if meta.get("last_session") and meta["last_session"] > str(day):
        raise ValueError("out_of_order_paper_session")
    pending = pos.state in {TradeState.SIGNAL_CREATED, TradeState.PENDING_ENTRY}
    if pending and (not entry_ready or not fundamental_eligible):
        cancel_entry(
            pos,
            "entry_gate_unavailable" if not entry_ready else "module_a_ineligible",
            day,
        )
    process_next_open(pos, day, bar["open"])
    limit = 0
    if pos.state == TradeState.PENDING_ENTRY:
        if indicators is None:
            cancel_entry(pos, "entry_indicators_unavailable", day)
        else:
            actual_entry = max(bar["open"], pos.signal_high)
            pos.capital = budget.nav
            size = sizing(cfg, budget.nav, actual_entry, pos.signal_low)
            limit = budget.limit(
                cfg,
                instrument=pos.instrument_id,
                sector=meta["sector"],
                entry=actual_entry,
                stop=size.stop,
                adtv=adtv,
                capacity=capacity,
                ceiling=min(size.shares, int(meta["admitted_shares"])),
            )
            # Fees are monotone in quantity; binary search avoids both cash overspend and a long decrement loop.
            lo, hi = 0, limit
            while lo < hi:
                mid = (lo + hi + 1) // 2
                fees = estimate_delivery_costs(
                    trade_date=day,
                    side="BUY",
                    price=actual_entry,
                    quantity=mid,
                    cfg=cfg["backtest"],
                ).total
                if actual_entry * mid + fees <= budget.cash:
                    lo = mid
                else:
                    hi = mid - 1
            limit = lo
            process_entry_bar(
                pos,
                day,
                bar["open"],
                bar["high"],
                cfg,
                fundamental_eligible=fundamental_eligible,
                entry_share_limit=limit,
            )
    # Protection must never depend on Module A/E readiness or EMA availability.
    process_protective_stop(pos, day, bar["open"], bar["low"])
    issues = []
    if pos.remaining_shares:
        if indicators is not None:
            process_position_bar(
                pos,
                day,
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                **indicators,
            )
        else:
            issues.append("ema_history_unavailable_protection_only")
    expire_entry_at_close(pos, day, cfg)
    events = pos.history[old_count:]
    fills = []
    for index, event in enumerate(events, old_count):
        kind = event["event"]
        if kind not in {"FILLED", "LEG1_EXECUTED", "CLOSED"}:
            continue
        side = "BUY" if kind == "FILLED" else "SELL"
        price = float(event["entry"] if side == "BUY" else event["price"])
        qty = int(event["shares"])
        fees = estimate_delivery_costs(
            trade_date=day, side=side, price=price, quantity=qty, cfg=cfg["backtest"]
        ).total
        if side == "BUY":
            meta["entry_fees"] = fees
            meta["original_shares"] = qty
        allocated_entry_fee = (
            (meta.get("entry_fees", 0) * qty / meta.get("original_shares", qty))
            if side == "SELL"
            else 0
        )
        fills.append(
            dict(
                index=index,
                side=side,
                price=price,
                quantity=qty,
                fees=fees,
                realized=(
                    (price - pos.entry) * qty - fees - allocated_entry_fee
                    if side == "SELL"
                    else 0
                ),
                event=kind,
                reason=event.get("reason", kind),
            )
        )
    meta.update(checkpoint=pos.checkpoint(), last_session=str(day), bar_hash=digest)
    return meta, fills, issues


class FormalPaperExecution:
    def __init__(self, engine, cfg=None, *, clock=None):
        self.engine = engine
        self.cfg = cfg or load_strategy_config()
        self.clock = clock or (lambda: datetime.now(IST))
        self.a = ModuleAPipeline(engine, self.cfg)
        self.b = ModuleBPipeline(engine, self.cfg)
        self.version = self.cfg["version"] + ":" + VERSION + ":PAPER"
        self.c_version = (
            self.cfg["version"]
            + ":"
            + self.cfg["ranking"]["implementation_version"]
            + ":PAPER"
        )

    def now(self):
        value = self.clock()
        if value.tzinfo is None:
            raise ValueError("PAPER clock must be timezone aware")
        return value.astimezone(IST)

    def session(self, day=None):
        now = self.now()
        day = day or now.date()
        if day != now.date():
            raise ValueError(
                "Formal PAPER processing requires today in IST; historical fills are not forward validation"
            )
        calendar = MarketCalendar.from_database(self.engine, now)
        forward = calendar.forward(day, 1)
        if not forward:
            return day, calendar, "BLOCKED", "exchange_calendar_unavailable"
        if forward[0] != day:
            return day, calendar, "SKIPPED", "exchange_non_session"
        if (now.hour, now.minute) < (15, 30):
            return day, calendar, "SKIPPED", "market_not_closed"
        if day not in calendar.sessions:
            return day, calendar, "BLOCKED", "official_session_not_observed"
        return day, calendar, "READY", None

    @staticmethod
    def lock(conn):
        conn.execute(
            text("select pg_advisory_xact_lock(hashtext(:key))"), {"key": ACCOUNT_LOCK}
        )

    def _rows(self, conn, *, active_only=True):
        condition = "and t.state not in ('CLOSED','CANCELLED')" if active_only else ""
        return [
            dict(r)
            for r in conn.execute(
                text(
                    """select t.*,o.id entry_order_id,o.status order_status,o.created_at order_created_at,
          o.raw_response->'formal_paper' metadata,i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) symbol
          from trades t join broker_orders o on o.strategy_trade_id=t.id and o.side='BUY' and o.mode='PAPER'
          join instruments i on i.id=t.instrument_id
          where t.strategy_version=:version """
                    + condition
                    + """ order by t.created_at,t.id for update of t,o"""
                ),
                {"version": self.version},
            )
            .mappings()
            .all()
        ]

    def _account(self, conn, day, rows):
        ledger = [
            dict(r)
            for r in conn.execute(
                text(
                    """select instrument_id,sum(quantity)::int qty
            from portfolio_ledger where account_mode='PAPER' and event_type='FILL' group by instrument_id having sum(quantity)<>0"""
                )
            )
            .mappings()
            .all()
        ]
        known = {
            int(r["instrument_id"]): int(r["remaining_shares"])
            for r in rows
            if int(r["remaining_shares"]) > 0
        }
        actual = {int(r["instrument_id"]): int(r["qty"]) for r in ledger}
        reasons = []
        if known != actual or len(known) != sum(
            int(r["remaining_shares"]) > 0 for r in rows
        ):
            reasons.append("paper_positions_unreconciled")
        cash = float(self.cfg["backtest"]["initial_capital"]) + float(
            conn.execute(
                text(
                    "select coalesce(sum(cash_delta),0) from portfolio_ledger where account_mode='PAPER'"
                )
            ).scalar_one()
        )
        nav = cash
        marks = {}
        for pk, qty in actual.items():
            mark = conn.execute(
                text(
                    """select close from daily_prices where instrument_id=:id and trade_date<=:d
                order by trade_date desc,case when source='ANGEL_ONE' then 0 when source like 'NSE%' then 1 else 2 end,source limit 1"""
                ),
                {"id": pk, "d": day},
            ).scalar_one_or_none()
            if mark is None or not math.isfinite(float(mark)) or float(mark) <= 0:
                reasons.append("paper_position_mark_unavailable")
            else:
                nav += qty * float(mark)
                marks[pk] = float(mark)
        unmanaged = int(
            conn.execute(
                text(
                    """select count(*) from broker_orders o where mode='PAPER' and side='BUY'
             and status in ('OPEN','PENDING','SUBMITTED','PARTIALLY_FILLED')
             and not exists(select 1 from trades t where t.id=o.strategy_trade_id and t.strategy_version=:version)"""
                ),
                {"version": self.version},
            ).scalar_one()
        )
        if unmanaged:
            reasons.append("unmanaged_paper_orders")
        budget = Budget(nav, cash, 0, 0)
        for row in rows:
            meta = row.get("metadata") or {}
            qty = int(row["remaining_shares"]) or (
                int(row["shares"])
                if row["state"] in {"SIGNAL_CREATED", "PENDING_ENTRY"}
                else 0
            )
            if not qty:
                continue
            sector = meta.get("sector")
            entry = float(row["entry"] or 0)
            stop = float(row["stop"] or 0)
            if not sector or not 0 < stop <= entry:
                reasons.append("paper_risk_metadata_unavailable")
                continue
            fee = (
                estimate_delivery_costs(
                    trade_date=day,
                    side="BUY",
                    price=entry,
                    quantity=qty,
                    cfg=self.cfg["backtest"],
                ).total
                if not row["remaining_shares"]
                else 0
            )
            budget.reserve(row["symbol"], sector, entry, stop, qty, fee)
            # Filled holdings consume slots/heat but their cost is already in ledger cash.
            if row["remaining_shares"]:
                budget.cash += entry * qty
        if budget.cash < 0 or not math.isfinite(nav) or nav <= 0:
            reasons.append("paper_account_invalid")
        return budget, list(dict.fromkeys(reasons))

    def _entry_gate(self, conn, day, calendar):
        if os.getenv('PMS_CLOUD_COLLECTION_REQUIRED','0')=='1':
            from packages.pms.cloud_collection import collection_gate
            now=self.now()
            if not collection_gate(conn,now.date(),now):
                return 0,['cloud_collection_missing_partial_failed_or_stale']
        prior = max((d for d in calendar.sessions if d < day), default=None)
        safety = (
            conn.execute(
                text(
                    "select execution_mode,emergency_stop,locked_reason,new_orders_enabled from safety_state where singleton=true"
                )
            )
            .mappings()
            .first()
        )
        reasons = []
        # Explicit PAPER opt-in is required; protective SELLs never consult this flag.
        if (
            not safety
            or safety["execution_mode"] != "PAPER"
            or safety["emergency_stop"]
            or safety["locked_reason"]
            or not safety["new_orders_enabled"]
        ):
            reasons.append("new_entries_disabled_or_locked")
        errors = int(
            conn.execute(
                text(
                    "select count(*) from data_quality_issues where severity='ERROR' and resolved_at is null"
                )
            ).scalar_one()
        )
        if errors:
            reasons.append("critical_data_quality_errors")
        regime = (
            conn.execute(
                text("select * from market_regime_history where trade_date=:d"),
                {"d": prior},
            )
            .mappings()
            .first()
            if prior
            else None
        )
        cap = 0
        if (
            not regime
            or regime["strategy_version"] != self.cfg["version"]
            or (regime["inputs"] or {}).get("_module_e_version")
            != self.cfg["regime"]["implementation_version"]
            or not ((regime["inputs"] or {}).get("_data_quality") or {}).get(
                "data_ready"
            )
        ):
            reasons.append("prior_session_regime_unavailable")
        else:
            cap = 0 if regime["hard_override"] else int(regime["max_open_trades"])
        if not 0 <= cap <= 7:
            reasons.append("regime_capacity_invalid")
            cap = 0
        return cap, reasons

    def _persist(self, conn, row, meta, fills, day, old_history):
        pos = TradePosition.restore(meta["checkpoint"])
        now = self.now()
        conn.execute(
            text(
                """update trades set state=:state,entry=:entry,stop=:stop,shares=:shares,remaining_shares=:remaining,
            realized_pnl=:pnl,exit_reason=:reason,updated_at=:now where id=:id and strategy_version=:version"""
            ),
            dict(
                id=str(row["id"]),
                version=self.version,
                state=pos.state.value,
                entry=pos.entry or row["entry"],
                stop=pos.stop or row["stop"],
                shares=pos.shares or row["shares"],
                remaining=pos.remaining_shares,
                pnl=pos.realized_pnl,
                reason=pos.exit_reason,
                now=now,
            ),
        )
        status = (
            "CANCELLED"
            if pos.state == TradeState.CANCELLED
            else "FILLED" if pos.filled_on else "OPEN"
        )
        conn.execute(
            text(
                "update broker_orders set status=:status,raw_response=jsonb_set(raw_response,'{formal_paper}',cast(:meta as jsonb)),updated_at=:now where id=:id and mode='PAPER'"
            ),
            dict(
                id=str(row["entry_order_id"]), status=status, meta=encode(meta), now=now
            ),
        )
        for fill in fills:
            if fill["side"] == "BUY":
                cash = float(self.cfg["backtest"]["initial_capital"]) + float(
                    conn.execute(
                        text(
                            "select coalesce(sum(cash_delta),0) from portfolio_ledger where account_mode='PAPER'"
                        )
                    ).scalar_one()
                )
                if fill["price"] * fill["quantity"] + fill["fees"] > cash:
                    raise ValueError("insufficient_paper_cash_at_atomic_commit")
            else:
                held = int(
                    conn.execute(
                        text(
                            "select coalesce(sum(quantity),0) from portfolio_ledger where account_mode='PAPER' and event_type='FILL' and instrument_id=:id"
                        ),
                        {"id": row["instrument_id"]},
                    ).scalar_one()
                )
                if held < fill["quantity"]:
                    raise ValueError("insufficient_paper_holdings_at_atomic_commit")
            key = str(row["id"]) + ":" + str(fill["index"])
            order_id = (
                str(row["entry_order_id"])
                if fill["side"] == "BUY"
                else identity(key + ":order")
            )
            if fill["side"] == "SELL":
                conn.execute(
                    text(
                        """insert into broker_orders(id,broker,mode,instrument_id,side,order_type,quantity,status,strategy_trade_id,idempotency_key,raw_response)
                    values(:id,'PAPER','PAPER',:pk,'SELL','MODELED',:qty,'FILLED',:trade,:key,cast(:raw as jsonb))"""
                    ),
                    dict(
                        id=order_id,
                        pk=row["instrument_id"],
                        qty=fill["quantity"],
                        trade=str(row["id"]),
                        key="formal:" + key,
                        raw=encode(
                            {"implementation": VERSION, "reason": fill["reason"]}
                        ),
                    ),
                )
            else:
                conn.execute(
                    text("update broker_orders set quantity=:qty where id=:id"),
                    dict(id=order_id, qty=fill["quantity"]),
                )
            event_time = datetime.combine(day, time(15, 30), tzinfo=IST)
            conn.execute(
                text(
                    """insert into broker_fills(id,order_id,external_trade_id,fill_time,price,quantity,fees,raw_response)
                values(:id,:order,:key,:time,:price,:qty,:fees,cast(:raw as jsonb))"""
                ),
                dict(
                    id=identity(key + ":fill"),
                    order=order_id,
                    key="formal:" + key,
                    time=event_time,
                    price=fill["price"],
                    qty=fill["quantity"],
                    fees=fill["fees"],
                    raw=encode(
                        {
                            "implementation": VERSION,
                            "observed_bar_date": str(day),
                            "modeled_daily_bar": True,
                            "recorded_at": now,
                        }
                    ),
                ),
            )
            conn.execute(
                text(
                    """insert into portfolio_ledger(event_time,account_mode,event_type,instrument_id,order_id,quantity,price,cash_delta,realized_pnl_delta,fees,metadata)
                values(:time,'PAPER','FILL',:pk,:order,:qty,:price,:cash,:pnl,:fees,cast(:meta as jsonb))"""
                ),
                dict(
                    time=event_time,
                    pk=row["instrument_id"],
                    order=order_id,
                    qty=fill["quantity"] * (1 if fill["side"] == "BUY" else -1),
                    price=fill["price"],
                    cash=(
                        -fill["price"] * fill["quantity"]
                        if fill["side"] == "BUY"
                        else fill["price"] * fill["quantity"]
                    )
                    - fill["fees"],
                    pnl=fill["realized"],
                    fees=fill["fees"],
                    meta=encode(
                        {
                            "implementation": VERSION,
                            "side": fill["side"],
                            "trade_id": str(row["id"]),
                        }
                    ),
                ),
            )
        for event in pos.history[old_history:]:
            conn.execute(
                text(
                    """insert into trade_state_events(trade_id,instrument_id,event_time,state,event_type,payload,strategy_version)
                values(:trade,:pk,:time,:state,:event,cast(:payload as jsonb),:version)"""
                ),
                dict(
                    trade=str(row["id"]),
                    pk=row["instrument_id"],
                    time=now,
                    state=pos.state.value,
                    event=event["event"],
                    payload=encode(event),
                    version=self.version,
                ),
            )
        conn.execute(
            text(
                """insert into trade_state_events(trade_id,instrument_id,event_time,state,event_type,payload,strategy_version)
            values(:trade,:pk,:time,:state,'PAPER_CHECKPOINT',cast(:payload as jsonb),:version)"""
            ),
            dict(
                trade=str(row["id"]),
                pk=row["instrument_id"],
                time=now,
                state=pos.state.value,
                payload=encode(meta),
                version=self.version,
            ),
        )

    def _inputs(self, conn, row, day, calendar):
        prices = [
            dict(r)
            for r in conn.execute(
                text(
                    """select open,high,low,close,volume,source from daily_prices
            where instrument_id=:id and trade_date=:d order by case when source='ANGEL_ONE' then 0 when source like 'NSE%' then 1 else 2 end,source"""
                ),
                dict(id=row["instrument_id"], d=day),
            )
            .mappings()
            .all()
        ]
        if not prices:
            raise ValueError("observed_bar_unavailable")
        raw = sorted(prices, key=lambda r: self.b._source_priority(r["source"]))[0]
        bar = {k: float(raw[k]) for k in ("open", "high", "low", "close")}
        validate_ohlc(*bar.values())
        actions = int(
            conn.execute(
                text(
                    "select count(*) from corporate_actions where instrument_id=:id and ex_date>:signal and ex_date<=:day and action_type in ('SPLIT','BONUS')"
                ),
                dict(id=row["instrument_id"], signal=row["signal_date"], day=day),
            ).scalar_one()
        )
        if actions:
            raise ValueError("corporate_action_requires_position_reconciliation")
        indicators = None
        adtv = 0
        technical = False
        try:
            candles = self.b._candles(row["symbol"], day)
            frame = compute_features(candles).data
            required = sorted(d for d in calendar.sessions if d <= day)[-252:]
            if len(required) != 252 or not set(required).issubset(
                set(frame.index.date)
            ):
                raise ValueError("indicator_sessions_missing")
            last = frame.iloc[-1]
            if frame.index[-1].date() != day or any(
                abs(float(last[k]) - bar[k]) > 1e-8 for k in bar
            ):
                raise ValueError("indicator_bar_source_mismatch")
            previous = frame.iloc[-2]
            indicators = dict(
                ema9=float(last.ema9),
                previous_ema9=float(previous.ema9),
                previous_ema21=float(previous.ema21),
            )
            if any(not math.isfinite(v) or v <= 0 for v in indicators.values()):
                indicators = None
            prior = frame[frame.index.date < day]
            adtv = float(prior.iloc[-1].avg_turnover20)
            prev = prior.iloc[-1]
            technical = (
                float(prev.close) > float(self.cfg["technical"]["min_price"])
                and float(prev.avg_volume20)
                > float(self.cfg["technical"]["min_avg_volume_20d"])
                and float(prev.worst_volume20)
                > float(self.cfg["technical"]["min_worst_day_volume_20d"])
                and float(self.cfg["technical"]["min_avg_turnover_20d_cr"]) * 10000000
                <= adtv
                <= float(self.cfg["technical"]["max_avg_turnover_20d_cr"]) * 10000000
                and 100
                * (float(prev.high_52w) - float(prev.close))
                / float(prev.high_52w)
                <= float(self.cfg["technical"]["max_distance_from_52w_high_pct"])
                and not self.b.reference.is_index_member(row["symbol"], "NIFTY50", day)
                and float(prior.iloc[-1].adr20_pct)
                >= float(self.cfg["technical"]["min_adr_20d_pct"])
                and adtv > 0
            )
        except (ValueError, KeyError, IndexError, TypeError):
            indicators = None
        opening = datetime.combine(day, time(9, 15), tzinfo=IST)
        fundamental = False
        if row["state"] in {"SIGNAL_CREATED", "PENDING_ENTRY"}:
            snapshot, decision = self.a.evaluate(row["symbol"], opening)
            fundamental = (
                bool(decision.eligible)
                and str(snapshot.sector or "").strip().casefold()
                == str((row.get("metadata") or {}).get("sector") or "")
                .strip()
                .casefold()
            )
            # A daily high cannot establish whether a trigger preceded an intraday filing.
            # Gap-at-open entries remain ordered; ambiguous intraday entries need review.
            if (intraday_trigger(bar["open"], bar["high"], float(row["entry"]))
                    and intraday_information_changed(conn, row["instrument_id"], day)):
                raise ValueError("intraday_filing_trigger_order_unknown")
            dates = [
                day,
                *calendar.forward(
                    day + timedelta(days=1),
                    int(self.cfg["technical"]["earnings_blackout_trading_days"]),
                ),
            ]
            technical = (
                technical
                and len(dates)
                == int(self.cfg["technical"]["earnings_blackout_trading_days"]) + 1
                and not self.b.reference.earnings_within_trading_window(
                    row["symbol"], day, dates, as_of=opening
                )
            )
        return bar, indicators, adtv, technical, fundamental

    def settle(self, day=None):
        day, calendar, status, reason = self.session(day)
        report = dict(
            status=status,
            reason=reason,
            trade_date=str(day),
            implementation=VERSION,
            mode="PAPER",
            fills=[],
            issues=[],
            external_broker_orders_submitted=0,
        )
        if status != "READY":
            return report
        with self.engine.begin() as conn:
            self.lock(conn)
            rows = self._rows(conn)
            previous = max((d for d in calendar.sessions if d < day), default=None)
            budget, account_reasons = self._account(conn, previous or day, rows)
            cap, gate_reasons = self._entry_gate(conn, day, calendar)
            existing_fills = int(
                conn.execute(
                    text(
                        """select count(*) from broker_fills f join broker_orders o on o.id=f.order_id
                where o.mode='PAPER' and o.raw_response->'formal_paper' is not null
                and (f.fill_time at time zone 'Asia/Kolkata')::date=:d"""
                    ),
                    {"d": day},
                ).scalar_one()
            )
            if existing_fills:
                gate_reasons.append("session_already_partly_settled")
            report["entry_blockers"] = account_reasons + gate_reasons
            rows.sort(
                key=lambda r: (
                    r["state"] in {"SIGNAL_CREATED", "PENDING_ENTRY"},
                    -float((r.get("metadata") or {}).get("score", 0)),
                    str(r["id"]),
                )
            )
            # At zero capacity cancellations need no market bar or indicators.
            for row in rows:
                meta = row.get("metadata") or {}
                if not meta.get("checkpoint") or meta.get("implementation") != VERSION:
                    report["issues"].append(
                        dict(
                            trade_id=str(row["id"]),
                            reason="paper_checkpoint_unavailable",
                        )
                    )
                    continue
                pending = row["state"] in {"SIGNAL_CREATED", "PENDING_ENTRY"}
                if pending and meta.get("admitted_on", str(day)) >= str(day):
                    continue
                pos = TradePosition.restore(meta["checkpoint"])
                if int(row["remaining_shares"]) != pos.remaining_shares or (
                    not pending and row["state"] != pos.state.value
                ):
                    report["issues"].append(
                        dict(
                            trade_id=str(row["id"]),
                            reason="checkpoint_trade_state_mismatch",
                        )
                    )
                    continue
                if pending and row["order_status"] == "CANCELLED":
                    cancel_entry(pos, "pending_order_cancelled", day)
                    meta["checkpoint"] = pos.checkpoint()
                    self._persist(conn, row, meta, [], day, len(pos.history) - 1)
                    continue
                try:
                    with conn.begin_nested():
                        last = date.fromisoformat(
                            meta.get("last_session") or meta["admitted_on"]
                        )
                        if last != day and [
                            d for d in sorted(calendar.sessions) if last < d <= day
                        ] != [day]:
                            raise ValueError(
                                "unprocessed_exchange_session_requires_review"
                            )
                        if (
                            pending
                            and meta.get("last_session") != str(day)
                            and (
                                gate_reasons
                                or account_reasons
                                or cap == 0
                                or meta.get("config_hash") != fingerprint(self.cfg)
                            )
                        ):
                            before = len(pos.history)
                            cancel_entry(pos, "new_entry_gate_blocked", day)
                            meta["checkpoint"] = pos.checkpoint()
                            self._persist(conn, row, meta, [], day, before)
                            continue
                        bar, indicators, adtv, technical, fundamental = self._inputs(
                            conn, row, day, calendar
                        )
                        work = deepcopy(budget)
                        if pending:
                            entry = float(row["entry"])
                            qty = int(row["shares"])
                            stop = float(row["stop"])
                            fee = estimate_delivery_costs(
                                trade_date=day,
                                side="BUY",
                                price=entry,
                                quantity=qty,
                                cfg=self.cfg["backtest"],
                            ).total
                            work.cash += entry * qty + fee
                            work.risk -= max(0, entry - stop) * qty
                            work.risk = max(0, work.risk)
                            work.slots -= 1
                            work.sectors[meta["sector"].strip().casefold()] -= 1
                            work.instruments.discard(row["symbol"])
                            if row["order_created_at"].astimezone(
                                IST
                            ) >= datetime.combine(day, time(9, 15), tzinfo=IST):
                                raise ValueError(
                                    "order_not_present_before_session_open"
                                )
                        updated, fills, issues = simulate_session(
                            meta,
                            day,
                            bar,
                            indicators,
                            meta.get("config") or self.cfg,
                            work,
                            entry_ready=not gate_reasons
                            and not account_reasons
                            and technical
                            and cap > 0,
                            fundamental_eligible=fundamental,
                            capacity=cap,
                            adtv=adtv,
                        )
                        if meta.get("last_session") == str(day):
                            continue
                        self._persist(conn, row, updated, fills, day, len(pos.history))
                        buy = next((f for f in fills if f["side"] == "BUY"), None)
                        if buy:
                            work.reserve(
                                row["symbol"],
                                meta["sector"],
                                buy["price"],
                                TradePosition.restore(updated["checkpoint"]).history[
                                    len(pos.history)
                                ]["stop"],
                                buy["quantity"],
                                buy["fees"],
                            )
                            budget = work
                        report["fills"].extend(
                            dict(trade_id=str(row["id"]), **f) for f in fills
                        )
                        report["issues"].extend(
                            dict(trade_id=str(row["id"]), reason=v) for v in issues
                        )
                except (ValueError, TypeError, KeyError, IndexError) as exc:
                    report["issues"].append(
                        dict(
                            trade_id=str(row["id"]),
                            reason=(
                                str(exc)
                                if type(exc) is ValueError
                                else type(exc).__name__
                            ),
                        )
                    )
            report["status"] = "BLOCKED" if report["issues"] else "SUCCESS"
            if account_reasons:
                report["status"] = "BLOCKED"
            # A report and its fills commit together. Repeated empty sessions do not certify readiness.
            conn.execute(
                text(
                    """insert into worker_runs(id,worker_name,job_name,status,finished_at,details)
                values(:id,'execution_worker','formal_paper_settlement',:status,now(),cast(:payload as jsonb))
                on conflict(id) do update set status=excluded.status,finished_at=excluded.finished_at,details=excluded.details"""
                ),
                dict(
                    id=identity("settlement:" + str(day)),
                    status=report["status"],
                    payload=encode(report),
                ),
            )
        return report

    def reentry_signals(self, day):
        result = {}
        calendar = MarketCalendar.from_database(self.engine, self.now())
        with self.engine.begin() as conn:
            self.lock(conn)
            for row in self._rows(conn, active_only=False):
                meta = row.get("metadata") or {}
                if not meta.get("checkpoint"):
                    continue
                pos = TradePosition.restore(meta["checkpoint"])
                if not reentry_allowed(pos) or meta.get("config_hash") != fingerprint(
                    self.cfg
                ):
                    continue
                sessions = calendar.forward(
                    pos.reentry_signal_date + timedelta(days=1),
                    int(self.cfg["technical"]["entry_window_days"]),
                )
                if (
                    len(sessions) != int(self.cfg["technical"]["entry_window_days"])
                    or day >= sessions[-1]
                ):
                    continue
                signal = dict(meta["signal"])
                signal.update(
                    signal_date=str(pos.reentry_signal_date),
                    signal_high=pos.reentry_signal_high,
                    signal_low=pos.reentry_signal_low,
                    reentry_source_trade_id=str(row["id"]),
                    setup="SETUP_1",
                    status="ACTIVE",
                )
                result[row["symbol"]] = signal
        return result

    def admit(self, day=None):
        day, calendar, status, reason = self.session(day)
        result = dict(
            status=status,
            reason=reason,
            trade_date=str(day),
            implementation=VERSION,
            paper_orders_created=0,
            orders=[],
            rejected=[],
            external_broker_orders_submitted=0,
        )
        if status != "READY":
            return result
        with self.engine.begin() as conn:
            self.lock(conn)
            if os.getenv('PMS_CLOUD_COLLECTION_REQUIRED','0')=='1':
                from packages.pms.cloud_collection import collection_gate
                now=self.now()
                if not collection_gate(conn,day,now):
                    return {**result,'status':'BLOCKED','reason':'cloud_collection_missing_partial_failed_or_stale'}
            run = (
                conn.execute(
                    text(
                        """select status,details from worker_runs where job_name='daily_orchestrator'
                and details->>'trade_date'=:day order by started_at desc,id desc limit 1"""
                    ),
                    {"day": str(day)},
                )
                .mappings()
                .first()
            )
            if (
                not run
                or run["status"] != "SUCCESS"
                or run["details"].get("config_hash") != fingerprint(self.cfg)
                or run["details"].get("mode") != "PAPER"
                or run["details"].get("implementation") != "DAILY_ORCHESTRATOR_v1.1"
                or ((run["details"].get("stages") or {}).get("paper_settlement") or {})
                .get("result", {})
                .get("status")
                != "SUCCESS"
                or not (
                    (run["details"].get("stages") or {}).get("paper_reconciliation")
                    or {}
                )
                .get("result", {})
                .get("ok")
            ):
                return {
                    **result,
                    "status": "BLOCKED",
                    "reason": "successful_matching_daily_run_required",
                }
            # Admission is AFTER the close; today's E record gates tomorrow's orders.
            tomorrow = calendar.forward(day + timedelta(days=1), 1)
            if not tomorrow:
                return {
                    **result,
                    "status": "BLOCKED",
                    "reason": "next_exchange_session_unavailable",
                }
            cap, blockers = self._entry_gate(conn, tomorrow[0], calendar)
            rows = self._rows(conn)
            budget, account_blockers = self._account(conn, day, rows)
            if blockers or account_blockers or cap == 0:
                return {
                    **result,
                    "status": "BLOCKED",
                    "reason": ",".join(
                        blockers
                        + account_blockers
                        + (["zero_capacity"] if cap == 0 else [])
                    ),
                }
            proposals = [
                dict(r)
                for r in conn.execute(
                    text(
                        """select s.decision,s.instrument_id source_instrument_pk,i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) source_symbol from strategy_decisions s join instruments i on i.id=s.instrument_id where module='C'
                and (as_of at time zone 'Asia/Kolkata')::date=:d and as_of>=:started and as_of<=:finished and strategy_version=:version and decision->>'selected'='true'
                order by (decision->>'rank')::int,instrument_id"""
                    ),
                    dict(
                        d=day,
                        version=self.c_version,
                        started=run["details"]["started_at"],
                        finished=run["details"]["finished_at"],
                    ),
                )
                .mappings()
                .all()
            ]
            for item in proposals:
                proposal = item["decision"]
                if (
                    int(item["source_instrument_pk"]) != int(proposal["instrument_pk"])
                    or item["source_symbol"] != proposal["instrument_id"]
                ):
                    result["rejected"].append(
                        dict(
                            instrument_id=proposal.get("instrument_id"),
                            reason="proposal_instrument_identity_mismatch",
                        )
                    )
                    continue
                symbol = proposal["instrument_id"]
                signal = proposal["signal"]
                key = (
                    symbol
                    + ":"
                    + signal["signal_date"]
                    + ":"
                    + str(signal.get("reentry_source_trade_id") or "initial")
                )
                trade_id = identity(key)
                existing = conn.execute(
                    text("select id from trades where id=:id"), {"id": trade_id}
                ).scalar_one_or_none()
                if existing:
                    result["orders"].append(dict(trade_id=trade_id, replayed=True))
                    continue
                try:
                    with conn.begin_nested():
                        if (
                            proposal.get("global_blockers")
                            or not proposal.get("qualified")
                            or signal.get("setup") != "SETUP_1"
                        ):
                            raise ValueError("proposal_not_production_qualified")
                        signal_day = date.fromisoformat(signal["signal_date"])
                        sessions = calendar.forward(
                            signal_day + timedelta(days=1),
                            int(self.cfg["technical"]["entry_window_days"]),
                        )
                        if (
                            len(sessions)
                            != int(self.cfg["technical"]["entry_window_days"])
                            or tomorrow[0] not in sessions
                        ):
                            raise ValueError("entry_window_expired")
                        # No reuse of stale C membership: revalidate A and B at admission.
                        b = self.b.evaluate(symbol, day, require_module_a=True)
                        if not b.eligible_universe or b.research_only:
                            raise ValueError("fresh_a_b_eligibility_failed")
                        if not signal.get("reentry_source_trade_id") and (
                            not b.signal
                            or any(
                                b.signal[k] != signal[k]
                                for k in ("signal_date", "signal_high", "signal_low")
                            )
                        ):
                            raise ValueError("signal_changed_since_allocation")
                        pk = int(proposal["instrument_pk"])
                        sector = proposal["sector"]
                        entry = float(proposal["buy_stop"])
                        low = float(signal["signal_low"])
                        size = sizing(self.cfg, budget.nav, entry, low)
                        qty = budget.limit(
                            self.cfg,
                            instrument=symbol,
                            sector=sector,
                            entry=entry,
                            stop=size.stop,
                            adtv=float(proposal["stats"]["adtv"]),
                            capacity=cap,
                            ceiling=min(size.shares, int(proposal["shares"])),
                        )
                        if qty < 1:
                            raise ValueError("portfolio_limits_at_admission")
                        fees = estimate_delivery_costs(
                            trade_date=tomorrow[0],
                            side="BUY",
                            price=entry,
                            quantity=qty,
                            cfg=self.cfg["backtest"],
                        ).total
                        if entry * qty + fees > budget.cash:
                            raise ValueError("cash_including_fees_at_admission")
                        parent = None
                        if signal.get("reentry_source_trade_id"):
                            parent = next(
                                (
                                    r
                                    for r in self._rows(conn, active_only=False)
                                    if str(r["id"]) == signal["reentry_source_trade_id"]
                                ),
                                None,
                            )
                            if not parent:
                                raise ValueError("reentry_parent_unavailable")
                            parent_meta = deepcopy(parent["metadata"])
                            original = TradePosition.restore(parent_meta["checkpoint"])
                            pos = create_reentry(
                                original,
                                budget.nav,
                                fundamental_eligible=True,
                                entry_sessions=sessions,
                            )
                            if (pos.signal_date, pos.signal_high, pos.signal_low) != (
                                signal_day,
                                float(signal["signal_high"]),
                                float(signal["signal_low"]),
                            ):
                                raise ValueError("reentry_candle_mismatch")
                        else:
                            pos = TradePosition(
                                symbol,
                                signal_day,
                                float(signal["signal_high"]),
                                low,
                                budget.nav,
                                entry_sessions=tuple(sessions),
                            )
                        activate_entry(pos)
                        meta = dict(
                            implementation=VERSION,
                            config_hash=fingerprint(self.cfg),
                            config=self.cfg,
                            sector=sector,
                            score=float(proposal["score"]),
                            signal=signal,
                            admitted_on=str(day),
                            admitted_shares=qty,
                            daily_run_id=str(run["details"]["run_id"]),
                            checkpoint=pos.checkpoint(),
                        )
                        order_id = identity(key + ":entry")
                        conn.execute(
                            text(
                                """insert into trades(id,instrument_id,signal_date,state,entry,stop,shares,remaining_shares,strategy_version)
                            values(:id,:pk,:day,'PENDING_ENTRY',:entry,:stop,:qty,0,:version)"""
                            ),
                            dict(
                                id=trade_id,
                                pk=pk,
                                day=signal_day,
                                entry=entry,
                                stop=size.stop,
                                qty=qty,
                                version=self.version,
                            ),
                        )
                        conn.execute(
                            text(
                                """insert into broker_orders(id,broker,mode,instrument_id,side,order_type,quantity,trigger_price,status,strategy_trade_id,idempotency_key,raw_response)
                            values(:id,'PAPER','PAPER',:pk,'BUY','STOP_MARKET',:qty,:entry,'OPEN',:trade,:key,cast(:raw as jsonb))"""
                            ),
                            dict(
                                id=order_id,
                                pk=pk,
                                qty=qty,
                                entry=entry,
                                trade=trade_id,
                                key="formal:" + key,
                                raw=encode({"formal_paper": meta}),
                            ),
                        )
                        record = dict(
                            id=trade_id,
                            instrument_id=pk,
                            entry=entry,
                            stop=size.stop,
                            shares=qty,
                            entry_order_id=order_id,
                        )
                        self._persist(conn, record, meta, [], day, 0)
                        if parent:
                            parent_meta["checkpoint"] = original.checkpoint()
                            self._persist(
                                conn,
                                parent,
                                parent_meta,
                                [],
                                day,
                                len(original.history) - 1,
                            )
                        budget.reserve(symbol, sector, entry, size.stop, qty, fees)
                        result["orders"].append(
                            dict(
                                trade_id=trade_id,
                                order_id=order_id,
                                quantity=qty,
                                replayed=False,
                            )
                        )
                        result["paper_orders_created"] += 1
                except (ValueError, TypeError, KeyError) as exc:
                    result["rejected"].append(
                        dict(
                            instrument_id=symbol,
                            reason=(
                                str(exc)
                                if type(exc) is ValueError
                                else type(exc).__name__
                            ),
                        )
                    )
            result["status"] = "SUCCESS"
        return result
