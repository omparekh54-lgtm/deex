from __future__ import annotations
import json, math
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
from sqlalchemy import text
import pandas as pd

from packages.data_layer.reference_data import CorporateAction
from packages.pms.config import load_strategy_config
from packages.pms.market_calendar import MarketCalendar
from packages.pms.policies import regime_benchmark
from packages.pms.module_a_context import IST, end_of_day, as_ist
from packages.pms.module_e import (
    ENGINE_VERSION,
    RegimeInputs,
    RegimeDecision,
    RegimeTracker,
    pending_entry_plan,
)
from packages.pms.module_e_data import (
    COVERAGE_SQL,
    classify_liquidity,
    verified_breadth,
)


class ModuleEPipeline:
    """Verified after-close regime; persists session state and formal PAPER controls."""

    MIDSMALL_CODE = "NIFTYMIDSMALLCAP400"
    VIX_CODE = "INDIAVIX"

    def __init__(self, engine, cfg=None):
        self.engine = engine
        self.cfg = cfg or load_strategy_config()
        self.MIDSMALL_CODE = regime_benchmark(self.cfg)
        self.last_report = {}

    def _benchmark_rows(self, code, expected, start):
        with self.engine.connect() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    text("""select distinct on(trade_date) trade_date,close from module_e_benchmark_bars
                where benchmark_code=:code and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date between :start and :end
                order by trade_date,source desc"""),
                    {"code": code, "start": start, "end": expected},
                )
                .mappings()
                .all()
            ]

    def _benchmarks(self, expected, calendar):
        sessions = sorted(d for d in calendar.sessions if d <= expected)
        if len(sessions) < 205:
            raise ValueError("benchmark_exchange_history_incomplete")
        output = {}
        for code, count in ((self.MIDSMALL_CODE, 205), (self.VIX_CODE, 6)):
            needed = sessions[-count:]
            rows = self._benchmark_rows(code, expected, needed[0])
            values = {r["trade_date"]: float(r["close"]) for r in rows}
            if not set(needed).issubset(values) or any(
                not math.isfinite(v) or v <= 0 for v in values.values()
            ):
                raise ValueError(code + "_official_sessions_missing_or_invalid")
            output[code] = pd.Series([values[d] for d in needed], index=needed)
        index = output[self.MIDSMALL_CODE]
        vix = output[self.VIX_CODE]
        dma200 = index.rolling(200).mean()
        return {
            "midcap_close": float(index.iloc[-1]),
            "midcap_dma50": float(index.iloc[-50:].mean()),
            "midcap_dma200": float(dma200.iloc[-1]),
            "midcap_dma200_slope_pct": (
                float(dma200.iloc[-1]) / float(dma200.iloc[-6]) - 1
            )
            * 100,
            "india_vix": float(vix.iloc[-1]),
            "india_vix_5d_change_pct": (float(vix.iloc[-1]) / float(vix.iloc[-6]) - 1)
            * 100,
        }

    def _coverage(self, expected):
        with self.engine.connect() as conn:
            rows = [
                dict(r)
                for r in conn.execute(text(COVERAGE_SQL), {"d": expected})
                .mappings()
                .all()
            ]
        return classify_liquidity(
            rows,
            expected,
            float(self.cfg["regime"].get("breadth_min_turnover_cr", 1)) * 10000000,
        )

    def _breadth(self, liquid, expected, calendar):
        ids = json.dumps([r["id"] for r in liquid])
        start = expected - timedelta(days=1000)
        with self.engine.connect() as conn:
            prices = [
                dict(r)
                for r in conn.execute(
                    text(
                        """select instrument_id,trade_date,open,high,low,close,volume,turnover,source
                from daily_prices where trade_date between :start and :d
                and instrument_id in(select value::bigint from jsonb_array_elements_text(cast(:ids as jsonb)))
                order by instrument_id,trade_date"""
                    ),
                    {"start": start, "d": expected, "ids": ids},
                )
                .mappings()
                .all()
            ]
            action_rows = (
                conn.execute(
                    text(
                        """select i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) instrument_id,
                c.action_type,c.ex_date,c.ratio_from,c.ratio_to,c.cash_amount,c.announced_at,c.source
                from corporate_actions c join instruments i on i.id=c.instrument_id
                where c.ex_date between :start and :d
                and c.instrument_id in(select value::bigint from jsonb_array_elements_text(cast(:ids as jsonb)))"""
                    ),
                    {"start": start, "d": expected, "ids": ids},
                )
                .mappings()
                .all()
            )
        actions = [CorporateAction(**dict(r)) for r in action_rows]
        return verified_breadth(liquid, prices, actions, calendar, expected, self.cfg)

    def _strategy_feedback(self, stamp):
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text("""select t.id,t.realized_pnl,t.updated_at,
              array(select distinct o.mode from broker_orders o where o.strategy_trade_id=t.id and o.created_at<=:stamp) account_modes,
              (select e.payload from trade_state_events e where e.trade_id=t.id and e.event_type in ('FILLED','ENTRY_FILLED')
                and e.event_time<=:stamp order by e.event_time,e.id limit 1) initial_fill,
              coalesce((select sum(f.fees) from broker_fills f join broker_orders o on o.id=f.order_id
                where o.strategy_trade_id=t.id and o.mode='PAPER' and f.fill_time<=:stamp),0) fees
              from trades t where t.state='CLOSED' and t.updated_at<=:stamp
              order by t.updated_at desc,t.id desc"""),
                    {"stamp": stamp},
                )
                .mappings()
                .all()
            )
        return strategy_feedback([dict(r) for r in rows])

    def _previous_tracker(self, expected, calendar):
        previous = max((d for d in calendar.sessions if d < expected), default=None)
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "select trade_date,inputs,strategy_version from market_regime_history where trade_date<:d order by trade_date desc limit 1"
                    ),
                    {"d": expected},
                )
                .mappings()
                .first()
            )
        if (
            row
            and row["trade_date"] == previous
            and row["strategy_version"] == self.cfg["version"]
        ):
            payload = row["inputs"] or {}
            if payload.get("_module_e_version") == ENGINE_VERSION and payload.get(
                "_tracker_after"
            ):
                try:
                    tracker = RegimeTracker.restore(payload["_tracker_after"])
                    if tracker.last_session == previous:
                        return tracker, "previous_consecutive_session"
                except (ValueError, TypeError, KeyError):
                    pass
        return RegimeTracker(), "cold_start_or_data_gap"

    def resolve_session(self, as_of):
        if as_of > datetime.now(IST).date():
            raise ValueError("as_of must not be in the future")
        stamp = min(end_of_day(as_of), datetime.now(IST))
        calendar = MarketCalendar.from_database(self.engine, stamp)
        if (
            stamp.date() == as_of
            and stamp.hour * 60 + stamp.minute < 15 * 60 + 30
            and calendar.forward(as_of, 1) == [as_of]
        ):
            raise ValueError("Module E runs after market close")
        closed = (
            stamp.date()
            if stamp.hour * 60 + stamp.minute >= 15 * 60 + 30
            else stamp.date() - timedelta(days=1)
        )
        sessions = sorted(d for d in calendar.sessions if d <= closed)
        if not sessions:
            raise ValueError("Official exchange calendar unavailable")
        return stamp, calendar, sessions[-1], closed

    def evaluate(self, as_of, *, persist=True):
        stamp, calendar, expected, closed = self.resolve_session(as_of)
        reasons = []
        quality = {}
        inputs = None
        checkpoint = None
        if (closed - expected).days > 7:
            reasons.append("market_calendar_stale")
        try:
            benchmark = self._benchmarks(expected, calendar)
        except ValueError as exc:
            benchmark = {}
            reasons.append(str(exc))
        quality["benchmarks"] = {
            "data_ready": bool(benchmark),
            "source": "OFFICIAL_NSE_HISTORY_OR_DAILY_INDEX",
            "index_code": self.MIDSMALL_CODE,
            "values": benchmark,
        }
        liquid, coverage = self._coverage(expected)
        quality["liquidity"] = coverage
        if not coverage["master_universe"]:
            reasons.append("breadth_universe_unavailable")
        if coverage["unclassified"]:
            reasons.append("breadth_liquidity_coverage_incomplete")
        if not liquid:
            reasons.append("no_verified_liquid_stocks")
        breadth = None
        if liquid and not coverage["unclassified"]:
            breadth, measured = self._breadth(liquid, expected, calendar)
            quality["breadth"] = measured
            if breadth is None:
                reasons.append("breadth_price_coverage_incomplete")
        feedback = self._strategy_feedback(min(stamp, end_of_day(expected)))
        quality["feedback"] = feedback
        reasons.extend(feedback["reasons"])
        prior = "not_loaded_for_invalid_data"
        if not reasons:
            inputs = RegimeInputs(
                **benchmark,
                **breadth,
                strategy_expectancy_r=feedback["expectancy_r"],
                closed_trades=feedback["closed_trades"],
            )
            tracker, prior = self._previous_tracker(expected, calendar)
            decision = tracker.update(
                inputs, self.cfg, session=expected, require_full_window=True
            )
            checkpoint = tracker.checkpoint()
        else:
            decision = RegimeDecision(
                0,
                0,
                "UNAVAILABLE",
                0,
                False,
                data_ready=False,
                reasons=tuple(dict.fromkeys(reasons)),
                reason="New entries blocked: " + ", ".join(dict.fromkeys(reasons)),
                trade_date=expected,
            )
        payload = asdict(inputs) if inputs else {}
        payload.update(
            _module_e_version=ENGINE_VERSION,
            _data_quality={
                "data_ready": decision.data_ready,
                **compact_quality(quality),
            },
            _reasons=list(decision.reasons),
            _reason=decision.reason,
            _tracker_after=checkpoint,
            _tracker_source=prior,
            _requested_date=as_of.isoformat(),
            _evaluated_at=stamp.isoformat(),
            _benchmark_name="NIFTY MIDSMALLCAP 400",
            _feedback_mode="PAPER",
            _read_only=not persist,
        )
        self.last_report = {
            "requested_date": as_of.isoformat(),
            "trade_date": expected.isoformat(),
            "strategy_version": self.cfg["version"],
            "implementation_version": ENGINE_VERSION,
            "inputs": asdict(inputs) if inputs else None,
            "decision": asdict(decision),
            "data_quality": quality,
            "tracker_source": prior,
            "persisted": persist,
            "broker_orders_submitted": 0,
        }
        if persist:
            actions = self._persist(expected, decision, payload, stamp)
            self.last_report["paper_pending_controls"] = actions
        return inputs, decision

    def _persist(self, expected, decision, payload, stamp):
        params = {
            "d": expected,
            "raw": decision.raw_score,
            "score": decision.score,
            "band": decision.band,
            "cap": decision.max_open_trades,
            "hard": decision.hard_override,
            "components": json.dumps(decision.components, allow_nan=False),
            "inputs": json.dumps(payload, default=str, allow_nan=False),
            "version": self.cfg["version"],
        }
        with self.engine.begin() as conn:
            conn.execute(
                text("select pg_advisory_xact_lock(hashtext(:account_lock))"),
                {"account_lock": "pms_formal_paper_account"},
            )
            conn.execute(
                text(
                    "select pg_advisory_xact_lock(hashtext('module_e_regime_and_paper_capacity'))"
                )
            )
            later = conn.execute(
                text("select count(*) from market_regime_history where trade_date>:d"),
                params,
            ).scalar_one()
            if later:
                raise ValueError(
                    "Historical persistence requires a chronological replay; later regime rows already exist"
                )
            controls = self._pending_controls(conn, expected, decision.max_open_trades)
            payload["_paper_pending_controls"] = controls
            params["inputs"] = json.dumps(payload, default=str, allow_nan=False)
            conn.execute(
                text(
                    """insert into market_regime_history(trade_date,raw_score,smoothed_score,band,max_open_trades,hard_override,components,inputs,strategy_version)
                values(:d,:raw,:score,:band,:cap,:hard,cast(:components as jsonb),cast(:inputs as jsonb),:version)
                on conflict(trade_date) do update set raw_score=excluded.raw_score,smoothed_score=excluded.smoothed_score,
                band=excluded.band,max_open_trades=excluded.max_open_trades,hard_override=excluded.hard_override,
                components=excluded.components,inputs=excluded.inputs,strategy_version=excluded.strategy_version"""
                ),
                params,
            )
        return controls

    def _pending_controls(self, conn, expected, capacity):
        version = (
            self.cfg["version"]
            + ":"
            + self.cfg["ranking"].get("implementation_version", "MODULE_C_v1.1")
            + ":PAPER"
        )
        params = {"d": expected, "version": version}
        rows = [
            dict(r)
            for r in conn.execute(
                text(
                    """select o.id,o.strategy_trade_id,o.created_at,
            (select r.score from module_c_rankings r where r.instrument_id=o.instrument_id and r.trade_date<=:d
             and r.strategy_version=:version order by r.trade_date desc limit 1) score
            from broker_orders o where o.mode='PAPER' and o.side='BUY'
            and o.status in ('OPEN','PENDING','SUBMITTED','PARTIALLY_FILLED') order by o.id for update"""
                ),
                params,
            )
            .mappings()
            .all()
        ]
        if not rows:
            return {
                "keep": [],
                "cancel": [],
                "reason": "regime_capacity",
                "capacity": capacity,
                "open_positions": None,
            }
        snapshot = conn.execute(
            text(
                "select positions from portfolio_snapshots where account_mode='PAPER' and snapshot_date=:d"
            ),
            params,
        ).scalar_one_or_none()
        known = isinstance(snapshot, list)
        try:
            if any(int(p["quantity"]) < 0 for p in snapshot or []):
                known = False
            open_count = len(
                {
                    int(p["instrument_id"])
                    for p in snapshot or []
                    if int(p["quantity"]) > 0
                }
            )
        except (TypeError, KeyError, ValueError):
            known = False
            open_count = 0
        # Count fills since the snapshot as well; an old snapshot can overcount
        # already-closed holdings, but must never hide a newly filled position.
        current_open = (
            conn.execute(
                text(
                    """select distinct t.instrument_id from trades t
            where t.remaining_shares>0 and t.state not in ('CLOSED','CANCELLED')
            and exists(select 1 from broker_orders o where o.strategy_trade_id=t.id and o.mode='PAPER')"""
                )
            )
            .scalars()
            .all()
        )
        if known:
            open_count = len(
                {int(p["instrument_id"]) for p in snapshot if int(p["quantity"]) > 0}
                | {int(i) for i in current_open}
            )
        plan = pending_entry_plan(open_count, rows, capacity, account_known=known)
        if plan["cancel"]:
            cancel_params = {
                "ids": json.dumps(plan["cancel"]),
                "reason": plan["reason"],
            }
            conn.execute(
                text(
                    """update broker_orders set status='CANCELLED',updated_at=now(),
                raw_response=coalesce(raw_response,'{}'::jsonb)||jsonb_build_object('module_e_cancel_reason',:reason)
                where mode='PAPER' and side='BUY' and status in ('OPEN','PENDING','SUBMITTED','PARTIALLY_FILLED')
                and id in(select value::uuid from jsonb_array_elements_text(cast(:ids as jsonb)))"""
                ),
                cancel_params,
            )
            conn.execute(
                text(
                    """update trades t set state='CANCELLED',exit_reason=:reason,updated_at=now()
                where t.state in ('SIGNAL_CREATED','PENDING_ENTRY') and coalesce(t.remaining_shares,0)=0
                and exists(select 1 from broker_orders o where o.strategy_trade_id=t.id and o.mode='PAPER'
                  and o.id in(select value::uuid from jsonb_array_elements_text(cast(:ids as jsonb))))
                and not exists(select 1 from broker_orders o where o.strategy_trade_id=t.id and o.mode<>'PAPER')
                and not exists(select 1 from broker_fills f join broker_orders o on o.id=f.order_id
                  where o.strategy_trade_id=t.id and o.side='BUY' and f.quantity>0)"""
                ),
                cancel_params,
            )
        return plan


def strategy_feedback(rows):
    selected = []
    reasons = []
    for row in rows:
        modes = set(row.get("account_modes") or [])
        if modes and "PAPER" not in modes:
            continue
        if modes != {"PAPER"}:
            reasons.append("strategy_trade_account_mode_unverified")
            continue
        selected.append(row)
        if len(selected) == 15:
            break
    count = len(selected)
    result = {
        "closed_trades": count,
        "expectancy_r": None,
        "reasons": list(dict.fromkeys(reasons)),
        "neutral_until_ten_trades": count < 10,
        "sample_limit": 15,
        "pnl_basis": "realized_pnl less recorded PAPER fill fees",
    }
    if count < 10:
        return result
    multiples = []
    for row in selected:
        initial = row.get("initial_fill") or {}
        try:
            risk = initial.get("initial_risk")
            if risk is None:
                risk = (float(initial["entry"]) - float(initial["stop"])) * int(
                    initial["shares"]
                )
            risk = float(risk)
            pnl = float(row["realized_pnl"]) - float(row.get("fees") or 0)
            if not math.isfinite(risk) or risk <= 0 or not math.isfinite(pnl):
                raise ValueError()
            multiples.append(pnl / risk)
        except (KeyError, TypeError, ValueError):
            result["reasons"].append("strategy_initial_risk_or_pnl_unavailable")
    if len(multiples) == count:
        result["expectancy_r"] = sum(multiples) / count
    result["reasons"] = list(dict.fromkeys(result["reasons"]))
    return result


def compact_quality(value):
    """Bound persisted/API diagnostic samples while retaining exact issue counts."""
    from copy import deepcopy

    result = deepcopy(value)
    for item in result.values():
        if isinstance(item, dict) and "exceptions" in item:
            item["exception_count"] = len(item["exceptions"])
            item["exceptions"] = item["exceptions"][:25]
    return result
