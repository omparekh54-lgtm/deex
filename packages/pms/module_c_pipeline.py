from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, field, asdict, replace
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

from sqlalchemy import text
from packages.data_layer.features import compute_features
from packages.pms.config import load_strategy_config
from packages.pms.costs import estimate_delivery_costs
from packages.pms.module_a_context import IST, end_of_day
from packages.pms.module_a_pipeline import ModuleAPipeline
from packages.pms.module_b_pipeline import ModuleBPipeline
from packages.pms.module_c import (
    Candidate,
    allocate_candidates,
    candidate_rejections,
    rank_candidate,
)
from packages.pms.module_c_inputs import (
    measure_features,
    ranking_inputs,
    portfolio_state,
)
from packages.pms.risk import size_position


@dataclass(frozen=True)
class ModuleCGenerationResult:
    trade_date: date
    universe_size: int
    a_eligible: int
    b_signals: int
    ranked: int
    qualified: int
    selected: int
    rejected: int
    regime_capacity: int
    watchlist: tuple[dict, ...] = ()
    order_sheet: tuple[dict, ...] = ()
    results: tuple[dict, ...] = ()
    blockers: tuple[str, ...] = ()
    strategy_version: str = ""
    persisted: bool = False
    projected_heat_pct: float = 0.0
    remaining_cash: float = 0.0
    rejection_counts: dict[str, int] = field(default_factory=dict)
    upstream: dict = field(default_factory=dict)


def _clamp100(value):
    if not math.isfinite(float(value)):
        raise ValueError("Non-finite ranking value")
    return max(0.0, min(100.0, float(value)))


def _percentile_map(values, *, reverse=False):
    if any(not math.isfinite(float(v)) for v in values.values()):
        raise ValueError("Non-finite percentile input")
    if not values:
        return {}
    if len(values) == 1:
        return {next(iter(values)): 50.0}
    ordered = sorted(values.items(), key=lambda kv: (kv[1], kv[0]), reverse=not reverse)
    output = {}
    i = 0
    n = len(ordered)
    while i < n:
        j = i
        while j + 1 < n and ordered[j + 1][1] == ordered[i][1]:
            j += 1
        score = 100 * (1 - (i + j) / 2 / (n - 1))
        for k in range(i, j + 1):
            output[ordered[k][0]] = round(score, 4)
        i = j + 1
    return output


class ModuleCPipeline:
    """After-close PAPER order proposals; never submits orders or invents account state."""

    def _evaluate_signal(self, instrument, day):
        return self.b.evaluate(instrument, day, require_module_a=True)

    def _validate_signal(self, result):
        if result.research_only:
            raise ValueError("research_signal_not_production")
        if result.signal and (
            result.signal.get("setup") != "SETUP_1"
            or result.signal.get("status") not in {"ACTIVE", "SIGNAL_CREATED"}
        ):
            raise ValueError("unsupported_or_inactive_signal")

    def __init__(self, engine, cfg=None):
        self.engine = engine
        self.cfg = cfg or load_strategy_config()
        self.a = ModuleAPipeline(engine, self.cfg)
        self.b = ModuleBPipeline(engine, self.cfg)

    @property
    def version(self):
        return (
            self.cfg["version"]
            + ":"
            + self.cfg["ranking"].get("implementation_version", "MODULE_C_v1.1")
            + ":PAPER"
        )

    @staticmethod
    def _as_of_end(d):
        return min(end_of_day(d), datetime.now(IST))

    def _benchmark_returns(self, expected, calendar):
        needed = sorted(day for day in calendar.sessions if day <= expected)[-127:]
        if len(needed) != 127:
            raise ValueError("ranking_exchange_history_unavailable")
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text("""select distinct on(trade_date) trade_date,close from module_e_benchmark_bars
                where benchmark_code='NIFTY500' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX')
                and trade_date between :start and :end order by trade_date,source desc"""),
                    {"start": needed[0], "end": expected},
                )
                .mappings()
                .all()
            )
        values = {r["trade_date"]: float(r["close"]) for r in rows}
        if not set(needed).issubset(values) or any(
            not math.isfinite(v) or v <= 0 for v in values.values()
        ):
            raise ValueError("nifty500_history_unavailable")
        return {
            n: (values[needed[-1]] / values[needed[-n - 1]] - 1) * 100
            for n in (63, 126)
        }

    def _regime_capacity(self, expected):
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "select max_open_trades,hard_override,strategy_version,inputs from market_regime_history where trade_date=:d"
                    ),
                    {"d": expected},
                )
                .mappings()
                .first()
            )
        if not row:
            return 0, ("market_regime_unavailable",)
        if row["strategy_version"] != self.cfg["version"]:
            return 0, ("market_regime_version_mismatch",)
        metadata = row.get("inputs") or {}
        if metadata.get("_module_e_version") != self.cfg["regime"].get(
            "implementation_version", "MODULE_E_v1.1"
        ):
            return 0, ("market_regime_implementation_unverified",)
        if not (metadata.get("_data_quality") or {}).get("data_ready"):
            return 0, ("market_regime_data_unavailable",)
        value = int(row["max_open_trades"])
        if value < 0 or value > int(self.cfg["risk"]["max_open_trades"]):
            return 0, ("market_regime_invalid",)
        return (0 if row["hard_override"] else value), ()

    def _portfolio_state(self, expected, as_of, sectors):
        with self.engine.connect() as conn:
            snapshot = (
                conn.execute(
                    text("""select * from portfolio_snapshots
                where account_mode='PAPER' and snapshot_date<=:d and created_at<=:stamp
                order by snapshot_date desc,created_at desc limit 1"""),
                    {"d": expected, "stamp": as_of},
                )
                .mappings()
                .first()
            )
            trades = (
                conn.execute(
                    text(
                        """select t.*,array(select distinct o.mode from broker_orders o
                where o.strategy_trade_id=t.id and o.created_at<=:stamp) account_modes
                from trades t where t.created_at<=:stamp and (t.state not in ('CLOSED','CANCELLED') or t.updated_at>:stamp)"""
                    ),
                    {"stamp": as_of},
                )
                .mappings()
                .all()
            )
            orders = (
                conn.execute(
                    text(
                        """select o.*,coalesce((select sum(f.quantity) from broker_fills f
                where f.order_id=o.id and f.fill_time<=:stamp),0) filled_quantity
                from broker_orders o where o.mode='PAPER' and o.created_at<=:stamp
                and (o.status in ('OPEN','PENDING','SUBMITTED','PARTIALLY_FILLED') or o.updated_at>:stamp)"""
                    ),
                    {"stamp": as_of},
                )
                .mappings()
                .all()
            )
        return portfolio_state(
            dict(snapshot) if snapshot else None,
            [dict(r) for r in trades],
            [dict(r) for r in orders],
            sectors,
            expected,
            as_of,
            self.cfg,
        )

    def generate(
        self,
        d: date,
        *,
        persist=True,
        persist_upstream=False,
        reentry_signals=None,
        selection_priority=None,
    ):
        if selection_priority is not None and (persist or persist_upstream):
            raise ValueError("Alternate ordering is research-only")
        if persist_upstream and not persist:
            raise ValueError("Upstream persistence requires persisted Module C output")
        if d > datetime.now(IST).date():
            raise ValueError("as_of must not be in the future")
        stamp = self._as_of_end(d)
        calendar = self.b._calendar(d)
        if (
            stamp.date() == d
            and stamp.hour * 60 + stamp.minute < 15 * 60 + 30
            and calendar.forward(d, 1) == [d]
        ):
            raise ValueError("Module C runs after market close")
        closed = (
            stamp.date()
            if stamp.hour * 60 + stamp.minute >= 15 * 60 + 30
            else stamp.date() - timedelta(days=1)
        )
        observed = sorted(day for day in calendar.sessions if day <= closed)
        expected = observed[-1] if observed else d
        blockers = []
        if not observed or (closed - expected).days > int(
            self.cfg["technical"].get("max_price_age_days", 7)
        ):
            blockers.append("market_calendar_unavailable_or_stale")
        try:
            benchmark = self._benchmark_returns(expected, calendar)
        except ValueError as exc:
            benchmark = {}
            blockers.append(str(exc))
        a_scan = self.a.scan(stamp, persist=persist_upstream)
        if persist_upstream and not a_scan.get("configuration_ready", False):
            blockers.append("module_a_configuration_not_ready")
        if persist_upstream and not a_scan["results"]:
            blockers.append("module_a_universe_unavailable")
        b_results = []
        sectors = {}
        identities = {}
        records = {}
        ready = []
        for item in a_scan["results"]:
            instrument = item["instrument_id"]
            snapshot = item["snapshot"]
            decision = item["decision"]
            pk = int(snapshot["reference_context"]["id"])
            identities[instrument] = pk
            sector = snapshot.get("sector")
            if sector:
                sectors[pk] = sector
            row = {
                "instrument_id": instrument,
                "instrument_pk": pk,
                "sector": sector,
                "score": 0.0,
                "qualified": False,
                "selected": False,
                "components": {},
                "rejected_reasons": [],
                "kind": "REJECTED",
                "module_a_reasons": list(decision.get("failed_rules") or []),
            }
            records[instrument] = row
            if not decision["eligible"]:
                row["rejected_reasons"] = ["module_a_ineligible"]
                continue
            if not sector:
                row["rejected_reasons"] = ["sector_unavailable_as_of"]
                continue
            try:
                b = self._evaluate_signal(instrument, d)
                b_results.append(b)
                self._validate_signal(b)
                if not b.eligible_universe:
                    row["rejected_reasons"] = list(b.rejected_reasons)
                    continue
                if reentry_signals and instrument in reentry_signals:
                    b = replace(
                        b, signal=dict(reentry_signals[instrument]), watchlist=None
                    )
                if not b.signal and not b.watchlist:
                    row["rejected_reasons"] = ["no_live_setup"]
                    continue
                if not benchmark:
                    raise ValueError("nifty500_history_unavailable")
                signal = dict(b.signal or b.watchlist)
                if b.signal and date.fromisoformat(signal["signal_date"]) > d:
                    raise ValueError("future_signal")
                frame = self.b.features(self.b._candles(instrument, d)).data
                if frame.empty or frame.index[-1].date() != expected:
                    raise ValueError("ranking_price_history_stale")
                signal_date = (
                    date.fromisoformat(signal["signal_date"]) if b.signal else expected
                )
                stats = measure_features(frame, signal_date, benchmark)
                row.update(
                    kind="SIGNAL" if b.signal else "WATCHLIST",
                    signal=signal,
                    stats=stats,
                )
                ready.append(
                    {"item": item, "row": row, "stats": stats, "signal": signal, "b": b}
                )
            except (ValueError, KeyError) as exc:
                row["rejected_reasons"] = [str(exc)]
        state, held = self._portfolio_state(expected, stamp, sectors)
        blockers.extend(state.blockers)
        capacity, regime_blockers = self._regime_capacity(expected)
        blockers.extend(regime_blockers)
        blockers = list(dict.fromkeys(blockers))
        state = replace(state, blockers=tuple(blockers))
        relatives = {
            r["row"]["instrument_pk"]: (
                r["stats"]["relative_63"] + r["stats"]["relative_126"]
            )
            / 2
            for r in ready
        }
        relative_rank = _percentile_map(relatives)
        sector_returns = {}
        for r in ready:
            sector_returns.setdefault(r["row"]["sector"], []).append(
                relatives[r["row"]["instrument_pk"]]
            )
        medians = {s: median(v) for s, v in sector_returns.items()}
        sector_rank = _percentile_map(medians)
        candidates = []
        watchlist = []
        for value in ready:
            row = value["row"]
            stats = value["stats"]
            signal = value["signal"]
            pk = row["instrument_pk"]
            try:
                inputs = ranking_inputs(
                    signal,
                    stats,
                    value["item"]["decision"]["score"],
                    bool(value["item"]["decision"].get("triggers")),
                    relative_rank[pk],
                    sector_rank[row["sector"]],
                    self.cfg,
                )
                score = rank_candidate(inputs, self.cfg)
                row.update(
                    score=score.score,
                    components=score.components,
                    ranking_inputs=asdict(inputs),
                    qualified=score.qualified,
                    relative_strength_universe=len(ready),
                )
                if row["kind"] == "WATCHLIST":
                    row["readiness_distance_atr"] = stats["watchlist_distance_atr"]
                    watchlist.append(row)
                    continue
                signal_date = date.fromisoformat(signal["signal_date"])
                # After-close proposal is for the NEXT session: signal day=1,
                # after entry session two=3; after session three=4 (expired).
                age = calendar.age(signal_date, d) + 1
                expiry = calendar.forward(
                    signal_date + timedelta(days=1),
                    int(self.cfg["technical"]["entry_window_days"]),
                )
                if len(expiry) != int(self.cfg["technical"]["entry_window_days"]):
                    raise ValueError("entry_calendar_unavailable")
                pos = size_position(
                    state.nav or 0,
                    float(signal["signal_high"]),
                    float(signal["signal_low"]),
                    **{
                        k: float(self.cfg["risk"][k])
                        for k in (
                            "risk_per_trade_pct",
                            "max_position_pct",
                            "raw_stop_buffer_pct",
                            "max_stop_risk_pct",
                        )
                    },
                )
                entry = float(signal["signal_high"])
                risk = pos.shares * (entry - pos.stop)
                costs = (
                    estimate_delivery_costs(
                        trade_date=expiry[min(max(age - 1, 0), len(expiry) - 1)],
                        side="BUY",
                        price=entry,
                        quantity=pos.shares,
                        cfg=self.cfg["backtest"],
                    ).total
                    if pos.shares
                    else 0.0
                )
                next_session = calendar.forward(d + timedelta(days=1), 1)
                order_day = next_session[0] if next_session else d
                dates = (
                    [
                        order_day,
                        *calendar.forward(
                            order_day + timedelta(days=1),
                            int(
                                self.cfg["technical"]["earnings_blackout_trading_days"]
                            ),
                        ),
                    ]
                    if next_session
                    else []
                )
                blackout_clear = len(dates) == int(
                    self.cfg["technical"]["earnings_blackout_trading_days"]
                ) + 1 and not self.b.reference.earnings_within_trading_window(
                    row["instrument_id"], order_day, dates, as_of=stamp
                )
                candidate = Candidate(
                    row["instrument_id"],
                    row["sector"],
                    True,
                    blackout_clear,
                    age,
                    stats["adr20"],
                    entry * pos.shares,
                    stats["adtv"],
                    pk in held,
                    pos.shares,
                    (entry - pos.stop) / entry * 100,
                    inputs,
                    risk,
                    costs,
                )
                candidates.append(candidate)
                row["qualified"] = not candidate_rejections(candidate, self.cfg)
                row.update(
                    buy_stop=entry,
                    stop=pos.stop,
                    shares=pos.shares,
                    rupee_risk=round(risk, 4),
                    position_pct=pos.position_pct,
                    estimated_entry_costs=costs,
                    days_left=max(
                        0, int(self.cfg["technical"]["entry_window_days"]) - age + 1
                    ),
                    next_entry_session=(
                        expiry[age - 1].isoformat() if 1 <= age <= len(expiry) else None
                    ),
                    expires_on=expiry[-1].isoformat(),
                )
            except (ValueError, KeyError) as exc:
                row["qualified"] = False
                row["rejected_reasons"] = [str(exc)]
        allocation = allocate_candidates(
            candidates, state, capacity, self.cfg, priority=selection_priority
        )
        selected = {c.instrument_id for c in allocation.selected}
        for candidate in candidates:
            row = records[candidate.instrument_id]
            row["selected"] = candidate.instrument_id in selected
            row["rejected_reasons"] = list(
                allocation.rejected.get(candidate.instrument_id, ())
            )
        order_sheet = []
        for rank, candidate in enumerate(allocation.selected, 1):
            row = records[candidate.instrument_id]
            row["rank"] = rank
            row["reason"] = (
                f"Score {row['score']}; A/B passed; portfolio limits reserved."
            )
            order_sheet.append(row)
        watchlist.sort(
            key=lambda r: (r["readiness_distance_atr"], -r["score"], r["instrument_id"])
        )
        for rank, row in enumerate(watchlist, 1):
            row["watchlist_rank"] = rank
        for row in records.values():
            row["global_blockers"] = blockers
            if row["rejected_reasons"]:
                row["reason"] = "; ".join(row["rejected_reasons"])
            row["as_of"] = stamp.isoformat()
            row["data_session"] = expected.isoformat()
        report = ModuleCGenerationResult(
            d,
            len(records),
            int(a_scan["eligible"]),
            sum(r["kind"] == "SIGNAL" for r in records.values()),
            sum(bool(r["components"]) for r in records.values()),
            sum(r["qualified"] and r["kind"] == "SIGNAL" for r in records.values()),
            len(order_sheet),
            sum(
                r["kind"] != "WATCHLIST" and not r["selected"] for r in records.values()
            ),
            capacity,
            tuple(watchlist),
            tuple(order_sheet),
            tuple(records.values()),
            tuple(blockers),
            self.version,
            persist,
            allocation.projected_heat_pct,
            allocation.remaining_cash,
            rejection_counts=dict(
                Counter(
                    reason
                    for row in records.values()
                    for reason in row["rejected_reasons"]
                )
            ),
        )
        report = replace(
            report,
            upstream={
                "configuration_ready": a_scan.get("configuration_ready", False),
                "a_evaluated": a_scan.get("evaluated", len(a_scan["results"])),
                "a_persisted": bool(persist_upstream),
                "a_rejection_counts": a_scan.get("rejection_counts", {}),
                "b_evaluated": len(b_results),
                "b_persisted": bool(persist_upstream),
            },
        )
        if persist_upstream:
            self.b.persist_results(b_results, stamp)
        if persist:
            self._persist(report, stamp)
        return report

    def _persist(self, report, stamp):
        rows = list(report.results)
        payload = json.dumps(rows, default=str, allow_nan=False)
        params = {
            "rows": payload,
            "d": report.trade_date,
            "stamp": stamp,
            "version": self.version,
            "stale": json.dumps(
                {
                    "selected": False,
                    "qualified": False,
                    "kind": "REJECTED",
                    "rejected_reasons": ["not_in_current_scan"],
                }
            ),
            "lock": "module_c:" + str(report.trade_date) + ":" + self.version,
        }
        with self.engine.begin() as conn:
            conn.execute(
                text("select pg_advisory_xact_lock(hashtext(:account_lock))"),
                {"account_lock": "pms_formal_paper_account"},
            )
            conn.execute(text("select pg_advisory_xact_lock(hashtext(:lock))"), params)
            # A rerun must clear selections which disappeared from the universe.
            conn.execute(
                text(
                    "update module_c_rankings set selected=false,qualified=false,rejected_reasons='[\"not_in_current_scan\"]'::jsonb where trade_date=:d and strategy_version=:version"
                ),
                params,
            )
            conn.execute(
                text(
                    """insert into module_c_rankings(trade_date,instrument_id,score,qualified,selected,components,rejected_reasons,strategy_version)
                select :d,r.instrument_pk,r.score,r.qualified,r.selected,r.components,r.rejected_reasons,:version
                from jsonb_to_recordset(cast(:rows as jsonb)) r(instrument_pk bigint,score numeric,qualified boolean,selected boolean,components jsonb,rejected_reasons jsonb)
                on conflict(trade_date,instrument_id,strategy_version) do update set score=excluded.score,
                qualified=excluded.qualified,selected=excluded.selected,components=excluded.components,rejected_reasons=excluded.rejected_reasons"""
                ),
                params,
            )
            # Detailed order-sheet/watchlist/rejection records use the existing
            # strategy_decisions JSONB contract, without changing the numeric components API.
            conn.execute(
                text(
                    """update strategy_decisions set decision=decision || cast(:stale as jsonb)
                    where module='C' and strategy_version=:version and (as_of at time zone 'Asia/Kolkata')::date=:d"""
                ),
                params,
            )
            conn.execute(
                text(
                    """update strategy_decisions s set as_of=:stamp,decision=r
                from jsonb_array_elements(cast(:rows as jsonb)) r
                where s.instrument_id=(r->>'instrument_pk')::bigint and s.module='C'
                and s.strategy_version=:version and (s.as_of at time zone 'Asia/Kolkata')::date=:d"""
                ),
                params,
            )
            conn.execute(
                text(
                    """insert into strategy_decisions(instrument_id,module,as_of,strategy_version,decision)
                select (r->>'instrument_pk')::bigint,'C',:stamp,:version,r
                from jsonb_array_elements(cast(:rows as jsonb)) r
                where not exists(select 1 from strategy_decisions s where s.instrument_id=(r->>'instrument_pk')::bigint
                  and s.module='C' and s.strategy_version=:version and (s.as_of at time zone 'Asia/Kolkata')::date=:d)"""
                ),
                params,
            )
