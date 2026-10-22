from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from packages.data_layer.corporate_actions import adjust_ohlcv_for_actions
from packages.data_layer.features import candles_to_frame, compute_features
from packages.data_layer.models import Candle
from packages.data_layer.queries import PointInTimeQueries
from packages.data_layer.reference_data import ReferenceDataQueries
from packages.pms.config import load_strategy_config
from packages.pms.module_a_pipeline import ModuleAPipeline
from packages.pms.module_b import Bar, RallyState, RallyTracker, advance_rally
from packages.pms.module_a_context import end_of_day, IST
from packages.pms.market_calendar import MarketCalendar


@dataclass(frozen=True)
class ModuleBResult:
    instrument_id: str
    as_of: date
    eligible_universe: bool
    rejected_reasons: tuple[str, ...]
    signal: dict[str, Any] | None
    watchlist: dict[str, Any] | None = None
    research_only: bool = False
    module_a_reasons: tuple[str, ...] = ()
    diagnostics: dict[str, Any] = field(default_factory=dict)


class ModuleBPipeline:
    def __init__(self, engine, cfg=None):
        self.engine = engine
        self.prices = PointInTimeQueries(engine)
        self.reference = ReferenceDataQueries(engine)
        self.module_a = ModuleAPipeline(engine, cfg)
        self.cfg = load_strategy_config() if cfg is None else cfg
        self._calendar_cache = {}

    def _candles(
        self, instrument_id: str, as_of: date, lookback_days: int = 500
    ) -> list[Candle]:
        start = as_of - timedelta(days=lookback_days * 2)
        rows = self.prices.daily_prices(
            instrument_id, start, as_of, preferred_source=None
        )
        sessions = self._calendar(as_of).sessions
        stamp = self._stamp(as_of)
        closed = (
            stamp.date()
            if stamp.hour * 60 + stamp.minute >= 15 * 60 + 30
            else stamp.date() - timedelta(days=1)
        )
        rows = [
            row
            for row in rows
            if row["trade_date"] in sessions and row["trade_date"] <= min(as_of, closed)
        ]
        by_date: dict[date, Candle] = {}
        for row in sorted(
            rows, key=lambda r: (r["trade_date"], self._source_priority(r["source"]))
        ):
            try:
                volume = float(row["volume"])
                if not math.isfinite(volume) or volume < 0 or not volume.is_integer():
                    raise ValueError("invalid_price_history")
            except (TypeError, OverflowError):
                raise ValueError("invalid_price_history") from None
            candle = Candle(
                instrument_id=instrument_id,
                timestamp=datetime.combine(row["trade_date"], datetime.min.time()),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(volume),
                source=row["source"],
                adjusted=row["source"]
                in self.cfg["technical"].get("adjusted_price_sources", []),
                turnover=(
                    Decimal(str(row["turnover"]))
                    if row.get("turnover") is not None
                    else None
                ),
            )
            existing = by_date.get(candle.timestamp.date())
            if existing is None:
                by_date[candle.timestamp.date()] = candle
        raw = [by_date[d] for d in sorted(by_date)]
        if not raw:
            return []

        for candle in raw:
            prices = [
                float(candle.open),
                float(candle.high),
                float(candle.low),
                float(candle.close),
            ]
            if (
                not all(math.isfinite(v) and v > 0 for v in prices)
                or not math.isfinite(float(candle.volume))
                or candle.volume < 0
            ):
                raise ValueError("invalid_price_history")
            if candle.low > min(candle.open, candle.close) or candle.high < max(
                candle.open, candle.close
            ):
                raise ValueError("invalid_ohlc_history")
            if candle.turnover is not None and (
                not candle.turnover.is_finite() or candle.turnover < 0
            ):
                raise ValueError("invalid_turnover_history")
        actions = self.reference.corporate_actions_between(instrument_id, start, as_of)
        actions = [
            a
            for a in actions
            if a.ex_date <= as_of and a.action_type.upper() in {"SPLIT", "BONUS"}
        ]
        unique = {}
        for action in actions:
            key = (action.action_type.upper(), action.ex_date)
            old = unique.get(key)
            if old and (old.ratio_from, old.ratio_to) != (
                action.ratio_from,
                action.ratio_to,
            ):
                raise ValueError("conflicting_corporate_actions")
            unique[key] = action
        actions = list(unique.values())
        adjusted_sources = set(self.cfg["technical"].get("adjusted_price_sources", []))
        states = {r.source in adjusted_sources for r in raw}
        if actions and len(states) > 1:
            raise ValueError("mixed_price_adjustment_basis")
        if (
            actions
            and any(r.source.casefold() == "yfinance" for r in raw)
            and "yfinance" not in adjusted_sources
        ):
            raise ValueError("yfinance_adjustment_basis_unverified")
        if not actions or states == {True}:
            return raw
        adjusted = adjust_ohlcv_for_actions(candles_to_frame(raw), actions).frame
        return [
            Candle(
                instrument_id=instrument_id,
                timestamp=idx.to_pydatetime(),
                open=Decimal(str(row.open)),
                high=Decimal(str(row.high)),
                low=Decimal(str(row.low)),
                close=Decimal(str(row.close)),
                volume=int(round(row.volume)),
                source="adjusted",
                adjusted=True,
                turnover=Decimal(str(row.turnover)),
            )
            for idx, row in adjusted.iterrows()
        ]

    def features(self, candles):
        return compute_features(candles)

    @staticmethod
    def _source_priority(source):
        value = str(source).casefold()
        return (
            0 if value == "angel_one" else 1 if value.startswith("nse") else 2,
            value,
        )

    def _stamp(self, as_of: date):
        return min(end_of_day(as_of), datetime.now(IST))

    def _calendar(self, as_of: date):
        if as_of not in self._calendar_cache:
            self._calendar_cache[as_of] = MarketCalendar.from_database(
                self.engine, self._stamp(as_of)
            )
        return self._calendar_cache[as_of]

    def _trading_dates(self, start: date, sessions: int):
        return self._calendar(start).forward(start, sessions)

    def _setup1_state(self, features, as_of: date):
        tracker = RallyTracker()
        latest_signal = None
        frame = features[features.index.date <= as_of]
        for i in range(50, len(frame)):
            row, prev = frame.iloc[i], frame.iloc[i - 1]
            if not math.isfinite(float(row["ema21"])) or not math.isfinite(
                float(prev["ema21"])
            ):
                continue
            bar = Bar(
                str(frame.index[i].date()),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row["ema21"]),
            )
            baseline = float(frame.iloc[i - 50 : i]["volume"].mean())
            tracker = advance_rally(
                tracker,
                bar,
                float(prev["close"]),
                float(prev["ema21"]),
                baseline,
                self.cfg,
            )
            if tracker.state == RallyState.SIGNAL_CREATED:
                latest_signal = {
                    **self._rally_payload(tracker),
                    "signal_date": tracker.signal_date,
                    "signal_high": tracker.signal_high,
                    "signal_low": tracker.signal_low,
                    "setup": "SETUP_1",
                    "entry_window_days": int(
                        self.cfg["technical"]["entry_window_days"]
                    ),
                }
                tracker = RallyTracker(last_date=bar.date)
        watchlist = (
            self._rally_payload(tracker)
            if tracker.state == RallyState.RALLY_VALID
            else None
        )
        return latest_signal, watchlist

    @staticmethod
    def _rally_payload(tracker):
        return {
            "rally_start": tracker.start_date,
            "rally_high_date": tracker.highest_high_date,
            "rally_pct": round(tracker.rally_pct(), 4),
            "velocity": round(tracker.velocity(), 4),
            "rally_duration_sessions": tracker.peak_trading_days,
            "volume_ratio": round(tracker.volume_ratio(), 4),
            "baseline_volume_50d": tracker.baseline_volume_50d,
            "state": tracker.state.value,
        }

    def _latest_setup1_signal(self, features, as_of: date):
        signal, _ = self._setup1_state(features, as_of)
        return self._live_window(signal, as_of)

    def _live_window(self, signal, as_of):
        if signal is None:
            return None
        signal_date = date.fromisoformat(str(signal["signal_date"]))
        age = self._calendar(as_of).age(signal_date, as_of)
        days = int(self.cfg["technical"]["entry_window_days"])
        if signal_date > as_of or age >= days:
            return None
        expiry = self._calendar(as_of).forward(signal_date + timedelta(days=1), days)
        return {
            **signal,
            "age_trading_sessions": age,
            "days_left": max(0, days - age),
            "expires_on": expiry[-1].isoformat() if len(expiry) == days else None,
            "status": "SIGNAL_CREATED" if age == 0 else "ACTIVE",
        }

    def evaluate(
        self, instrument_id: str, as_of: date, *, require_module_a: bool = True
    ) -> ModuleBResult:
        if as_of > datetime.now(IST).date():
            raise ValueError("as_of must not be in the future")
        t = self.cfg["technical"]
        rejected: list[str] = []
        a_reasons = ()
        if require_module_a:
            _, a_decision = self.module_a.evaluate(instrument_id, self._stamp(as_of))
            if not a_decision.eligible:
                return ModuleBResult(
                    instrument_id,
                    as_of,
                    False,
                    ("module_a_ineligible",),
                    None,
                    module_a_reasons=a_decision.failed_rules,
                )
        try:
            candles = self._candles(instrument_id, as_of)
        except ValueError as exc:
            return ModuleBResult(
                instrument_id,
                as_of,
                False,
                (str(exc),),
                None,
                research_only=not require_module_a,
            )
        if len(candles) < int(t["min_history_days"]):
            return ModuleBResult(
                instrument_id,
                as_of,
                False,
                ("insufficient_history",),
                None,
                research_only=not require_module_a,
            )
        stamp = self._stamp(as_of)
        latest_closed = (
            stamp.date()
            if stamp.hour * 60 + stamp.minute >= 15 * 60 + 30
            else stamp.date() - timedelta(days=1)
        )
        calendar = self._calendar(as_of)
        expected = sorted(d for d in calendar.sessions if d <= latest_closed)
        if not expected or (latest_closed - expected[-1]).days > int(
            t.get("max_price_age_days", 7)
        ):
            rejected.append("market_calendar_unavailable_or_stale")
        elif candles[-1].timestamp.date() != expected[-1]:
            rejected.append("price_history_stale")
        needed = set(expected[-int(t["min_history_days"]) :])
        if len(needed) < int(t["min_history_days"]) or not needed.issubset(
            {c.timestamp.date() for c in candles}
        ):
            rejected.append("price_history_missing_sessions")

        features = self.features(candles).data
        latest = features.iloc[-1]
        required = (
            "close",
            "avg_volume20",
            "worst_volume20",
            "avg_turnover20",
            "adr20_pct",
            "high_52w",
        )
        if any(
            not math.isfinite(float(latest.get(name, float("nan"))))
            for name in required
        ):
            return ModuleBResult(
                instrument_id,
                as_of,
                False,
                ("indicators_unavailable",),
                None,
                research_only=not require_module_a,
            )
        if float(latest["close"]) <= float(t["min_price"]):
            rejected.append("price")
        if float(latest.get("avg_volume20", 0) or 0) <= float(t["min_avg_volume_20d"]):
            rejected.append("avg_volume_20d")
        if float(latest.get("worst_volume20", 0) or 0) <= float(
            t["min_worst_day_volume_20d"]
        ):
            rejected.append("worst_volume_20d")
        turnover_cr = float(latest.get("avg_turnover20", 0) or 0) / 10_000_000.0
        if turnover_cr < float(t["min_avg_turnover_20d_cr"]):
            rejected.append("turnover_low")
        if turnover_cr > float(t["max_avg_turnover_20d_cr"]):
            rejected.append("turnover_high")
        if float(latest.get("adr20_pct", 0) or 0) < float(t["min_adr_20d_pct"]):
            rejected.append("adr")
        high52 = float(latest.get("high_52w", 0) or 0)
        close = float(latest["close"])
        distance_pct = ((high52 - close) / high52 * 100.0) if high52 > 0 else 100.0
        if distance_pct > float(t["max_distance_from_52w_high_pct"]):
            rejected.append("distance_52w_high")
        if self.reference.is_index_member(instrument_id, "NIFTY50", as_of):
            rejected.append("nifty50_excluded")

        blackout_n = int(t["earnings_blackout_trading_days"])
        future_sessions = self._calendar(as_of).forward(
            as_of + timedelta(days=1), blackout_n
        )
        trading_dates = [as_of, *future_sessions]
        if len(future_sessions) < blackout_n:
            # A future blackout cannot be verified without a market calendar; fail closed.
            rejected.append("earnings_calendar_market_dates_unavailable")
        elif self.reference.earnings_within_trading_window(
            instrument_id, as_of, trading_dates, as_of=self._stamp(as_of)
        ):
            rejected.append("earnings_blackout")

        if rejected:
            return ModuleBResult(
                instrument_id,
                as_of,
                False,
                tuple(dict.fromkeys(rejected)),
                None,
                research_only=not require_module_a,
            )
        signal, watchlist = self._setup1_state(features, as_of)
        signal = self._live_window(signal, as_of)
        if (
            require_module_a
            and signal
            and self._signal_cancelled(instrument_id, signal)
        ):
            signal = None
        return ModuleBResult(
            instrument_id,
            as_of,
            True,
            (),
            signal,
            watchlist=watchlist,
            research_only=not require_module_a,
            diagnostics={
                "latest_price_date": candles[-1].timestamp.date().isoformat(),
                "turnover_basis": (
                    "reported"
                    if all(c.turnover is not None for c in candles[-20:])
                    else "includes_close_times_volume_fallback"
                ),
                "universe_recheck": "every_scan",
                "entry_window_sessions": int(t["entry_window_days"]),
            },
        )

    @property
    def version(self):
        return (
            self.cfg["version"]
            + ":"
            + self.cfg["technical"].get("implementation_version", "MODULE_B_v1.1")
        )

    def _signal_cancelled(self, instrument, signal):
        with self.engine.connect() as conn:
            pk = self.prices._db_instrument_id(conn, instrument)
            status = conn.execute(
                text(
                    """select status from module_b_signals where instrument_id=:id
                and signal_date=:date and setup='SETUP_1' and strategy_version=:version"""
                ),
                {
                    "id": pk,
                    "date": date.fromisoformat(signal["signal_date"]),
                    "version": self.version,
                },
            ).scalar_one_or_none()
        return status in {"CANCELLED", "CONSUMED", "SUPERSEDED", "EXPIRED"}

    def scan(self, as_of: date, *, persist: bool = False):
        if as_of > datetime.now(IST).date():
            raise ValueError("as_of must not be in the future")
        a_scan = self.module_a.scan(self._stamp(as_of), persist=False)
        results = []
        for row in a_scan["results"]:
            if not row["decision"]["eligible"]:
                result = ModuleBResult(
                    row["instrument_id"],
                    as_of,
                    False,
                    ("module_a_ineligible",),
                    None,
                    module_a_reasons=tuple(row["decision"]["failed_rules"]),
                    diagnostics={"technical_evaluation_skipped": True},
                )
            else:
                result = self.evaluate(
                    row["instrument_id"], as_of, require_module_a=True
                )
            results.append(result)
        if persist:
            self.persist_results(results, self._stamp(as_of))
        reasons = Counter(reason for r in results for reason in r.rejected_reasons)
        return {
            "as_of": as_of.isoformat(),
            "strategy_version": self.version,
            "evaluated": len(results),
            "module_a_eligible": a_scan["eligible"],
            "technical_eligible": sum(r.eligible_universe for r in results),
            "signals": sum(r.signal is not None for r in results),
            "watchlist": sum(r.watchlist is not None for r in results),
            "rejection_counts": dict(reasons),
            "persisted": persist,
            "results": [asdict(r) for r in results],
        }

    def persist_results(self, results: list[ModuleBResult], stamp: datetime):
        if any(r.research_only for r in results):
            raise PermissionError(
                "Research results cannot be persisted as production signals"
            )
        if not results:
            return
        if len({r.as_of for r in results}) != 1:
            raise ValueError("A persisted scan must have one as_of date")
        with self.engine.begin() as conn:
            identities = dict(
                conn.execute(
                    text(
                        "select exchange||':'||nse_symbol,id from instruments where exchange='NSE' and active"
                    )
                ).all()
            )
            records = [
                {"instrument_id": identities[r.instrument_id], "decision": asdict(r)}
                for r in results
            ]
            payload = json.dumps(records, default=str, allow_nan=False)
            params = {
                "rows": payload,
                "as_of": stamp,
                "version": self.version,
                "trade_date": results[0].as_of,
                "lock": "module_b:" + stamp.isoformat() + ":" + self.version,
            }
            # Serialize the check-and-insert because the legacy decisions table
            # has no unique key. The lock is transaction-scoped and releases on error.
            conn.execute(text("select pg_advisory_xact_lock(hashtext(:lock))"), params)
            conn.execute(
                text("""
                update strategy_decisions s set decision=r.decision
                from jsonb_to_recordset(cast(:rows as jsonb)) r(instrument_id bigint,decision jsonb)
                where s.instrument_id=r.instrument_id and s.module='B' and s.as_of=:as_of and s.strategy_version=:version
            """),
                params,
            )
            conn.execute(
                text("""
                insert into strategy_decisions(instrument_id,module,as_of,strategy_version,decision)
                select r.instrument_id,'B',:as_of,:version,r.decision
                from jsonb_to_recordset(cast(:rows as jsonb)) r(instrument_id bigint,decision jsonb)
                where not exists(select 1 from strategy_decisions s where s.instrument_id=r.instrument_id
                    and s.module='B' and s.as_of=:as_of and s.strategy_version=:version)
            """),
                params,
            )
            conn.execute(
                text("""
                update module_b_signals b
                set status=case when b.expires_on<=:trade_date then 'EXPIRED' else 'CANCELLED' end
                from jsonb_to_recordset(cast(:rows as jsonb)) r(instrument_id bigint,decision jsonb)
                where b.instrument_id=r.instrument_id and b.strategy_version=:version
                and b.signal_date<=:trade_date and b.status in ('ACTIVE','SIGNAL_CREATED')
                and (not (r.decision->>'eligible_universe')::boolean or r.decision->>'signal' is null
                     or b.signal_date<>(r.decision->'signal'->>'signal_date')::date)
            """),
                params,
            )
            signals = []
            for r in results:
                if r.signal is None or not r.eligible_universe:
                    continue
                signal = r.signal
                signals.append(
                    {
                        "id": identities[r.instrument_id],
                        "date": signal["signal_date"],
                        "high": signal["signal_high"],
                        "low": signal["signal_low"],
                        "start": signal["rally_start"],
                        "peak": signal["rally_high_date"],
                        "rally": signal["rally_pct"],
                        "velocity": signal["velocity"],
                        "volume": signal["volume_ratio"],
                        "expires": signal["expires_on"],
                        "status": signal["status"],
                        "payload": json.dumps(signal, default=str, allow_nan=False),
                        "version": self.version,
                    }
                )
            if signals:
                conn.execute(
                    text(
                        """insert into module_b_signals(instrument_id,signal_date,setup,signal_high,signal_low,
                    rally_start,rally_high_date,rally_pct,velocity,volume_ratio,expires_on,status,payload,strategy_version)
                    values(:id,:date,'SETUP_1',:high,:low,:start,:peak,:rally,:velocity,:volume,:expires,:status,cast(:payload as jsonb),:version)
                    on conflict(instrument_id,signal_date,setup,strategy_version) do update
                    set signal_high=excluded.signal_high,signal_low=excluded.signal_low,payload=excluded.payload,
                    rally_start=excluded.rally_start,rally_high_date=excluded.rally_high_date,
                    rally_pct=excluded.rally_pct,velocity=excluded.velocity,volume_ratio=excluded.volume_ratio,
                    expires_on=excluded.expires_on,status=case when module_b_signals.status in
                        ('CANCELLED','CONSUMED','SUPERSEDED','EXPIRED') then module_b_signals.status else excluded.status end
                """
                    ),
                    signals,
                )
