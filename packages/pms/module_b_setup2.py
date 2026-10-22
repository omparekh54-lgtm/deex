from __future__ import annotations

from datetime import date, datetime, time
from packages.data_layer.features import compute_features
from packages.pms.module_a_context import IST, as_ist
from packages.pms.module_b_pipeline import ModuleBResult, ModuleBPipeline


class EarningsAnchoredSetup2:
    """Research-only earnings variant. Never emits production-persistable signals."""

    def __init__(self, pipeline: ModuleBPipeline):
        self.pipeline = pipeline

    def evaluate(
        self, instrument_id: str, as_of: date, *, require_module_a: bool = True
    ) -> ModuleBResult:
        universe = self.pipeline.evaluate(
            instrument_id, as_of, require_module_a=require_module_a
        )
        if not universe.eligible_universe:
            return ModuleBResult(
                instrument_id,
                as_of,
                False,
                universe.rejected_reasons,
                None,
                research_only=True,
                module_a_reasons=universe.module_a_reasons,
            )
        candles = self.pipeline._candles(instrument_id, as_of, 800)
        frame = compute_features(candles).data.dropna(subset=["ema21"])
        history = self.pipeline.prices.financial_history_as_of(
            instrument_id, self.pipeline._stamp(as_of), limit_periods=24
        )
        quarterly = [
            r
            for r in history
            if str(r.get("filing_kind") or "quarter").lower()
            in {"quarter", "quarterly"}
        ]
        latest_signal = None
        for filing in sorted(quarterly, key=lambda r: as_ist(r["filed_at"])):
            filed_at = as_ist(filing["filed_at"])
            if filed_at > self.pipeline._stamp(as_of):
                continue
            _, decision = self.pipeline.module_a.evaluate(instrument_id, filed_at)
            if (require_module_a and not decision.eligible) or not decision.triggers:
                continue
            candidate = self._from_filing(frame, filed_at, as_of)
            if candidate is not None:
                latest_signal = candidate
        return ModuleBResult(
            instrument_id,
            as_of,
            True,
            (),
            self.pipeline._live_window(latest_signal, as_of),
            research_only=True,
            diagnostics={"setup": "SETUP_2", "backtest_only": True},
        )

    def _from_filing(self, frame, filed_at: datetime, as_of: date):
        filed_at = as_ist(filed_at)
        t = self.pipeline.cfg["technical"]
        frame = frame[frame.index.date <= as_of]
        # The pre-result close depends on the exact filing timestamp, not just
        # its UTC date or the close immediately preceding the reaction session.
        before = [
            i
            for i, stamp in enumerate(frame.index)
            if datetime.combine(stamp.date(), time(15, 30), tzinfo=IST) < filed_at
        ]
        after = [
            i
            for i, stamp in enumerate(frame.index)
            if filed_at.date() < stamp.date() <= as_of
        ]
        if not before or not after:
            return None
        first = after[0]
        if first < 20:
            return None
        base = float(frame.iloc[before[-1]]["close"])
        baseline = float(frame.iloc[first - 20 : first]["volume"].mean())
        reaction_volume = float(frame.iloc[first]["volume"])
        if (
            base <= 0
            or baseline <= 0
            or reaction_volume / baseline
            < float(t.get("setup2_reaction_volume_ratio", 2))
        ):
            return None
        highest = base
        highest_day = 0
        valid = False
        qualification = None
        expiry = int(t.get("setup2_expiry_sessions", 60))
        qualifying_days = int(t.get("setup2_qualification_sessions", 30))
        for day, (_, row) in enumerate(
            frame.iloc[first : first + expiry].iterrows(), 1
        ):
            # A signal bar ends a previously qualifying rally; its intraday
            # high cannot retroactively qualify an invalid rally.
            below = float(row["close"]) < float(row["ema21"])
            if below and not valid:
                return None
            if not below and float(row["high"]) > highest:
                highest = float(row["high"])
                highest_day = day
            rally = (highest / base - 1) * 100
            velocity = rally / max(highest_day, 1)
            if (
                not valid
                and day <= qualifying_days
                and rally + 1e-9 >= float(t["rally_min_pct"])
                and velocity + 1e-9 >= float(t["rally_min_velocity_pct_per_day"])
            ):
                valid = True
                qualification = {
                    "qualification_date": row.name.date().isoformat(),
                    "qualifying_rally_pct": round(rally, 4),
                    "qualifying_velocity": round(velocity, 4),
                }
            if below:
                if not valid:
                    return None
                return {
                    "setup": "SETUP_2",
                    "signal_date": row.name.date().isoformat(),
                    "signal_high": float(row["high"]),
                    "signal_low": float(row["low"]),
                    "rally_start": frame.index[first].date().isoformat(),
                    "rally_high_date": frame.index[first + highest_day - 1]
                    .date()
                    .isoformat(),
                    "rally_pct": round(rally, 4),
                    "velocity": round(velocity, 4),
                    "reaction_volume_ratio": round(reaction_volume / baseline, 4),
                    "filing_timestamp": filed_at.isoformat(),
                    "pre_result_close": base,
                    "entry_window_days": int(t["entry_window_days"]),
                    **qualification,
                }
            if not valid and day >= qualifying_days:
                return None
        return None
