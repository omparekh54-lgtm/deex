"""Market regime scoring, session stability and pending-entry capacity plans."""

from __future__ import annotations
import json, math
from dataclasses import asdict, dataclass, field
from datetime import date

ENGINE_VERSION = "MODULE_E_v1.1"


@dataclass(frozen=True)
class RegimeInputs:
    midcap_close: float
    midcap_dma50: float
    midcap_dma200: float
    midcap_dma200_slope_pct: float
    breadth_above_50dma_pct: float
    breadth_above_21ema_pct: float
    advance_decline_10d: float
    new_highs_pct: float
    new_lows_pct: float
    india_vix: float
    india_vix_5d_change_pct: float
    strategy_expectancy_r: float | None = None
    valid_rallies_pct: float | None = None
    closed_trades: int = 0
    net_highs_trend_pp: float = 0.0


@dataclass(frozen=True)
class RegimeDecision:
    score: float
    raw_score: float
    band: str
    max_open_trades: int
    hard_override: bool
    components: dict[str, float] = field(default_factory=dict)
    data_ready: bool = True
    reasons: tuple[str, ...] = ()
    reason: str = ""
    trade_date: date | None = None


def _clamp(v, lo=0.0, hi=100.0):
    if not math.isfinite(float(v)):
        raise ValueError("Non-finite regime input")
    return max(lo, min(hi, float(v)))


def validate_inputs(i):
    for k, v in asdict(i).items():
        if v is not None and (isinstance(v, bool) or not math.isfinite(float(v))):
            raise ValueError("Invalid regime input: " + k)
    if min(i.midcap_close, i.midcap_dma50, i.midcap_dma200, i.india_vix) <= 0:
        raise ValueError("Regime prices must be positive")
    for key in (
        "breadth_above_50dma_pct",
        "breadth_above_21ema_pct",
        "new_highs_pct",
        "new_lows_pct",
        "valid_rallies_pct",
    ):
        v = getattr(i, key)
        if v is not None and not 0 <= v <= 100:
            raise ValueError("Invalid percentage: " + key)
    if (
        i.advance_decline_10d < 0
        or not isinstance(i.closed_trades, int)
        or i.closed_trades < 0
    ):
        raise ValueError("Invalid breadth ratio/trade count")
    if i.closed_trades >= 10 and (
        i.strategy_expectancy_r is None or i.valid_rallies_pct is None
    ):
        raise ValueError(
            "Strategy feedback requires actual R expectancy and rally coverage"
        )


def _band(score, cfg):
    score = _clamp(score)
    r = cfg["regime"]
    thresholds = [
        float(r[k])
        for k in ("defensive_below", "weak_below", "neutral_below", "healthy_below")
    ]
    if int(r.get("healthy_capacity", 6)) not in (5, 6):
        raise ValueError("Healthy capacity must be 5 or 6")
    if not 0 < thresholds[0] < thresholds[1] < thresholds[2] < thresholds[3] <= 100:
        raise ValueError("Invalid regime thresholds")
    for bound, band, capacity in zip(
        thresholds,
        ("DEFENSIVE", "WEAK", "NEUTRAL", "HEALTHY"),
        (0, 2, 4, int(r.get("healthy_capacity", 6))),
    ):
        if score < bound:
            return band, capacity
    return "STRONG", 7


def calculate_regime_score(i):
    """Explicit initial 30/25/15/15/15 transforms; no data gaps replaced by zero."""
    validate_inputs(i)
    trend = (
        35 * (i.midcap_close > i.midcap_dma50)
        + 45 * (i.midcap_close > i.midcap_dma200)
        + 20 * (i.midcap_dma200_slope_pct > 0)
    )
    breadth = (
        0.4 * i.breadth_above_50dma_pct
        + 0.4 * i.breadth_above_21ema_pct
        + 0.2 * _clamp((i.advance_decline_10d - 0.5) * 100)
    )
    level = _clamp(50 + 5 * (i.new_highs_pct - i.new_lows_pct))
    trend_highs = _clamp(50 + 10 * i.net_highs_trend_pp)
    highs = 0.7 * level + 0.3 * trend_highs
    volatility = 0.7 * _clamp((30 - i.india_vix) / 20 * 100) + 0.3 * _clamp(
        50 - 3 * i.india_vix_5d_change_pct
    )
    feedback = (
        50
        if i.closed_trades < 10
        else 0.65 * _clamp(50 + i.strategy_expectancy_r * 50)
        + 0.35 * i.valid_rallies_pct
    )
    components = {
        key: round(value * weight / 100, 2)
        for key, value, weight in (
            ("trend", trend, 30),
            ("breadth", breadth, 25),
            ("highs_lows", highs, 15),
            ("volatility", volatility, 15),
            ("strategy_feedback", feedback, 15),
        )
    }
    return round(sum(components.values()), 2), components


def decide_regime(
    score,
    nifty_midcap_400_above_200dma,
    breadth_above_50dma_pct,
    cfg,
    *,
    raw_score=None,
    components=None,
    below_200dma=None,
):
    score = _clamp(score)
    raw_score = score if raw_score is None else _clamp(raw_score)
    if not 0 <= breadth_above_50dma_pct <= 100:
        raise ValueError("Invalid breadth percentage")
    below = not nifty_midcap_400_above_200dma if below_200dma is None else below_200dma
    hard = bool(below and breadth_above_50dma_pct < 30)
    band, capacity = ("DEFENSIVE", 0) if hard else _band(score, cfg)
    return RegimeDecision(
        round(score, 2),
        round(raw_score, 2),
        band,
        capacity,
        hard,
        components or {},
        reason=(
            "Hard override: index below DMA200 and breadth below 30%."
            if hard
            else f"{band}: score {score:.2f}; capacity {capacity}."
        ),
    )


@dataclass
class RegimeTracker:
    raw_scores: list[float] = field(default_factory=list)
    current: RegimeDecision | None = None
    upgrade_candidate: str | None = None
    upgrade_closes: int = 0
    last_session: date | None = None
    last_fingerprint: str | None = None

    def checkpoint(self):
        result = asdict(self)
        if self.current:
            result["current"]["trade_date"] = (
                self.current.trade_date.isoformat() if self.current.trade_date else None
            )
        result["last_session"] = (
            self.last_session.isoformat() if self.last_session else None
        )
        result["version"] = ENGINE_VERSION
        return result

    @classmethod
    def restore(cls, payload):
        from copy import deepcopy

        value = deepcopy(payload)
        if value.pop("version", None) != ENGINE_VERSION:
            raise ValueError("Unverified regime checkpoint version")
        value["last_session"] = (
            date.fromisoformat(value["last_session"])
            if value.get("last_session")
            else None
        )
        if value.get("current"):
            current = value["current"]
            current["trade_date"] = (
                date.fromisoformat(current["trade_date"])
                if current.get("trade_date")
                else None
            )
            current["reasons"] = tuple(current.get("reasons", ()))
            value["current"] = RegimeDecision(**current)
        if len(value["raw_scores"]) > 3 or any(
            not math.isfinite(x) or not 0 <= x <= 100 for x in value["raw_scores"]
        ):
            raise ValueError("Invalid regime checkpoint scores")
        if value.get("current"):
            current = value["current"]
            caps = {"DEFENSIVE": 0, "WEAK": 2, "NEUTRAL": 4, "HEALTHY": 6, "STRONG": 7}
            if (
                current.band not in caps
                or not 0 <= current.max_open_trades <= caps[current.band]
                or not current.data_ready
            ):
                raise ValueError("Invalid regime checkpoint capacity")
        if value.get("upgrade_candidate") not in (
            None,
            "DEFENSIVE",
            "WEAK",
            "NEUTRAL",
            "HEALTHY",
            "STRONG",
        ) or value.get("upgrade_closes") not in (0, 1):
            raise ValueError("Invalid upgrade checkpoint")
        return cls(**value)

    def update(self, inputs, cfg, *, session=None, require_full_window=False):
        from dataclasses import replace

        raw, components = calculate_regime_score(inputs)
        fingerprint = json.dumps(asdict(inputs), sort_keys=True, allow_nan=False)
        if session is not None and self.last_session is not None:
            if session < self.last_session:
                raise ValueError("Out-of-order regime session")
            if session == self.last_session:
                if fingerprint != self.last_fingerprint:
                    raise ValueError("Conflicting regime session replay")
                return self.current
        self.raw_scores = (self.raw_scores + [raw])[-3:]
        smoothed = sum(self.raw_scores) / len(self.raw_scores)
        proposed = decide_regime(
            smoothed,
            inputs.midcap_close > inputs.midcap_dma200,
            inputs.breadth_above_50dma_pct,
            cfg,
            raw_score=raw,
            components=components,
            below_200dma=inputs.midcap_close < inputs.midcap_dma200,
        )
        order = {"DEFENSIVE": 0, "WEAK": 1, "NEUTRAL": 2, "HEALTHY": 3, "STRONG": 4}
        if require_full_window and len(self.raw_scores) < 3:
            self.current = replace(
                proposed,
                band="DEFENSIVE",
                max_open_trades=0,
                reasons=("stability_warmup",),
                reason="Stability warmup: three consecutive verified sessions are required.",
            )
            self.upgrade_candidate = None
            self.upgrade_closes = 0
        elif self.current is None or proposed.hard_override:
            self.current = proposed
            self.upgrade_candidate = None
            self.upgrade_closes = 0
        elif order[proposed.band] < order[self.current.band]:
            self.current = proposed
            self.upgrade_candidate = None
            self.upgrade_closes = 0
        elif order[proposed.band] > order[self.current.band]:
            self.upgrade_closes = (
                self.upgrade_closes + 1
                if self.upgrade_candidate == proposed.band
                else 1
            )
            self.upgrade_candidate = proposed.band
            if self.upgrade_closes >= 2:
                self.current = proposed
                self.upgrade_candidate = None
                self.upgrade_closes = 0
            else:
                self.current = replace(
                    proposed,
                    band=self.current.band,
                    max_open_trades=self.current.max_open_trades,
                    reasons=("upgrade_confirmation_pending",),
                    reason=f"Upgrade to {proposed.band} awaits a second consecutive close.",
                )
        else:
            self.current = proposed
            self.upgrade_candidate = None
            self.upgrade_closes = 0
        self.current = replace(self.current, trade_date=session)
        self.last_session = session
        self.last_fingerprint = fingerprint
        return self.current


def pending_entry_plan(open_positions, pending, capacity, *, account_known=True):
    """Keep highest-ranked pending entries; filled positions are never liquidated."""
    if not isinstance(open_positions, int) or open_positions < 0:
        raise ValueError("Invalid open-position count")
    capacity = max(0, min(7, int(capacity)))
    slots = max(0, capacity - open_positions) if account_known else 0

    def key(row):
        score = row.get("score")
        if score is not None and not math.isfinite(float(score)):
            raise ValueError("Invalid pending rank")
        return (
            -(float(score) if score is not None else -1),
            str(row.get("created_at", "")),
            str(row["id"]),
        )

    ordered = sorted(pending, key=key)
    return {
        "keep": [str(r["id"]) for r in ordered[:slots]],
        "cancel": [str(r["id"]) for r in ordered[slots:]],
        "reason": "regime_capacity" if account_known else "portfolio_state_unavailable",
        "capacity": int(capacity),
        "open_positions": open_positions,
    }
