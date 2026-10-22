"""Pure readiness scoring and conservative capital allocation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RankingInputs:
    rally_quality: float
    pullback_quality: float
    fundamental_strength: float
    relative_strength: float
    proximity_52w_high: float
    sector_strength: float


@dataclass(frozen=True)
class RankingDecision:
    score: float
    qualified: bool
    components: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class Candidate:
    instrument_id: str
    sector: str
    module_a_eligible: bool
    blackout_clear: bool
    signal_age_trading_days: int
    adr20_pct: float
    position_value: float
    adtv: float
    already_held_or_pending: bool
    shares: int
    stop_risk_pct: float
    ranking: RankingInputs
    rupee_risk: float | None = None
    estimated_costs: float = 0.0


@dataclass(frozen=True)
class AllocationState:
    open_positions: int
    pending_entries: int
    portfolio_heat_pct: float
    cash: float
    sector_counts: dict[str, int]
    nav: float | None = None
    blockers: tuple[str, ...] = ()


@dataclass(frozen=True)
class AllocationDecision:
    selected: tuple[Candidate, ...]
    rejected: dict[str, tuple[str, ...]]
    projected_heat_pct: float = 0.0
    remaining_cash: float = 0.0


def _finite(*values):
    return all(
        isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
        for v in values
    )


def rank_candidate(inputs: RankingInputs, cfg: dict) -> RankingDecision:
    r = cfg["ranking"]
    components = {}
    for key, value in vars(inputs).items():
        weight = float(r[key + "_points"])
        if not _finite(value, weight) or weight < 0:
            raise ValueError("Non-finite score input or invalid weight")
        components[key] = round(max(0.0, min(100.0, value)) / 100 * weight, 2)
    if abs(sum(float(r[k + "_points"]) for k in components) - 100) > 1e-9:
        raise ValueError("Ranking weights must total 100")
    score = round(sum(components.values()), 2)
    return RankingDecision(score, score >= float(r["minimum_score"]), components)


def candidate_rejections(c: Candidate, cfg: dict) -> list[str]:
    reasons = []
    if not c.module_a_eligible:
        reasons.append("module_a_ineligible")
    if not c.blackout_clear:
        reasons.append("earnings_blackout")
    if not isinstance(
        c.signal_age_trading_days, int
    ) or not 1 <= c.signal_age_trading_days <= int(
        cfg["technical"]["entry_window_days"]
    ):
        reasons.append("signal_expired")
    if not c.sector or not c.sector.strip():
        reasons.append("sector_unavailable")
    if not _finite(
        c.adr20_pct, c.position_value, c.adtv, c.stop_risk_pct, c.estimated_costs
    ):
        reasons.append("invalid_candidate_data")
    else:
        if c.adr20_pct < float(cfg["technical"]["min_adr_20d_pct"]):
            reasons.append("adr")
        if c.adtv <= 0 or c.position_value > c.adtv * 0.02 + 1e-9:
            reasons.append("position_gt_2pct_adtv")
        if c.position_value <= 0 or c.stop_risk_pct <= 0 or c.estimated_costs < 0:
            reasons.append("invalid_candidate_data")
        if c.stop_risk_pct > float(cfg["risk"]["max_stop_risk_pct"]) + 1e-9:
            reasons.append("stop_risk_cap")
    if c.rupee_risk is not None and (not _finite(c.rupee_risk) or c.rupee_risk <= 0):
        reasons.append("invalid_candidate_data")
    if c.already_held_or_pending:
        reasons.append("already_held_or_pending")
    if not isinstance(c.shares, int) or isinstance(c.shares, bool) or c.shares < 1:
        reasons.append("position_size_zero")
    try:
        if not rank_candidate(c.ranking, cfg).qualified:
            reasons.append("score_below_minimum")
    except ValueError:
        reasons.append("ranking_inputs_invalid")
    return list(dict.fromkeys(reasons))


def allocate_candidates(
    candidates, state, max_open_trades_from_regime, cfg, *, priority=None
):
    """Reserve pending slots, cash including costs, sector slots and exact NAV heat."""
    from collections import Counter

    nav = (
        state.cash if state.nav is None else state.nav
    )  # legacy pure-call compatibility
    valid = (
        _finite(nav, state.cash, state.portfolio_heat_pct)
        and nav > 0
        and state.cash >= 0
        and state.portfolio_heat_pct >= 0
        and all(
            isinstance(v, int) and not isinstance(v, bool) and v >= 0
            for v in (
                state.open_positions,
                state.pending_entries,
                *state.sector_counts.values(),
            )
        )
    )
    blockers = state.blockers if valid else (*state.blockers, "portfolio_state_invalid")
    if sum(state.sector_counts.values()) < state.open_positions + state.pending_entries:
        blockers = (*blockers, "portfolio_sector_counts_incomplete")
    duplicate = Counter(c.instrument_id for c in candidates)
    rejected = {}
    eligible = []
    for c in candidates:
        reasons = candidate_rejections(c, cfg) + list(blockers)
        if duplicate[c.instrument_id] > 1:
            reasons.append("duplicate_candidate")
        if reasons:
            rejected[c.instrument_id] = tuple(dict.fromkeys(reasons))
        else:
            eligible.append((c, rank_candidate(c.ranking, cfg)))
    eligible.sort(
        key=lambda item: (
            priority(item[0]) if priority is not None else -item[1].score,
            item[0].stop_risk_pct,
            -item[0].adtv,
            item[0].instrument_id,
        )
    )
    counts = {}
    for sector, count in state.sector_counts.items():
        key = sector.strip().casefold()
        counts[key] = counts.get(key, 0) + count
    slots = state.open_positions + state.pending_entries
    heat = float(state.portfolio_heat_pct) if _finite(state.portfolio_heat_pct) else 0.0
    cash = float(state.cash) if _finite(state.cash) else 0.0
    risk_cfg = cfg["risk"]
    selected = []
    capacity = max(
        0, min(int(risk_cfg["max_open_trades"]), int(max_open_trades_from_regime))
    )
    for c, _ in eligible:
        reasons = []
        key = c.sector.strip().casefold()
        risk = (
            c.position_value * c.stop_risk_pct / 100
            if c.rupee_risk is None
            else c.rupee_risk
        )
        # A supplied risk amount must agree with the reported stop geometry.
        if abs(risk - c.position_value * c.stop_risk_pct / 100) > max(
            0.01, risk * 1e-6
        ):
            reasons.append("risk_geometry_mismatch")
        increment = risk / nav * 100
        if slots >= capacity:
            reasons.append("capacity")
        if counts.get(key, 0) >= int(risk_cfg["max_positions_per_sector"]):
            reasons.append("sector_limit")
        if c.position_value > nav * float(risk_cfg["max_position_pct"]) / 100 + 1e-9:
            reasons.append("position_cap")
        if risk > nav * float(risk_cfg["risk_per_trade_pct"]) / 100 + 1e-9:
            reasons.append("trade_risk_cap")
        if heat + increment > float(risk_cfg["max_portfolio_heat_pct"]) + 1e-9:
            reasons.append("portfolio_heat")
        required = c.position_value + c.estimated_costs
        if required > cash + 1e-9:
            reasons.append("cash")
        if reasons:
            rejected[c.instrument_id] = tuple(reasons)
            continue
        selected.append(c)
        slots += 1
        heat += increment
        cash -= required
        counts[key] = counts.get(key, 0) + 1
    return AllocationDecision(tuple(selected), rejected, round(heat, 8), round(cash, 2))
