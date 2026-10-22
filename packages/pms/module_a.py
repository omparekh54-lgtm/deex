from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math

from packages.pms.policies import financial_sector


@dataclass(frozen=True)
class FundamentalSnapshot:
    market_cap_cr: float
    avg_traded_value_cr: float
    asm_gsm: bool
    promoter_pledge_pct: float
    quarterly_sales_cr: float
    pat_cr: float
    sales_yoy_pct: float | None
    operating_profit_yoy_pct: float | None
    pat_yoy_pct: float | None
    eps_yoy_pct: float | None
    positive_growth_quarters_last_4: int
    ttm_sales_rising: bool
    ttm_profit_rising: bool
    ocf_to_pat: float | None
    quality_pass: bool = True
    growth_acceleration_pp: float | None = None
    margin_expansion_bps: float | None = None
    debt_to_equity: float | None = None
    interest_coverage: float | None = None
    record_sales_8q: bool = False
    record_pat_8q: bool = False
    turnaround: bool = False
    triggers: tuple[str, ...] = field(default_factory=tuple)
    flags: tuple[str, ...] = field(default_factory=tuple)
    sector: str | None = None
    industry: str | None = None
    filing_timestamp: datetime | None = None
    eligible_until: datetime | None = None
    data_failures: tuple[str, ...] = field(default_factory=tuple)
    quality_failures: tuple[str, ...] = field(default_factory=tuple)
    pbt_yoy_pct: float | None = None
    core_pbt_yoy_pct: float | None = None
    cyclical: bool = False
    valuation_pe: float | None = None
    reference_context: dict = field(default_factory=dict)


@dataclass(frozen=True)
class FundamentalDecision:
    eligible: bool
    score: float
    failed_rules: tuple[str, ...]
    flags: tuple[str, ...]
    triggers: tuple[str, ...] = field(default_factory=tuple)
    components: dict[str, float] = field(default_factory=dict)
    reason: str = ''
    filing_timestamp: datetime | None = None
    eligible_until: datetime | None = None
    earnings_breakout: bool = False


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _passes_min(value: float | None, threshold: float) -> bool:
    return value is not None and math.isfinite(value) and value >= threshold


def _growth_component(snapshot: FundamentalSnapshot, f: dict) -> float:
    pairs = (
        (snapshot.sales_yoy_pct, f["min_sales_yoy_pct"]),
        (snapshot.operating_profit_yoy_pct, f["min_operating_profit_yoy_pct"]),
        (snapshot.pat_yoy_pct, f["min_pat_yoy_pct"]),
        (snapshot.eps_yoy_pct, f["min_eps_yoy_pct"]),
    )
    scores: list[float] = []
    for value, threshold in pairs:
        if value is None or not math.isfinite(value):
            scores.append(0.0)
        else:
            scores.append(min(max(value, 0.0) / max(float(threshold), 1.0), 1.5) / 1.5)
    return (sum(scores) / len(scores)) * 25.0


def evaluate_fundamentals(snapshot: FundamentalSnapshot, cfg: dict) -> FundamentalDecision:
    """Evaluate Module A.

    Eligibility follows Stages 1, 2 and 4. Stage 3 earnings-breakout triggers
    contribute to score/confidence but do not independently make a stock eligible.
    """
    f = cfg["fundamental"]
    failed: list[str] = list(snapshot.data_failures) + list(snapshot.quality_failures)
    # Reject NaN/infinity/negative physical quantities before comparisons/scoring.
    for name in ('market_cap_cr','avg_traded_value_cr','promoter_pledge_pct',
                 'quarterly_sales_cr','pat_cr','sales_yoy_pct','operating_profit_yoy_pct',
                 'pat_yoy_pct','eps_yoy_pct','ocf_to_pat','growth_acceleration_pp',
                 'margin_expansion_bps','debt_to_equity','interest_coverage'):
        value = getattr(snapshot, name)
        if value is not None and not math.isfinite(value):
            failed.append('non_finite_' + name)
    if not 0 <= snapshot.promoter_pledge_pct <= 100:
        failed.append('promoter_pledge_invalid')
    if not 0 <= snapshot.positive_growth_quarters_last_4 <= 4:
        failed.append('growth_consistency_invalid')
    if financial_sector(snapshot.sector):
        failed.append('financial_sector_excluded')

    hard_checks = (
        (snapshot.market_cap_cr > f["min_market_cap_cr"], "market_cap"),
        (snapshot.avg_traded_value_cr >= f["min_avg_traded_value_cr"], "liquidity"),
        (not snapshot.asm_gsm, "asm_gsm"),
        (snapshot.promoter_pledge_pct <= f["max_promoter_pledge_pct"], "promoter_pledge"),
        (snapshot.quarterly_sales_cr >= f["min_quarterly_sales_cr"], "minimum_sales_base"),
        ((snapshot.pat_cr > 0) if f["require_positive_pat"] else True, "positive_pat"),
    )
    for passed, name in hard_checks:
        if not passed:
            failed.append(name)

    growth_checks = (
        (_passes_min(snapshot.sales_yoy_pct, f["min_sales_yoy_pct"]), "sales_yoy"),
        (
            _passes_min(snapshot.operating_profit_yoy_pct, f["min_operating_profit_yoy_pct"]),
            "operating_profit_yoy",
        ),
        (_passes_min(snapshot.pat_yoy_pct, f["min_pat_yoy_pct"]), "pat_yoy"),
        (_passes_min(snapshot.eps_yoy_pct, f["min_eps_yoy_pct"]), "eps_yoy"),
        (
            snapshot.positive_growth_quarters_last_4 >= f["min_positive_quarters_last_4"],
            "growth_consistency",
        ),
        (snapshot.ttm_sales_rising, "ttm_sales"),
        (snapshot.ttm_profit_rising, "ttm_profit"),
    )
    for passed, name in growth_checks:
        if not passed:
            failed.append(name)

    if snapshot.ocf_to_pat is None:
        failed.append("ocf_to_pat_missing")
    elif snapshot.ocf_to_pat < f["min_ocf_to_pat"]:
        failed.append("ocf_to_pat")
    if not snapshot.quality_pass:
        failed.append("quality_guards")

    # Scoring follows the rule document's 30/25/15/15/15 structure.
    acceleration = 0.0
    if snapshot.growth_acceleration_pp is not None and math.isfinite(snapshot.growth_acceleration_pp):
        acceleration = min(max(snapshot.growth_acceleration_pp, 0.0) / 10.0, 1.0) * 30.0
    if snapshot.record_sales_8q or snapshot.record_pat_8q:
        acceleration = max(acceleration, 22.5)
    if snapshot.turnaround:
        acceleration = max(acceleration, 15.0)  # explicitly lower-confidence trigger

    growth = _growth_component(snapshot, f)

    margin = 0.0
    if snapshot.margin_expansion_bps is not None and math.isfinite(snapshot.margin_expansion_bps):
        margin = min(max(snapshot.margin_expansion_bps, 0.0) / 200.0, 1.0) * 15.0

    quality = 0.0
    if snapshot.quality_pass and not snapshot.quality_failures and snapshot.ocf_to_pat is not None and math.isfinite(snapshot.ocf_to_pat):
        quality = min(max(snapshot.ocf_to_pat, 0.0) / max(float(f["min_ocf_to_pat"]), 0.01), 1.0) * 15.0

    consistency = min(max(snapshot.positive_growth_quarters_last_4, 0) / 4.0, 1.0) * 15.0
    components = {
        "acceleration": round(acceleration, 2),
        "growth": round(growth, 2),
        "margin": round(margin, 2),
        "quality": round(quality, 2),
        "consistency": round(consistency, 2),
    }
    score = round(_clamp(sum(components.values())), 2)

    flags = list(snapshot.flags)
    valuation_limit = f.get('cyclical_high_pe_threshold')
    if snapshot.cyclical:
        flags.append('cyclical')
        if snapshot.valuation_pe is None:
            flags.append('valuation_unavailable')
        elif valuation_limit is None:
            flags.append('valuation_policy_unconfigured')
        elif snapshot.valuation_pe > valuation_limit:
            flags.append('high_valuation')
            score = max(0.0, score - float(f.get('cyclical_valuation_penalty_points', 5)))
    triggers = list(snapshot.triggers)
    if snapshot.growth_acceleration_pp is not None and snapshot.growth_acceleration_pp >= 10:
        triggers.append("growth_acceleration_10pp")
    if snapshot.margin_expansion_bps is not None and snapshot.margin_expansion_bps >= 200:
        triggers.append("margin_expansion_200bps")
    if snapshot.record_sales_8q:
        triggers.append("record_sales_8q")
    if snapshot.record_pat_8q:
        triggers.append("record_pat_8q")
    if snapshot.turnaround:
        triggers.append("turnaround")

    if any(name.startswith('non_finite_') for name in failed):
        score = 0.0
    return FundamentalDecision(
        eligible=not failed,
        score=score,
        failed_rules=tuple(dict.fromkeys(failed)),
        flags=tuple(dict.fromkeys(flags)),
        reason=('Eligible: Stages 1, 2 and 4 passed' if not failed else 'Ineligible: ' + ', '.join(dict.fromkeys(failed))),
        filing_timestamp=snapshot.filing_timestamp,
        eligible_until=snapshot.eligible_until,
        earnings_breakout=not failed and bool(triggers),
        triggers=tuple(dict.fromkeys(triggers)),
        components=components,
    )
