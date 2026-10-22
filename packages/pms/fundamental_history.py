from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import math
from typing import Any, Iterable


CORE_GROWTH_METRICS = ("sales", "operating_profit", "pat", "diluted_eps")


@dataclass(frozen=True)
class FinancialPeriod:
    period_end: date
    filed_at: datetime
    filing_kind: str
    consolidated: bool
    source: str
    facts: dict[str, float]
    revision_number: int = 1


@dataclass(frozen=True)
class FinancialHistoryMetrics:
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
    growth_acceleration_pp: float | None
    margin_expansion_bps: float | None
    debt_to_equity: float | None
    interest_coverage: float | None
    record_sales_8q: bool
    record_pat_8q: bool
    turnaround: bool
    quality_pass: bool
    triggers: tuple[str, ...] = field(default_factory=tuple)
    flags: tuple[str, ...] = field(default_factory=tuple)
    pbt_yoy_pct: float | None = None
    core_pbt_yoy_pct: float | None = None
    quality_failures: tuple[str, ...] = field(default_factory=tuple)
    latest_filing_timestamp: datetime | None = None
    latest_period_end: date | None = None
    annual_period_end: date | None = None
    latest_consolidated: bool = True
    interest_free: bool = False
    ttm_pat_cr: float | None = None
    normalization_method: str = 'pre_tax_ex_other_income_and_exceptional_items'


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _pct_growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous <= 0:
        return None
    return (current / previous - 1.0) * 100.0


def _margin(op: float | None, sales: float | None) -> float | None:
    if op is None or sales is None or sales == 0:
        return None
    return op / sales


def _sum_metric(periods: list[FinancialPeriod], key: str) -> float | None:
    values = [_number(p.facts.get(key)) for p in periods]
    if any(v is None for v in values):
        return None
    return sum(v for v in values if v is not None)


def _as_period(row: FinancialPeriod | dict[str, Any]) -> FinancialPeriod:
    if isinstance(row, FinancialPeriod):
        return row
    facts = {k: n for k, v in dict(row.get("facts") or {}).items() if (n := _number(v)) is not None}
    return FinancialPeriod(
        period_end=row["period_end"],
        filed_at=row["filed_at"],
        filing_kind=str(row.get("filing_kind") or "quarter").lower(),
        consolidated=bool(row.get("consolidated")),
        source=str(row.get("source") or "unknown"),
        facts=facts,
        revision_number=int(row.get("revision_number") or 1),
    )


def _quarter_key(period: date) -> int:
    return period.year * 4 + (period.month - 1) // 3


def _prior_year(quarters: list[FinancialPeriod], index: int) -> FinancialPeriod | None:
    if index >= len(quarters):
        return None
    current = quarters[index]
    return next((q for q in quarters if _quarter_key(q.period_end) == _quarter_key(current.period_end)-4
                 and q.consolidated == current.consolidated), None)


def _quarter_yoy(quarters: list[FinancialPeriod], index: int, key: str) -> float | None:
    previous = _prior_year(quarters, index)
    if previous is None:
        return None
    return _pct_growth(_number(quarters[index].facts.get(key)), _number(previous.facts.get(key)))


def _core_pbt(period: FinancialPeriod) -> float | None:
    # Exceptional items are signed: a positive gain is removed and a negative
    # expense is added back. Unknown line items are not assumed to be zero.
    values = [_number(period.facts.get(k)) for k in ('pbt', 'other_income', 'exceptional_items')]
    return values[0]-values[1]-values[2] if all(v is not None for v in values) else None


def calculate_financial_history(
    history: Iterable[FinancialPeriod | dict[str, Any]],
    *,
    consistency_metrics: tuple[str, ...] = ("sales", "pat"),
    min_ocf_to_pat: float = 0.7,
    as_of: datetime | None = None,
    max_cash_flow_age_days: int = 550,
    min_core_profit_growth: float = 25.0,
) -> FinancialHistoryMetrics:
    """Calculate Module A metrics from point-in-time-selected exchange filings.

    The source rules say "positive YoY in >=3/4 quarters" without identifying a
    specific line item. That interpretation is therefore explicit/configurable;
    v1 defaults to requiring both Sales and PAT YoY to be positive for a quarter
    to count toward the consistency test.
    """
    periods = [_as_period(row) for row in history]
    if as_of is not None:
        from packages.pms.module_a_context import as_ist
        periods = [p for p in periods if as_ist(p.filed_at) <= as_ist(as_of)
                   and p.period_end <= as_ist(as_of).date()]
    # A duplicate source/revision must never masquerade as another quarter.
    selected = {}
    for period in periods:
        if period.filing_kind.lower() not in {'quarter','quarterly','annual','year','yearly'}:
            continue
        kind = 'annual' if period.filing_kind.lower() in {'annual','year','yearly'} else 'quarter'
        key = (period.period_end, kind)
        old = selected.get(key)
        from packages.pms.module_a_context import as_ist
        if old is None or (period.consolidated, as_ist(period.filed_at), period.revision_number) > (old.consolidated, as_ist(old.filed_at), old.revision_number):
            selected[key] = period
    periods = list(selected.values())
    quarters = sorted(
        [p for p in periods if p.filing_kind.lower() in {"quarter", "quarterly"}],
        key=lambda p: p.period_end,
        reverse=True,
    )
    annual = sorted(
        [p for p in periods if p.filing_kind.lower() in {"annual", "year", "yearly"}],
        key=lambda p: p.period_end,
        reverse=True,
    )
    if not quarters:
        raise ValueError("at least one quarterly financial filing is required")

    latest = quarters[0]
    flags: list[str] = []
    triggers: list[str] = []
    quality_failures: list[str] = []
    latest_key = _quarter_key(latest.period_end)
    contiguous = len(quarters) >= 8 and [_quarter_key(q.period_end) for q in quarters[:8]] == list(range(latest_key, latest_key-8, -1))
    same_basis = all(q.consolidated == latest.consolidated for q in quarters[:8])
    if not contiguous:
        quality_failures.append('quarter_history_gaps')
    if not same_basis:
        flags.append('mixed_reporting_basis')
        quality_failures.append('mixed_reporting_basis')
    if not latest.consolidated:
        flags.append('standalone_fallback')

    latest_sales = _number(latest.facts.get("sales"))
    latest_pat = _number(latest.facts.get("pat"))
    if latest_sales is None:
        flags.append("sales_missing")
    if latest_pat is None:
        flags.append("pat_missing")

    yoy = {key: _quarter_yoy(quarters, 0, key) for key in CORE_GROWTH_METRICS}
    for key, value in yoy.items():
        if value is None:
            flags.append(f"{key}_yoy_unavailable")

    positive_quarters = 0
    for idx in range(min(4, len(quarters))):
        if _quarter_key(quarters[idx].period_end) != latest_key-idx:
            continue
        growth = [_quarter_yoy(quarters, idx, key) for key in consistency_metrics]
        if growth and all(value is not None and value > 0 for value in growth):
            positive_quarters += 1
    if len(quarters) < 8:
        flags.append("eight_quarter_history_incomplete")

    current_ttm_sales = _sum_metric(quarters[:4], "sales") if contiguous and same_basis else None
    previous_ttm_sales = _sum_metric(quarters[4:8], "sales") if contiguous and same_basis else None
    current_ttm_pat = _sum_metric(quarters[:4], "pat") if contiguous and same_basis else None
    previous_ttm_pat = _sum_metric(quarters[4:8], "pat") if contiguous and same_basis else None
    ttm_sales_rising = (
        current_ttm_sales is not None
        and previous_ttm_sales is not None
        and current_ttm_sales > previous_ttm_sales
    )
    ttm_profit_rising = (
        current_ttm_pat is not None
        and previous_ttm_pat is not None
        and current_ttm_pat > previous_ttm_pat
    )

    acceleration_values: list[float] = []
    for key in CORE_GROWTH_METRICS:
        current_yoy = yoy[key]
        previous_yoy = _quarter_yoy(quarters, 1, key) if len(quarters) >= 2 and _quarter_key(quarters[1].period_end) == latest_key-1 else None
        if current_yoy is not None and previous_yoy is not None:
            acceleration_values.append(current_yoy - previous_yoy)
    growth_acceleration = max(acceleration_values) if acceleration_values else None
    if growth_acceleration is not None and growth_acceleration >= 10.0:
        triggers.append("growth_acceleration_10pp")

    current_margin = _margin(_number(latest.facts.get("operating_profit")), latest_sales)
    prior_year_margin = None
    prior = _prior_year(quarters, 0)
    if prior is not None:
        prior_year_margin = _margin(
            _number(prior.facts.get("operating_profit")),
            _number(prior.facts.get("sales")),
        )
    margin_expansion_bps = None
    if current_margin is not None and prior_year_margin is not None:
        margin_expansion_bps = (current_margin - prior_year_margin) * 10_000.0
        if margin_expansion_bps >= 200.0:
            triggers.append("margin_expansion_200bps")
    else:
        flags.append("margin_expansion_unavailable")

    record_sales = False
    record_pat = False
    if contiguous and same_basis:
        sales_values = [_number(q.facts.get("sales")) for q in quarters[:8]]
        pat_values = [_number(q.facts.get("pat")) for q in quarters[:8]]
        if latest_sales is not None and all(v is not None for v in sales_values[1:]):
            record_sales = latest_sales > max(v for v in sales_values[1:] if v is not None)
        if latest_pat is not None and all(v is not None for v in pat_values[1:]):
            record_pat = latest_pat > max(v for v in pat_values[1:] if v is not None)
    if record_sales:
        triggers.append("record_sales_8q")
    if record_pat:
        triggers.append("record_pat_8q")

    turnaround = False
    if prior is not None and latest_pat is not None:
        prior_pat = _number(prior.facts.get("pat"))
        turnaround = prior_pat is not None and prior_pat <= 0 < latest_pat
    if turnaround:
        triggers.append("turnaround")
        flags.append("turnaround_lower_confidence")

    ocf_to_pat: float | None = None
    annual_for_cash = next((p for p in annual if p.consolidated == latest.consolidated
                            and p.period_end <= latest.period_end), None)
    if annual_for_cash:
        annual_pat = _number(annual_for_cash.facts.get("pat"))
        annual_ocf = _number(annual_for_cash.facts.get("operating_cash_flow"))
        if annual_pat is not None and annual_pat > 0 and annual_ocf is not None:
            ocf_to_pat = annual_ocf / annual_pat
    if ocf_to_pat is None:
        flags.append("annual_ocf_to_pat_unavailable")
        quality_failures.append('annual_ocf_to_pat_unavailable')
    elif ocf_to_pat < min_ocf_to_pat:
        quality_failures.append('annual_ocf_to_pat')
    if annual_for_cash is not None:
        anchor = as_of.date() if as_of is not None else latest.period_end
        if (anchor-annual_for_cash.period_end).days > max_cash_flow_age_days:
            quality_failures.append('annual_cash_flow_stale')

    balance = next((p for p in sorted(periods, key=lambda p:p.period_end, reverse=True)
                    if p.period_end <= latest.period_end and p.consolidated == latest.consolidated
                    and _number(p.facts.get('debt')) is not None and _number(p.facts.get('equity')) is not None), None)
    debt = _number(balance.facts.get("debt")) if balance else None
    equity = _number(balance.facts.get("equity")) if balance else None
    if balance and (latest.period_end-balance.period_end).days > max_cash_flow_age_days:
        quality_failures.append('balance_sheet_stale')
    debt_to_equity = debt / equity if debt is not None and equity is not None and equity > 0 else None
    if debt_to_equity is None:
        flags.append("debt_to_equity_unavailable")

    interest = _number(latest.facts.get("interest_expense"))
    op = _number(latest.facts.get("operating_profit"))
    interest_coverage = op / interest if op is not None and interest is not None and interest > 0 else None
    interest_free = interest == 0 and debt == 0 and op is not None and op > 0
    if interest_free:
        flags.append('interest_free')
    elif interest_coverage is None:
        flags.append("interest_coverage_unavailable")

    pbt_yoy = _quarter_yoy(quarters, 0, 'pbt')
    core_pbt_yoy = _pct_growth(_core_pbt(latest), _core_pbt(prior)) if prior is not None else None
    if pbt_yoy is None:
        quality_failures.append('pbt_growth_unavailable')
    elif pbt_yoy <= 0:
        quality_failures.append('pbt_not_growing')
    if core_pbt_yoy is None:
        quality_failures.append('normalized_profit_growth_unavailable')
    elif core_pbt_yoy < min_core_profit_growth:
        quality_failures.append('normalized_profit_growth')
    if pbt_yoy is not None and yoy['pat'] is not None and yoy['pat'] > pbt_yoy:
        flags.append('tax_rate_distortion_review')
    if yoy['pat'] is not None and yoy['diluted_eps'] is not None and yoy['diluted_eps'] < yoy['pat']-1e-9:
        flags.append('dilution')
    if prior is not None and (_number(prior.facts.get('pat')) or 0) < 0.1 * max(latest_pat or 0, 0):
        flags.append('low_base')
    exceptional = _number(latest.facts.get('exceptional_items'))
    if exceptional is not None and exceptional != 0:
        flags.append('exceptional_items_present')
    if (_number(latest.facts.get('other_income')) or 0) != 0:
        flags.append('other_income_excluded_from_quality_growth')
    quality_pass = not quality_failures

    return FinancialHistoryMetrics(
        quarterly_sales_cr=latest_sales or 0.0,
        pat_cr=latest_pat or 0.0,
        sales_yoy_pct=yoy["sales"],
        operating_profit_yoy_pct=yoy["operating_profit"],
        pat_yoy_pct=yoy["pat"],
        eps_yoy_pct=yoy["diluted_eps"],
        positive_growth_quarters_last_4=positive_quarters,
        ttm_sales_rising=ttm_sales_rising,
        ttm_profit_rising=ttm_profit_rising,
        ocf_to_pat=ocf_to_pat,
        growth_acceleration_pp=growth_acceleration,
        margin_expansion_bps=margin_expansion_bps,
        debt_to_equity=debt_to_equity,
        interest_coverage=interest_coverage,
        record_sales_8q=record_sales,
        record_pat_8q=record_pat,
        turnaround=turnaround,
        quality_pass=quality_pass,
        triggers=tuple(dict.fromkeys(triggers)),
        flags=tuple(dict.fromkeys(flags)),
        pbt_yoy_pct=pbt_yoy, core_pbt_yoy_pct=core_pbt_yoy,
        quality_failures=tuple(dict.fromkeys(quality_failures)),
        latest_filing_timestamp=latest.filed_at, latest_period_end=latest.period_end,
        annual_period_end=annual_for_cash.period_end if annual_for_cash else None,
        latest_consolidated=latest.consolidated, interest_free=interest_free,
        ttm_pat_cr=current_ttm_pat,
    )
