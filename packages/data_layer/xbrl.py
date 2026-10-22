from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Iterable
from xml.etree import ElementTree as ET


INR_PER_CRORE = Decimal("10000000")


@dataclass(frozen=True)
class XbrlFact:
    name: str
    value: Decimal
    unit: str | None
    context_id: str | None
    period_start: date | None
    period_end: date | None
    instant: date | None
    dimensions: tuple[tuple[str, str], ...] = ()
    unsupported_context: bool = False
    context_valid: bool = True
    entity_identifier: str | None = None


FACT_ALIASES: dict[str, tuple[str, ...]] = {
    "sales": (
        "RevenueFromOperations",
        "RevenueFromOperationsNet",
        "Revenue",
        "TotalRevenue",
        "IncomeFromOperations",
    ),
    "operating_profit": (
        "OperatingProfit",
        "ProfitFromOperationsBeforeOtherIncomeFinanceCostsAndExceptionalItems",
        "ProfitLossFromOperationsBeforeOtherIncomeFinanceCostsAndExceptionalItems",
    ),
    "pat": (
        "ProfitLossForPeriod",
        "ProfitLoss",
        "ProfitAfterTax",
        "NetProfitLossForThePeriod",
    ),
    "diluted_eps": (
        "DilutedEarningsLossPerShare",
        "DilutedEarningsPerShare",
        "DilutedEPS",
    ),
    "pbt": (
        "ProfitBeforeTax",
        "ProfitLossBeforeTax",
        "ProfitBeforeExceptionalItemsAndTax",
    ),
    "other_income": ("OtherIncome",),
    "exceptional_items": (
        "ExceptionalItemsBeforeTax",
        "ExceptionalItems",
        "ExceptionalItem",
    ),
    "operating_cash_flow": (
        "CashFlowsFromUsedInOperatingActivities",
        "NetCashFlowsFromUsedInOperatingActivities",
        "NetCashFromOperatingActivities",
    ),
    "debt": (
        "Borrowings",
        "TotalBorrowings",
        "Debt",
    ),
    "equity": (
        "Equity",
        "TotalEquity",
        "EquityAttributableToOwnersOfParent",
    ),
    "interest_expense": (
        "InterestExpense",
        "FinanceCosts",
        "FinanceCost",
    ),
}


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()[:10]
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _context_details(root: ET.Element) -> dict[str, dict]:
    contexts = {}
    for node in root.iter():
        if _local_name(node.tag) != "context":
            continue
        context_id = node.attrib.get("id")
        if not context_id:
            continue
        if context_id in contexts:
            raise ValueError("duplicate_xbrl_context")
        start = end = instant = identifier = None
        dimensions = []
        unsupported = False
        for child in node.iter():
            name = _local_name(child.tag)
            if name == "startDate":
                start = _parse_date(child.text)
            elif name == "endDate":
                end = _parse_date(child.text)
            elif name == "instant":
                instant = _parse_date(child.text)
            elif name == "identifier":
                identifier = (child.text or "").strip()
            elif name == "explicitMember":
                dimensions.append((child.attrib.get("dimension", ""), (child.text or "").strip()))
            elif name == "typedMember":
                unsupported = True
            elif name in {"segment", "scenario"}:
                # Non-dimensional segment/scenario content must not silently
                # become an entity total either.
                unsupported |= any(_local_name(c.tag) not in {"explicitMember", "typedMember"} for c in child)
        contexts[context_id] = dict(
            period_start=start, period_end=end, instant=instant,
            dimensions=tuple(sorted(dimensions)), unsupported_context=unsupported,
            context_valid=bool(identifier and ((instant and not start and not end)
                or (start and end and start <= end and not instant))),
            entity_identifier=identifier,
        )
    return contexts


def _unit_map(root: ET.Element) -> dict[str, str]:
    units: dict[str, str] = {}
    for node in root.iter():
        if _local_name(node.tag) != "unit":
            continue
        unit_id = node.attrib.get("id")
        if not unit_id:
            continue
        measures = [
            (child.text or "").strip()
            for child in node.iter()
            if _local_name(child.tag) == "measure" and child.text
        ]
        units[unit_id] = "/".join(measures) if measures else unit_id
    return units


def parse_xbrl_facts(xml: bytes | str) -> list[XbrlFact]:
    raw = xml.encode("utf-8") if isinstance(xml, str) else xml
    if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise ValueError("xbrl_entities_forbidden")
    root = ET.parse(BytesIO(raw)).getroot()
    contexts = _context_details(root)
    units = _unit_map(root)
    facts: list[XbrlFact] = []

    for node in root.iter():
        context_id = node.attrib.get("contextRef")
        if not context_id or node.text is None:
            continue
        text_value = node.text.strip().replace(",", "")
        if not text_value:
            continue
        try:
            value = Decimal(text_value)
        except InvalidOperation:
            continue
        if not value.is_finite() or node.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}nil") in {"true", "1"}:
            continue
        context = contexts.get(context_id, dict(period_start=None, period_end=None, instant=None, context_valid=False))
        unit_ref = node.attrib.get("unitRef")
        facts.append(
            XbrlFact(
                name=_local_name(node.tag),
                value=value,
                unit=units.get(unit_ref, unit_ref) if unit_ref else None,
                context_id=context_id,
                **context,
            )
        )
    return facts


EXTRACTION_VERSION = "FINANCIAL_XBRL_v2"
INSTANT_FACTS = {"debt", "equity"}
PERIOD_DAYS = {"quarter": (75, 105), "half_year": (150, 210), "nine_months": (240, 300), "annual": (330, 380)}


def _scope_matches(fact: XbrlFact, statement_scope: str | None) -> bool:
    if not fact.context_valid or fact.unsupported_context:
        return False
    scopes = []
    for axis, member in fact.dimensions:
        axis = _local_name(axis).lower()
        member = _local_name(member).lower()
        if (any(x in axis for x in ("consolidat", "standalone", "statementtype", "natureofreport"))
                and member in {"consolidatedmember", "standalonemember", "consolidated", "standalone"}):
            scopes.append("consolidated" if member.startswith("consolidated") else "standalone")
        else:
            return False  # Segment, geography, class of share, typed axis, etc.
    return not scopes or (len(set(scopes)) == 1 and scopes[0] == statement_scope)


def _duration_matches(fact: XbrlFact, key: str, period_end: date, kind: str) -> bool:
    if key in INSTANT_FACTS:
        return fact.instant == period_end and fact.period_start is None
    if fact.instant is not None or fact.period_end != period_end or not fact.period_start:
        return False
    lo, hi = PERIOD_DAYS[kind]
    return lo <= (fact.period_end - fact.period_start).days + 1 <= hi


def _normalize_value(key: str, fact: XbrlFact) -> Decimal | None:
    # A currency/share ratio is valid only for EPS, never for sales or profit.
    if not fact.value.is_finite():
        return None
    measures = [_local_name(part).lower() for part in (fact.unit or "").split("/")]
    if key == "diluted_eps":
        return fact.value if measures == ["inr", "shares"] else None
    return fact.value / INR_PER_CRORE if measures == ["inr"] else None


def extract_canonical_financials(
    facts: Iterable[XbrlFact], period_end: date, *, aliases=None,
    filing_kind: str = "quarter", statement_scope: str | None = None,
    diagnostics: dict | None = None,
) -> dict[str, Decimal | None]:
    """Select entity totals for one explicit duration and scope, never XML order.

    Equal-priority contradictory facts remain unavailable. No YTD subtraction,
    segment aggregation, unknown-unit conversion or missing-value zero fill is
    performed. Optional diagnostics retain the selected source contexts.
    """
    if filing_kind not in PERIOD_DAYS:
        raise ValueError("unsupported_financial_period")
    fact_list = list(facts)
    result = {key: None for key in FACT_ALIASES}
    for key, names in (aliases or FACT_ALIASES).items():
        matching = [f for f in fact_list if f.name in names
            and _scope_matches(f, statement_scope)
            and _duration_matches(f, key, period_end, filing_kind)]
        # Prefer the first declared, semantically specific concept over generic
        # Revenue/TotalRevenue aliases, which can include other income.
        concept = next((name for name in names if any(f.name == name for f in matching)), None)
        candidates = [f for f in matching if f.name == concept]
        normalized = [(f, _normalize_value(key, f)) for f in candidates]
        values = {value for _, value in normalized if value is not None}
        entities = {f.entity_identifier for f, value in normalized if value is not None}
        invalid_units = any(value is None for _, value in normalized)
        # Different duration starts or entities are ambiguous even when the
        # numerical totals happen to agree.
        starts = {f.period_start for f, value in normalized if value is not None}
        valid = len(values) == 1 and len(entities) == 1 and len(starts) == 1 and not invalid_units
        if valid:
            result[key] = next(iter(values))
        if diagnostics is not None:
            diagnostics[key] = dict(
                status=("selected" if valid else "ambiguous" if candidates else "unavailable"),
                concept=concept,
                contexts=sorted({f.context_id for f in candidates if f.context_id}),
                starts=sorted(str(v) for v in starts if v),
                unit_basis="INR/share" if key == "diluted_eps" else "INR crore",
            )
    return result
