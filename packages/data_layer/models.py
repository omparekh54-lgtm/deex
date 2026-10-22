from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    exchange: str
    symbol: str
    trading_symbol: str
    token: str | None = None
    isin: str | None = None
    series: str | None = None
    name: str | None = None
    sector: str | None = None
    active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Candle:
    instrument_id: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    source: str
    adjusted: bool = False
    turnover: Decimal | None = None


@dataclass(frozen=True)
class Quote:
    instrument_id: str
    timestamp: datetime
    last_price: Decimal
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: int | None = None
    source: str = "unknown"


@dataclass(frozen=True)
class FinancialFiling:
    instrument_id: str
    period_end: date
    filed_at: datetime
    statement_scope: str
    source: str
    version: int
    sales: Decimal | None = None
    operating_profit: Decimal | None = None
    pat: Decimal | None = None
    diluted_eps: Decimal | None = None
    pbt: Decimal | None = None
    other_income: Decimal | None = None
    exceptional_items: Decimal | None = None
    operating_cash_flow: Decimal | None = None
    debt: Decimal | None = None
    equity: Decimal | None = None
    interest_expense: Decimal | None = None
    submission_type: str | None = None
    audit_status: str | None = None
    revised_at: datetime | None = None
    revision_remarks: str | None = None
    exchange_received_at: datetime | None = None
    exchange_disseminated_at: datetime | None = None
    filing_kind: str = "quarter"
    external_id: str | None = None
    details_uri: str | None = None
    xbrl_uri: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SurveillanceState:
    instrument_id: str
    observed_at: datetime
    asm: bool = False
    gsm: bool = False
    stage: str | None = None
    source: str = "NSE"


@dataclass(frozen=True)
class DataQualityIssue:
    dataset: str
    instrument_id: str | None
    severity: str
    code: str
    message: str
    observed_at: datetime
