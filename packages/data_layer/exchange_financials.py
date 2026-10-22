from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from .models import FinancialFiling
from .xbrl import EXTRACTION_VERSION, FACT_ALIASES, extract_canonical_financials, parse_xbrl_facts


FINANCIAL_ACK_DATASET = "CLOUD_FINANCIAL_XML_V2"


IST_FORMATS = (
    "%d-%b-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


@dataclass(frozen=True)
class ExchangeFilingRecord:
    exchange: str
    symbol: str
    company_name: str | None
    period_end: date
    filed_at: datetime
    statement_scope: str
    submission_type: str
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

    @property
    def instrument_id(self) -> str:
        return f"{self.exchange}:{self.symbol}"


class ExchangeFinancialsError(RuntimeError):
    pass


def _norm_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _pick(row: dict[str, Any], *names: str) -> Any:
    normalized = {_norm_key(str(k)): v for k, v in row.items()}
    for name in names:
        key = _norm_key(name)
        if key in normalized and normalized[key] not in (None, "", "-"):
            return normalized[key]
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", "-"):
        return None
    if isinstance(value, datetime):
        return value.astimezone(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) if value.tzinfo else value
    text = str(value).strip().replace("  ", " ")
    if text.endswith("Z"):
        try:
            return _parse_datetime(datetime.fromisoformat(text.replace("Z", "+00:00")))
        except ValueError:
            pass
    try:
        return _parse_datetime(datetime.fromisoformat(text))
    except ValueError:
        pass
    for fmt in IST_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_date(value: Any) -> date | None:
    if value in (None, "", "-"):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _scope(value: Any) -> str:
    text = str(value or "Standalone").strip().lower()
    return "consolidated" if "consol" in text else "standalone"


def _filing_kind(row: dict[str, Any]) -> str:
    value = _pick(row, "filing_kind", "filingKind", "periodType", "typeOfResult", "resultType", "period_type")
    label = _norm_key(str(value or "quarter"))
    if label in {"annual", "annually", "year", "yearly", "fullyear", "yearended", "financialyear"}:
        return "annual"
    if label in {"halfyear", "halfyearly", "sixmonths"}:
        return "half_year"
    if label == "ninemonths":
        return "nine_months"
    # Audited status, the month March, and a fiscal-year label do not prove
    # annual duration. XML must supply a full-year context for an annual record.
    return "quarter"


def _submission_version(submission_type: str) -> int:
    text = submission_type.lower()
    return 2 if "revis" in text else 1


def _absolute(base: str, uri: Any) -> str | None:
    if not uri:
        return None
    text = str(uri).strip()
    if not text or text == "-":
        return None
    return urljoin(base, text)


def _records_from_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "records", "results", "content", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            nested = _records_from_json(value)
            if nested:
                return nested
    return []


class NSEIntegratedFinancialsProvider:
    """Official NSE Integrated Filing - Financials source.

    Uses NSE's public `/api/integrated-filing-results` endpoint. The response
    schema has changed before, so field aliases are intentionally tolerant while
    preserving the complete raw exchange payload for audit/replay.
    """

    BASE_URL = "https://www.nseindia.com"
    API_PATH = "/api/integrated-filing-results"
    LANDING_PATH = "/companies-listing/corporate-integrated-filing"

    def __init__(self, client: httpx.Client | None = None, timeout: float = 20.0):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/134 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self.BASE_URL + self.LANDING_PATH,
            "X-Requested-With": "XMLHttpRequest",
        }
        self.client = client or httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)
        self._primed = False

    def _prime(self) -> None:
        if self._primed:
            return
        response = self.client.get(self.BASE_URL + self.LANDING_PATH)
        response.raise_for_status()
        self._primed = True

    def list_filings(
        self,
        symbol: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        page: int = 1,
        size: int = 100,
        segment: str = "equities",
        strict: bool = False,
    ) -> list[ExchangeFilingRecord]:
        self._prime()
        params: dict[str, Any] = {
            "type": "Integrated Filing- Financials",
            "page": page,
            "size": size,
            "index": segment,
        }
        if symbol:
            params["symbol"] = symbol.upper()
            params["period_ended"] = "all"
        if from_date and to_date:
            params["from_date"] = from_date.strftime("%d-%m-%Y")
            params["to_date"] = to_date.strftime("%d-%m-%Y")
        elif from_date or to_date:
            raise ValueError("from_date and to_date must be supplied together")

        response = self.client.get(self.BASE_URL + self.API_PATH, params=params)
        response.raise_for_status()
        rows = _records_from_json(response.json())
        parsed = []
        for row in rows:
            record = self._parse_row(row)
            if record is None:
                if strict:
                    raise ExchangeFinancialsError("Financial discovery returned an unparseable filing")
                continue
            parsed.append(record)
        return parsed

    def _parse_row(self, row: dict[str, Any]) -> ExchangeFilingRecord | None:
        symbol = _pick(row, "symbol", "sm_symbol", "securitySymbol")
        period_end = _parse_date(_pick(row, "quarterEndDate", "quarter_end_date", "periodEnded", "period_end", "qe_Date"))
        filed_at = _parse_datetime(
            _pick(row, "broadcastDateTime", "broadcast_date_time", "sort_date", "filingDateTime", "exchdisstime", "broadcast_Date", "revised_Date", "creation_Date")
        )
        if not symbol or period_end is None or filed_at is None:
            return None
        publication_times = [filed_at]
        for names in (("creation_Date", "exchangeDisseminationTime", "exchange_dissemination_time"),
                      ("exchangeReceivedTime", "exchange_received_time"),
                      ("revisedDateTime", "revised_date_time", "revisedAt", "revised_Date")):
            parsed = _parse_datetime(_pick(row, *names))
            if parsed:
                publication_times.append(parsed)
        filed_at = max(publication_times)

        submission_type = str(_pick(row, "typeOfSubmission", "submissionType", "type_of_submission", "type_Sub") or "Original")
        return ExchangeFilingRecord(
            exchange="NSE",
            symbol=str(symbol).upper(),
            company_name=_pick(row, "companyName", "company_name", "sm_name", "issuer", "cmName"),
            period_end=period_end,
            filed_at=filed_at,
            statement_scope=_scope(_pick(row, "consolidatedStandalone", "consolidated_standalone", "statementType", "scope", "consolidated")),
            submission_type=submission_type,
            audit_status=_pick(row, "auditedUnaudited", "auditStatus", "audited_unaudited", "audited"),
            revised_at=_parse_datetime(_pick(row, "revisedDateTime", "revised_date_time", "revisedAt", "revised_Date")),
            revision_remarks=_pick(row, "revisionRemarks", "revision_remarks", "remarks", "revision_Remark"),
            exchange_received_at=_parse_datetime(_pick(row, "exchangeReceivedTime", "exchange_received_time")),
            exchange_disseminated_at=_parse_datetime(_pick(row, "exchangeDisseminationTime", "exchange_dissemination_time", "creation_Date")),
            external_id=str(_pick(row, "id", "filingId", "seqId", "sequenceNo") or "") or None,
            details_uri=_absolute(self.BASE_URL, _pick(row, "details", "detailsLink", "attachment", "attchmntFile")),
            xbrl_uri=_absolute(self.BASE_URL, _pick(row, "xbrl", "xbrlLink", "xbrlFile", "xbrl_file_link")),
            filing_kind=_filing_kind(row),
            raw_payload=row,
        )

    def fetch_xbrl(self, record: ExchangeFilingRecord) -> bytes:
        if not record.xbrl_uri:
            raise ExchangeFinancialsError("NSE filing has no XBRL URI in exchange payload")
        response = self.client.get(record.xbrl_uri)
        response.raise_for_status()
        return response.content


class BSEFinancialResultsProvider:
    """Official BSE financial-results page/XBRL discovery provider.

    BSE's public company results page exposes New/Revised status and filing time.
    We parse that official page and its XBRL links. An optional JSON endpoint can
    be supplied through BSE_FINANCIALS_API_URL if BSE changes the public page.
    """

    BASE_URL = "https://www.bseindia.com"
    PAGE_PATH = "/corporates/Comp_Results.aspx"

    def __init__(self, client: httpx.Client | None = None, timeout: float = 20.0):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/134 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Referer": self.BASE_URL + "/corporates/",
        }
        self.client = client or httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)

    def list_filings(self, bse_code: str, symbol: str | None = None) -> list[ExchangeFilingRecord]:
        override = os.getenv("BSE_FINANCIALS_API_URL")
        if override:
            response = self.client.get(override, params={"Code": str(bse_code)})
            response.raise_for_status()
            return [
                record
                for row in _records_from_json(response.json())
                if (record := self._parse_json_row(row, symbol or str(bse_code))) is not None
            ]

        url = self.BASE_URL + self.PAGE_PATH
        response = self.client.get(url, params={"Code": str(bse_code)})
        response.raise_for_status()
        return self._parse_html(response.text, symbol or str(bse_code), str(response.url))

    def _parse_json_row(self, row: dict[str, Any], symbol: str) -> ExchangeFilingRecord | None:
        period_end = _parse_date(_pick(row, "quarterEndDate", "periodEnd", "period_end"))
        filed_at = _parse_datetime(_pick(row, "filingDateTime", "filing_date_time", "broadcastDateTime"))
        if period_end is None or filed_at is None:
            return None
        revised_at = _parse_datetime(_pick(row, "revisedDateTime", "revised_date_time"))
        if revised_at:
            filed_at = max(filed_at, revised_at)
        status = str(_pick(row, "status", "submissionType") or "New")
        return ExchangeFilingRecord(
            exchange="BSE",
            symbol=symbol.upper(),
            company_name=_pick(row, "securityName", "companyName"),
            period_end=period_end,
            filed_at=filed_at,
            statement_scope=_scope(_pick(row, "scope", "standaloneConsolidated", "type")),
            submission_type="Revised" if "revis" in status.lower() else "Original",
            revised_at=_parse_datetime(_pick(row, "revisedDateTime", "revised_date_time")),
            revision_remarks=_pick(row, "revisionReason", "revision_reason"),
            xbrl_uri=_absolute(self.BASE_URL, _pick(row, "xbrl", "xbrlLink")),
            external_id=str(_pick(row, "id", "resultId") or "") or None,
            filing_kind=_filing_kind(row),
            raw_payload=row,
        )

    def _parse_html(self, html: str, symbol: str, base_url: str) -> list[ExchangeFilingRecord]:
        soup = BeautifulSoup(html, "lxml")
        records: list[ExchangeFilingRecord] = []
        for tr in soup.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            joined = " | ".join(cells)
            if not cells or not re.search(r"\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}", joined):
                continue

            filing_match = re.search(r"(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})", joined)
            period_match = re.search(r"(?:Standalone|Consolidated)-([A-Za-z]{3})-(\d{2})", joined, re.I)
            if not filing_match or not period_match:
                continue
            month = datetime.strptime(period_match.group(1), "%b").month
            year2 = int(period_match.group(2))
            year = 2000 + year2
            # BSE row's fiscal-year text disambiguates Mar rows when needed; the
            # quarter label itself gives the calendar period end month/year.
            import calendar

            period_end = date(year, month, calendar.monthrange(year, month)[1])
            scope = "consolidated" if "Consolidated-" in joined else "standalone"
            submission = "Revised" if re.search(r"\bRevised\b", joined, re.I) else "Original"
            links = [urljoin(base_url, a.get("href")) for a in tr.find_all("a") if a.get("href")]
            xbrl_links = [u for u in links if "xbrl" in u.lower() or u.lower().endswith((".xml", ".xbrl"))]
            records.append(
                ExchangeFilingRecord(
                    exchange="BSE",
                    symbol=symbol.upper(),
                    company_name=None,
                    period_end=period_end,
                    filed_at=datetime.strptime(filing_match.group(1), "%d-%m-%Y %H:%M:%S"),
                    statement_scope=scope,
                    submission_type=submission,
                    revised_at=None,
                    revision_remarks=None,
                    xbrl_uri=xbrl_links[0] if xbrl_links else None,
                    filing_kind="annual" if re.search(r"\b(?:Annual|Yearly|Year Ended)\b", joined, re.I) and not re.search(r"\bQuarter\b", joined, re.I) else "quarter",
                    raw_payload={"cells": cells, "links": links},
                )
            )
        return records

    def fetch_xbrl(self, record: ExchangeFilingRecord) -> bytes:
        if not record.xbrl_uri:
            raise ExchangeFinancialsError("BSE filing has no XBRL URI in exchange page")
        response = self.client.get(record.xbrl_uri)
        response.raise_for_status()
        return response.content


def filing_from_exchange_record(
    record: ExchangeFilingRecord,
    xbrl: bytes | str | None = None,
) -> FinancialFiling:
    canonical: dict[str, Decimal | None] = {
        "sales": None,
        "operating_profit": None,
        "pat": None,
        "diluted_eps": None,
        "pbt": None,
        "other_income": None,
        "exceptional_items": None,
        "operating_cash_flow": None,
        "debt": None,
        "equity": None,
        "interest_expense": None,
    }
    raw_payload = dict(record.raw_payload)
    if xbrl:
        aliases = dict(FACT_ALIASES)
        source = urlparse(record.xbrl_uri or '')
        profiles = {
            'BANKING': ('Income', 'ProfitLossForThePeriod', 'BANK_TOTAL_INCOME'),
            'GI': ('OperatingIncome', 'ProfitLossAfterTax', 'GENERAL_INSURANCE_OPERATING_INCOME'),
            'LI': ('NetPremiumIncome', 'ProfitLossAfterTaxAndExtraordinaryItems', 'LIFE_INSURANCE_NET_PREMIUM'),
            'INS_COM_IND_AS': ('InsuranceRevenue', 'ProfitLossForPeriod', 'IND_AS_INSURANCE_REVENUE'),
        }
        match = re.fullmatch(r'INTEGRATED_FILING_(BANKING|GI|LI)_[0-9]+_[0-9]+_WEB.xml',
                             source.path.rsplit('/', 1)[-1])
        insurance_match = re.fullmatch(
            r'IF_(INS_COM_IND_AS)_[0-9]+_WebXMLFile_[0-9]{8}_[0-9]+\.xml',
            source.path.rsplit('/', 1)[-1])
        match = match or insurance_match
        if (record.exchange == 'NSE' and source.scheme == 'https'
                and source.hostname == 'nsearchives.nseindia.com' and match):
            revenue, pat, basis = profiles[match.group(1)]
            aliases.update(sales=(revenue,), pat=(pat,))
            # These are sector-specific income measures, not industrial sales.
            # Preserve the basis; Module A excludes financial-sector issuers.
            raw_payload = {**raw_payload, 'canonical_metric_basis': {
                'sales': basis, 'sales_fact': revenue, 'pat_fact': pat}}
        diagnostics = {}
        if (record.raw_payload.get('financial_source') or {}).get('format')=='NSE_RENDERED_TABLES':
            from .nse_financial_render import parse_financial_render
            facts=parse_financial_render(xbrl,record)
        else:
            facts=parse_xbrl_facts(xbrl)
        canonical.update(extract_canonical_financials(
            facts, record.period_end, aliases=aliases,
            filing_kind=record.filing_kind, statement_scope=record.statement_scope,
            diagnostics=diagnostics))
        raw_payload["financial_extraction"] = dict(version=EXTRACTION_VERSION,
            filing_kind=record.filing_kind, statement_scope=record.statement_scope,
            fields=diagnostics)

    return FinancialFiling(
        instrument_id=record.instrument_id,
        period_end=record.period_end,
        filed_at=record.filed_at,
        statement_scope=record.statement_scope,
        source=record.exchange,
        version=_submission_version(record.submission_type),
        submission_type=record.submission_type,
        audit_status=record.audit_status,
        revised_at=record.revised_at,
        revision_remarks=record.revision_remarks,
        exchange_received_at=record.exchange_received_at,
        exchange_disseminated_at=record.exchange_disseminated_at,
        filing_kind=record.filing_kind,
        external_id=record.external_id,
        details_uri=record.details_uri,
        xbrl_uri=record.xbrl_uri,
        raw_payload=raw_payload,
        **canonical,
    )


def filings_from_exchange_record(
    record: ExchangeFilingRecord, xbrl: bytes | str | None = None,
) -> list[FinancialFiling]:
    """Preserve quarter and full-year observations from a single disclosure.

    Both keep the exchange filing timestamp, revision and original payload.
    Annual records are sourced from actual full-year contexts, never inferred
    from a March date or calculated by multiplying a quarter by four.
    """
    if xbrl and record.filing_kind=='quarter' and not record.raw_payload.get('financial_source'):
        # SME half-year declarations are not three-month quarterly observations.
        from xml.etree import ElementTree as ET
        raw=xbrl.encode() if isinstance(xbrl,str) else xbrl
        if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
            raise ValueError('xbrl_entities_forbidden')
        root=ET.fromstring(raw)
        labels={_norm_key(x.text or '') for x in root
            if x.tag.rsplit('}',1)[-1]=='ReportingQuarter'}
        if labels and labels<={'halfyear','halfyearly','sixmonths'}:
            record=replace(record,filing_kind='half_year',raw_payload={**record.raw_payload,
                'source_reported_filing_kind':'half_year','exchange_summary_filing_kind':record.filing_kind})
    primary = filing_from_exchange_record(record, xbrl)
    filings = [primary]
    if xbrl:
        other_kind = "annual" if record.filing_kind == "quarter" else "quarter"
        other = filing_from_exchange_record(replace(record, filing_kind=other_kind), xbrl)
        if (primary.sales is None and primary.pat is None
                and other.sales is not None and other.pat is not None):
            # Some exchange summaries omit the annual label. The actual XML
            # duration, not an empty default quarter, identifies that report.
            return [other]
        if other.sales is not None or other.pat is not None or (
            other_kind == "annual" and other.operating_cash_flow is not None
        ):
            filings.append(other)
    return filings


def prefer_nse_then_bse(
    nse: Iterable[FinancialFiling], bse: Iterable[FinancialFiling]
) -> list[FinancialFiling]:
    """Merge official filings without silently replacing exchange observations.

    NSE records are preferred for identical period/scope/filed-at observations,
    while genuinely different timestamps/revisions from BSE are retained as
    separate point-in-time evidence.
    """
    merged: dict[tuple[Any, ...], FinancialFiling] = {}
    for filing in list(bse) + list(nse):
        key = (
            filing.instrument_id.split(":", 1)[-1],
            filing.period_end,
            filing.statement_scope,
            filing.filing_kind,
            filing.filed_at,
            filing.version,
        )
        if key not in merged or filing.source == "NSE":
            merged[key] = filing
    return sorted(merged.values(), key=lambda f: (f.period_end, f.filed_at, f.source))
