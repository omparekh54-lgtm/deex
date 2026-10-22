"""Bounded official NSE sources for unattended EOD collection.

Archive prices are raw/unadjusted. A successful HTTP request is never a data gate.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
ARCHIVE = "https://nsearchives.nseindia.com"
NSE = "https://www.nseindia.com"
ALLOWED_HOSTS = {
    "nsearchives.nseindia.com",
    "archives.nseindia.com",
    "www.nseindia.com",
    "www.niftyindices.com",
}
BENCHMARKS = {
    "Nifty 50": "NIFTY50",
    "Nifty 500": "NIFTY500",
    "Nifty MidSmallcap 400": "NIFTYMIDSMALLCAP400",
    "India VIX": "INDIAVIX",
}
MAX_BYTES = 20 * 1024 * 1024


class SourceError(RuntimeError):
    """Stable error code only; never echo a remote response or credentials."""


def allowed_url(url):
    p = urllib.parse.urlsplit(url)
    if (
        p.scheme != "https"
        or p.hostname not in ALLOWED_HOSTS
        or p.username
        or p.password
        or p.port not in (None, 443)
    ):
        raise SourceError("source_url_not_allowed")
    return url


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        allowed_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


@dataclass(frozen=True)
class Document:
    url: str
    body: bytes
    sha256: str
    observed_at: datetime
    size: int = 0
    archive_key: str | None = None


class OfficialHTTP:
    def __init__(self, *, seconds=1500, opener=None, sleep=time.sleep, archive=None):
        from packages.data_layer.source_pacing import PUBLIC_SOURCE_PACER

        self.deadline = time.monotonic() + seconds
        self.pacer = PUBLIC_SOURCE_PACER if opener is None else None
        self.opener = opener or urllib.request.build_opener(SafeRedirect())
        self.sleep = sleep
        self.documents = []
        self.downloaded_bytes = 0
        self.archive = archive

    def fetch(self, url, *, financial_render=False):
        allowed_url(url)
        if financial_render and not (urllib.parse.urlsplit(url).hostname=='nsearchives.nseindia.com'
                and re.fullmatch(r'/corporate/ixbrl/INTEGRATED_FILING_INDAS_[0-9]+_[0-9]+_iXBRL_WEB\.html',urllib.parse.urlsplit(url).path)):
            raise SourceError('financial_render_url_not_allowed')
        for attempt in range(3):
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise SourceError("collection_deadline_exceeded")
            try:
                if self.pacer is not None:
                    self.pacer.wait(url, self.deadline)
                request = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": NSE + "/",
                        "Accept": "application/json,text/csv,*/*",
                    },
                )
                with self.opener.open(request, timeout=min(20, remaining)) as response:
                    allowed_url(response.geturl())
                    body = response.read(MAX_BYTES + 1)
                    if len(body) > MAX_BYTES:
                        raise SourceError("source_too_large")
                if not body or (not financial_render and body.lstrip().lower().startswith(
                    (b"<!doctype html", b"<html")
                )):
                    raise SourceError("empty_or_html_source")
                self.downloaded_bytes += len(body)
                if self.downloaded_bytes > 256 * 1024 * 1024:
                    raise SourceError("run_download_budget_exceeded")
                doc = Document(
                    url,
                    body,
                    hashlib.sha256(body).hexdigest(),
                    datetime.now(IST),
                    len(body),
                )
                if self.archive is not None:
                    from dataclasses import replace
                    doc = replace(doc, archive_key=self.archive.put(doc))
                # Keep bounded metadata, not hundreds of XML bodies in memory.
                self.documents.append(
                    Document(url, b"", doc.sha256, doc.observed_at, len(body), doc.archive_key)
                )
                return doc
            except urllib.error.HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise SourceError("source_http_" + str(exc.code)) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == 2:
                    raise SourceError("source_transport_unavailable") from None
            if time.monotonic() + 2**attempt >= self.deadline:
                raise SourceError("collection_deadline_exceeded")
            self.sleep(2**attempt)
        raise SourceError("source_unavailable")

    def api(self, path, **params):
        return self.fetch(
            NSE
            + "/api/"
            + path
            + ("?" + urllib.parse.urlencode(params) if params else "")
        )


def rows_csv(body, required):
    reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")))
    if reader.fieldnames is None or len(reader.fieldnames) != len(
        set(c.strip() for c in reader.fieldnames)
    ):
        raise SourceError("csv_header_invalid")
    reader.fieldnames = [c.strip() for c in reader.fieldnames]
    if not set(required).issubset(reader.fieldnames):
        raise SourceError("csv_schema_changed")
    rows = []
    for i, row in enumerate(reader):
        if i >= 50000:
            raise SourceError("csv_row_budget_exceeded")
        rows.append({k: v.strip() if isinstance(v, str) else v for k, v in row.items()})
    if not rows or any(None in row for row in rows):
        raise SourceError("csv_empty_or_malformed")
    return rows


def json_rows(doc, *, allow_empty=False):
    payload = json.loads(doc.body)
    rows = (
        payload
        if isinstance(payload, list)
        else payload.get("data") if isinstance(payload, dict) else None
    )
    if (
        not isinstance(rows, list)
        or any(not isinstance(row, dict) for row in rows)
        or (not rows and not allow_empty)
    ):
        raise SourceError("json_schema_changed_or_empty")
    return rows


def number(value, *, minimum=0, integer=False):
    try:
        if len(str(value)) > 80:
            raise ValueError()
        n = Decimal(str(value).replace(",", ""))
        if n.is_finite() and n > Decimal("1e24"):
            raise ValueError()
        if not n.is_finite() or n < minimum or (integer and n != n.to_integral_value()):
            raise ValueError()
        return int(n) if integer else n
    except Exception:
        raise SourceError("invalid_numeric_value") from None


def parse_day(raw):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(raw), fmt).date()
        except ValueError:
            pass
    raise SourceError("invalid_source_date")


def parse_stamp(raw):
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(raw), fmt).replace(tzinfo=IST)
        except ValueError:
            pass
    try:
        stamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return (
            stamp.replace(tzinfo=IST) if stamp.tzinfo is None else stamp.astimezone(IST)
        )
    except ValueError:
        raise SourceError("invalid_source_timestamp") from None


def bhavcopy_url(day):
    return (
        ARCHIVE
        + "/content/cm/BhavCopy_NSE_CM_0_0_0_"
        + day.strftime("%Y%m%d")
        + "_F_0000.csv.zip"
    )


def parse_bhavcopy(body, day):
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = archive.infolist()
        expected = "BhavCopy_NSE_CM_0_0_0_" + day.strftime("%Y%m%d") + "_F_0000.csv"
        if (
            len(members) != 1
            or members[0].filename != expected
            or members[0].file_size > MAX_BYTES
        ):
            raise SourceError("bhavcopy_archive_invalid")
        data = archive.read(members[0])
    rows = rows_csv(
        data,
        [
            "TradDt",
            "TckrSymb",
            "SctySrs",
            "ISIN",
            "OpnPric",
            "HghPric",
            "LwPric",
            "ClsPric",
            "TtlTradgVol",
            "TtlTrfVal",
        ],
    )
    output = []
    seen = set()
    for row in rows:
        if parse_day(row["TradDt"]) != day:
            raise SourceError("bhavcopy_wrong_date")
        if row["SctySrs"] not in {"EQ", "BE", "BZ", "SM", "ST", "SZ"}:
            continue
        key = (row["TckrSymb"], row["SctySrs"])
        if key in seen:
            raise SourceError("bhavcopy_duplicate_identity")
        seen.add(key)
        prices = {
            k: number(row[c])
            for k, c in (
                ("open", "OpnPric"),
                ("high", "HghPric"),
                ("low", "LwPric"),
                ("close", "ClsPric"),
            )
        }
        volume = number(row["TtlTradgVol"], integer=True)
        # Non-traded rows are not manufactured into usable candles.
        if volume == 0 and not any(prices.values()):
            continue
        if (
            min(prices.values()) <= 0
            or prices["low"] > min(prices["open"], prices["close"])
            or prices["high"] < max(prices["open"], prices["close"])
        ):
            raise SourceError("bhavcopy_invalid_ohlc")
        output.append(
            dict(
                symbol=row["TckrSymb"],
                isin=row["ISIN"],
                series=row["SctySrs"],
                trade_date=day,
                volume=volume,
                turnover=number(row["TtlTrfVal"]),
                **prices,
            )
        )
    if not output:
        raise SourceError("bhavcopy_no_equities")
    return output


def parse_indices(body, day):
    rows = rows_csv(
        body,
        [
            "Index Name",
            "Index Date",
            "Open Index Value",
            "High Index Value",
            "Low Index Value",
            "Closing Index Value",
        ],
    )
    output = {}
    for r in rows:
        if r["Index Name"] not in BENCHMARKS:
            continue
        code = BENCHMARKS[r["Index Name"]]
        if code in output or parse_day(r["Index Date"]) != day:
            raise SourceError("benchmark_wrong_date_or_duplicate")
        values = {
            k: number(r[c], minimum=Decimal("0.00000001"))
            for k, c in (
                ("open", "Open Index Value"),
                ("high", "High Index Value"),
                ("low", "Low Index Value"),
                ("close", "Closing Index Value"),
            )
        }
        if values["high"] < max(values["open"], values["close"]) or values["low"] > min(
            values["open"], values["close"]
        ):
            raise SourceError("benchmark_invalid_ohlc")
        output[code] = dict(benchmark_code=code, trade_date=day, **values)
    if set(output) != set(BENCHMARKS.values()):
        raise SourceError("benchmark_missing_codes")
    return list(output.values())


def parse_master(body):
    rows = rows_csv(
        body, ["SYMBOL", "NAME OF COMPANY", "SERIES", "DATE OF LISTING", "ISIN NUMBER"]
    )
    result = {}
    identities = {}
    for row in rows:
        symbol, isin = row["SYMBOL"], row["ISIN NUMBER"]
        if (
            not symbol
            or not row["SERIES"]
            or len(isin) != 12
            or not isin.startswith("IN")
        ):
            raise SourceError("master_identity_invalid")
        parsed = dict(
            symbol=symbol,
            isin=isin,
            series=row["SERIES"],
            listed_from=parse_day(row["DATE OF LISTING"]),
            name=row["NAME OF COMPANY"],
        )
        if symbol in result:
            previous = result[symbol]
            series = previous.get('series_list', [previous['series']])
            # NSE may publish one equity identity in several trading series.
            # Preserve all series; never resolve conflicting identities by order.
            if (parsed['series'] in series or
                    identities[symbol] != {k: v for k, v in row.items() if k != 'SERIES'}):
                raise SourceError("master_identity_invalid")
            previous['series_list'] = sorted([*series, parsed['series']])
            previous['series'] = None
        else:
            result[symbol] = parsed
            identities[symbol] = {k: v for k, v in row.items() if k != 'SERIES'}
    return result


def cash_equity_master(rows, session_day):
    """Ordinary equities listed by the requested price session; retain exclusions."""
    eligible, excluded = {}, []
    for symbol, row in rows.items():
        reason = None
        if symbol.endswith('-RE') and row['name'].upper().endswith('-RE'):
            reason = 'RIGHTS_ENTITLEMENT_OUTSIDE_CASH_EQUITY_UNIVERSE'
        elif row['listed_from'] > session_day:
            reason = 'LISTING_NOT_YET_EFFECTIVE'
        if reason:
            excluded.append({**row, 'reason': reason})
        else:
            eligible[symbol] = row
    return eligible, excluded


def fetch_classification(http, symbol, isin):
    path = "NextApi/apiClient/GetQuoteApi"
    metadata_doc = http.api(path, functionName="getMetaData", symbol=symbol)
    metadata = json.loads(metadata_doc.body)
    if not isinstance(metadata, dict) or (
        metadata.get("symbol") and metadata["symbol"] != symbol
    ):
        raise SourceError("classification_metadata_symbol_mismatch")
    series = list(
        dict.fromkeys(
            (metadata.get("activeSeries") or [])
            + (metadata.get("tempSuspendedSeries") or [])
        )
    ) or ["EQ", "BE", "BZ", "SM", "ST"]
    for item in series:
        try:
            doc = http.api(
                path,
                functionName="getSymbolData",
                marketType=str(metadata.get("marketType") or "N"),
                series=item,
                symbol=symbol,
            )
        except SourceError as exc:
            if str(exc) == "source_http_404":
                continue
            raise
        rows = json.loads(doc.body).get("equityResponse", [])
        if not rows:
            continue
        row = rows[0] if isinstance(rows, list) and rows else {}
        quote = row.get("metaData") or {}
        sec = row.get("secInfo") or {}
        if not quote and not sec:
            # Some unavailable series return a populated response envelope with
            # null fields instead of 404. Continue to other official series.
            continue
        if quote.get("symbol") != symbol:
            raise SourceError("classification_identity_mismatch")
        identity_basis = 'DETAILED_QUOTE'
        identity_review = None
        if quote.get("isinCode") != isin:
            # Call-auction quotes can omit the ISIN. Only accept an absent value
            # when the separate official metadata binds this exact current ISIN,
            # symbol, series and market. An explicit conflicting ISIN still fails.
            if (quote.get('isinCode') not in {None, ''}
                    or metadata.get('symbol') != symbol
                    or quote.get('series') != item
                    or quote.get('marketType') != metadata.get('marketType')):
                raise SourceError("classification_identity_mismatch")
            identity_basis = 'METADATA_AND_QUOTE'
            if metadata.get('isin') != isin:
                from packages.data_layer.classification_identity import reviewed_classification_identity
                identity_review = reviewed_classification_identity(
                    http, metadata_doc, metadata, symbol, isin, item)
                identity_basis = 'REVIEWED_METADATA_LIVE_MASTER_AND_QUOTE'
        sector = sec.get("sector") or sec.get("macro")
        if isinstance(sector, str) and sector.upper() not in {
            "",
            "NA",
            "N/A",
            "-",
            "UNKNOWN",
        }:
            return doc, dict(
                sector=sector,
                industry=sec.get("industryInfo")
                or sec.get("industry")
                or sec.get("basicIndustry"),
                series=item,
                identity_basis=identity_basis,
                metadata_sha256=metadata_doc.sha256,
                identity_review=identity_review,
                is_suspended=str(sec.get("isSuspended")).lower()
                in {"true", "suspended"},
            )
    raise SourceError("classification_sector_unavailable")


def parse_surveillance(body):
    lt = "Long_Term_Additional_Surveillance_Measure (Long Term ASM)"
    st = "Short_Term_Additional_Surveillance_Measure (Short Term ASM)"
    rows = rows_csv(body, ["Symbol", "Status", "Series", "GSM", lt, st])
    result = {}
    for row in rows:
        symbol = row["Symbol"]
        if not symbol or symbol in result or row["Status"] not in {"A", "S"}:
            raise SourceError("surveillance_identity_invalid")
        # 100 means inactive; GSM zero is an active stage.
        gsm, long, short = [number(row[k], integer=True) for k in ("GSM", lt, st)]
        if (
            gsm not in {0, 1, 2, 3, 4, 5, 6, 100}
            or long not in {0, 1, 2, 3, 4, 100}
            or short not in {0, 1, 2, 100}
        ):
            raise SourceError("surveillance_unknown_indicator")
        result[symbol] = dict(
            status=row["Status"],
            series=row["Series"],
            GSM=None if gsm == 100 else "GSM" + str(gsm),
            ASM="+".join(
                (["LT" + str(long)] if long != 100 else [])
                + (["ST" + str(short)] if short != 100 else [])
            )
            or None,
        )
    return result


def paged_financials(http, start, end, *, max_pages=50):
    # New disclosures can shift offset pages while they are being read. Restart
    # the entire bounded snapshot; never combine pages from different versions.
    for attempt in range(3):
        try:
            return _paged_financials_once(http, start, end, max_pages=max_pages)
        except SourceError as exc:
            if attempt == 2 or str(exc) not in {
                'financial_pagination_changed',
                'financial_duplicate_or_ignored_date_filter',
                'financial_pagination_truncated',
            }:
                raise


def _paged_financials_once(http, start, end, *, max_pages=50):
    from packages.data_layer.exchange_financials import NSEIntegratedFinancialsProvider

    parser = object.__new__(NSEIntegratedFinancialsProvider)
    seen = set()
    records = []
    expected = None
    for page in range(1, max_pages + 1):
        doc = http.api(
            "integrated-filing-results",
            type="Integrated Filing- Financials",
            index="equities",
            from_date=start.strftime("%d-%m-%Y"),
            to_date=end.strftime("%d-%m-%Y"),
            page=page,
            size=100,
        )
        payload = json.loads(doc.body)
        if not isinstance(payload, dict) or not isinstance(
            payload.get("totalCount"), int
        ):
            raise SourceError("financial_pagination_contract_missing")
        if expected is None:
            expected = payload["totalCount"]
        if payload["totalCount"] != expected or payload.get("page") != page - 1:
            raise SourceError("financial_pagination_changed")
        rows = json_rows(doc, allow_empty=True)
        for row in rows:
            record = parser._parse_row(row)
            if record is None or not record.external_id or not record.xbrl_uri:
                raise SourceError("financial_record_invalid")
            key = (record.external_id, record.symbol)
            if key in seen or not start <= record.filed_at.date() <= end:
                raise SourceError("financial_duplicate_or_ignored_date_filter")
            seen.add(key)
            records.append(record)
        if len(records) == expected:
            return records
        if not rows or len(records) > expected:
            raise SourceError("financial_pagination_truncated")
    raise SourceError("financial_page_budget_exceeded")


def parse_shareholding(xml, record, *, expected_isin=None):
    if b"<!DOCTYPE" in xml.upper() or b"<!ENTITY" in xml.upper():
        raise SourceError("xbrl_entities_forbidden")
    root = ET.fromstring(xml)
    local = lambda s: s.rsplit("}", 1)[-1].split(":")[-1]
    period = parse_day(record["date"])
    identity_review = None
    if expected_isin is not None:
        from packages.data_layer.shareholding_identity import reviewed_identity
        record, expected_isin, identity_review = reviewed_identity(xml, record, expected_isin)
        if identity_review and identity_review.get('xml_period'):
            # Retain the actual XML context date, never backdate listing-day
            # share counts to the API summary's earlier quarter-end.
            period = parse_day(identity_review['xml_period'])
        # The API listing can retain a pre-split ISIN. The filing itself must
        # bind the current listing identity in a single period-matched header.
        headers = []
        for c in root:
            if local(c.tag) != "context" or any(
                local(e.tag) in {"explicitMember", "typedMember"} for e in c.iter()
            ):
                continue
            ends = [e.text for e in c.iter() if local(e.tag) in {"instant", "endDate"}]
            if ends != [period.isoformat()]:
                continue
            values = lambda name: {e.text for e in root if local(e.tag) == name
                                   and e.attrib.get("contextRef") == c.attrib.get("id")}
            if values("Symbol") == {record["symbol"]} and values("ISIN") == {expected_isin}:
                headers.append(c)
        if not headers:
            raise SourceError("shareholding_isin_mismatch")
        record = {**record, "isin": expected_isin}
    # Dual-listed issuers may use BSE's numeric ScripCode as the XBRL entity
    # identifier even though NSE publishes the document. Admit that identity only
    # when one undimensioned header context links the same code, NSE symbol and
    # listing ISIN for the requested reporting period.
    bse_scheme = "http://www.bseindia.com/in-bse-shp/ScripCode"
    linked_entities = set()
    for c in root:
        if local(c.tag) != "context":
            continue
        if any(local(e.tag) in {"explicitMember", "typedMember"} for e in c.iter()):
            continue
        ends = [e.text for e in c.iter() if local(e.tag) in {"instant", "endDate"}]
        identifiers = [(e.attrib.get("scheme", ""), e.text) for e in c.iter()
                       if local(e.tag) == "identifier"]
        if ends != [period.isoformat()] or len(identifiers) != 1:
            continue
        scheme, code = identifiers[0]
        if scheme != bse_scheme or not code or not re.fullmatch(r"[0-9]{6}", code):
            continue
        def header_values(name):
            return {e.text for e in root if local(e.tag) == name
                    and e.attrib.get("contextRef") == c.attrib.get("id")}
        if (record.get("isin") and header_values("Symbol") == {record["symbol"]}
                and header_values("ISIN") == {record["isin"]}
                and header_values("ScripCode") == {code}):
            linked_entities.add((scheme, code))
    if len(linked_entities) > 1:
        raise SourceError("shareholding_entity_ambiguous")
    contexts = {}
    for c in root:
        if local(c.tag) != "context":
            continue
        instants = [e.text for e in c.iter() if local(e.tag) == "instant"]
        if instants != [period.isoformat()]:
            continue
        members = [
            (local(e.attrib.get("dimension", "")), local(e.text or ""))
            for e in c.iter()
            if local(e.tag) in {"explicitMember", "typedMember"}
        ]
        identifiers = [(e.attrib.get("scheme", ""), e.text) for e in c.iter()
                       if local(e.tag) == "identifier"]
        if len(identifiers) != 1 or not (
            (identifiers[0][1] == record["symbol"] and identifiers[0][0] != bse_scheme)
            or identifiers[0] in linked_entities
        ):
            continue
        if c.attrib["id"] in contexts:
            raise SourceError("shareholding_duplicate_context")
        contexts[c.attrib["id"]] = members

    def fact(name, members, *, optional=False):
        values = [
            e.text
            for e in root
            if local(e.tag) == name
            and e.attrib.get("contextRef") in contexts
            and contexts[e.attrib["contextRef"]] == members
            and e.text is not None
        ]
        if not values and optional:
            return None
        if len(set(values)) != 1:
            raise SourceError("shareholding_fact_missing_or_ambiguous")
        return values[0]

    promoter = [
        ("CategoryOfShareholdersAxis", "ShareholdingOfPromoterAndPromoterGroupMember")
    ]
    total = [("CategoryOfShareholdersAxis", "ShareholdingPatternMember")]
    shares = number(fact("NumberOfShares", total), integer=True, minimum=1)
    reported_shares = shares
    paid_equity_shares = shares
    drs = fact("NumberOfSharesUnderlyingOutstandingDepositoryReceipts", total, optional=True)
    if drs is not None and number(drs, integer=True):
        # The percentage table can exclude DR categories. Its denominator is
        # separate from the reported total equity shares (including underlying
        # DR shares); do not shrink shares_outstanding to fit a percentage.
        paid = number(fact("NumberOfFullyPaidUpEquityShares", total), integer=True)
        partly = fact("NumberOfPartlyPaidUpEquityShares", total, optional=True)
        equity = paid + (number(partly, integer=True) if partly is not None else 0)
        if equity <= 0 or equity + number(drs, integer=True) != shares:
            raise SourceError("shareholding_totals_invalid")
        paid_equity_shares = equity
    public = fact("NumberOfShares", [
        ("CategoryOfShareholdersAxis", "PublicShareholdingMember")], optional=True)
    nonpublic = fact("NumberOfShares", [
        ("CategoryOfShareholdersAxis", "SharesHeldByNonPromoterNonPublicShareholdersMember")],
        optional=True)
    nonpublic_count = number(nonpublic, integer=True) if nonpublic is not None else 0
    promoter_count = fact("NumberOfShares", promoter, optional=True)
    if promoter_count is None:
        # Professionally managed issuers can omit the empty promoter section.
        # Require an explicit zero summary and a full reconciliation of the
        # non-overlapping public/non-public categories before deriving zero.
        if (number(record["pr_and_prgrp"]) != 0 or public is None
                or number(public, integer=True) + nonpublic_count != reported_shares):
            raise SourceError("shareholding_promoter_zero_unverified")
        promoter_shares = Decimal(0)
    else:
        promoter_shares = number(promoter_count, integer=True)
    if promoter_shares > shares:
        raise SourceError("shareholding_totals_invalid")
    denominator = paid_equity_shares
    if public is not None:
        if promoter_shares + number(public, integer=True) + nonpublic_count != reported_shares:
            raise SourceError("shareholding_totals_invalid")
    declared = fact('ShareholdingAsAPercentageOfTotalNumberOfShares', promoter, optional=True)
    if declared is not None:
        declared_pct = Decimal(100) * number(declared)
        if declared_pct > 100:
            raise SourceError('shareholding_xml_percentage_mismatch')
        # Exchange taxonomies report percentages on different table bases.
        # Resolve the base from the XML's own percentage and reconciled category
        # totals, never by selecting a base that happens to fit the API summary.
        bases = {paid_equity_shares, reported_shares}
        if public is not None:
            bases.add(promoter_shares + number(public, integer=True))
            nonpublic_drs = fact('NumberOfSharesUnderlyingOutstandingDepositoryReceipts', [
                ('CategoryOfShareholdersAxis', 'SharesHeldByNonPromoterNonPublicShareholdersMember')],
                optional=True)
            if nonpublic_drs is not None and number(nonpublic_drs, integer=True):
                dr_count = number(nonpublic_drs, integer=True)
                paid = number(fact('NumberOfFullyPaidUpEquityShares', [
                    ('CategoryOfShareholdersAxis', 'SharesHeldByNonPromoterNonPublicShareholdersMember')]), integer=True)
                partly = fact('NumberOfPartlyPaidUpEquityShares', [
                    ('CategoryOfShareholdersAxis', 'SharesHeldByNonPromoterNonPublicShareholdersMember')], optional=True)
                if (paid + (number(partly, integer=True) if partly is not None else 0)
                        + dr_count != nonpublic_count
                        or drs is None or dr_count > number(drs, integer=True)):
                    raise SourceError('shareholding_totals_invalid')
                # A+B+C2: exclude non-public DRs (C1), retain employee trusts
                # (C2). Require both category and total counts to reconcile.
                bases.add(reported_shares - dr_count)
        bases.discard(0)
        denominator = min(bases, key=lambda n: abs(Decimal(100) * promoter_shares / n - declared_pct))
        rounding = min(Decimal('.1'), max(Decimal('.005'),
            Decimal(50).scaleb(Decimal(declared).as_tuple().exponent)))
        if abs(Decimal(100) * promoter_shares / denominator - declared_pct) > rounding:
            raise SourceError('shareholding_xml_percentage_mismatch')
    if denominator <= 0:
        raise SourceError("shareholding_totals_invalid")
    percentage = Decimal(100) * promoter_shares / denominator
    # Listing percentage is rounded; compare within 0.1 percentage point.
    if abs(percentage - number(record["pr_and_prgrp"])) > Decimal("0.1"):
        raise SourceError("shareholding_percentage_mismatch")
    pledged = fact(
        "NumberOfSharesPledgedOrOtherwiseEncumbered", promoter, optional=True
    )
    encumbered = fact("NumberOfSharesEncumbered", promoter, optional=True)
    if pledged is not None and encumbered is not None and number(pledged) != number(encumbered):
        raise SourceError("shareholding_pledge_ambiguous")
    if pledged is None:
        pledged = encumbered
    if pledged is not None:
        pledge_count = number(pledged, integer=True)
        if pledge_count > promoter_shares or (not promoter_shares and pledge_count):
            raise SourceError("shareholding_pledge_invalid")
        pledge = (
            Decimal(100) * pledge_count / promoter_shares
            if promoter_shares
            else Decimal(0)
        )
    elif fact(
        "WhetherAnySharesHeldByPromotersAreEncumberedUnderPledged", [], optional=True
    ) in {"false", "0"}:
        pledge = Decimal(0)
    else:
        raise SourceError("promoter_pledge_unverified")
    return dict(
        period_end=period,
        filed_at=parse_stamp(record["broadcastDate"]),
        promoter_holding_pct=percentage,
        promoter_pledge_pct=pledge,
        total_shares=shares,
        percentage_reconciliation={
            'reported_total_shares': reported_shares,
            'paid_equity_shares': paid_equity_shares,
            'promoter_shares': promoter_shares,
            'percentage_denominator': denominator,
            'declared_ratio': declared},
        identity_review=identity_review,
        revision_number=1 if record.get("revisedData") == "Y" else 0,
    )
