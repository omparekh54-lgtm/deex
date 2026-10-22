"""Unattended official collection. Collection success never grants PAPER permission."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, date, timedelta
from uuid import uuid4
from dataclasses import replace
from sqlalchemy import text

from packages.data_layer.cloud_sources import (
    ARCHIVE,
    NSE,
    IST,
    OfficialHTTP,
    SourceError,
    bhavcopy_url,
    parse_bhavcopy,
    parse_indices,
    parse_master,
    cash_equity_master,
    parse_surveillance,
    rows_csv,
    json_rows,
    parse_day,
    parse_stamp,
    parse_shareholding,
    paged_financials,
)
from packages.data_layer.cloud_storage import CloudStore, encoded
from packages.data_layer.price_observations import (
    reviewed_suspensions, security_report_url, parse_security_report,
)
from packages.pms.market_calendar import parse_holiday_calendar

JOB = "cloud_collection"
GATE_SQL = """select status,started_at,finished_at,details from pms.worker_runs
where job_name='cloud_collection' and details->>'trade_date'=:day
order by started_at desc,id desc limit 1"""


def collection_gate(conn, day, now):
    r = conn.execute(text(GATE_SQL), {"day": day.isoformat()}).mappings().first()
    if not r or r["status"] != "SUCCESS" or r["finished_at"] is None:
        return False
    stamp = r["finished_at"]
    if stamp.tzinfo is None:
        return False
    details = r["details"] or {}
    return (
        timedelta(0) <= now - stamp <= timedelta(hours=2)
        and details.get("version") == "CLOUD_COLLECTION_v1"
        and details.get("complete") is True
        and details.get("trade_date") == day.isoformat()
        and isinstance(details.get("raw_archive"), dict)
        and details["raw_archive"].get("status") == "VERIFIED"
        and details["raw_archive"].get("anonymous_read_denied") is True
    )


def record_key(raw):
    return hashlib.sha256(
        json.dumps(raw, sort_keys=True, default=str).encode()
    ).hexdigest()


def parse_meetings(doc, now):
    # Announcements can arrive while earlier collection stages are running.
    # Bound knowability by this document's receipt, never a later wall clock.
    if now.tzinfo is None or doc.observed_at.tzinfo is None:
        raise SourceError("earnings_observation_timezone_required")
    cutoff = doc.observed_at
    output = []
    for r in json_rows(doc, allow_empty=True):
        subject = str(r.get("bm_purpose", "")) + " " + str(r.get("bm_desc", ""))
        if not re.search(r"financial|results", subject, re.I):
            continue
        changed = bool(re.search(r"cancel|postpon|reschedul", subject, re.I))
        symbol = r.get("bm_symbol")
        stamp = parse_stamp(r.get("bm_timestamp"))
        event = parse_day(r.get("bm_date"))
        if not symbol or stamp > cutoff:
            raise SourceError("earnings_identity_or_timestamp_invalid")
        # Keep earlier blackouts; a cancellation never silently erases prior evidence.
        output.append(
            dict(
                symbol=symbol,
                event_date=event,
                announced_at=stamp,
                raw={
                    **r,
                    "change_notice": changed,
                    "blackout_policy": "CONSERVATIVE_RETAIN",
                },
            )
        )
    return output


def parse_actions(doc, now):
    from packages.data_layer.reference_gap_ingestion import (
        ReferenceGapIngestor as Parser,
    )

    output = []
    for r in json_rows(doc, allow_empty=True):
        if not r.get("symbol") or not r.get("isin") or not r.get("subject"):
            raise SourceError("action_identity_invalid")
        subject = r["subject"]
        kind = Parser._action_type(subject)
        rf, rt = Parser._ratio(subject, kind)
        if kind in {"SPLIT", "BONUS"} and (not rf or not rt or rf <= 0 or rt <= 0):
            raise SourceError("action_ratio_unverified")
        cash = None
        if kind == "DIVIDEND":
            m = re.search(r"\b(?:RS|RE|INR)\.?\s*(\d+(?:\.\d+)?)", subject.upper())
            cash = float(m.group(1)) if m else None
        # Do not invent historical announcement times from an ex-date.
        announcement = (
            parse_stamp(r["caBroadcastDate"]) if r.get("caBroadcastDate") else now
        )
        if announcement > now:
            raise SourceError("action_announcement_in_future")
        output.append(
            dict(
                symbol=r["symbol"],
                isin=r["isin"],
                action_type=kind,
                ex_date=parse_day(r["exDate"]),
                record_date=(
                    None
                    if r.get("recDate") in (None, "", "-")
                    else parse_day(r["recDate"])
                ),
                ratio_from=rf,
                ratio_to=rt,
                cash_amount=cash,
                announced_at=announcement,
                raw=r,
            )
        )
    return output


class CloudCollector:
    def __init__(self, store=None, http=None, *, now=None, max_filings=200,
                 catch_up_date=None, max_classifications=100):
        if not 1 <= max_filings <= 500:
            raise ValueError("max_filings must be 1..500")
        self.store = store
        self.http = http or OfficialHTTP()
        self.now = now or datetime.now(IST)
        self.max_filings = max_filings
        self.catch_up_date = catch_up_date
        if not 1 <= max_classifications <= 500:
            raise ValueError("max_classifications must be 1..500")
        self.max_classifications = max_classifications

    def core(self, day, *, apply=False):
        http = self.http
        calendar_doc = http.api("holiday-master", type="trading")
        calendar = parse_holiday_calendar(json.loads(calendar_doc.body))
        if str(day.year) not in calendar["holiday_years"]:
            raise SourceError("calendar_year_missing")
        if apply:
            for snapshot in self.store.calendar_history():
                old = snapshot["metadata"] or {}
                for year, holidays in old.get("holiday_years", {}).items():
                    calendar["holiday_years"].setdefault(year, holidays)
                calendar.setdefault("special_sessions", []).extend(
                    old.get("special_sessions", [])
                )
        special = set(calendar.get("special_sessions", []))
        if (
            day.isoformat() in calendar.get("unverified_special_session_dates", [])
            and day.isoformat() not in special
        ):
            raise SourceError("special_session_requires_verified_circular")
        if apply:
            self.store.calendar(calendar, calendar_doc)
        if day.isoformat() not in special and (
            day.weekday() >= 5
            or day.isoformat() in calendar["holiday_years"][str(day.year)]
        ):
            # A real special weekend requires explicit sourced configuration, never guessing.
            return {"status": "SKIPPED", "reason": "exchange_non_session", "stages": {}}
        if apply:
            historical = self.catch_up_date is not None
            if historical:
                if day != self.catch_up_date or not 1 <= (self.now.date() - day).days <= 7:
                    raise SourceError("past_completed_session_required")
            elif day != self.now.date() or (self.now.hour, self.now.minute) < (15, 30):
                raise SourceError("current_completed_session_required")
        master_doc = http.fetch(ARCHIVE + "/content/equities/EQUITY_L.csv")
        raw_master = parse_master(master_doc.body)
        master, excluded = cash_equity_master(raw_master, day)
        stages = {}
        self.core_stages = stages
        coverage_complete = True
        if apply:
            stages["master"] = self.store.master(master, master_doc)
        else:
            stages["master"] = {"source_rows": len(master)}
        stages['master'].update(raw_source_rows=len(raw_master),
                                excluded_from_cash_equity=excluded)
        # Archives validate every requested date. No previous-session fallback labelled as today.
        dates = (
            [day] if not apply else [day - timedelta(days=n) for n in range(6, -1, -1)]
        )
        for d in dates:
            if str(d.year) not in calendar["holiday_years"]:
                raise SourceError("lookback_calendar_year_unavailable")
            if d.isoformat() not in special and (
                d.weekday() >= 5
                or d.isoformat() in calendar["holiday_years"][str(d.year)]
            ):
                continue
            docs = [
                http.fetch(
                    ARCHIVE + "/content/cm/" + kind + d.strftime("%d%m%y") + ".csv"
                )
                for kind in ("REG_IND", "REG1_IND")
            ]
            states = [parse_surveillance(doc.body) for doc in docs]
            if states[0] != states[1]:
                raise SourceError("surveillance_archives_disagree")
            states = states[0]
            price_doc = http.fetch(bhavcopy_url(d))
            prices = parse_bhavcopy(price_doc.body, d)
            index_doc = http.fetch(
                ARCHIVE
                + "/content/indices/ind_close_all_"
                + d.strftime("%d%m%Y")
                + ".csv"
            )
            indices = parse_indices(index_doc.body, d)
            if apply:
                self.store.benchmarks(indices, index_doc)
                states, aliases = self.store.session_surveillance(prices, states, d)
                if aliases:
                    stages['identity_aliases_' + d.isoformat()] = aliases
                secondary_doc = http.fetch(security_report_url(d))
                secondary = parse_security_report(secondary_doc.body, d)
                suspensions, suspension_docs = reviewed_suspensions(
                    http, d, self.store.instruments())
                coverage = self.store.prices(prices, price_doc, d, states,
                    secondary=secondary, secondary_doc=secondary_doc,
                    suspensions=suspensions, suspension_docs=suspension_docs)
                stages["prices_" + d.isoformat()] = coverage
                if not coverage["complete"]:
                    coverage_complete = False
                if d == day:
                    if coverage['uncovered_symbols']:
                        stages['surveillance'] = dict(status='BLOCKED',
                            uncovered_symbols=coverage['uncovered_symbols'])
                    else:
                        stages["surveillance"] = self.store.surveillance(states, docs, d)
            else:
                stages["prices"] = {"source_rows": len(prices), "date": str(d)}
                stages["benchmarks"] = {
                    "codes": sorted(r["benchmark_code"] for r in indices),
                    "date": str(d),
                }
                stages["surveillance"] = {"source_rows": len(states)}
        return {"status": "SUCCESS" if coverage_complete else "PARTIAL", "stages": stages}

    def references(self, day, *, apply=False):
        stages = {}
        self.reference_stages = stages
        now = datetime.now(IST) if apply else self.now
        known = self.store.instruments() if apply else None
        # Validate the entire cheap identity snapshot before spending the filing
        # budget. Retain and use this exact document later in the same pass.
        membership_doc = self.http.fetch(
            "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv")
        members = rows_csv(membership_doc.body,
            ["Company Name", "Industry", "Symbol", "Series", "ISIN Code"])
        if len({r['Symbol'] for r in members}) < 490:
            raise SourceError('index_snapshot_incomplete')
        if apply:
            from packages.data_layer.index_membership import equity_membership
            equity_membership(members, known)
        from packages.data_layer.exchange_financials import FINANCIAL_ACK_DATASET, filings_from_exchange_record

        # Versioned receipts reparse queued disclosures accepted by older parsers.
        # Lookback recovers overnight/recent revisions; historical backfill remains separate.
        records = paged_financials(
            self.http,
            self.store.financial_window(day) if apply else day - timedelta(days=7),
            day,
        )
        if apply:
            records = [r for r in records if r.symbol in known]
            pending_raw = self.store.queue_records(
                "CLOUD_FINANCIAL_PENDING",
                FINANCIAL_ACK_DATASET,
                [r.raw_payload for r in records],
            )
            self.store.financial_discovered(day)
            from packages.data_layer.exchange_financials import (
                NSEIntegratedFinancialsProvider,
            )

            parser = object.__new__(NSEIntegratedFinancialsProvider)
            records = [parser._parse_row(r) for r in pending_raw]
            if any(r is None for r in records):
                raise SourceError("persisted_financial_record_invalid")
        acked = self.store.acked(FINANCIAL_ACK_DATASET) if apply else set()
        pending = [r for r in records if record_key(r.raw_payload) not in acked]
        written = 0
        errors = []
        for r in pending[: self.max_filings if apply else 1]:
            source_key=record_key(r.raw_payload)
            try:
                from packages.pms.module_a_context import as_ist

                r = replace(
                    r,
                    **{
                        k: as_ist(getattr(r, k)) if getattr(r, k) else None
                        for k in (
                            "filed_at",
                            "revised_at",
                            "exchange_received_at",
                            "exchange_disseminated_at",
                        )
                    },
                )
                if r.filed_at > now:
                    raise SourceError("financial_filing_in_future")
                from packages.data_layer.nse_financial_render import financial_document
                r,doc = financial_document(self.http,r,
                    expected_isin=known[r.symbol]['isin'] if apply else None)
                rendered=(r.raw_payload.get('financial_source') or {}).get('format')=='NSE_RENDERED_TABLES'
                if b"<!ENTITY" in doc.body.upper() or (not rendered and b"<!DOCTYPE" in doc.body.upper()):
                    raise SourceError("xbrl_entities_forbidden")
                filings = filings_from_exchange_record(r, doc.body)
                filing = filings[0]
                if filing.sales is None or filing.pat is None:
                    raise SourceError("financial_core_facts_unavailable")
                if apply:
                    self.store.financial(filings)
                    self.store.ack(
                        FINANCIAL_ACK_DATASET, doc, source_key
                    )
                written += 1
            except Exception as exc:
                if apply:
                    self.store.pending_failed('CLOUD_FINANCIAL_PENDING', source_key)
                errors.append(
                    {
                        "symbol": r.symbol,
                        "error_type": type(exc).__name__,
                        "code": (
                            str(exc)
                            if isinstance(exc, SourceError)
                            else "financial_xml_unavailable"
                        ),
                    }
                )
        remaining = len(pending) - written
        stages["financials"] = {
            "discovered": len(records),
            "processed": written,
            "remaining": remaining if apply else None,
            "errors": errors[:20],
        }
        shp_doc = self.http.api("corporate-share-holdings-master", index="equities")
        shp = json_rows(shp_doc)
        if apply:
            shp = [r for r in shp if r.get("symbol") in known]
            shp = self.store.queue_records(
                "CLOUD_SHAREHOLDING_PENDING", "CLOUD_SHAREHOLDING_XML", shp
            )
        # Each latest official record is checkpointed only after fully parsed facts commit.
        acked = self.store.acked("CLOUD_SHAREHOLDING_XML") if apply else set()
        pending = [r for r in shp if record_key(r) not in acked]
        written = 0
        shp_errors = []
        for r in pending[: self.max_filings if apply else 1]:
            try:
                if parse_stamp(r.get("broadcastDate")) > now:
                    raise SourceError("shareholding_filing_in_future")
                doc = self.http.fetch(r["xbrl"])
                from packages.data_layer.dated_identity_repair import shareholding_expected_isin
                values = parse_shareholding(doc.body, r,
                    expected_isin=shareholding_expected_isin(known[r["symbol"]], r) if apply else None)
                if apply:
                    self.store.shareholding(r["symbol"], values, r, doc)
                    self.store.ack("CLOUD_SHAREHOLDING_XML", doc, record_key(r))
                written += 1
            except Exception as exc:
                if apply:
                    self.store.pending_failed('CLOUD_SHAREHOLDING_PENDING', record_key(r))
                shp_errors.append(
                    {
                        "symbol": r.get("symbol"),
                        "error_type": type(exc).__name__,
                        "code": (
                            str(exc)
                            if isinstance(exc, SourceError)
                            else "shareholding_xml_unavailable"
                        ),
                    }
                )
        stages["shareholding"] = {
            "discovered": len(shp),
            "processed": written,
            "remaining": len(pending) - written if apply else None,
            "errors": shp_errors[:20],
        }
        start = (day - timedelta(days=7)).strftime("%d-%m-%Y")
        end = (day + timedelta(days=45)).strftime("%d-%m-%Y")
        doc = self.http.api(
            "corporate-board-meetings", index="equities", from_date=start, to_date=end
        )
        meetings = parse_meetings(doc, now)
        stages["earnings"] = (
            self.store.events("CLOUD_EARNINGS", meetings, doc)
            if apply
            else {"source_rows": len(meetings)}
        )
        doc = self.http.api(
            "corporates-corporateActions",
            index="equities",
            from_date=start,
            to_date=end,
        )
        actions = parse_actions(doc, now)
        if apply:
            from packages.data_layer.action_identity import validate_action_identities
            stages['action_identity_reviews'] = validate_action_identities(actions, known)
        stages["corporate_actions"] = (
            self.store.events("CLOUD_ACTIONS", actions, doc)
            if apply
            else {"source_rows": len(actions)}
        )
        # Current constituent snapshots cannot be retroactively asserted as history.
        doc = membership_doc
        if apply:
            stages["index_membership"] = self.store.membership(members, doc, day)
        else:
            stages["index_membership"] = {"source_rows": len(members)}
        if not apply:
            # Probe classification connectivity separately from full-universe refresh.
            from packages.data_layer.cloud_sources import fetch_classification

            fetch_classification(
                self.http, members[0]["Symbol"], members[0]["ISIN Code"]
            )
            stages["classifications"] = {"sampled": 1, "full_universe_verified": False}
        return {
            "status": (
                "PARTIAL"
                if apply
                and (remaining or stages["shareholding"]["remaining"])
                or errors
                or shp_errors
                else "SUCCESS"
            ),
            "stages": stages,
        }

    def collect(self, day, *, apply=False):
        result = self.core(day, apply=apply)
        if result["status"] == "SKIPPED":
            return result
        # Live reference snapshots are observed today even when the price archives
        # are being caught up. Never backdate current membership/classification.
        reference_day = self.now.date() if apply and self.catch_up_date else day
        ref = self.references(reference_day, apply=apply)
        result["stages"].update(ref["stages"])
        result["status"] = 'SUCCESS' if result['status'] == ref['status'] == 'SUCCESS' else 'PARTIAL'
        if apply:
            result["stages"]["classifications"] = self.store.classifications(
                self.http, reference_day, limit=self.max_classifications,
                **({'listed_through': day} if self.catch_up_date else {})
            )
            if result["stages"]["classifications"]["remaining"]:
                result["status"] = "PARTIAL"
        return result


def run_collection(engine, *, now=None, max_filings=200, http=None, require_archive=False,
                   catch_up_date=None, max_classifications=100):
    if require_archive and (http is None or getattr(http, "archive", None) is None):
        raise SourceError("private_archive_required")
    now = (now or datetime.now(IST)).astimezone(IST)
    run_id = str(uuid4())
    if catch_up_date is not None and not 1 <= (now.date() - catch_up_date).days <= 7:
        raise SourceError("past_completed_session_required")
    day = catch_up_date or now.date()
    report = dict(
        version="CLOUD_COLLECTION_v1",
        run_id=run_id,
        trade_date=day.isoformat(),
        started_at=now.isoformat(),
        status="RUNNING",
        complete=False,
        campaign_started=False,
        stages={},
    )
    if catch_up_date is not None:
        report.update(mode="INITIAL_CATCH_UP", reference_observation_date=str(now.date()))
    # Session lock covers all stage commits and all invokers, including manual runs.
    with engine.connect() as guard:
        acquired = guard.execute(
            text("select pg_try_advisory_lock(hashtext('pms_cloud_collection'))")
        ).scalar_one()
        guard.commit()
        if not acquired:
            return {**report, "status": "BUSY"}
        try:
            with engine.begin() as c:
                c.execute(
                    text(
                        """insert into pms.worker_runs(id,worker_name,job_name,status,started_at,details)
                values(:id,'cloud_collector','cloud_collection','RUNNING',:started,cast(:details as jsonb))"""
                    ),
                    {"id": run_id, "started": now, "details": encoded(report)},
                )
            collector = CloudCollector(
                CloudStore(engine), http, now=now, max_filings=max_filings,
                catch_up_date=catch_up_date, max_classifications=max_classifications
            )
            try:
                collector.store.resolve_tables()
                outcome = collector.collect(day, apply=True)
                report.update(outcome)
                report["complete"] = report["status"] == "SUCCESS"
            except Exception as exc:
                report.update(
                    status="FAILED",
                    stages={**getattr(collector, 'core_stages', {}),
                            **getattr(collector, 'reference_stages', {})},
                    error_type=type(exc).__name__,
                    error_code=(
                        str(exc)
                        if isinstance(exc, SourceError)
                        else "collection_exception"
                    ),
                )
            report["documents"] = [
                dict(
                    url=d.url,
                    sha256=d.sha256,
                    observed_at=d.observed_at.isoformat(),
                    bytes=d.size,
                    archive_key=d.archive_key,
                )
                for d in collector.http.documents
            ]
            report["finished_at"] = datetime.now(IST).isoformat()
            archive = getattr(collector.http, "archive", None)
            report["raw_archive"] = archive.summary() if archive is not None else {"status": "NOT_CONFIGURED"}
            if require_archive and (report["raw_archive"]["status"] != "VERIFIED" or
                                    any(not d.archive_key for d in collector.http.documents)):
                report.update(status="FAILED", complete=False, error_code="private_archive_required")
            with engine.begin() as c:
                c.execute(
                    text(
                        """update pms.worker_runs set status=:status,finished_at=:finished,details=cast(:details as jsonb) where id=:id"""
                    ),
                    {
                        "id": run_id,
                        "status": report["status"],
                        "finished": datetime.now(IST),
                        "details": encoded(report),
                    },
                )
            return report
        finally:
            guard.execute(
                text("select pg_advisory_unlock(hashtext('pms_cloud_collection'))")
            )
            guard.commit()
