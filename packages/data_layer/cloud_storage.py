"""Idempotent cloud ingestion with source receipts; never modifies execution permissions."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from sqlalchemy import text
from packages.data_layer.cloud_sources import IST, SourceError

MASTER_SQL = """select id,nse_symbol symbol,isin,listed_from,sector,metadata from pms.instruments
where exchange='NSE' and active=true order by id"""
RECEIPT_SQL = """insert into pms.provider_snapshots(provider,dataset,content_sha256,source_uri,metadata)
values('NSE',:dataset,:sha,:uri,cast(:metadata as jsonb))"""
PRICE_SQL = """insert into pms.daily_prices(instrument_id,trade_date,open,high,low,close,volume,turnover,source)
values(:id,:trade_date,:open,:high,:low,:close,:volume,:turnover,'NSE_UDIFF')
on conflict(instrument_id,trade_date,source) do update set open=excluded.open,high=excluded.high,
low=excluded.low,close=excluded.close,volume=excluded.volume,turnover=excluded.turnover,ingested_at=now()"""
INDEX_SQL = """insert into pms.module_e_benchmark_bars(benchmark_code,trade_date,open,high,low,close,source)
values(:benchmark_code,:trade_date,:open,:high,:low,:close,'NSE_DAILY_INDEX')
on conflict(benchmark_code,trade_date,source) do update set open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close"""

WRITABLE_TARGETS = {
    "daily_prices": "cloud_daily_prices",
    "financial_filings": "financial_filings_local",
    "financial_facts": "financial_facts_local",
    "earnings_calendar": "earnings_calendar_local",
    "shares_outstanding_history": "shares_outstanding_history_local",
    "index_constituents_history": "index_constituents_history_local",
}
LAYOUT_SQL = """select c.relname,c.relkind,case when c.relkind='v' then pg_get_viewdef(c.oid,true) else null end definition
from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='pms'
and c.relname in ('daily_prices','cloud_daily_prices','financial_filings','financial_filings_local',
'financial_facts','financial_facts_local','earnings_calendar','earnings_calendar_local',
'shares_outstanding_history','shares_outstanding_history_local','index_constituents_history','index_constituents_history_local')"""


def encoded(value):
    return json.dumps(value, default=str, allow_nan=False)


def reviewed_price_isin_matches(instrument, isin, day):
    if instrument['isin'] == isin:
        for review in (instrument.get('metadata') or {}).get('reviewed_isin_history', []):
            if (isinstance(review, dict) and review.get('reviewed') is True
                    and review.get('current_isin') == isin and 'current_effective_from' in review):
                try:
                    if day < date.fromisoformat(review['current_effective_from']):
                        return False
                except (ValueError, TypeError):
                    return False
        return True
    # Only explicitly reviewed same-instrument history, bounded to already
    # completed archives. Unknown changes still fail the master identity gate.
    history = (instrument.get('metadata') or {}).get('reviewed_isin_history', [])
    if not isinstance(history, list):
        return False
    for review in history:
        if not isinstance(review, dict) or review.get('reviewed') is not True:
            continue
        if review.get('current_isin') != instrument['isin'] or review.get('previous_isin') != isin:
            continue
        if not all(re.fullmatch(r'[0-9a-f]{64}', review.get(k, ''))
                   for k in ['quote_sha256', 'master_sha256']):
            continue
        try:
            through = date.fromisoformat(review['allow_previous_through'])
            observed = date.fromisoformat(review['observed_on'])
        except (KeyError, ValueError, TypeError):
            continue
        if day <= through < observed:
            return True
    return False


class CloudStore:
    def __init__(self, engine):
        self.engine = engine
        self._known = None
        self._tables = None

    def resolve_tables(self):
        if self._tables is not None:
            return self._tables
        with self.engine.connect() as c:
            catalog = {
                r["relname"]: dict(r)
                for r in c.execute(text(LAYOUT_SQL)).mappings().all()
            }
        tables = {}
        for logical, local in WRITABLE_TARGETS.items():
            kind = catalog.get(logical, {}).get("relkind")
            if kind in {"r", "p"}:
                tables[logical] = logical
            elif (
                kind == "v"
                and catalog.get(local, {}).get("relkind") in {"r", "p"}
                and local in (catalog[logical]["definition"] or "")
            ):
                tables[logical] = local
            else:
                raise SourceError("writable_schema_requires_reviewed_migration_010")
        self._tables = tables
        return tables

    def execute(self, conn, statement, params=None):
        sql = str(statement)
        if sql.lstrip().lower().startswith(("insert", "update", "delete")):
            for logical, target in self.resolve_tables().items():
                sql = re.sub(r"\b" + logical + r"\b", target, sql)
        return conn.execute(text(sql), params or {})

    def instruments(self):
        if self._known is not None:
            return self._known
        with self.engine.connect() as c:
            rows = [dict(r) for r in c.execute(text(MASTER_SQL)).mappings().all()]
        by_symbol = {}
        for r in rows:
            r["canonical_symbol"] = r["symbol"]
            classification = (r.get("metadata") or {}).get("sector_classification", {})
            alias = classification.get("symbol_alias") or {}
            # Reuse only the exchange-circular alias already verified during backfill.
            if (
                classification.get("quote_symbol") == alias.get("symbol")
                and alias.get("source_url", "").startswith(
                    "https://nsearchives.nseindia.com/content/circulars/"
                )
                and alias.get("effective_from", "9999")
                <= datetime.now(IST).date().isoformat()
            ):
                r["symbol"] = alias["symbol"]
            if not r["symbol"] or r["symbol"] in by_symbol:
                raise SourceError("ambiguous_database_identity")
            by_symbol[r["symbol"]] = r
        if not by_symbol:
            raise SourceError("database_universe_empty")
        self._known = by_symbol
        return by_symbol

    def receipt(self, c, dataset, docs, metadata):
        for doc in docs:
            self.execute(
                c,
                text(RECEIPT_SQL),
                dict(
                    dataset=dataset,
                    sha=doc.sha256,
                    uri=doc.url,
                    metadata=encoded(
                        {**metadata, "fetched_at": doc.observed_at.isoformat(),
                         "archive_key": doc.archive_key}
                    ),
                ),
            )

    def calendar(self, metadata, doc):
        with self.engine.begin() as c:
            self.receipt(c, "NSE_TRADING_CALENDAR", [doc], metadata)

    def master(self, rows, doc):
        known = self.instruments()
        # An ISIN change may be a split or identity event. Never overwrite history blindly.
        conflicts = [
            s
            for s, r in rows.items()
            if s in known and known[s]["isin"] and known[s]["isin"] != r["isin"]
        ]
        if conflicts:
            raise SourceError("master_isin_change_requires_review")
        if any(s not in known and any(i['isin'] == r['isin'] for i in known.values())
               for s,r in rows.items()):
            raise SourceError('master_symbol_change_requires_review')
        with self.engine.begin() as c:
            for symbol, r in rows.items():
                if symbol in known:
                    if not known[symbol]["listed_from"] or not known[symbol]["isin"]:
                        self.execute(
                            c,
                            text(
                                """update pms.instruments set listed_from=coalesce(listed_from,:listed_from),isin=coalesce(isin,:isin)
                        where id=:id"""
                            ),
                            {**r, "id": known[symbol]["id"]},
                        )
                else:
                    self.execute(
                        c,
                        text(
                            """insert into pms.instruments(exchange,trading_symbol,nse_symbol,isin,name,listed_from,active,metadata)
                    values('NSE',:trading_symbol,:symbol,:isin,:name,:listed_from,true,cast(:metadata as jsonb))
                    on conflict do nothing"""
                        ),
                        {
                            **r,
                            "trading_symbol": symbol + "-EQ",
                            "metadata": encoded(
                                {
                                    "cloud_master_observed_at": doc.observed_at,
                                    "series": r["series"],
                                    "series_list": r.get("series_list", [r["series"]]),
                                }
                            ),
                        },
                    )
            self.receipt(
                c,
                "CLOUD_MASTER",
                [doc],
                {
                    "symbols": len(rows),
                    "observed_only": True,
                    "absence_is_not_delisting": True,
                },
            )
        self._known = None
        return {"source_rows": len(rows), "identity_conflicts": 0}

    def prices(self, rows, doc, day, surveillance, *, secondary=None,
               secondary_doc=None, suspensions=None, suspension_docs=()):
        known = self.instruments()
        by_symbol = {}
        for r in rows:
            symbol = r["symbol"]
            if symbol not in known:
                aliases = [
                    s
                    for s, i in known.items()
                    if reviewed_price_isin_matches(i, r['isin'], day)
                ]
                if len(aliases) > 1:
                    raise SourceError('price_isin_mapping_ambiguous')
                if len(aliases) == 1:
                    symbol = aliases[0]
            if symbol not in known:
                continue
            if not reviewed_price_isin_matches(known[symbol], r['isin'], day) or symbol in by_symbol:
                raise SourceError("price_identity_or_series_ambiguity")
            by_symbol[symbol] = r
        expected = {
            s
            for s, i in known.items()
            if i["listed_from"]
            and i["listed_from"] <= day
            and surveillance.get(s, {}).get("status") == "A"
        }
        missing = expected - by_symbol.keys()
        uncovered = {
            s
            for s, i in known.items()
            if i["listed_from"] and i["listed_from"] <= day and s not in surveillance
        }
        no_reported_bar = []
        suspensions = suspensions or {}
        # A reviewed circular explains only this bounded price/status exception;
        # it does not manufacture historical ASM/GSM classifications.
        if set(suspensions) & by_symbol.keys():
            raise SourceError('suspended_security_has_reported_bar')
        if secondary is not None:
            from packages.data_layer.price_observations import verify_absences
            if secondary_doc is None:
                raise SourceError('secondary_price_receipt_required')
            no_reported_bar = verify_absences(rows, secondary, missing, suspensions)
            missing -= set(no_reported_bar) | set(suspensions)
            uncovered -= set(suspensions)
        # Partial data may be stored for repair, but it never receives a complete gate.
        with self.engine.begin() as c:
            if by_symbol:
                self.execute(
                    c,
                    text(
                        """insert into pms.daily_prices(instrument_id,trade_date,open,high,low,close,volume,turnover,source)
                select id,trade_date,open,high,low,close,volume,turnover,'NSE_UDIFF'
                from jsonb_to_recordset(cast(:rows as jsonb)) as x(id bigint,trade_date date,open numeric,high numeric,low numeric,close numeric,volume numeric,turnover numeric)
                on conflict(instrument_id,trade_date,source) do update set open=excluded.open,high=excluded.high,
                low=excluded.low,close=excluded.close,volume=excluded.volume,turnover=excluded.turnover,ingested_at=now()"""
                    ),
                    {
                        "rows": encoded(
                            [{**r, "id": known[s]["id"]} for s, r in by_symbol.items()]
                        )
                    },
                )
            self.receipt(
                c,
                "CLOUD_PRICES",
                [doc] + ([secondary_doc] if secondary_doc else []) + list(suspension_docs),
                {
                    "date": day,
                    "written": len(by_symbol),
                    "missing": len(missing),
                    "uncovered": len(uncovered),
                    "no_reported_bar_symbols": no_reported_bar,
                    "reviewed_suspensions": suspensions,
                    "synthetic_candles": False,
                    "complete": bool(expected) and not missing and not uncovered,
                },
            )
        return {
            "written": len(by_symbol),
            "expected": len(expected),
            "missing_symbols": sorted(missing),
            "uncovered_symbols": sorted(uncovered),
            "no_reported_bar_symbols": no_reported_bar,
            "reviewed_suspensions": suspensions,
            "complete": bool(expected) and not missing and not uncovered,
        }

    def session_surveillance(self, rows, states, day):
        """Link dated source tickers only through a unique verified ISIN."""
        known = self.instruments()
        normalized = dict(states)
        aliases = []
        for row in rows:
            old = row['symbol']
            if old in known or old not in states:
                continue
            matches = [s for s,i in known.items()
                       if reviewed_price_isin_matches(i, row['isin'], day)]
            if len(matches) > 1:
                raise SourceError('price_isin_mapping_ambiguous')
            if len(matches) != 1:
                continue
            current = matches[0]
            if current in states and states[current] != states[old]:
                raise SourceError('surveillance_symbol_mapping_conflict')
            normalized[current] = states[old]
            aliases.append(dict(source_symbol=old, database_symbol=current,
                                isin=row['isin'], trade_date=str(day)))
        return normalized, aliases

    def benchmarks(self, rows, doc):
        with self.engine.begin() as c:
            from decimal import Decimal

            previous = (
                self.execute(
                    c,
                    text(
                        """select benchmark_code,close from pms.module_e_benchmark_bars
            where trade_date=:day and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX')"""
                    ),
                    {"day": rows[0]["trade_date"]},
                )
                .mappings()
                .all()
            )
            current = {r["benchmark_code"]: r["close"] for r in rows}
            if any(
                r["benchmark_code"] in current
                and abs(Decimal(str(r["close"])) - current[r["benchmark_code"]])
                > Decimal("0.0001")
                for r in previous
            ):
                raise SourceError("benchmark_official_source_conflict")
            self.execute(c, text(INDEX_SQL), rows)
            self.receipt(
                c,
                "CLOUD_BENCHMARKS",
                [doc],
                {
                    "date": rows[0]["trade_date"],
                    "codes": sorted(r["benchmark_code"] for r in rows),
                },
            )
        return {"written": len(rows)}

    def surveillance(self, rows, docs, day):
        known = self.instruments()
        unknown = sorted(set(known) - rows.keys())
        if unknown:
            raise SourceError("surveillance_universe_not_covered")
        stamp = datetime.combine(day, time(), tzinfo=IST)
        end = stamp + timedelta(days=1) - timedelta(microseconds=1)
        active = []
        for s, r in rows.items():
            if s not in known:
                continue
            for kind in ("ASM", "GSM"):
                if r[kind]:
                    active.append(
                        dict(
                            id=known[s]["id"],
                            kind=kind,
                            stage=r[kind],
                            start=stamp,
                            end=end,
                        )
                    )
        with self.engine.begin() as c:
            # Only a complete, validated official snapshot can end older open restrictions.
            self.execute(
                c,
                text(
                    """update pms.surveillance_history set effective_to=
            case when source='NSE_SURVEILLANCE' then cast(:stamp as timestamptz) else cast(:prior as timestamptz) end
            where source like 'NSE_%' and surveillance_type in ('ASM','GSM') and effective_to is null and effective_from<:stamp"""
                ),
                {"stamp": stamp, "prior": stamp - timedelta(microseconds=1)},
            )
            if active:
                self.execute(
                    c,
                    text(
                        """insert into pms.surveillance_history(instrument_id,surveillance_type,stage,effective_from,effective_to,source)
                select id,kind,stage,start,finish,'NSE_CLOUD_REG_IND' from jsonb_to_recordset(cast(:rows as jsonb)) as x(id bigint,kind text,stage text,start timestamptz,finish timestamptz)
                on conflict(instrument_id,surveillance_type,effective_from,source) do update set stage=excluded.stage,effective_to=excluded.effective_to"""
                    ),
                    {"rows": encoded([{**r, "finish": r["end"]} for r in active])},
                )
            observed = max(d.observed_at for d in docs)
            self.execute(
                c,
                text(
                    """update pms.instruments i set metadata=i.metadata || jsonb_build_object('cloud_status',x.status)
            from jsonb_to_recordset(cast(:rows as jsonb)) as x(id bigint,status jsonb) where i.id=x.id"""
                ),
                {
                    "rows": encoded(
                        [
                            {
                                "id": known[s]["id"],
                                "status": {
                                    "suspended": rows[s]["status"] == "S",
                                    "observed_at": observed,
                                },
                            }
                            for s in known
                        ]
                    )
                },
            )
            self.receipt(
                c,
                "CLOUD_SURVEILLANCE",
                docs,
                {
                    "date": day,
                    "complete": True,
                    "types": ["ASM", "GSM"],
                    "universe": len(known),
                },
            )
        return {"source_rows": len(rows), "restrictions": len(active), "complete": True}

    def acked(self, dataset):
        with self.engine.connect() as c:
            return set(
                self.execute(
                    c,
                    text(
                        """select metadata->>'record_key' from pms.provider_snapshots
            where provider='NSE' and dataset=:dataset and metadata->>'complete'='true'"""
                    ),
                    {"dataset": dataset},
                )
                .scalars()
                .all()
            )

    def ack(self, dataset, doc, key):
        with self.engine.begin() as c:
            self.receipt(c, dataset, [doc], {"record_key": key, "complete": True})

    def queue_records(self, pending_dataset, ack_dataset, records):
        """Keep unprocessed discoveries beyond rolling API windows and quarter changes."""
        import hashlib

        payload = [
            {
                "key": hashlib.sha256(
                    json.dumps(r, sort_keys=True, default=str).encode()
                ).hexdigest(),
                "record": r,
            }
            for r in records
        ]
        with self.engine.begin() as c:
            self.execute(
                c,
                """insert into pms.provider_snapshots(provider,dataset,content_sha256,metadata)
            select 'NSE',:dataset,x.key,jsonb_build_object('record_key',x.key,'record',x.record)
            from jsonb_to_recordset(cast(:rows as jsonb)) as x(key text,record jsonb)
            where not exists(select 1 from pms.provider_snapshots p where p.provider='NSE' and p.dataset=:dataset and p.metadata->>'record_key'=x.key)""",
                {"dataset": pending_dataset, "rows": encoded(payload)},
            )
            rows = (
                c.execute(
                    text(
                        """select p.metadata->'record' as record from pms.provider_snapshots p
            where p.provider='NSE' and p.dataset=:pending and not exists(
            select 1 from pms.provider_snapshots a where a.provider='NSE' and a.dataset=:ack
            and a.metadata->>'record_key'=p.metadata->>'record_key' and a.metadata->>'complete'='true')
            order by coalesce((p.metadata->>'last_attempted_at')::timestamptz,
                              '-infinity'::timestamptz),p.observed_at,p.id"""
                    ),
                    {"pending": pending_dataset, "ack": ack_dataset},
                )
                .mappings()
                .all()
            )
        return [r["record"] for r in rows]

    def pending_failed(self, dataset, key):
        # Move failed records behind untouched discoveries without acknowledging
        # them or losing the original source record. Remaining still includes all
        # failures; a retry is never a completion waiver.
        if dataset not in {'CLOUD_FINANCIAL_PENDING', 'CLOUD_SHAREHOLDING_PENDING'}:
            raise SourceError('unsupported_pending_dataset')
        with self.engine.begin() as c:
            self.execute(c, """update pms.provider_snapshots
                set metadata=metadata || jsonb_build_object('last_attempted_at',now())
                where provider='NSE' and dataset=:dataset and metadata->>'record_key'=:key""",
                {'dataset': dataset, 'key': key})

    def financial_window(self, day):
        with self.engine.connect() as c:
            latest = c.execute(
                text(
                    """select max((metadata->>'end_date')::date) from pms.provider_snapshots
            where provider='NSE' and dataset='CLOUD_FINANCIAL_DISCOVERY' and metadata->>'complete'='true'"""
                )
            ).scalar_one_or_none()
        start = (
            min(day - timedelta(days=7), latest - timedelta(days=1))
            if latest
            else day - timedelta(days=30)
        )
        if (day - start).days > 90:
            raise SourceError("financial_discovery_gap_requires_backfill")
        return start

    def financial_discovered(self, day):
        with self.engine.begin() as c:
            self.execute(
                c,
                """insert into pms.provider_snapshots(provider,dataset,metadata)
            values('NSE','CLOUD_FINANCIAL_DISCOVERY',cast(:metadata as jsonb))""",
                {"metadata": encoded({"end_date": day, "complete": True})},
            )

    def shareholding(self, symbol, values, raw, doc):
        known = self.instruments()
        with self.engine.begin() as c:
            params = {
                **values,
                "id": known[symbol]["id"],
                "raw": encoded({**raw, "official_xml_sha256": doc.sha256,
                                "identity_review": values.get('identity_review'),
                                "percentage_reconciliation": values.get('percentage_reconciliation')}),
            }
            self.execute(
                c,
                text(
                    """insert into pms.shareholding_history(instrument_id,period_end,filed_at,promoter_holding_pct,promoter_pledge_pct,total_shares,source,revision_number,raw_payload)
            values(:id,:period_end,:filed_at,:promoter_holding_pct,:promoter_pledge_pct,:total_shares,'NSE_SHP_XBRL',:revision_number,cast(:raw as jsonb))
            on conflict(instrument_id,period_end,filed_at,source,revision_number) do update set promoter_holding_pct=excluded.promoter_holding_pct,promoter_pledge_pct=excluded.promoter_pledge_pct,total_shares=excluded.total_shares,raw_payload=excluded.raw_payload"""
                ),
                params,
            )
            self.execute(
                c,
                text(
                    """insert into pms.shares_outstanding_history(instrument_id,effective_from,shares_outstanding,source,knowable_at,raw_payload)
            values(:id,:period_end,:total_shares,'NSE_SHP_XBRL',:filed_at,cast(:raw as jsonb))
            on conflict(instrument_id,effective_from,source,knowable_at) do update set shares_outstanding=excluded.shares_outstanding,raw_payload=excluded.raw_payload"""
                ),
                params,
            )

    def financial(self, filing):
        from packages.data_layer.postgres_storage import PostgresStorage

        known = self.instruments()
        store = self

        class RoutedEngine:
            @contextmanager
            def begin(self):
                with store.engine.begin() as conn:

                    class RoutedConnection:
                        def execute(self, statement, params=None):
                            return store.execute(conn, statement, params)

                    yield RoutedConnection()

        class ResolvedStorage(PostgresStorage):
            def _lookup_id(self, conn, instrument_id):
                return known[instrument_id.split(":", 1)[1]]["id"]

        ResolvedStorage(engine=RoutedEngine()).append_filings(
            list(filing) if isinstance(filing, (list, tuple)) else [filing], refresh_module_a=False
        )

    def events(self, dataset, rows, doc):
        known = self.instruments()
        with self.engine.begin() as c:
            for r in rows:
                if r["symbol"] not in known:
                    continue
                params = {
                    **r,
                    "id": known[r["symbol"]]["id"],
                    "raw": encoded(r.get("raw", {})),
                }
                if dataset == "CLOUD_EARNINGS":
                    self.execute(
                        c,
                        text(
                            """insert into pms.earnings_calendar(instrument_id,event_date,announced_at,source,raw_payload)
                    values(:id,:event_date,:announced_at,'NSE_BOARD_MEETINGS',cast(:raw as jsonb))
                    on conflict(instrument_id,event_date,source) do update set announced_at=excluded.announced_at,raw_payload=excluded.raw_payload"""
                        ),
                        params,
                    )
                elif dataset == "CLOUD_ACTIONS":
                    self.execute(
                        c,
                        text(
                            """insert into pms.corporate_actions(instrument_id,action_type,ex_date,record_date,ratio_from,ratio_to,cash_amount,announced_at,source,raw_payload)
                    values(:id,:action_type,:ex_date,:record_date,:ratio_from,:ratio_to,:cash_amount,:announced_at,'NSE_CORPORATE_ACTIONS',cast(:raw as jsonb))
                    on conflict(instrument_id,action_type,ex_date,source) do update set record_date=excluded.record_date,ratio_from=excluded.ratio_from,ratio_to=excluded.ratio_to,cash_amount=excluded.cash_amount,announced_at=excluded.announced_at,raw_payload=excluded.raw_payload"""
                        ),
                        params,
                    )
                else:
                    raise SourceError("unsupported_dataset")
            self.receipt(
                c, dataset, [doc], {"source_rows": len(rows), "complete": True}
            )
        return {"source_rows": len(rows)}

    def calendar_history(self):
        with self.engine.connect() as c:
            return [
                dict(r)
                for r in c.execute(
                    text(
                        """select metadata from pms.provider_snapshots
            where provider='NSE' and dataset='NSE_TRADING_CALENDAR' order by observed_at desc"""
                    )
                )
                .mappings()
                .all()
            ]

    def membership(self, members, doc, day):
        from packages.data_layer.index_membership import equity_membership

        known = self.instruments()
        source_members = members
        members, excluded = equity_membership(members, known)
        # Current snapshots are knowable now, not at past index inclusion dates.
        effective = doc.observed_at.date() + timedelta(days=1)
        with self.engine.begin() as c:
            current = [
                int(r["id"])
                for r in known.values()
                if r["symbol"] in {m["Symbol"] for m in members}
            ]
            self.execute(
                c,
                text("""delete from pms.index_constituents_history
            where index_code='NIFTY500' and source='NSE_CLOUD_CURRENT' and effective_from=:effective
            and instrument_id<>all(cast(:ids as bigint[]))"""),
                {"effective": effective, "ids": current},
            )
            self.execute(
                c,
                text(
                    """update pms.index_constituents_history set effective_to=:effective
            where index_code='NIFTY500' and source='NSE_CLOUD_CURRENT' and effective_to is null
            and effective_from<:effective and instrument_id<>all(cast(:ids as bigint[]))"""
                ),
                {"effective": effective, "ids": current},
            )
            for r in members:
                iid = known[r["Symbol"]]["id"]
                self.execute(
                    c,
                    text(
                        """insert into pms.index_constituents_history(index_code,instrument_id,effective_from,source)
                select 'NIFTY500',:id,:effective,'NSE_CLOUD_CURRENT'
                where not exists(select 1 from pms.index_constituents_history where index_code='NIFTY500'
                and instrument_id=:id and source='NSE_CLOUD_CURRENT' and effective_to is null)
                on conflict do nothing"""
                    ),
                    {"id": iid, "effective": effective},
                )
            self.receipt(
                c,
                "CLOUD_INDEX_MEMBERSHIP",
                [doc],
                {
                    "observed_on": day,
                    "effective_from": effective,
                    "source_rows": len(source_members),
                    "equity_members": len(members),
                    "excluded_from_equity_universe": excluded,
                    "historical_reconstruction": False,
                    "members": source_members,
                },
            )
        return {"source_rows": len(source_members), "equity_members": len(members),
                "excluded_from_equity_universe": excluded, "effective_from": str(effective)}

    def classifications(self, http, day, *, limit, listed_through=None):
        from packages.data_layer.cloud_sources import fetch_classification

        known = self.instruments()
        candidates = []
        overdue = set()
        for s, r in known.items():
            if r.get('listed_from') and r['listed_from'] > (listed_through or day):
                continue
            observed = (
                (r.get("metadata") or {})
                .get("sector_classification", {})
                .get("observed_at")
            )
            # Start rolling refresh before expiry to avoid pausing entries for routine rotation.
            due = (
                not r["sector"]
                or not observed
                or datetime.fromisoformat(observed).date() < day - timedelta(days=60)
            )
            if (
                not r["sector"]
                or not observed
                or datetime.fromisoformat(observed).date() < day - timedelta(days=90)
            ):
                overdue.add(s)
            if due:
                candidates.append((s, r))
        # Repair missing dated coverage before refreshing already-covered stocks.
        # Rotation by last attempt still prevents a failed symbol starving others.
        covered=set()
        if candidates:
            with self.engine.connect() as c:
                covered=set(c.execute(text('''select distinct instrument_id from pms.sector_history
                    where effective_from<=:effective and (effective_to is null or effective_to>:effective)
                    and btrim(sector)<>'' '''),{'effective':day+timedelta(days=1)}).scalars())
        candidates.sort(key=lambda item: (
            item[1]['id'] in covered,
            (item[1].get('metadata') or {}).get('classification_last_attempted_at', ''),
            item[1]['id']))
        errors = []
        written = 0
        for s, r in candidates[:limit]:
            try:
                doc, classification = fetch_classification(http, s, r["isin"])
                sector = classification["sector"]
                provenance = {
                    **((r.get("metadata") or {}).get("sector_classification") or {}),
                    "observed_at": doc.observed_at.isoformat(),
                    "source": "NSE_NEXT_API",
                    "industry": classification["industry"],
                    "series": classification["series"],
                    "is_suspended": classification["is_suspended"],
                    "identity_basis": classification.get('identity_basis', 'DETAILED_QUOTE'),
                    "metadata_sha256": classification.get('metadata_sha256'),
                    "identity_review": classification.get('identity_review'),
                }
                with self.engine.begin() as c:
                    self.execute(
                        c,
                        text(
                            """update pms.instruments set sector=:sector,metadata=metadata || jsonb_build_object('sector_classification',cast(:metadata as jsonb)) where id=:id"""
                        ),
                        {
                            "id": r["id"],
                            "sector": sector,
                            "metadata": encoded(provenance),
                        },
                    )
                    # Current quote is knowable now; do not backdate a historical sector change.
                    effective = doc.observed_at.date() + timedelta(days=1)
                    self.execute(
                        c,
                        text(
                            """delete from pms.sector_history where instrument_id=:id and source='NSE_CLOUD_QUOTE'
                    and effective_from=:effective and sector<>:sector"""
                        ),
                        {"id": r["id"], "effective": effective, "sector": sector},
                    )
                    self.execute(
                        c,
                        text(
                            """update pms.sector_history set effective_to=:effective
                    where instrument_id=:id and effective_to is null and effective_from<:effective and sector<>:sector"""
                        ),
                        {"id": r["id"], "effective": effective, "sector": sector},
                    )
                    self.execute(
                        c,
                        text(
                            """insert into pms.sector_history(instrument_id,sector,industry,effective_from,source)
                    select :id,:sector,:industry,:effective,'NSE_CLOUD_QUOTE'
                    where not exists(select 1 from pms.sector_history where instrument_id=:id and sector=:sector and effective_to is null)
                    on conflict do nothing"""
                        ),
                        {
                            "id": r["id"],
                            "sector": sector,
                            "industry": provenance["industry"],
                            "effective": effective,
                        },
                    )
                    self.receipt(
                        c,
                        "CLOUD_CLASSIFICATIONS",
                        [doc],
                        {
                            "instrument_id": r["id"],
                            "sector": sector,
                            "observed_only": True,
                        },
                    )
                written += 1
                overdue.discard(s)
            except Exception as exc:
                with self.engine.begin() as c:
                    self.execute(c, """update pms.instruments
                        set metadata=metadata || jsonb_build_object('classification_last_attempted_at',now())
                        where id=:id""", {'id': r['id']})
                errors.append({'symbol': s, 'code': str(exc) if isinstance(exc, SourceError)
                               else 'classification_unavailable', 'error_type': type(exc).__name__})
        return {
            "due": len(candidates),
            "written": written,
            "remaining": len(overdue),
            "refresh_backlog": len(candidates) - written,
            "errors": errors[:20],
        }
