"""Durable acquisition/staging lake. This is NOT a replacement PMS replay DB.

Only standard-library storage plus DEEX's existing official-source parsers.
Raw receipts are immutable; job completion and normalized rows commit together.
"""
from __future__ import annotations

import calendar
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from .cloud_sources import (ARCHIVE, MAX_BYTES, OfficialHTTP, SourceError,
                           bhavcopy_url, number, parse_bhavcopy, parse_indices,
                           parse_day, rows_csv, allowed_url)

ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS = json.loads((ROOT / 'config/data_requirements.json').read_text())
SERIES = {'EQ', 'BE', 'BZ', 'SM', 'ST', 'SZ'}


def stamp():
    return datetime.now(timezone.utc).isoformat()


def years_before(day, years):
    return day.replace(year=day.year-years,
                       day=min(day.day, calendar.monthrange(day.year-years, day.month)[1]))


def dumps(obj):
    return json.dumps(obj, default=str, sort_keys=True, allow_nan=False)


def legacy_url(day):
    return (ARCHIVE + '/content/historical/EQUITIES/' + day.strftime('%Y/%b').upper()
            + '/cm' + day.strftime('%d%b%Y').upper() + 'bhav.csv.zip')


def parse_legacy(body, day):
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        members = z.infolist()
        expected = 'cm' + day.strftime('%d%b%Y').upper() + 'bhav.csv'
        if len(members) != 1 or members[0].filename != expected or members[0].file_size > MAX_BYTES:
            raise SourceError('legacy_archive_invalid')
        data = z.read(members[0])
    required = ['SYMBOL', 'SERIES', 'ISIN', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW',
                'CLOSE', 'TOTTRDQTY', 'TOTTRDVAL']
    # Old NSE rows have an extra trailing comma. Its empty column is harmless;
    # do not relax missing required columns or malformed data rows.
    rows = rows_csv(data, required)
    result, seen = [], set()
    for r in rows:
        if parse_day(r['TIMESTAMP']) != day:
            raise SourceError('legacy_wrong_date')
        if r['SERIES'] not in SERIES:
            continue
        key = (r['SYMBOL'], r['SERIES'])
        if key in seen:
            raise SourceError('legacy_duplicate_identity')
        seen.add(key)
        o, h, l, c = [number(r[k]) for k in ('OPEN', 'HIGH', 'LOW', 'CLOSE')]
        volume = number(r['TOTTRDQTY'], integer=True)
        if volume == 0 and not any((o, h, l, c)):
            continue
        if min(o, h, l, c) <= 0 or l > min(o, c) or h < max(o, c) or h < l:
            raise SourceError('legacy_invalid_ohlc')
        result.append(dict(symbol=r['SYMBOL'], series=r['SERIES'], isin=r['ISIN'],
                           trade_date=day, open=o, high=h, low=l, close=c,
                           volume=volume, turnover=number(r['TOTTRDVAL'])))
    if not result:
        raise SourceError('legacy_empty')
    return result


class Lake:
    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.root / 'raw').mkdir(exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.root / 'catalog.sqlite', timeout=30)
        self.db.row_factory = sqlite3.Row
        # Rollback journals also work on Windows and mounted workspaces where
        # SQLite WAL shared-memory semantics may not be supported.
        self.db.execute('pragma journal_mode=DELETE')
        self.db.execute('pragma foreign_keys=ON')
        self.db.execute('pragma synchronous=FULL')
        self.db.executescript('''
        create table if not exists plans (
          id text primary key, start text not null, end text not null,
          price_start text not null, fact_start text not null, created_at text not null);
        create table if not exists jobs (
          dataset text, start text, end text, status text not null default 'PENDING',
          attempts integer not null default 0, rows integer not null default 0,
          error text, updated_at text, primary key(dataset,start,end));
        create table if not exists receipts (
          sha256 text primary key, url text not null, observed_at text not null,
          size integer not null, path text not null);
        create table if not exists job_receipts (
          dataset text, start text, end text, sha256 text references receipts(sha256),
          primary key(dataset,start,end,sha256));
        create table if not exists prices (
          trade_date text, symbol text, isin text, series text,
          open text not null, high text not null, low text not null, close text not null,
          volume integer not null, turnover text not null,
          receipt text not null references receipts(sha256),
          primary key(trade_date,symbol,isin,series));
        create index if not exists prices_identity on prices(isin,trade_date);
        create index if not exists prices_symbol on prices(symbol,trade_date);
        create table if not exists indices (
          trade_date text, code text, open text, high text, low text, close text,
          receipt text not null references receipts(sha256), primary key(trade_date,code));
        create table if not exists records (
          dataset text, key text, effective_date text, known_at text,
          payload text not null, receipt text not null references receipts(sha256),
          primary key(dataset,key,receipt));
        create index if not exists record_lookup on records(dataset,effective_date,known_at);
        create table if not exists documents (
          dataset text, key text, url text not null, payload text not null,
          status text not null default 'PENDING', error text,
          primary key(dataset,key));
        create table if not exists company_targets (
          symbol text, isin text, series text, name text,
          listed_from_hint text, first_price_date text, last_price_date text,
          in_current_master integer not null default 0, security_kind text not null,
          metadata_asof text, primary key(symbol,isin,series));
        ''')

    def close(self):
        self.db.close()

    def plan(self, start, end):
        if end < start or end >= datetime.now(ZoneInfo('Asia/Kolkata')).date():
            raise ValueError('Use an ordered range of completed dates')
        price_start = years_before(start, REQUIREMENTS['price_warmup_years'])
        fact_start = years_before(start, REQUIREMENTS['fundamental_warmup_years'])
        plan_id = hashlib.sha256(f'{start}/{end}/v1'.encode()).hexdigest()[:16]
        with self.db:
            self.db.execute('insert or ignore into plans values(?,?,?,?,?,?)',
                            (plan_id, str(start), str(end), str(price_start), str(fact_start), stamp()))
            # Include every calendar day. A 404 is MISSING, never an inferred holiday.
            day = price_start
            while day <= end:
                for dataset in ('prices', 'indices'):
                    self.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',
                                    (dataset, str(day), str(day)))
                day += timedelta(days=1)
            # Small monthly discovery windows bound API limits. Responses are staged,
            # never accepted as a complete historical dataset just because HTTP worked.
            day = fact_start
            while day <= end:
                last = min(day.replace(day=calendar.monthrange(day.year, day.month)[1]), end)
                for dataset in ('actions', 'earnings', 'financial_discovery', 'shareholding_discovery'):
                    self.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',
                                    (dataset, str(day), str(last)))
                day = last + timedelta(days=1)
        return dict(self.db.execute('select * from plans where id=?', (plan_id,)).fetchone())

    def archive(self, doc):
        digest = hashlib.sha256(doc.body).hexdigest()
        if digest != doc.sha256 or len(doc.body) > MAX_BYTES or not doc.body:
            raise SourceError('receipt_invalid')
        path = self.root / 'raw' / (digest + '.bin')
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise SourceError('raw_archive_corrupt')
        else:
            # Exclusive temporary name also avoids readers observing a partial file.
            import uuid
            tmp = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
            with tmp.open('xb') as f:
                f.write(doc.body)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(path)
        self.db.execute('insert or ignore into receipts values(?,?,?,?,?)',
                        (digest, doc.url, str(doc.observed_at), len(doc.body), path.name))
        return digest

    def _store_prices(self, rows, digest):
        values = []
        for r in rows:
            if len(r['isin']) != 12 or not r['isin'].startswith('IN') or not r['symbol']:
                raise SourceError('price_identity_invalid')
            values.append(tuple(str(r[k]) for k in ('trade_date', 'symbol', 'isin', 'series',
                         'open', 'high', 'low', 'close')) + (int(r['volume']), str(r['turnover']), digest))
        self.db.executemany('insert or replace into prices values(?,?,?,?,?,?,?,?,?,?,?)', values)
        # Every historical observation expands the register; current-master absence
        # never removes a delisted or renamed candidate.
        self._update_targets(rows)

    def _update_targets(self, rows):
        etfs = set(REQUIREMENTS['etfs']) | {REQUIREMENTS['optional_cash_etf']}
        self.db.executemany('''insert into company_targets(symbol,isin,series,first_price_date,last_price_date,security_kind)
              values(?,?,?,?,?,?) on conflict(symbol,isin,series) do update set
              first_price_date=min(coalesce(company_targets.first_price_date,excluded.first_price_date),excluded.first_price_date),
              last_price_date=max(coalesce(company_targets.last_price_date,excluded.last_price_date),excluded.last_price_date)''',
              [(r['symbol'], r['isin'], r['series'], str(r['trade_date']), str(r['trade_date']),
                'ETF_SPECIFIED' if r['symbol'] in etfs else 'CLASSIFICATION_REQUIRED') for r in rows])

    def refresh_targets(self):
        etfs = set(REQUIREMENTS['etfs']) | {REQUIREMENTS['optional_cash_etf']}
        for r in self.db.execute('select symbol,isin,series,min(trade_date) first_day,max(trade_date) last_day '
                                 'from prices group by symbol,isin,series').fetchall():
            self.db.execute('''insert into company_targets(symbol,isin,series,first_price_date,last_price_date,security_kind)
                 values(?,?,?,?,?,?) on conflict(symbol,isin,series) do update set
                 first_price_date=excluded.first_price_date,last_price_date=excluded.last_price_date''',
                 (r['symbol'], r['isin'], r['series'], r['first_day'], r['last_day'],
                  'ETF_SPECIFIED' if r['symbol'] in etfs else 'CLASSIFICATION_REQUIRED'))

    def refresh_current_master(self, http):
        from .cloud_sources import parse_master
        doc = http.fetch(ARCHIVE + '/content/equities/EQUITY_L.csv')
        rows = parse_master(doc.body)
        etfs = set(REQUIREMENTS['etfs']) | {REQUIREMENTS['optional_cash_etf']}
        with self.db:
            digest = self.archive(doc)
            self.db.execute('update company_targets set in_current_master=0')
            for r in rows.values():
                for series in r.get('series_list', [r['series']]):
                    self.db.execute('''insert into company_targets(symbol,isin,series,name,listed_from_hint,
                       in_current_master,security_kind,metadata_asof) values(?,?,?,?,?,1,?,?)
                       on conflict(symbol,isin,series) do update set name=excluded.name,
                       listed_from_hint=excluded.listed_from_hint,in_current_master=1,metadata_asof=excluded.metadata_asof''',
                       (r['symbol'], r['isin'], series, r['name'], str(r['listed_from']),
                        'ETF_SPECIFIED' if r['symbol'] in etfs else 'CLASSIFICATION_REQUIRED', str(doc.observed_at)))
            self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                            ('current_master_only', digest, str(doc.observed_at.date()), str(doc.observed_at),
                             dumps({'rows': len(rows), 'historical_membership_authority': False}), digest))
            self.refresh_targets()
        return self.target_summary()

    def target_summary(self):
        self.refresh_targets()
        self.db.commit()
        rows = self.db.execute('select count(*) from company_targets').fetchone()[0]
        return dict(candidate_identity_series_rows=rows,
                    unique_isins=self.db.execute('select count(distinct isin) from company_targets').fetchone()[0],
                    historical_only_rows=self.db.execute('select count(*) from company_targets where in_current_master=0 and first_price_date is not null').fetchone()[0],
                    known_etf_symbols=len(REQUIREMENTS['etfs']), exact_company_count=None,
                    status='EXPANDING_UNIVERSE_NOT_A_CERTIFIED_COMPANY_COUNT')

    def seed_targets(self, path):
        """Load versioned candidate hints, never price coverage or live membership."""
        path = Path(path)
        if not path.is_file():
            return 0
        count = 0
        with self.db, path.open(newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                if not r.get('symbol') or len(r.get('isin', '')) != 12 or not r.get('series'):
                    raise ValueError('Invalid candidate seed')
                self.db.execute('''insert or ignore into company_targets
                    (symbol,isin,series,name,listed_from_hint,security_kind,metadata_asof)
                    values(?,?,?,?,?,?,?)''', (r['symbol'], r['isin'], r['series'], r.get('name') or None,
                    r.get('listed_from_hint') or None, r.get('security_kind') or 'CLASSIFICATION_REQUIRED',
                    r.get('metadata_asof') or None))
                count += 1
        return count

    def export_targets(self, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self.db:
            self.refresh_targets()
        fields = ['symbol','isin','series','name','listed_from_hint','first_price_date','last_price_date',
                  'in_current_master','security_kind','metadata_asof']
        with destination.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields + ['required_datasets','coverage_status'], lineterminator='\n')
            writer.writeheader()
            for r in self.db.execute('select * from company_targets order by symbol,isin,series'):
                d = dict(r)
                deps = REQUIREMENTS['strategies']['S10'] if d['security_kind'] == 'ETF_SPECIFIED' else REQUIREMENTS['strategies']['PMS_SETUP1']
                writer.writerow({**d, 'required_datasets': '|'.join(deps), 'coverage_status': 'UNVERIFIED'})
        return str(destination.resolve())

    def _job(self, dataset, start, end, http):
        if dataset == 'prices':
            day = date.fromisoformat(start)
            legacy = day < date(2024, 7, 8)
            doc = http.fetch(legacy_url(day) if legacy else bhavcopy_url(day))
            digest = self.archive(doc)
            rows = parse_legacy(doc.body, day) if legacy else parse_bhavcopy(doc.body, day)
            self._store_prices(rows, digest)
            return len(rows), [digest]
        if dataset == 'indices':
            day = date.fromisoformat(start)
            doc = http.fetch(ARCHIVE + '/content/indices/ind_close_all_' + day.strftime('%d%m%Y') + '.csv')
            digest = self.archive(doc)
            # Older reports may lack one of the four required indices. Preserve
            # available rows, then report the absent index explicitly.
            from .cloud_sources import BENCHMARKS
            raw = rows_csv(doc.body, ['Index Name', 'Index Date', 'Open Index Value',
                                     'High Index Value', 'Low Index Value', 'Closing Index Value'])
            available = {r['Index Name'] for r in raw} & set(BENCHMARKS)
            if not available:
                raise SourceError('indices_empty')
            # Parse each row with its required identity, without changing global parsers.
            rows = []
            for r in raw:
                if r['Index Name'] not in available:
                    continue
                if parse_day(r['Index Date']) != day:
                    raise SourceError('index_wrong_date')
                c = number(r['Closing Index Value'], minimum=Decimal('0.00000001'))
                o, h, l = [None if r[k] in ('', '-', 'NA', 'N/A') else
                           number(r[k], minimum=Decimal('0.00000001')) for k in
                           ('Open Index Value', 'High Index Value', 'Low Index Value')]
                if all(v is not None for v in (o, h, l)) and (l > min(o, c) or h < max(o, c) or h < l):
                    raise SourceError('index_invalid_ohlc')
                rows.append((str(day), BENCHMARKS[r['Index Name']],
                             *(str(v) if v is not None else None for v in (o, h, l, c)), digest))
            if len({r[1] for r in rows}) != len(rows):
                raise SourceError('index_duplicate')
            self.db.executemany('insert or replace into indices values(?,?,?,?,?,?,?)', rows)
            return len(rows), [digest]
        endpoints = {
            'actions': ('corporates-corporateActions', {'index': 'equities'}),
            'earnings': ('corporate-board-meetings', {'index': 'equities'}),
            'shareholding_discovery': ('corporate-share-holdings-master', {'index': 'equities'}),
            'financial_discovery': ('corporates-financial-results', {'index': 'equities', 'period': 'Quarterly'}),
            'annual_financial_discovery': ('corporates-financial-results', {'index': 'equities', 'period': 'Annual'}),
        }
        path, params = endpoints[dataset]
        if dataset == 'financial_discovery' and end >= '2025-03-01':
            return self._integrated_discovery(start, end, http)
        doc = http.api(path, **params, from_date=date.fromisoformat(start).strftime('%d-%m-%Y'),
                       to_date=date.fromisoformat(end).strftime('%d-%m-%Y'))
        digest = self.archive(doc)
        # Raw discovery is useful evidence even if pagination/date filters change.
        # Never turn this into a historical coverage certificate.
        payload = json.loads(doc.body)
        self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                        (dataset + '_raw', start + '/' + end, start, None, dumps(payload), digest))
        if dataset in ('actions', 'earnings'):
            from .cloud_sources import parse_stamp
            rows = payload if isinstance(payload, list) else payload.get('data', []) if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                raise SourceError('discovery_schema_changed')
            for row in rows:
                # Actions lacking an original broadcast time stay raw. Never use
                # an ex-date as evidence of when the market knew about the action.
                known_raw = row.get('caBroadcastDate') if dataset == 'actions' else row.get('bm_timestamp')
                effective_raw = row.get('exDate') if dataset == 'actions' else row.get('bm_date')
                symbol = row.get('symbol') if dataset == 'actions' else row.get('bm_symbol')
                if not known_raw or not effective_raw or not symbol:
                    continue
                known, effective = parse_stamp(known_raw), parse_day(effective_raw)
                if known > doc.observed_at:
                    raise SourceError('event_known_in_future')
                key = hashlib.sha256(dumps(row).encode()).hexdigest()
                self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                                (dataset, key, str(effective), known.isoformat(), dumps({'symbol': symbol, 'source_record': row}), digest))
        if dataset in ('financial_discovery', 'annual_financial_discovery', 'shareholding_discovery'):
            rows = payload if isinstance(payload, list) else payload.get('data', []) if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                raise SourceError('discovery_schema_changed')
            for row in rows:
                url = row.get('xbrl') or row.get('xbrlFile') or row.get('xbrl_file_link')
                if not url:
                    continue
                allowed_url(url)
                key = hashlib.sha256(dumps(row).encode()).hexdigest()
                self.db.execute('insert or ignore into documents(dataset,key,url,payload) values(?,?,?,?)',
                                ('shareholding' if dataset == 'shareholding_discovery' else 'financials', key, url, dumps(row)))
        return 1, [digest]

    def _integrated_discovery(self, start, end, http):
        """Keep all bounded pages; verify the endpoint's count, dates and identity."""
        from .exchange_financials import NSEIntegratedFinancialsProvider
        parser = object.__new__(NSEIntegratedFinancialsProvider)
        expected, seen, receipts, count = None, set(), [], 0
        for page in range(1, 101):
            doc = http.api('integrated-filing-results', type='Integrated Filing- Financials',
                           index='equities', from_date=date.fromisoformat(start).strftime('%d-%m-%Y'),
                           to_date=date.fromisoformat(end).strftime('%d-%m-%Y'), page=page, size=100)
            digest = self.archive(doc)
            receipts.append(digest)
            payload = json.loads(doc.body)
            if not isinstance(payload, dict) or not isinstance(payload.get('totalCount'), int):
                raise SourceError('financial_pagination_contract_missing')
            if expected is None:
                expected = payload['totalCount']
            if payload['totalCount'] != expected or payload.get('page') != page-1:
                raise SourceError('financial_pagination_changed')
            rows = payload.get('data')
            if not isinstance(rows, list):
                raise SourceError('financial_schema_changed')
            self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                            ('financial_discovery_raw', f'{start}/{end}/{page}', start, None, dumps(payload), digest))
            for row in rows:
                r = parser._parse_row(row)
                if not r or not r.external_id or not r.xbrl_uri or not start <= str(r.filed_at.date()) <= end:
                    raise SourceError('financial_identity_or_date_filter_failed')
                key = f'{r.symbol}/{r.external_id}'
                if key in seen:
                    raise SourceError('financial_repeated_page')
                seen.add(key)
                allowed_url(r.xbrl_uri)
                self.db.execute('insert or ignore into documents(dataset,key,url,payload) values(?,?,?,?)',
                                ('financials', hashlib.sha256(dumps(row).encode()).hexdigest(), r.xbrl_uri, dumps(row)))
                count += 1
            if count == expected:
                return count, receipts
            if not rows or count > expected:
                raise SourceError('financial_pagination_truncated')
        raise SourceError('financial_page_budget_exceeded')

    def documents(self, *, limit=100, http_factory=None, pending_only=False):
        """Process original XBRL; errors are retained for review, never zero-filled."""
        if not 1 <= limit <= 10000:
            raise ValueError('limit must be 1..10000')
        from dataclasses import asdict
        from .cloud_sources import parse_shareholding
        from .exchange_financials import NSEIntegratedFinancialsProvider, filings_from_exchange_record
        provider = object.__new__(NSEIntegratedFinancialsProvider)
        condition = "status='PENDING'" if pending_only else "status!='DONE'"
        jobs = self.db.execute("select * from documents where " + condition + " order by dataset,key limit ?", (limit,)).fetchall()
        results = []
        for job in jobs:
            try:
                http = http_factory() if http_factory else OfficialHTTP(seconds=90)
                doc = http.fetch(job['url'])
                raw = json.loads(job['payload'])
                if b'<!DOCTYPE' in doc.body.upper() or b'<!ENTITY' in doc.body.upper():
                    raise SourceError('xbrl_entities_forbidden')
                if job['dataset'] == 'financials':
                    record = provider._parse_row(raw)
                    if record is None:
                        raise SourceError('financial_identity_missing')
                    values = [asdict(f) for f in filings_from_exchange_record(record, doc.body)]
                    if not values or any(v['sales'] is None or v['pat'] is None for v in values):
                        raise SourceError('financial_core_facts_missing')
                    pairs = [(str(v['period_end']), v['filed_at'].replace(tzinfo=__import__('zoneinfo').ZoneInfo('Asia/Kolkata')).isoformat(), v) for v in values]
                else:
                    values = parse_shareholding(doc.body, raw)
                    pairs = [(str(values['period_end']), values['filed_at'].isoformat(), {**values, 'symbol': raw.get('symbol')})]
                with self.db:
                    digest = self.archive(doc)
                    for i, (effective, known, value) in enumerate(pairs):
                        if datetime.fromisoformat(known) > datetime.now(timezone.utc):
                            raise SourceError('filing_timestamp_in_future')
                        self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                                        (job['dataset'], job['key'] + '/' + str(i), effective, known, dumps(value), digest))
                    self.db.execute('update documents set status=\'DONE\',error=NULL where dataset=? and key=?',
                                    (job['dataset'], job['key']))
                results.append(dict(dataset=job['dataset'], status='DONE', rows=len(pairs)))
            except Exception:
                with self.db:
                    self.db.execute("update documents set status='FAILED',error='document_parse_or_fetch_failed' where dataset=? and key=?",
                                    (job['dataset'], job['key']))
                results.append(dict(dataset=job['dataset'], status='FAILED', error='document_parse_or_fetch_failed'))
        return results

    def fetch(self, *, datasets=('prices', 'indices'), limit=100, retry=False,
              start=None, end=None, http_factory=None, exact=False):
        if not 1 <= limit <= 10000:
            raise ValueError('limit must be 1..10000')
        allowed = {'prices', 'indices', 'actions', 'earnings', 'financial_discovery', 'annual_financial_discovery', 'shareholding_discovery'}
        if not datasets or not set(datasets) <= allowed:
            raise ValueError('Unsupported acquisition dataset')
        where = 'dataset in (' + ','.join('?' for _ in datasets) + ') and status '
        where += "in ('PENDING','MISSING','FAILED','PARTIAL')" if retry else "='PENDING'"
        params = list(datasets)
        if start:
            where += ' and start'+('=?' if exact else '>=?'); params.append(str(start))
        if end:
            where += ' and end'+('=?' if exact else '<=?'); params.append(str(end))
        jobs = self.db.execute('select * from jobs where ' + where + ' order by start,dataset limit ?',
                               params + [limit]).fetchall()
        result = []
        for job in jobs:
            key = (job['dataset'], job['start'], job['end'])
            try:
                # Fresh transport budget per task: one failed task cannot poison later jobs.
                http = http_factory() if http_factory else OfficialHTTP(seconds=90)
                with self.db:
                    count, digests = self._job(*key, http)
                    for digest in digests:
                        self.db.execute('insert or ignore into job_receipts values(?,?,?,?)', (*key, digest))
                    # Discovery is explicitly STAGED, not DONE/complete.
                    status = 'DONE' if key[0] in ('prices', 'indices') else 'STAGED'
                    if key[0] == 'indices' and (not set(REQUIREMENTS['benchmarks']).issubset(
                            {r[0] for r in self.db.execute('select code from indices where trade_date=?', (key[1],))}) or
                            self.db.execute('select count(*) from indices where trade_date=? and '
                                            '(open is null or high is null or low is null)', (key[1],)).fetchone()[0]):
                        status = 'PARTIAL'
                    self.db.execute('update jobs set status=?,rows=?,attempts=attempts+1,error=NULL,updated_at=? '
                                    'where dataset=? and start=? and end=?', (status, count, stamp(), *key))
                result.append(dict(dataset=key[0], start=key[1], status=status, rows=count))
            except Exception as exc:
                # Do not log response bodies, URLs with credentials or arbitrary exception text.
                code = str(exc) if isinstance(exc, SourceError) else 'fetch_or_parse_failed'
                import re
                if not re.fullmatch('[a-z0-9_]+', code):
                    code = 'fetch_or_parse_failed'
                status = 'MISSING' if code == 'source_http_404' else 'FAILED'
                with self.db:
                    self.db.execute('update jobs set status=?,attempts=attempts+1,error=?,updated_at=? '
                                    'where dataset=? and start=? and end=?', (status, code, stamp(), *key))
                result.append(dict(dataset=key[0], start=key[1], status=status, error=code))
        return result

    def import_evidence(self, path):
        """Stage reviewed vendor/Design Experience JSONL with original source evidence.

        Each line: dataset,key,effective_date,known_at,payload,source_url,source_file.
        Relative evidence files resolve beside the JSONL. The source bytes are retained.
        Imported rows STILL require semantic/interval audits before replay use.
        """
        from .cloud_sources import Document
        path = Path(path).resolve()
        count = 0
        with self.db, path.open(encoding='utf-8') as f:
            for line in f:
                row = json.loads(line)
                dataset = row['dataset']
                if dataset not in REQUIREMENTS['datasets'] or dataset in ('prices', 'indices'):
                    raise ValueError('Unsupported evidence dataset')
                date.fromisoformat(row['effective_date'])
                known = datetime.fromisoformat(row['known_at'])
                if known.tzinfo is None:
                    raise ValueError('Original known_at requires timezone')
                if known > datetime.now(timezone.utc):
                    raise ValueError('Future publication timestamp')
                # Imported source URLs may identify authorised vendor/exchange data;
                # never fetch these URLs or retain credentials/query strings.
                from urllib.parse import urlsplit
                url = urlsplit(row['source_url'])
                if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment:
                    raise ValueError('Expected credential-free HTTPS evidence URL')
                source = (path.parent / row['source_file']).resolve()
                with source.open('rb') as raw:
                    body = raw.read(MAX_BYTES + 1)
                digest = self.archive(Document(row['source_url'], body, hashlib.sha256(body).hexdigest(),
                                               datetime.now(timezone.utc), len(body)))
                self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                                (dataset, row['key'], row['effective_date'], known.isoformat(), dumps(row['payload']), digest))
                count += 1
        return count

    def status(self):
        jobs = [dict(r) for r in self.db.execute('select dataset,status,count(*) jobs,sum(rows) rows '
                                                'from jobs group by dataset,status order by dataset,status')]
        price = dict(self.db.execute('select count(*) rows,count(distinct isin) security_isins,'
                                     'count(distinct symbol) symbols,min(trade_date) first_date,max(trade_date) last_date from prices').fetchone())
        families = {r[0]: r[1] for r in self.db.execute('select dataset,count(*) from records group by dataset')}
        counts = {d: 0 for d in REQUIREMENTS['datasets']}
        counts.update(families)
        counts['prices'] = price['rows']
        counts['indices'] = self.db.execute('select count(*) from indices').fetchone()[0]
        missing = [k for k, n in counts.items() if not n]
        etfs = {}
        for symbol in REQUIREMENTS['etfs']:
            etfs[symbol] = dict(self.db.execute('select count(*) rows,min(trade_date) first_observation,'
                                              'max(trade_date) last_observation from prices where symbol=?', (symbol,)).fetchone())
        document_jobs = [dict(r) for r in self.db.execute('select dataset,status,count(*) jobs from documents group by dataset,status')]
        return dict(status='NOT_CERTIFIED_FOR_BACKTEST', prices=price, document_jobs=document_jobs,
                    exact_company_count=None, company_count_reason='ISINs/symbols are securities, not unique issuers. Dated identity mapping and complete calendar coverage required.',
                    datasets=counts, missing_datasets=missing, jobs=jobs, etfs=etfs,
                    strategies={s: {'status': 'BLOCKED' if any(not counts[d] for d in deps) else 'AUDIT_REQUIRED',
                                    'missing': [d for d in deps if not counts[d]]}
                                for s, deps in REQUIREMENTS['strategies'].items()},
                    notes=['Raw discovery jobs do not establish complete normalized history.',
                           '404s remain missing until checked against an official session calendar.',
                           'No ingestion count establishes point-in-time correctness or PMS replay compatibility.'])

    def verify(self):
        errors = []
        integrity = self.db.execute('pragma integrity_check').fetchone()[0]
        if integrity != 'ok':
            errors.append('sqlite_integrity_failed')
        foreign = list(self.db.execute('pragma foreign_key_check'))
        if foreign:
            errors.append('foreign_key_check_failed')
        checked = 0
        for r in self.db.execute('select * from receipts'):
            path = self.root / 'raw' / r['path']
            if (not path.is_file() or path.stat().st_size != r['size']
                    or hashlib.sha256(path.read_bytes()).hexdigest() != r['sha256']):
                errors.append('raw_checksum_failed:' + r['sha256'])
            checked += 1
        return dict(ok=not errors, receipts_checked=checked, errors=errors)

    def backup(self, destination):
        destination = Path(destination).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        if destination == self.root or self.root in destination.parents:
            raise ValueError('Backup must be outside the lake')
        import shutil
        target = sqlite3.connect(destination / 'catalog.sqlite')
        try:
            self.db.backup(target)
        finally:
            target.close()
        shutil.copytree(self.root / 'raw', destination / 'raw', dirs_exist_ok=True)
        return str(destination)
