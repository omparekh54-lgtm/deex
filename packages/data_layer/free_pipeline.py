"""Resumable public-source collection; completion of work is not data certification."""
from __future__ import annotations

import contextlib
import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import email.utils
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import random
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from .cloud_sources import (ARCHIVE, NSE, MAX_BYTES, Document, OfficialHTTP,
                            SafeRedirect, SourceError, allowed_url, parse_surveillance)
from .local_lake import Lake, dumps, stamp

CORE_DATASETS = ('financial_discovery', 'annual_financial_discovery', 'shareholding_discovery',
                 'actions', 'earnings', 'indices', 'prices')
DATASETS = CORE_DATASETS + ('price_supplements', 'surveillance_daily', 'current_master', 'current_nifty50', 'current_nifty500')
BLOCK_CODES = {401, 403, 429}


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


@contextlib.contextmanager
def collector_lock(root):
    """OS releases the lock on Ctrl+C/crash; no stale PID-file guessing."""
    path = Path(root) / '.collector.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as f:
        f.seek(0, 2)
        if f.tell() == 0:
            f.write(b'0'); f.flush()
        f.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SourceError('collector_already_running') from None
        try:
            yield
        finally:
            f.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)


class PoliteHTTP(OfficialHTTP):
    """One persistent session, disk response cache, durable pacing and circuit breaker.

    No proxy/IP rotation, CAPTCHA handling or access-control bypass. A blocked
    response ends this run; the next command also respects the saved cooldown.
    """
    def __init__(self, root, *, interval=1.5, clock=time.time, sleep=time.sleep,
                 opener=None, reuse_dirs=(), existing_lake=None):
        self.root = Path(root)
        self.clock, self.sleep = clock, sleep
        self.interval = max(1.0, interval)
        self.paused = False
        self.cache_hits = self.requests = 0
        self.reuse_dirs = tuple(Path(p) for p in reuse_dirs)
        self.opener = opener or urllib.request.build_opener(
            SafeRedirect(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.network = sqlite3.connect(self.root / 'http_cache.sqlite')
        self.network.execute('create table if not exists responses (url text primary key, path text, sha text, observed text)')
        self.network.execute('create table if not exists hosts (host text primary key, next_at real, blocked_until real)')
        self.session_ready = False
        self.existing_lake = existing_lake
        self.zip_handles = {}

    def close(self):
        for archive in self.zip_handles.values():
            archive.close()
        self.network.close()

    def _pace(self, url):
        host = urllib.parse.urlsplit(url).hostname
        # All NSE hosts share one budget; switching hostnames never avoids a block.
        group = 'nse' if host.endswith('nseindia.com') or host.endswith('niftyindices.com') else host
        row = self.network.execute('select next_at,blocked_until from hosts where host=?', (group,)).fetchone()
        now = self.clock()
        if self.paused or (row and row[1] > now):
            self.paused = True
            raise SourceError('source_cooldown_active')
        delay = max(0, row[0]-now) if row else 0
        if delay:
            self.sleep(delay)
        with self.network:
            self.network.execute('insert or replace into hosts values(?,?,0)',
                                 (group, self.clock()+self.interval+random.uniform(0, .25)))
        return group

    def _pause(self, group, code, headers=None):
        now = self.clock()
        delay = 3600 if code in (401, 403) else 900
        raw = headers.get('Retry-After') if headers else None
        if raw:
            try:
                delay = max(delay, float(raw))
            except ValueError:
                try:
                    delay = max(delay, email.utils.parsedate_to_datetime(raw).timestamp()-now)
                except (TypeError, ValueError, OverflowError):
                    pass
        with self.network:
            self.network.execute('insert or replace into hosts values(?,?,?)',
                                 (group, now+self.interval, now+delay))
        self.paused = True
        raise SourceError('source_http_' + str(code))

    def _request(self, url, *, html=False):
        for attempt in range(3):
            group = self._pace(url)
            request = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0', 'Referer': NSE+'/',
                'Accept': 'application/json,text/csv,application/xml,*/*'})
            self.requests += 1
            try:
                with self.opener.open(request, timeout=25) as response:
                    allowed_url(response.geturl())
                    body = response.read(MAX_BYTES+1)
                if not body or len(body) > MAX_BYTES:
                    raise SourceError('source_empty_or_too_large')
                if not html and body.lstrip().lower().startswith((b'<html', b'<!doctype html')):
                    self._pause(group, 403)
                return Document(url, body, hashlib.sha256(body).hexdigest(),
                                datetime.now(timezone.utc), len(body))
            except urllib.error.HTTPError as exc:
                if exc.code in BLOCK_CODES:
                    self._pause(group, exc.code, exc.headers)
                if exc.code not in (500, 502, 503, 504) or attempt == 2:
                    raise SourceError('source_http_'+str(exc.code)) from None
                retry_after = exc.headers.get('Retry-After') if exc.headers else None
                if retry_after:
                    # Respect server-requested waiting periods by stopping, not
                    # blocking the terminal or issuing an early retry.
                    self._pause(group, exc.code, exc.headers)
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
                if attempt == 2:
                    raise SourceError('source_transport_unavailable') from None
            self.sleep(2**(attempt+2)+random.uniform(0, 1))
        raise SourceError('source_transport_unavailable')

    def fetch(self, url, *, financial_render=False):
        allowed_url(url)
        row = self.network.execute('select path,sha,observed from responses where url=?', (url,)).fetchone()
        if row:
            path = self.root / 'response_cache' / row[0]
            dynamic = any(part in url for part in ('/api/', '/EQUITY_L.csv', '/IndexConstituent/'))
            if path.is_file() and (not dynamic or
                    self.clock()-datetime.fromisoformat(row[2]).timestamp() < 86400):
                body = path.read_bytes()
                if hashlib.sha256(body).hexdigest() == row[1]:
                    self.cache_hits += 1
                    return Document(url, body, row[1], datetime.fromisoformat(row[2]), len(body))
        # Reuse the owner's existing XML cache only at the exact source basename.
        # Bytes remain evidence; the filing parser still validates identity/period.
        name = Path(urllib.parse.urlsplit(url).path).name
        if name.lower().endswith('.xml'):
            if self.existing_lake is not None:
                from .existing_dataset import read_cached_xml
                body = read_cached_xml(self.existing_lake,name,self.zip_handles)
                if body and (b'<xbrl' in body.lower() or b':xbrl' in body.lower()):
                    self.cache_hits += 1
                    return self._cache(Document(url,body,hashlib.sha256(body).hexdigest(),
                                               datetime.now(timezone.utc),len(body)))
            for folder in self.reuse_dirs:
                path = folder / name
                if path.is_file() and 0 < path.stat().st_size <= MAX_BYTES:
                    body = path.read_bytes()
                    if b'<xbrl' in body.lower() or b':xbrl' in body.lower():
                        self.cache_hits += 1
                        return self._cache(Document(url, body, hashlib.sha256(body).hexdigest(),
                                                   datetime.now(timezone.utc), len(body)))
        if urllib.parse.urlsplit(url).hostname == 'www.nseindia.com' and not self.session_ready:
            self._request(NSE+'/', html=True)
            self.session_ready = True
        return self._cache(self._request(url, html=financial_render))

    def _cache(self, doc):
        folder = self.root / 'response_cache'
        folder.mkdir(exist_ok=True)
        name = doc.sha256+'.bin'
        path = folder / name
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != doc.sha256:
            tmp = path.with_suffix('.tmp')
            tmp.write_bytes(doc.body); tmp.replace(path)
        with self.network:
            self.network.execute('insert or replace into responses values(?,?,?,?)',
                                 (doc.url, name, doc.sha256, doc.observed_at.isoformat()))
        return doc


class FreeLake(Lake):
    def __init__(self, root):
        super().__init__(root)
        from .existing_dataset import SCHEMA
        self.db.executescript(SCHEMA)

    def plan_free(self, start, end):
        plan = self.plan(start, end)
        with self.db:
            # Legacy annual statements can contain cash-flow/balance-sheet facts
            # absent from quarterly submissions. Integrated filings share one queue.
            months = self.db.execute("select start,end from jobs where dataset='financial_discovery' and end<'2025-03-01'").fetchall()
            for a,b in months:
                self.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',
                                ('annual_financial_discovery',a,b))
        # The consolidated REG_IND daily file is a modern source. Do not invent
        # pre-2024 history or interpret an absent file as no surveillance.
        day = max(date.fromisoformat(plan['price_start']), date(2024, 1, 1))
        with self.db:
            while day <= end:
                self.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',
                                ('surveillance_daily', str(day), str(day)))
                day += timedelta(days=1)
        with self.db:
            for dataset in ('current_master', 'current_nifty50', 'current_nifty500'):
                self.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',
                                (dataset, str(end), str(end)))
        from .existing_dataset import apply_missing_plan
        apply_missing_plan(self)
        return plan

    def _job(self, dataset, start, end, http):
        if dataset == 'price_supplements':
            from .local_lake import legacy_url, parse_legacy
            from .cloud_sources import bhavcopy_url, parse_bhavcopy
            day = date.fromisoformat(start)
            legacy = day < date(2024,7,8)
            doc = http.fetch(legacy_url(day) if legacy else bhavcopy_url(day))
            rows = parse_legacy(doc.body,day) if legacy else parse_bhavcopy(doc.body,day)
            digest = self.archive(doc)
            existing = {r[0] for r in self.db.execute('select symbol from local_prices where trade_date=?',(start,))}
            missing = [r for r in rows if r['symbol'] not in existing]
            self._store_prices(missing,digest)
            self.db.executemany('insert or ignore into price_metadata values(?,?,?,?,?,?,?)',
                                [(start,r['symbol'],r['isin'],r['series'],str(r['turnover']),
                                  int(r['volume']),digest) for r in rows if r['symbol'] in existing])
            return len(rows),[digest]
        if dataset == 'indices':
            from .cloud_sources import BENCHMARKS, rows_csv, parse_day, number
            day = date.fromisoformat(start)
            doc = http.fetch(ARCHIVE+'/content/indices/ind_close_all_'+day.strftime('%d%m%Y')+'.csv')
            raw = rows_csv(doc.body, ['Index Name', 'Index Date', 'Open Index Value',
                                    'High Index Value', 'Low Index Value', 'Closing Index Value'])
            names = {name.casefold(): code for name, code in BENCHMARKS.items()}
            rows, seen = [], set()
            digest = self.archive(doc)
            for r in raw:
                name = r['Index Name']
                if not (name.casefold().startswith('nifty') or name.casefold() == 'india vix'):
                    continue
                if parse_day(r['Index Date']) != day:
                    raise SourceError('index_wrong_date')
                code = names.get(name.casefold(), 'INDEX:'+name.upper())
                if code in seen:
                    raise SourceError('index_duplicate')
                seen.add(code)
                c = number(r['Closing Index Value'], minimum=Decimal('0.00000001'))
                o,h,l = [None if r[k] in ('','-','NA','N/A') else number(r[k], minimum=Decimal('0.00000001'))
                         for k in ('Open Index Value','High Index Value','Low Index Value')]
                if all(v is not None for v in (o,h,l)) and (l>min(o,c) or h<max(o,c) or h<l):
                    raise SourceError('index_invalid_ohlc')
                rows.append((start,code,*(str(v) if v is not None else None for v in (o,h,l,c)),digest))
            if not rows:
                raise SourceError('indices_empty')
            self.db.executemany('insert or ignore into indices values(?,?,?,?,?,?,?)', rows)
            return len(rows), [digest]
        if dataset == 'current_master':
            self.refresh_current_master(http)
            doc = http.fetch(ARCHIVE+'/content/equities/EQUITY_L.csv')
            return self.db.execute('select count(*) from company_targets').fetchone()[0], [self.archive(doc)]
        if dataset in ('current_nifty50', 'current_nifty500'):
            from .cloud_sources import rows_csv
            name = 'ind_nifty50list.csv' if dataset == 'current_nifty50' else 'ind_nifty500list.csv'
            doc = http.fetch('https://www.niftyindices.com/IndexConstituent/'+name)
            rows = rows_csv(doc.body, ['Symbol', 'Industry', 'ISIN Code'])
            digest = self.archive(doc)
            self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                            (dataset, digest, str(doc.observed_at.date()), doc.observed_at.isoformat(),
                             dumps({'rows': rows, 'historical_authority': False}), digest))
            return len(rows), [digest]
        if dataset != 'surveillance_daily':
            return super()._job(dataset, start, end, http)
        day = date.fromisoformat(start)
        doc = http.fetch(ARCHIVE+'/content/cm/REG_IND'+day.strftime('%d%m%y')+'.csv')
        rows = parse_surveillance(doc.body)
        digest = self.archive(doc)
        # A dated snapshot is not an inferred continuous interval. It records
        # observed archive evidence, not an unverified historical publication time.
        self.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                        ('surveillance_daily', start, start, None,
                         dumps({'states': rows, 'coverage': 'DATED_SNAPSHOT_AUDIT_REQUIRED'}), digest))
        return len(rows), [digest]

    def fetch(self, *, datasets=('prices','indices'), limit=100, retry=False,
              start=None, end=None, http_factory=None, exact=False):
        # Use the inherited transactional engine for its standard dataset set.
        if set(datasets) <= set(CORE_DATASETS):
            return super().fetch(datasets=datasets, limit=limit, retry=retry,
                                 start=start, end=end, http_factory=http_factory, exact=exact)
        if len(datasets) != 1 or datasets[0] not in DATASETS:
            raise ValueError('Collect supplemental sources in their own phase')
        statuses = "('PENDING','FAILED','MISSING','PARTIAL')" if retry else "('PENDING')"
        query = 'select * from jobs where dataset=? and status in '+statuses
        params = [datasets[0]]
        if start:
            query += ' and start'+('=?' if exact else '>=?'); params.append(str(start))
        if end:
            query += ' and end'+('=?' if exact else '<=?'); params.append(str(end))
        jobs = self.db.execute(query+' order by start limit ?', params+[limit]).fetchall()
        result = []
        for job in jobs:
            key = (job['dataset'], job['start'], job['end'])
            try:
                with self.db:
                    count, receipts = self._job(*key, http_factory())
                    for receipt in receipts:
                        self.db.execute('insert or ignore into job_receipts values(?,?,?,?)', (*key,receipt))
                    self.db.execute("update jobs set status='STAGED',rows=?,attempts=attempts+1,error=NULL,updated_at=? where dataset=? and start=? and end=?",
                                    (count, stamp(), *key))
                result.append(dict(dataset=key[0], start=key[1], status='STAGED', rows=count))
            except Exception as exc:
                code = str(exc) if isinstance(exc, SourceError) else 'fetch_or_parse_failed'
                status = 'MISSING' if code == 'source_http_404' else 'FAILED'
                with self.db:
                    self.db.execute('update jobs set status=?,attempts=attempts+1,error=?,updated_at=? where dataset=? and start=? and end=?',
                                    (status, code, stamp(), *key))
                result.append(dict(dataset=key[0], start=key[1], status=status, error=code))
        return result


def progress(label, completed, total, outcome='', stream=None):
    stream = stream or sys.stdout
    ratio = completed/total if total else 1
    filled = min(24, int(24*ratio))
    line = f'{label:24} [{"#"*filled}{"-"*(24-filled)}] {ratio:6.1%} {completed}/{total} {outcome}'
    print(line, file=stream, flush=True)


def run_collection(lake, http, *, start, end, retry=False, progress_fn=progress):
    plan = lake.plan_free(start, end)
    if hasattr(http, 'network') and http.network.execute(
            'select count(*) from hosts where blocked_until>?', (http.clock(),)).fetchone()[0]:
        http.paused = True
        return 'PAUSED_BY_SOURCE'
    seed = Path(__file__).resolve().parents[2]/'config/company_universe_seed.csv'
    lake.seed_targets(seed)
    # Retry each previous failed document once per explicit retry run, never loop.
    if retry:
        with lake.db:
            lake.db.execute("update documents set status='PENDING' where status='FAILED'")
    statuses = ('PENDING','FAILED','MISSING','PARTIAL') if retry else ('PENDING',)
    placeholders = ','.join('?' for _ in statuses)
    scope = ('start>=? and end<=?')
    first = min(plan['price_start'], plan['fact_start'])
    todo = lake.db.execute('select dataset,start,end from jobs where status in ('+placeholders+
                           ') and '+scope+' order by start,dataset', (*statuses,first,str(end))).fetchall()
    todo = [r for r in todo if r['dataset'] in DATASETS]
    total, processed = len(todo), 0
    progress_fn('Download tasks', 0, total, 'processed is not successful coverage')
    failures = []
    def process_document():
        out=lake.documents(limit=1,http_factory=lambda: http,pending_only=True)
        status=out[0]['status'] if out else 'SKIPPED'
        reason=out[0].get('error','') if out else ''
        failures.append(status=='FAILED')
        del failures[:-20]
        bad_run=(len(failures)>=5 and all(failures[-5:])) or (len(failures)>=20 and sum(failures)>=15)
        return status,reason,bad_run
    for dataset in DATASETS:
        selected = [r for r in todo if r['dataset'] == dataset]
        for r in selected:
            out = lake.fetch(datasets=(dataset,), limit=1, retry=retry,
                             start=date.fromisoformat(r['start']), end=date.fromisoformat(r['end']),
                             http_factory=lambda: http, exact=True)
            processed += 1
            status = out[0]['status'] if out else 'SKIPPED'
            progress_fn('Download tasks', processed, total, f'{dataset} {r["start"]} {status}')
            atomic_json(lake.root/'progress.json', dict(phase=dataset, processed=processed,
                        total=total, last_status=status, updated_at=stamp()))
            if http.paused:
                return 'PAUSED_BY_SOURCE'
        if dataset in ('financial_discovery','annual_financial_discovery','shareholding_discovery') and selected:
            count = lake.db.execute("select count(*) from documents where status='PENDING'").fetchone()[0]
            progress_fn('Discovered documents', 0, count)
            for i in range(count):
                status,reason,bad_run=process_document()
                progress_fn('Discovered documents', i+1, count, status+' '+reason)
                if http.paused:
                    return 'PAUSED_BY_SOURCE'
                if bad_run:
                    return 'PAUSED_DOCUMENT_FAILURES'
    # Also drain a queue left by an interrupted discovery/doc phase.
    remaining=lake.db.execute("select count(*) from documents where status='PENDING'").fetchone()[0]
    for i in range(remaining):
        status,reason,bad_run=process_document()
        progress_fn('Remaining documents',i+1,remaining,status+' '+reason)
        if http.paused:
            return 'PAUSED_BY_SOURCE'
        if bad_run:
            return 'PAUSED_DOCUMENT_FAILURES'
    return 'WORK_QUEUE_PROCESSED'


def write_report(lake, http, outcome):
    report = lake.status()
    from .existing_dataset import import_summary
    report['existing_data'] = import_summary(lake)
    report.update(run_outcome=outcome, updated_at=stamp(), network_requests=http.requests,
                  response_cache_hits=http.cache_hits,
                  source_errors=[dict(r) for r in lake.db.execute(
                      'select dataset,status,error,count(*) tasks from jobs where error is not null group by dataset,status,error')],
                  limitations=[
                      'Processed tasks are not a certificate of complete history.',
                      'No complete free historical adapters for pre-2024 ASM/GSM, dated sector/index membership, identity changes or costs.',
                      'Surveillance daily snapshots remain separate from audited continuous intervals.',
                      'HTTP 404 is unknown/missing, never automatically a holiday.',
                      'Legacy listing pagination, dates, financial periods, revisions and original publication times require coverage audit.',
                      'Uploaded OHLCV is preserved separately with unverified adjustment basis; raw exchange rows supplement missing values without overwriting originals.',
                      'This SQLite acquisition lake does not replace the PostgreSQL PMS replay database.'])
    from .collection_diagnostics import diagnose
    atomic_json(lake.root/'document_diagnostics.json',diagnose(lake))
    atomic_json(lake.root/'coverage_report.json', report)
    with (lake.root/'failed_tasks.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(('kind','dataset','start_or_key','end','status','error'))
        for r in lake.db.execute("select dataset,start,end,status,error from jobs where status in ('FAILED','MISSING','PARTIAL')"):
            writer.writerow(('download',*tuple(r)))
        for r in lake.db.execute("select dataset,key,status,error from documents where status='FAILED'"):
            writer.writerow(('document',r['dataset'],r['key'],'',r['status'],r['error']))
    with (lake.root/'import_issues.csv').open('w',newline='',encoding='utf-8') as f:
        writer=csv.writer(f)
        writer.writerow(('source','member','row_number','error'))
        writer.writerows(tuple(r) for r in lake.db.execute('select * from import_issues order by source,member,row_number'))
    return report
