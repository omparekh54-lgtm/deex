from datetime import date, datetime, timezone
import hashlib
import io
import json
import urllib.error

import pytest

from packages.data_layer.cloud_sources import Document, SourceError
from packages.data_layer.free_pipeline import (FreeLake, PoliteHTTP, collector_lock,
                                               run_collection, write_report)
from test_local_lake import legacy


class Response:
    def __init__(self, body, url):
        self.body, self.url = body, url
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def read(self, n):
        return self.body[:n]
    def geturl(self):
        return self.url


class Clock:
    def __init__(self):
        self.now = 1000.
    def __call__(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


def test_cached_receipts_resume_without_network_and_detect_corruption(tmp_path):
    clock = Clock()
    calls = []
    class Opener:
        def open(self, req, timeout):
            calls.append(req.full_url)
            return Response(b'valid csv', req.full_url)
    url = 'https://nsearchives.nseindia.com/content/test.csv'
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    assert h.fetch(url).body == b'valid csv'
    h.close()
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    assert h.fetch(url).body == b'valid csv'
    assert len(calls) == 1 and h.cache_hits == 1
    next((tmp_path/'response_cache').glob('*.bin')).write_bytes(b'corrupt')
    assert h.fetch(url).body == b'valid csv'
    assert len(calls) == 2
    assert h.fetch(url).body == b'valid csv'
    assert len(calls) == 2  # The corrupt cache was repaired.
    h.close()


def test_429_stops_immediately_and_cooldown_survives_restart(tmp_path):
    clock = Clock()
    calls = []
    class Opener:
        def open(self, req, timeout):
            calls.append(req.full_url)
            raise urllib.error.HTTPError(req.full_url, 429, 'rate limit', {'Retry-After':'7200'}, None)
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    with pytest.raises(SourceError, match='source_http_429'):
        h.fetch('https://nsearchives.nseindia.com/a.csv')
    assert len(calls) == 1
    h.close()
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    # Changing exchange host never bypasses its saved cooldown.
    with pytest.raises(SourceError, match='source_cooldown_active'):
        h.fetch('https://www.nseindia.com/api/anything')
    assert len(calls) == 1
    clock.now += 7201
    h.close()


def test_retry_after_http_date_and_access_denied_no_retry(tmp_path):
    clock = Clock()
    class Opener:
        def open(self, req, timeout):
            raise urllib.error.HTTPError(req.full_url, 403, 'denied', {}, None)
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    with pytest.raises(SourceError, match='source_http_403'):
        h.fetch('https://nsearchives.nseindia.com/a.csv')
    assert h.requests == 1 and h.paused
    h.close()


def test_collection_progress_resume_and_missing_is_not_success(tmp_path, monkeypatch):
    l = FreeLake(tmp_path)
    day = date(2016,10,24)
    monkeypatch.setattr(l, 'plan_free', lambda a,b: {'price_start':str(day),'fact_start':str(day)})
    with l.db:
        l.db.execute('insert into jobs(dataset,start,end) values(?,?,?)', ('prices',str(day),str(day)))
        l.db.execute('insert into jobs(dataset,start,end) values(?,?,?)', ('indices',str(day),str(day)))
    class HTTP:
        paused = False
        requests = cache_hits = 0
        def fetch(self,url):
            self.requests += 1
            if 'ind_close' in url:
                raise SourceError('source_http_404')
            body = legacy(day)
            return Document(url,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    h = HTTP()
    updates = []
    assert run_collection(l,h,start=day,end=day,progress_fn=lambda *a: updates.append(a)) == 'WORK_QUEUE_PROCESSED'
    assert updates[-1][1:3] == (2,2)
    assert l.db.execute("select status from jobs where dataset='indices'").fetchone()[0] == 'MISSING'
    first = h.requests
    run_collection(l,h,start=day,end=day,progress_fn=lambda *a: None)
    assert h.requests == first
    run_collection(l,h,start=day,end=day,retry=True,progress_fn=lambda *a: None)
    assert h.requests == first+1  # Failed job tried once, never loops.
    report = write_report(l,h,'WORK_QUEUE_PROCESSED')
    assert report['status'] == 'NOT_CERTIFIED_FOR_BACKTEST'
    assert 'source_http_404' in (tmp_path/'failed_tasks.csv').read_text()
    l.close()


def test_pending_documents_do_not_retry_failed_forever(tmp_path):
    l = FreeLake(tmp_path)
    with l.db:
        l.db.execute("insert into documents(dataset,key,url,payload,status) values('financials','bad','https://nsearchives.nseindia.com/bad.xml','{}','FAILED')")
    class HTTP:
        def fetch(self, url):
            raise AssertionError('Failed documents should not be selected')
    assert l.documents(pending_only=True,http_factory=HTTP) == []
    l.close()


def test_only_one_writer(tmp_path):
    with collector_lock(tmp_path):
        with pytest.raises(SourceError, match='collector_already_running'):
            with collector_lock(tmp_path):
                pass
    with collector_lock(tmp_path):
        pass


def test_extra_sector_indices_cannot_hide_missing_benchmarks(tmp_path):
    l = FreeLake(tmp_path)
    day = date(2016,10,24)
    l.plan_free(day,day)
    lines = ['Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value']
    for name in ('Nifty 50','Nifty Bank','Nifty IT','Nifty Pharma','Nifty Auto'):
        lines.append(name+',24-10-2016,10,12,9,11')
    body = ('\n'.join(lines)+'\n').encode()
    class HTTP:
        def fetch(self,url):
            return Document(url,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    result = l.fetch(datasets=('indices',),start=day,end=day,http_factory=HTTP)
    assert result[0]['status'] == 'PARTIAL'
    assert l.db.execute('select count(*) from indices').fetchone()[0] == 5
    l.close()


def test_surveillance_404_does_not_imply_clear_history(tmp_path):
    l = FreeLake(tmp_path)
    day = date(2024,1,2)
    l.plan_free(day,day)
    class HTTP:
        def fetch(self,url):
            raise SourceError('source_http_404')
    out = l.fetch(datasets=('surveillance_daily',),start=day,end=day,http_factory=HTTP)
    assert out[0]['status'] == 'MISSING'
    assert l.db.execute("select count(*) from records where dataset='surveillance_daily'").fetchone()[0] == 0
    l.close()


def test_html_challenge_pauses_instead_of_retrying(tmp_path):
    clock = Clock()
    class Opener:
        def open(self, req, timeout):
            return Response(b'<!doctype html><html>challenge</html>', req.full_url)
    h = PoliteHTTP(tmp_path, opener=Opener(), clock=clock, sleep=clock.sleep)
    with pytest.raises(SourceError, match='source_http_403'):
        h.fetch('https://nsearchives.nseindia.com/data.csv')
    assert h.paused and h.requests == 1
    h.close()


def test_existing_xml_is_reused_and_original_bytes_retained(tmp_path):
    cache = tmp_path/'old_cache'
    cache.mkdir()
    xml = b'<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"/>'
    (cache/'filing.xml').write_bytes(xml)
    class Opener:
        def open(self, *a, **kw):
            raise AssertionError('An existing XML should not be downloaded again')
    h = PoliteHTTP(tmp_path, opener=Opener(), reuse_dirs=[cache])
    result = h.fetch('https://nsearchives.nseindia.com/corporate/filing.xml')
    assert result.body == xml and h.cache_hits == 1
    assert result.sha256 == hashlib.sha256(xml).hexdigest()
    h.close()


def test_annual_discovery_uses_annual_endpoint_and_queues_financial_document(tmp_path):
    l = FreeLake(tmp_path)
    day = date(2024,1,2)
    l.plan_free(day,day)
    class HTTP:
        def api(self,path,**params):
            assert path == 'corporates-financial-results' and params['period'] == 'Annual'
            body = json.dumps([{'symbol':'EXAMPLE','xbrl':'https://nsearchives.nseindia.com/original.xml'}]).encode()
            return Document('https://www.nseindia.com/api/'+path,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    result = l.fetch(datasets=('annual_financial_discovery',),start=date(2024,1,1),end=day,http_factory=HTTP)
    assert result[0]['status'] == 'STAGED'
    assert l.db.execute('select dataset from documents').fetchone()[0] == 'financials'
    l.close()


def test_retry_selects_exact_monthly_window(tmp_path):
    l = FreeLake(tmp_path)
    with l.db:
        for end in ('2024-01-10','2024-01-20'):
            l.db.execute("insert into jobs(dataset,start,end,status) values('actions','2024-01-01',?,'FAILED')",(end,))
    calls = []
    def fail(dataset,start,end,http):
        calls.append(end)
        raise SourceError('source_transport_unavailable')
    l._job = fail
    for end in ('2024-01-10','2024-01-20'):
        l.fetch(datasets=('actions',),start=date(2024,1,1),end=date.fromisoformat(end),
                retry=True,limit=1,http_factory=lambda: None,exact=True)
    assert calls == ['2024-01-10','2024-01-20']
    l.close()
