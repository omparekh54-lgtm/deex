from datetime import date
import json

from packages.data_layer.collection_diagnostics import diagnose
from packages.data_layer.exchange_financials import NSEIntegratedFinancialsProvider, _scope
from packages.data_layer.free_pipeline import FreeLake, run_collection
from test_local_lake import doc


def legacy_record():
    return dict(symbol='BHARATGEAR',toDate='31-Dec-2018',fromDate='01-Oct-2018',
                broadCastDate='31-Jan-2019 20:32:23',exchdisstime='31-Jan-2019 20:36:03',
                consolidated='Non-Consolidated',seqNumber='1054677',period='Quarterly',
                xbrl='https://nsearchives.nseindia.com/corporate/legacy.xml')


def test_legacy_exchange_metadata_aliases_preserve_period_identity_and_scope():
    p=object.__new__(NSEIntegratedFinancialsProvider)
    r=p._parse_row(legacy_record())
    assert r.period_end==date(2018,12,31)
    assert r.external_id=='1054677'
    assert r.statement_scope=='standalone'
    assert r.filed_at.hour==20 and r.filed_at.minute==36
    assert r.filing_kind=='quarter'
    a=legacy_record();a['period']='Annual'
    assert p._parse_row(a).filing_kind=='annual'


def test_non_consolidated_is_not_substring_matched_as_consolidated():
    assert _scope('Non-Consolidated')=='standalone'
    assert _scope('Standalone')=='standalone'
    assert _scope('Consolidated')=='consolidated'


def test_bad_identity_is_reported_before_any_download(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:l.db.execute("insert into documents(dataset,key,url,payload) values('financials','bad','https://nsearchives.nseindia.com/file.xml','{}')")
    def no_factory():raise AssertionError('Invalid metadata must not cause a network request')
    result=l.documents(limit=1,pending_only=True,http_factory=no_factory)
    assert result[0]['error']=='financial_identity_missing'
    d=diagnose(l)
    assert d['failed_samples'][0]['error_type']=='SourceError'
    assert d['failed_samples'][0]['frames']
    l.close()


def test_undefined_xml_contexts_remain_blocked_and_raw_is_saved(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:l.db.execute("insert into documents(dataset,key,url,payload) values('financials','legacy','https://nsearchives.nseindia.com/file.xml',?)",(json.dumps(legacy_record()),))
    body=b'''<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:f="https://example.invalid/financials">
    <xbrli:unit id="INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>
    <f:RevenueFromOperations contextRef="OneD" unitRef="INR">100</f:RevenueFromOperations>
    <f:ProfitLossForPeriod contextRef="OneD" unitRef="INR">10</f:ProfitLossForPeriod></xbrli:xbrl>'''
    class HTTP:
        def fetch(self,url):return doc(body,url)
    result=l.documents(limit=1,pending_only=True,http_factory=HTTP)
    assert result[0]['error']=='financial_core_facts_or_contexts_missing'
    assert l.db.execute("select count(*) from records where dataset='financials'").fetchone()[0]==0
    assert l.db.execute("select count(*) from records where dataset='financials_unparsed_raw'").fetchone()[0]==1
    assert l.verify()['ok']
    l.close()


def test_failure_circuit_stops_after_five_without_processing_huge_queue(tmp_path,monkeypatch):
    l=FreeLake(tmp_path)
    day=date(2016,10,24)
    monkeypatch.setattr(l,'plan_free',lambda a,b:dict(price_start=str(day),fact_start=str(day)))
    with l.db:
        for i in range(30):
            l.db.execute("insert into documents(dataset,key,url,payload) values('financials',?,'https://nsearchives.nseindia.com/file.xml','{}')",(str(i),))
    class HTTP:paused=False
    result=run_collection(l,HTTP(),start=day,end=day,progress_fn=lambda *a:None)
    assert result=='PAUSED_DOCUMENT_FAILURES'
    assert l.db.execute("select count(*) from documents where status='FAILED'").fetchone()[0]==5
    assert l.db.execute("select count(*) from documents where status='PENDING'").fetchone()[0]==25
    l.close()


def test_offline_diagnostics_preserve_jobs_and_strip_url_queries(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:
        l.db.execute("insert into documents(dataset,key,url,payload,status,error) values('financials','bad','https://nsearchives.nseindia.com/file.xml?token=SECRET','{}','FAILED','document_parse_or_fetch_failed')")
    before=list(l.db.execute('select * from documents'))
    d=diagnose(l)
    assert 'SECRET' not in json.dumps(d)
    assert [tuple(r) for r in l.db.execute('select * from documents')]==[tuple(r) for r in before]
    assert d['failed_samples'][0]['error']=='document_parse_or_fetch_failed'
    l.close()


def test_sample_retry_selects_only_requested_failed_document(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:
        for key in ('a','b','c'):
            l.db.execute("insert into documents(dataset,key,url,payload,status) values('financials',?,'https://nsearchives.nseindia.com/file.xml','{}','FAILED')",(key,))
    l.documents(limit=1,job_identity=('financials','b'))
    assert l.db.execute("select error from documents where key='a'").fetchone()[0] is None
    assert l.db.execute("select error from documents where key='b'").fetchone()[0]=='financial_identity_missing'
    assert l.db.execute("select error from documents where key='c'").fetchone()[0] is None
    l.close()
