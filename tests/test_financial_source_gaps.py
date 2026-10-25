from datetime import date,datetime
import json

from packages.data_layer.cloud_sources import SourceError
from packages.data_layer.collection_diagnostics import diagnose,quarantine_placeholders
from packages.data_layer.exchange_financials import NSEIntegratedFinancialsProvider,filings_from_exchange_record
from packages.data_layer.free_pipeline import FreeLake,run_collection
from packages.data_layer.xbrl import parse_xbrl_facts,extract_canonical_financials
from test_local_lake import doc,legacy
from test_collection_diagnostics import legacy_record


def financial_xml(quarter=True, inconsistent=True, contexts=True):
    start='2024-07-01' if quarter else '2024-04-01'
    one=f'''<x:context id="OneD"><x:entity><x:identifier scheme="NSESymbol">EXAMPLE</x:identifier></x:entity><x:period><x:startDate>{start}</x:startDate><x:endDate>2024-09-30</x:endDate></x:period></x:context>'''
    four='''<x:context id="FourD"><x:entity><x:identifier scheme="NSESymbol">EXAMPLE</x:identifier></x:entity><x:period><x:startDate>2024-07-01</x:startDate><x:endDate>2024-09-30</x:endDate></x:period></x:context>'''
    return f'''<x:xbrl xmlns:x="http://www.xbrl.org/2003/instance" xmlns:f="https://example.invalid/facts">
    {one if contexts else ''}{four if contexts and quarter else ''}
    <x:unit id="INR"><x:measure>iso4217:INR</x:measure></x:unit>
    <f:ReportingQuarter contextRef="OneD">Half yearly</f:ReportingQuarter>
    <f:DateOfStartOfReportingPeriod contextRef="OneD">{start}</f:DateOfStartOfReportingPeriod>
    <f:DateOfEndOfReportingPeriod contextRef="OneD">2024-09-30</f:DateOfEndOfReportingPeriod>
    <f:RevenueFromOperations contextRef="OneD" unitRef="INR">258913000</f:RevenueFromOperations>
    <f:ProfitLossForPeriod contextRef="OneD" unitRef="INR">35279000</f:ProfitLossForPeriod>
    {('<f:DateOfStartOfReportingPeriod contextRef="FourD">2024-04-01</f:DateOfStartOfReportingPeriod><f:DateOfEndOfReportingPeriod contextRef="FourD">2024-09-30</f:DateOfEndOfReportingPeriod>' if inconsistent else '') if quarter else ''}
    {('<f:RevenueFromOperations contextRef="FourD" unitRef="INR">668673000</f:RevenueFromOperations><f:ProfitLossForPeriod contextRef="FourD" unitRef="INR">4172000</f:ProfitLossForPeriod>') if quarter else ''}
    </x:xbrl>'''.encode()


def record():
    raw=legacy_record();raw.update(symbol='EXAMPLE',toDate='30-Sep-2024',exchdisstime='13-Nov-2024 20:11:12')
    return object.__new__(NSEIntegratedFinancialsProvider)._parse_row(raw)


def test_halfyear_label_does_not_discard_valid_quarter_or_select_contradictory_ytd():
    values=filings_from_exchange_record(record(),financial_xml())
    assert len(values)==1 and values[0].filing_kind=='quarter'
    assert str(values[0].sales)=='25.8913' and str(values[0].pat)=='3.5279'
    facts=parse_xbrl_facts(financial_xml())
    assert all(not f.context_valid for f in facts if f.context_id=='FourD')


def test_unexplained_conflicting_entity_totals_stay_unavailable():
    values=extract_canonical_financials(parse_xbrl_facts(financial_xml(inconsistent=False)),date(2024,9,30),statement_scope='standalone')
    assert values['sales'] is None and values['pat'] is None


def test_reporting_date_facts_never_create_missing_contexts():
    fs=parse_xbrl_facts(financial_xml(contexts=False))
    assert not any(f.context_valid for f in fs)
    assert extract_canonical_financials(fs,date(2024,9,30))['sales'] is None


def test_genuine_halfyear_duration_is_preserved():
    values=filings_from_exchange_record(record(),financial_xml(quarter=False))
    assert values[0].filing_kind=='half_year'
    assert str(values[0].sales)=='25.8913'


def test_placeholder_migration_preserves_rows_and_never_uses_network(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:
        for key,url in [('full','https://nsearchives.nseindia.com/corporate/xbrl/-'),('literal','-')]:
            l.db.execute("insert into documents(dataset,key,url,payload,status) values('financials',?,?,'{}','FAILED')",(key,url))
    assert quarantine_placeholders(l)==2
    assert l.db.execute('select count(*) from documents').fetchone()[0]==2
    assert l.db.execute("select count(*) from documents where status='UNAVAILABLE'").fetchone()[0]==2
    def no_http():raise AssertionError('No request for missing URL')
    assert l.documents(pending_only=True,http_factory=no_http)==[]
    result=l.documents(job_identity=('financials','full'),http_factory=no_http)
    assert result[0]['status']=='UNAVAILABLE' and result[0]['error']=='financial_attachment_missing'
    l.close()


def test_actual_404_is_a_reported_gap_not_a_transient_failure(tmp_path):
    l=FreeLake(tmp_path)
    with l.db:l.db.execute("insert into documents(dataset,key,url,payload) values('financials','missing','https://nsearchives.nseindia.com/missing.xml',?)",(json.dumps(legacy_record()),))
    class HTTP:
        def fetch(self,url):raise SourceError('source_http_404')
    out=l.documents(pending_only=True,http_factory=HTTP)
    assert out[0]['status']=='UNAVAILABLE'
    assert l.documents(pending_only=True,http_factory=HTTP)==[]
    assert diagnose(l)['failed_samples'][0]['error']=='source_http_404'
    l.close()


def test_independent_price_jobs_run_before_document_failure_circuit(tmp_path,monkeypatch):
    l=FreeLake(tmp_path);day=date(2016,10,24)
    monkeypatch.setattr(l,'plan_free',lambda a,b:dict(price_start=str(day),fact_start=str(day)))
    with l.db:
        l.db.execute("insert into jobs(dataset,start,end) values('prices',?,?)",(str(day),str(day)))
        for i in range(6):l.db.execute("insert into documents(dataset,key,url,payload) values('financials',?,'https://nsearchives.nseindia.com/file.xml','{}')",(str(i),))
    class HTTP:
        paused=False
        def fetch(self,url):return doc(legacy(day),url)
    outcome=run_collection(l,HTTP(),start=day,end=day,progress_fn=lambda *a:None)
    assert outcome=='PAUSED_DOCUMENT_FAILURES'
    assert l.db.execute('select count(*) from prices').fetchone()[0]==1
    l.close()


def test_discovery_records_missing_url_without_fetching_it(tmp_path):
    l=FreeLake(tmp_path);start=date(2024,1,1);end=date(2024,1,31);l.plan_free(start,end)
    class HTTP:
        def api(self,path,**params):
            return doc(json.dumps([{'xbrl':'https://nsearchives.nseindia.com/corporate/xbrl/-','symbol':'EXAMPLE'}]).encode())
    out=l.fetch(datasets=('financial_discovery',),start=start,end=end,http_factory=HTTP)
    assert out[0]['status']=='STAGED'
    assert l.db.execute('select status from documents').fetchone()[0]=='UNAVAILABLE'
    l.close()


def test_optional_annual_cashflow_does_not_discard_valid_quarter(tmp_path):
    l=FreeLake(tmp_path)
    raw=legacy_record();raw.update(symbol='EXAMPLE',toDate='30-Sep-2024',exchdisstime='13-Nov-2024 20:11:12')
    body=financial_xml().replace(b'</x:xbrl>',b'''<x:context id="AnnualD"><x:entity><x:identifier scheme="NSESymbol">EXAMPLE</x:identifier></x:entity><x:period><x:startDate>2023-10-01</x:startDate><x:endDate>2024-09-30</x:endDate></x:period></x:context><f:CashFlowsFromUsedInOperatingActivities contextRef="AnnualD" unitRef="INR">10000000</f:CashFlowsFromUsedInOperatingActivities></x:xbrl>''')
    with l.db:l.db.execute("insert into documents(dataset,key,url,payload) values('financials','quality','https://nsearchives.nseindia.com/file.xml',?)",(json.dumps(raw),))
    class HTTP:
        def fetch(self,url):return doc(body,url)
    out=l.documents(pending_only=True,http_factory=HTTP)
    assert out[0]['status']=='DONE' and out[0]['rows']==2
    values=[json.loads(r[0]) for r in l.db.execute("select payload from records where dataset='financials'")]
    annual=next(v for v in values if v['filing_kind']=='annual')
    assert annual['operating_cash_flow']=='1' and annual['pat'] is None
    l.close()


def test_integrated_missing_attachment_does_not_fail_entire_page(tmp_path):
    l=FreeLake(tmp_path);start=date(2026,9,1);end=date(2026,9,30);l.plan_free(start,end)
    class HTTP:
        def api(self,path,**params):
            payload={'totalCount':1,'page':0,'data':[{'symbol':'EXAMPLE','quarterEndDate':'30-Jun-2026','broadcastDateTime':'01-Sep-2026 10:00:00','id':'1','xbrl':'https://nsearchives.nseindia.com/corporate/xbrl/-'}]}
            return doc(json.dumps(payload).encode())
    out=l.fetch(datasets=('financial_discovery',),start=start,end=end,http_factory=HTTP)
    assert out[0]['status']=='STAGED'
    assert l.db.execute('select status,error from documents').fetchone()['status']=='UNAVAILABLE'
    l.close()
