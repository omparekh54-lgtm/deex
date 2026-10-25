from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pytest

from packages.data_layer.cloud_sources import Document
from packages.data_layer.existing_dataset import find_sources, import_source, import_summary
from packages.data_layer.free_pipeline import FreeLake, PoliteHTTP
from test_local_lake import legacy


HEADER = 'date,open,high,low,close,volume\n'
XML = b'<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"/>'


def dataset(path, invalid=False):
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('data/prices/OLDNAME.csv',HEADER+'2016-10-24,10,13,9,12,100\n'+
                   ('2016-10-25,10,9,11,12,100\n' if invalid else ''))
        z.writestr('data/indices/INDIAVIX.csv',HEADER+'2016-10-24,10,13,9,12,0\n')
        z.writestr('data/fundamentals/quarterly.csv',
                   'symbol,period_end,filed_ts,sales,op_profit,pat,eps_diluted,consolidated\n'
                   'OLDNAME,2016-06-30,2016-08-01 10:00:00,100,20,10,1,1\n')
        z.writestr('data/fundamentals/reference.csv',
                   'symbol,industry,is_financial,is_cyclical\nOLDNAME,Capital Goods,False,False\n')
        z.writestr('data/_cache/nse/filing.xml',XML)
    return path


def test_import_zip_offline_idempotent_and_invalid_rows_flagged(tmp_path):
    source = dataset(tmp_path/'data(2).zip',True)
    l = FreeLake(tmp_path/'lake')
    import_source(l,source)
    before = import_summary(l)
    assert before['local_price_rows'] == 1
    assert before['local_financial_rows'] == 1
    assert before['local_reference_rows'] == 1
    assert before['invalid_import_rows'] == 1
    assert before['xml_cache_entries'] == 1
    assert l.db.execute("select code from indices").fetchone()[0] == 'INDIAVIX'
    assert l.verify()['ok']
    import_source(l,source)
    after = import_summary(l)
    assert {k:v for k,v in after.items() if k!='sources'} == {k:v for k,v in before.items() if k!='sources'}
    l.close()


def test_import_detects_zip_and_directory_and_remembers_path(tmp_path):
    source = dataset(tmp_path/'data(2).zip')
    l = FreeLake(tmp_path/'lake')
    assert find_sources(l,tmp_path) == [source]
    import_source(l,source)
    assert find_sources(l,tmp_path/'different') == [source]
    project = tmp_path/'extracted'; project.mkdir()
    with zipfile.ZipFile(source) as z:
        z.extractall(project)
    other = FreeLake(tmp_path/'other')
    assert find_sources(other,project) == [project/'data']
    import_source(other,project/'data')
    assert import_summary(other)['local_price_rows'] == 1
    l.close();other.close()


def test_missing_input_does_not_silently_start_full_download(tmp_path):
    l = FreeLake(tmp_path/'lake')
    with pytest.raises(ValueError,match='Existing data not found'):
        find_sources(l,tmp_path)
    assert find_sources(l,tmp_path,fresh=True) == []
    l.close()


def test_imported_day_is_not_scheduled_for_price_history_redownload(tmp_path):
    l = FreeLake(tmp_path/'lake')
    import_source(l,dataset(tmp_path/'data.zip'))
    day = date(2016,10,24)
    l.plan_free(day,date(2016,10,25))
    reused = l.db.execute("select status from jobs where dataset='prices' and start='2016-10-24'").fetchone()[0]
    assert reused == 'REUSED_LOCAL'
    assert l.db.execute("select status from jobs where dataset='price_supplements' and start='2016-10-24'").fetchone()[0] == 'PENDING'
    assert l.db.execute("select status from jobs where dataset='prices' and start='2016-10-25'").fetchone()[0] == 'PENDING'
    plan = json.loads((l.root/'missing_data_plan.json').read_text())
    assert plan['local_price_rows'] == 1
    assert 'turnover' in plan['price_job_reasons']['price_supplements']
    l.close()


def test_supplement_fetches_missing_fields_without_replacing_existing_ohlcv(tmp_path):
    l = FreeLake(tmp_path/'lake')
    import_source(l,dataset(tmp_path/'data.zip'))
    day = date(2016,10,24); l.plan_free(day,day)
    calls=[]
    class HTTP:
        def fetch(self,url):
            calls.append(url)
            body=legacy(day,close='11')  # Different provider close must not replace the uploaded one.
            return Document(url,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    out=l.fetch(datasets=('price_supplements',),start=day,end=day,http_factory=HTTP)
    assert out[0]['status']=='STAGED'
    assert l.db.execute('select close from local_prices').fetchone()[0]=='12'
    assert l.db.execute('select count(*) from prices').fetchone()[0]==0  # No duplicate price history.
    assert l.db.execute('select turnover from price_metadata').fetchone()[0]=='1200'
    assert l.fetch(datasets=('prices',),start=day,end=day,http_factory=HTTP)==[]
    assert l.fetch(datasets=('price_supplements',),start=day,end=day,http_factory=HTTP)==[]
    assert len(calls)==1
    assert l.verify()['ok']
    l.close()


def test_uploaded_zip_xml_is_reused_without_extracting_or_network(tmp_path):
    l = FreeLake(tmp_path/'lake')
    import_source(l,dataset(tmp_path/'data.zip'))
    class NoNetwork:
        def open(self,*a,**kw):
            raise AssertionError('No download allowed')
    h=PoliteHTTP(l.root,opener=NoNetwork(),existing_lake=l)
    assert h.fetch('https://nsearchives.nseindia.com/corporate/filing.xml').body==XML
    assert h.cache_hits==1 and h.requests==0
    h.close();l.close()


def test_changed_import_fails_instead_of_retaining_stale_coverage(tmp_path):
    source=dataset(tmp_path/'data.zip')
    l=FreeLake(tmp_path/'lake'); import_source(l,source)
    dataset(source,True)
    with pytest.raises(ValueError,match='Previously imported file changed'):
        import_source(l,source)
    assert l.db.execute('select count(*) from local_prices').fetchone()[0]==1
    l.close()


def test_unsafe_zip_rejected_without_extraction(tmp_path):
    source=tmp_path/'data.zip'
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('../prices/BAD.csv',HEADER)
    l=FreeLake(tmp_path/'lake')
    with pytest.raises(ValueError,match='Unsafe ZIP'):
        import_source(l,source)
    l.close()


def test_supplement_also_fills_absent_symbol_without_duplicating_uploaded_one(tmp_path):
    l=FreeLake(tmp_path/'lake'); import_source(l,dataset(tmp_path/'data.zip'))
    day=date(2016,10,24);l.plan_free(day,day)
    raw=('SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,ISIN,\n'
         'OLDNAME,EQ,10,13,9,11,100,1100,24-OCT-2016,INE000000001,\n'
         'MISSING,EQ,20,23,19,22,200,4400,24-OCT-2016,INE000000002,\n').encode()
    out=io.BytesIO()
    with zipfile.ZipFile(out,'w') as z:
        z.writestr('cm24OCT2016bhav.csv',raw)
    body=out.getvalue()
    class HTTP:
        def fetch(self,url):
            return Document(url,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    l.fetch(datasets=('price_supplements',),start=day,end=day,http_factory=HTTP)
    assert [r[0] for r in l.db.execute('select symbol from prices')]==['MISSING']
    assert l.db.execute('select close from local_prices').fetchone()[0]=='12'
    l.close()


def test_fully_present_indices_skip_download_and_imported_vix_not_overwritten(tmp_path):
    l=FreeLake(tmp_path/'lake');import_source(l,dataset(tmp_path/'data.zip'))
    day=date(2016,10,24);l.plan_free(day,day)
    lines=['Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value']
    for name in ('India VIX','Nifty 50','Nifty 500','Nifty MidSmallcap 400',
                 'Nifty Bank','Nifty IT','Nifty Pharma','Nifty Auto','Nifty FMCG',
                 'Nifty Metal','Nifty Realty','Nifty Energy'):
        lines.append(name+',24-10-2016,10,13,9,11')
    body=('\n'.join(lines)+'\n').encode()
    class HTTP:
        def fetch(self,url):
            return Document(url,body,hashlib.sha256(body).hexdigest(),datetime.now(timezone.utc),len(body))
    l.fetch(datasets=('indices',),start=day,end=day,http_factory=HTTP)
    assert l.db.execute("select close from indices where code='INDIAVIX'").fetchone()[0]=='12'
    l.plan_free(day,day)
    assert l.fetch(datasets=('indices',),start=day,end=day,http_factory=HTTP)==[]
    l.close()


def test_relocated_zip_is_remembered_on_next_run(tmp_path):
    old=dataset(tmp_path/'old.zip');l=FreeLake(tmp_path/'lake')
    import_source(l,old)
    new=tmp_path/'new.zip';old.rename(new)
    assert find_sources(l,tmp_path,[new])==[new]
    import_source(l,new)
    assert find_sources(l,tmp_path)==[new]
    l.close()
