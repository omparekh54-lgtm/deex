from datetime import date, datetime
import json
import re
import zipfile

import pytest

from packages.data_layer.cloud_sources import SourceError
from packages.data_layer.local_lake import Lake, dumps
from scripts.run_full_download import collector_lock, run
from scripts.package_local_data import package
from scripts.merge_data_checkpoints import merge
from test_local_lake import doc, legacy


class PriceHTTP:
    def fetch(self, url):
        day = datetime.strptime(re.search(r'cm(\d{2}[A-Z]{3}\d{4})', url)[1], '%d%b%Y').date()
        return doc(legacy(day), url)


def quiet(*a, **kw):
    pass


def test_full_runner_checkpoints_and_resumes_only_the_requested_year_dates(tmp_path):
    lake = Lake(tmp_path / 'lake')
    a, b = date(2016,10,24), date(2016,10,25)
    lake.plan(a, b)
    first = run(lake, phases=('prices',), batch=1, max_tasks=1, documents=False,
                factory=PriceHTTP, emit=quiet, acquire_from=a, acquire_to=b)
    assert first['state'] == 'CHECKPOINTED_LIMIT'
    assert lake.db.execute('select count(*) from prices').fetchone()[0] == 1
    second = run(lake, phases=('prices',), documents=False, factory=PriceHTTP,
                 emit=quiet, acquire_from=a, acquire_to=b)
    assert second['state'] == 'ACQUISITION_PASS_FINISHED_AUDIT_REQUIRED'
    assert second['backtest_ready'] is False
    assert lake.db.execute('select count(*) from prices').fetchone()[0] == 2
    assert json.loads((lake.root / 'download_progress.json').read_text())['state'] == second['state']
    lake.close()


def test_exclusive_collector_lock_releases_on_exception(tmp_path):
    with collector_lock(tmp_path):
        with pytest.raises(RuntimeError):
            with collector_lock(tmp_path):
                pass
    with collector_lock(tmp_path):
        pass


def test_failed_document_cannot_starve_the_pending_queue_and_legacy_html_is_saved(tmp_path):
    lake = Lake(tmp_path / 'lake')
    with lake.db:
        lake.db.execute('insert into documents(dataset,key,url,payload) values(?,?,?,?)',
                        ('financials','a','https://nsearchives.nseindia.com/missing.xml','{}'))
        lake.db.execute('insert into documents(dataset,key,url,payload) values(?,?,?,?)',
                        ('financials','b','https://nsearchives.nseindia.com/archives/financial_results/financial_res_TEST_1.html',
                         dumps({'_deex_document_kind':'legacy_html','toDate':'30-Sep-2016'})))
    class HTTP:
        def fetch(self, url, **kw):
            if url.endswith('missing.xml'):
                raise SourceError('source_http_404')
            assert kw.get('financial_render') is True
            return doc(b'<html>original statement</html>', url)
    assert lake.documents(limit=1, http_factory=HTTP)[0]['status'] == 'FAILED'
    assert lake.documents(limit=1, http_factory=HTTP)[0]['status'] == 'STAGED'
    assert lake.documents(limit=1, http_factory=HTTP) == []
    assert lake.verify()['receipts_checked'] == 1
    lake.close()


def test_packaged_years_merge_without_losing_completed_jobs(tmp_path):
    roots=[]
    for day in (date(2016,10,24), date(2016,10,25)):
        root=tmp_path / str(day)
        lake=Lake(root)
        lake.plan(date(2016,10,24), date(2016,10,25))
        lake.fetch(datasets=('prices',), start=day, end=day, limit=1, http_factory=PriceHTTP)
        lake.close()
        archive=tmp_path / (str(day)+'.zip')
        package(root, archive)
        extracted=tmp_path / ('extract-'+str(day))
        with zipfile.ZipFile(archive) as z:
            z.extractall(extracted)
        roots.append(extracted/'deex_data')
    result=merge(tmp_path/'joined',roots)
    assert result['ok'] and result['receipts_checked']==2
    combined=Lake(tmp_path/'joined')
    assert combined.db.execute('select count(*) from prices').fetchone()[0]==2
    assert combined.db.execute("select count(*) from jobs where dataset='prices' and status='DONE'").fetchone()[0]==2
    combined.close()
