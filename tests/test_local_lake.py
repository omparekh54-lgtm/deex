from datetime import date, datetime, timezone
import hashlib
import io
import json
import zipfile

import pytest

from packages.data_layer.cloud_sources import Document, SourceError
from packages.data_layer.local_lake import Lake, legacy_url, parse_legacy


def legacy(day, close='12'):
    raw = ('SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,ISIN,\n'
           f'OLDNAME,EQ,10,13,9,{close},100,1200,{day.strftime("%d-%b-%Y").upper()},INE000000001,\n').encode()
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w') as z:
        z.writestr('cm' + day.strftime('%d%b%Y').upper() + 'bhav.csv', raw)
    return out.getvalue()


def doc(body, url='https://nsearchives.nseindia.com/test.csv'):
    return Document(url, body, hashlib.sha256(body).hexdigest(), datetime.now(timezone.utc), len(body))


def test_legacy_date_ohlc_and_archive_contract():
    day = date(2016, 10, 24)
    rows = parse_legacy(legacy(day), day)
    assert rows[0]['symbol'] == 'OLDNAME'
    assert rows[0]['isin'] == 'INE000000001'
    with pytest.raises(SourceError):
        parse_legacy(legacy(day, '99'), day)
    with pytest.raises(SourceError):
        parse_legacy(legacy(day), date(2016, 10, 25))


def test_resume_reopen_checksums_and_backup(tmp_path):
    root = tmp_path / 'lake'
    l = Lake(root)
    day = date(2016, 10, 24)
    l.plan(day, day)
    class HTTP:
        def fetch(self, url):
            assert url == legacy_url(day)
            return doc(legacy(day), url)
    assert l.fetch(datasets=('prices',), start=day, end=day, http_factory=HTTP)[0]['status'] == 'DONE'
    assert l.fetch(datasets=('prices',), start=day, end=day, http_factory=HTTP) == []
    assert l.verify()['ok']
    l.close()
    l = Lake(root)
    assert l.status()['prices']['security_isins'] == 1
    assert l.status()['strategies']['PMS_SETUP1']['status'] == 'BLOCKED'
    l.backup(tmp_path / 'backup')
    backup = Lake(tmp_path / 'backup')
    assert backup.verify()['ok']
    backup.close()
    receipt = l.db.execute('select path from receipts').fetchone()[0]
    (root / 'raw' / receipt).write_bytes(b'corrupt')
    assert not l.verify()['ok']
    l.close()


def test_404_never_becomes_holiday_or_success(tmp_path):
    l = Lake(tmp_path / 'lake')
    day = date(2016, 10, 24)
    l.plan(day, day)
    class HTTP:
        def fetch(self, url):
            raise SourceError('source_http_404')
    out = l.fetch(datasets=('prices',), start=day, end=day, http_factory=HTTP)
    assert out[0]['status'] == 'MISSING'
    assert l.db.execute('select count(*) from prices').fetchone()[0] == 0
    assert l.fetch(datasets=('prices',), start=day, end=day, http_factory=HTTP) == []
    assert l.fetch(datasets=('prices',), start=day, end=day, retry=True, http_factory=HTTP)[0]['status'] == 'MISSING'
    l.close()


def test_import_requires_original_evidence_and_knowability(tmp_path):
    l = Lake(tmp_path / 'lake')
    (tmp_path / 'original.xml').write_text('<official/>')
    row = dict(dataset='sectors', key='issuer/2016', effective_date='2016-01-01',
               known_at='2016-01-01T10:00:00+05:30', payload={'sector': 'Industrial'},
               source_url='https://www.nseindia.com/example', source_file='original.xml')
    path = tmp_path / 'references.jsonl'
    path.write_text(json.dumps(row) + '\n')
    assert l.import_evidence(path) == 1
    assert l.status()['datasets']['sectors'] == 1
    row['known_at'] = '2016-01-01T10:00:00'
    path.write_text(json.dumps(row) + '\n')
    with pytest.raises(ValueError):
        l.import_evidence(path)
    assert l.verify()['ok']
    l.close()


def test_invalid_price_task_rolls_back_and_remains_retryable(tmp_path):
    l = Lake(tmp_path / 'lake')
    day = date(2016, 10, 24)
    l.plan(day, day)
    class HTTP:
        def fetch(self, url):
            return doc(legacy(day, '99'), url)
    assert l.fetch(datasets=('prices',), start=day, end=day, http_factory=HTTP)[0]['status'] == 'FAILED'
    assert l.db.execute('select count(*) from prices').fetchone()[0] == 0
    assert l.verify()['ok']
    l.close()


def test_index_missing_ohlc_is_preserved_as_partial(tmp_path):
    l = Lake(tmp_path / 'lake')
    day = date(2016, 10, 24)
    l.plan(day, day)
    body = ('Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value\n'
            'Nifty MidSmallcap 400,24-10-2016,-,-,-,5267.71\n').encode()
    class HTTP:
        def fetch(self, url):
            return doc(body, url)
    out = l.fetch(datasets=('indices',), start=day, end=day, http_factory=HTTP)
    assert out[0]['status'] == 'PARTIAL'
    row = l.db.execute('select open,close from indices').fetchone()
    assert row['open'] is None and row['close'] == '5267.71'
    assert l.verify()['ok']
    l.close()


def test_repeated_financial_pages_cannot_complete(tmp_path):
    l = Lake(tmp_path / 'lake')
    start, end = date(2026, 9, 1), date(2026, 9, 30)
    l.plan(start, end)
    row = dict(symbol='EXAMPLE', quarterEndDate='30-Jun-2026',
               broadcastDateTime='01-Sep-2026 10:00:00', id='1',
               xbrl='https://nsearchives.nseindia.com/original.xml')
    class HTTP:
        def api(self, path, **params):
            body = json.dumps({'totalCount': 2, 'page': params['page']-1, 'data': [row]}).encode()
            return doc(body)
    out = l.fetch(datasets=('financial_discovery',), start=start, end=end, http_factory=HTTP)
    assert out[0]['status'] == 'SPLIT'
    assert out[0]['error'] == 'financial_repeated_page'
    assert l.db.execute('select count(*) from documents').fetchone()[0] == 0
    assert l.db.execute("select count(*) from jobs where dataset='financial_discovery' and status='PENDING' and start>='2026-09-01' and end<='2026-09-30'").fetchone()[0] == 2
    assert l.db.execute('select count(*) from receipts').fetchone()[0] > 0
    l.close()
