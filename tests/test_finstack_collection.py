from datetime import date
from types import SimpleNamespace

import pytest

from packages.data_layer.cloud_sources import SourceError
from packages.data_layer.finstack_collection import FinstackHTTP
from packages.data_layer.local_lake import Lake
from test_local_lake import doc, legacy


def test_finstack_transport_retains_official_bytes_and_disallows_other_hosts(tmp_path, monkeypatch):
    calls = []
    body = legacy(date(2016, 10, 24))
    def request(url):
        calls.append(url)
        return SimpleNamespace(status_code=200, url=url, content=body)
    fs = SimpleNamespace(nse_client=lambda: SimpleNamespace(_transport=SimpleNamespace(request=request)))
    monkeypatch.setattr('packages.data_layer.finstack_collection.load_finstack', lambda root: fs)
    h = FinstackHTTP(tmp_path)
    r = h.fetch('https://nsearchives.nseindia.com/test.zip')
    assert r.body == body and len(calls) == 1
    with pytest.raises(SourceError):
        h.fetch('https://example.com/arbitrary')
    assert len(calls) == 1


def test_company_register_preserves_historical_absent_names(tmp_path):
    lake = Lake(tmp_path / 'lake')
    with lake.db:
        raw = doc(legacy(date(2016, 10, 24)))
        digest = lake.archive(raw)
        from packages.data_layer.local_lake import parse_legacy
        lake._store_prices(parse_legacy(raw.body, date(2016, 10, 24)), digest)
    master = b'SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,ISIN NUMBER\nNEWNAME,New Company,EQ,01-Jan-2020,INE000000002\n'
    class HTTP:
        def fetch(self, url):
            return doc(master, url)
    summary = lake.refresh_current_master(HTTP())
    assert summary['candidate_identity_series_rows'] == 2
    assert summary['historical_only_rows'] == 1
    assert summary['exact_company_count'] is None
    old = dict(lake.db.execute("select * from company_targets where symbol='OLDNAME'").fetchone())
    assert old['first_price_date'] == '2016-10-24'
    assert old['in_current_master'] == 0
    out = tmp_path / 'targets.csv'
    lake.export_targets(out)
    assert 'OLDNAME' in out.read_text() and 'UNVERIFIED' in out.read_text()
    fresh = Lake(tmp_path / 'new_lake')
    assert fresh.seed_targets(out) == 2
    assert fresh.db.execute('select count(*) from prices').fetchone()[0] == 0
    assert fresh.db.execute('select sum(in_current_master) from company_targets').fetchone()[0] == 0
    assert fresh.db.execute('select count(first_price_date) from company_targets').fetchone()[0] == 0
    fresh.close()
    assert lake.verify()['ok']
    lake.close()
