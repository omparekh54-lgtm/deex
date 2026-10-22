from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import pytest
from fastapi.testclient import TestClient
from app.main import app,RunRequest
from app.universe import automatic_universe,UNIVERSE_SQL
from app.engine import DataGap

class Snapshot:
    def __init__(self,rows): self.rows=rows;self.calls=[]
    def execute(self,statement,params):
        self.calls.append((str(statement),params))
        return self
    def mappings(self):return self
    def all(self):return self.rows

def rows(count=100):
    return [{'instrument_id':'NSE:STOCK'+str(i),'adtv':Decimal('30000000'),
             'sector':'Industrials','industry':'Engineering','last_price_date':date(2021,12,31)} for i in range(count)]

def test_one_hundred_distinct_stocks_with_pre_start_policy():
    snapshot=Snapshot(rows());symbols,policy=automatic_universe(snapshot,date(2022,1,3))
    assert len(symbols)==len(set(symbols))==100
    assert policy['stock_count']==100
    assert policy['stocks'][0]['adtv']==30000000.
    sql,params=snapshot.calls[0]
    assert params['start']==date(2022,1,3) and params['limit']==100
    assert 'end' not in params
    assert 'trade_date<:start' in sql
    assert 'active=true' not in sql
    assert 'order by l.adtv desc,instrument_id' in sql
    assert 'history_sessions=252' in sql

def test_incomplete_sample_never_silently_becomes_a_smaller_run():
    with pytest.raises(DataGap,match='99 of the required 100'):
        automatic_universe(Snapshot(rows(99)),date(2022,1,3))

def test_conflicting_identities_block():
    sample=rows();sample[-1]['instrument_id']=sample[0]['instrument_id']
    with pytest.raises(DataGap,match='ambiguous'):
        automatic_universe(Snapshot(sample),date(2022,1,3))

@pytest.mark.parametrize('adtv',[float('nan'),float('inf'),-1,1e7])
def test_invalid_liquidity_blocks(adtv):
    sample=rows();sample[0]['adtv']=adtv
    with pytest.raises(DataGap,match='liquidity'):
        automatic_universe(Snapshot(sample),date(2022,1,3))

def test_no_user_symbols_or_strategy_parameters_in_the_public_page():
    with TestClient(app) as client:
        html=client.get('/').text
        assert 'id="symbols"' not in html and 'id="mode"' not in html
        assert 'Research parameters' not in html and 'demoButton' not in html
        assert 'Stocks selected automatically' in html
        assert client.get('/api/status').json()['stock_count']==100


def test_request_always_builds_the_documented_baseline():
    request=RunRequest(start='2022-01-03',end='2024-12-31').replay()
    assert request.mode=='FULL' and request.symbols==[]
    assert request.reentry and not request.always_armed
    assert request.adr_min==3 and request.adr_window==20 and request.adr_cap is None


def test_full_adapter_uses_same_snapshot_and_all_hundred_symbols(monkeypatch):
    from contextlib import contextmanager
    import sqlalchemy
    from app.engine import database
    from packages.pms import matrix_data,replay_service,matrix_replay
    from packages.pms.backtest import BacktestResult
    snapshot=Snapshot(rows());seen={}
    class Engine:
        def dispose(self):seen['disposed']=True
    def engine_factory(url,**kwargs):
        assert 'default_transaction_read_only=on' in kwargs['connect_args']['options']
        return Engine()
    @contextmanager
    def consistent(engine):yield snapshot
    def manifest(snap,cells,symbols):
        assert snap is snapshot and len(symbols)==100
        seen['symbols']=symbols
        return {'fingerprint':'test-dataset','scope':'TEST','universe':[{'instrument_id':s} for s in symbols]}
    class Prepared:
        def __init__(self,snap,manifest,period):
            self.snapshot=snap;self.manifest=manifest;self.period=period
            self.symbols=[r['instrument_id'] for r in manifest['universe']]
            self.code_version='test-code';self.gate=lambda *args:[]
    class Replay:
        def __init__(self,snap,symbols,cfg,case,start,end):
            assert snap is snapshot and symbols==seen['symbols']
            assert all((case.variant.use_module_a,case.variant.use_module_c,case.variant.use_module_e,case.variant.reentry))
            assert not case.variant.setup_2
            assert cfg['technical']['min_adr_20d_pct']==3
            assert 'research_leg1_always_armed' not in cfg['backtest']
            self.calendar=SimpleNamespace(sessions={start});self.start=start;self.cfg=cfg
            self.on_close=lambda *args:None
        def run(self):
            self.on_close(self.start,None)
            capital=self.cfg['backtest']['initial_capital']
            return BacktestResult(capital,capital,0,0,0,0,[],[],0,[(self.start,capital)],capital,0),{}
    monkeypatch.setattr(sqlalchemy,'create_engine',engine_factory)
    monkeypatch.setattr(matrix_data,'consistent_inputs',consistent)
    monkeypatch.setattr(matrix_data,'input_manifest',manifest)
    monkeypatch.setattr(replay_service,'PreparedReplay',Prepared)
    monkeypatch.setattr(matrix_replay,'MatrixReplay',Replay)
    request=RunRequest(start='2022-01-03',end='2024-12-31').replay()
    report=database(request,'postgresql://test-only',lambda *a:None,lambda:None)
    assert seen['disposed']
    assert report['mode']=='FULL' and len(report['parameters']['symbols'])==100
    assert report['diagnostics']['universe']['stock_count']==100
    assert report['status']=='NO_TRADES'
