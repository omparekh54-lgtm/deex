import json
import threading
import time
from datetime import date
import pytest
from fastapi.testclient import TestClient
from app import main, angel
from app.engine import Cancelled, DataGap, demo_data, technical, frame_for
from app.main import app, Run

@pytest.fixture(autouse=True)
def clear(monkeypatch):
    monkeypatch.delenv('PMS_DATABASE_URL',raising=False)
    with main.LOCK:
        main.SESSIONS.clear();main.JOBS.clear()
    yield

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def wait(client,job_id):
    for _ in range(200):
        response=client.get('/api/runs/'+job_id)
        value=response.json()
        if value['status'] not in ('RUNNING','QUEUED'):return value
        time.sleep(.02)
    pytest.fail('job timed out')

def test_health_security_and_no_order_route(client):
    response=client.get('/health')
    assert response.json()['execution_enabled'] is False
    assert 'HttpOnly' in response.headers['set-cookie']
    assert response.headers['cache-control']=='no-store'
    assert client.post('/api/orders',json={}).status_code==404
    assert client.get('/docs').status_code==404
    assert "script-src 'self'" in response.headers['content-security-policy']

def test_reject_cross_site_and_unauthenticated_study(client):
    assert client.post('/api/runs',json={},headers={'Origin':'https://evil.example'}).status_code==403
    result=client.post('/api/runs',json={'start':'2022-01-03','end':'2024-12-31'})
    assert result.status_code==409

def test_no_secret_reflected_by_validation_or_broker_errors(client,monkeypatch):
    secret='sensitive-password-001'
    response=client.post('/api/connect',json={'method':'invalid','pin':secret,'api_key':secret})
    assert response.status_code==422 and secret not in response.text
    def reject(*args): raise angel.BrokerError('Angel One rejected the request.')
    monkeypatch.setattr(angel,'authenticate',reject)
    result=client.post('/api/connect',json={'api_key':'key123','client_code':'A1','pin':secret,'totp':'123456'})
    assert result.status_code==400 and secret not in result.text

def test_mocked_login_and_disconnect_dont_export_secrets(client,monkeypatch):
    monkeypatch.setattr(angel,'authenticate',lambda *args:'super-private-token')
    result=client.post('/api/connect',json={'api_key':'key123','client_code':'A1','pin':'1234','totp':'123456'})
    assert result.status_code==200
    assert 'super-private-token' not in result.text
    assert client.get('/api/status').json()['broker_connected']
    client.post('/api/disconnect',json={})
    assert not client.get('/api/status').json()['broker_connected']

def test_full_strategy_blocks_without_historical_database(client):
    result=client.post('/api/runs',json={'start':'2022-01-03','end':'2024-12-31'})
    assert result.status_code==409 and '100 stocks' in result.text
    assert not main.JOBS

def test_old_variants_symbols_and_tuning_cannot_be_submitted(client):
    for field,value in [('mode','TECHNICAL'),('symbols',['DIXON']),('reentry',False),('adr_min',2.5),('always_armed',True)]:
        payload={'start':'2022-01-03','end':'2024-12-31',field:value}
        assert client.post('/api/runs',json=payload).status_code==422
    assert client.post('/api/demo',json={}).status_code==404

def test_full_report_downloads_and_session_isolation(client,monkeypatch):
    from app.engine import report,configuration
    from packages.pms.backtest import EventDrivenBacktester,MarketBar,Signal
    monkeypatch.setenv('PMS_DATABASE_URL','postgresql://not-used-in-this-mocked-test')
    def fake_database(request,url,progress,check):
        assert request.mode=='FULL' and request.reentry and not request.always_armed
        symbols=['S'+str(i).zfill(3) for i in range(100)]
        request=request.model_copy(update={'symbols':symbols})
        cfg=configuration(request)
        days=[date(2022,1,d) for d in (3,4,5,6)]
        bars=[]
        for symbol in symbols:
            for day,ohlc in zip(days,[(99,100,98,99),(101,104,100,103),(103,104,101,101),(101,103,99,100)]):
                bars.append(MarketBar(symbol,day,*ohlc,102,102,99))
        signals=[Signal(symbol,days[0],100,96) for symbol in symbols]
        result=EventDrivenBacktester(cfg=cfg).run(bars,lambda day:signals if day==days[0] else [])
        return report(request,cfg,result,{'source':'MOCKED_FULL_REPLAY_TEST','limitations':[],
             'universe':{'stock_count':100,'stocks':[{'instrument_id':s,'sector':'Test','adtv':3e7} for s in symbols]}})
    monkeypatch.setattr(main,'database',fake_database)
    result=client.post('/api/runs',json={'start':'2022-01-03','end':'2022-01-06'})
    assert result.status_code==202
    job_id=result.json()['id'];job=wait(client,job_id)
    assert job['status']=='COMPLETED'
    report=job['report'];assert report['mode']=='FULL' and not report['demo']
    assert len(report['parameters']['symbols'])==100
    assert report['metrics']['closed_trades']>0 and report['metrics']['total_costs']>0
    assert len(report['report_fingerprint'])==64
    assert report['metrics']['closed_trades']<len(report['fills'])
    assert len({r['instrument_id'] for r in report['fills'] if r['side']=='BUY'})<=7
    for kind in ('json','csv','html'):
        response=client.get(f'/api/runs/{job_id}/download/{kind}')
        assert response.status_code==200 and 'attachment' in response.headers['content-disposition']
        assert 'jwt' not in response.text.lower()
    with TestClient(app) as stranger:
        assert stranger.get('/api/runs/'+job_id).status_code==404
        assert stranger.get(f'/api/runs/{job_id}/download/json').status_code==404

def test_run_validation_rejects_dates_symbols_and_nan():
    from pydantic import ValidationError
    for overrides in ({'start':'2025-01-01','end':'2024-01-01'},{'capital':float('nan')},
                      {'symbols':['<script>']},{'end':'2099-01-01'},{'adr_min':4,'adr_cap':3}):
        args={'symbols':['DIXON'],'start':'2022-01-03','end':'2024-12-31'};args.update(overrides)
        with pytest.raises(ValidationError): Run(**args)

def test_causal_signals_identical_when_future_changes():
    request=Run(symbols=['DEMO_ALPHA'],start=date(2022,1,3),end=date(2022,12,30))
    data,calendar=demo_data();data={'DEMO_ALPHA':data['DEMO_ALPHA']}
    original=technical(request,data,calendar,lambda *a:None,lambda:None,demo=True)
    for d in list(data['DEMO_ALPHA']):
        if d>request.end: data['DEMO_ALPHA'][d]=[999,1001,998,1000,1000000]
    changed=technical(request,data,calendar,lambda *a:None,lambda:None,demo=True)
    assert original['fills']==changed['fills']
    assert original['metrics']==changed['metrics']

def test_missing_sessions_block_not_zero_return():
    request=Run(symbols=['DEMO_ALPHA'],start=date(2022,1,3),end=date(2022,12,30))
    data,calendar=demo_data();rows=data['DEMO_ALPHA'];del rows[date(2022,1,4)]
    with pytest.raises(DataGap,match='required NSE sessions'):
        technical(request,{'DEMO_ALPHA':rows},calendar,lambda *a:None,lambda:None)

def test_technical_cancel_checks_before_work():
    request=Run(symbols=['DEMO_ALPHA'],start=date(2022,1,3),end=date(2022,12,30))
    data,calendar=demo_data()
    def cancel(): raise Cancelled()
    with pytest.raises(Cancelled): technical(request,data,calendar,lambda *a:None,cancel)

def test_candle_chunks_and_invalid_payload(monkeypatch):
    calls=[]
    def fake(path,payload,*args,**kwargs):
        calls.append(payload)
        return []
    monkeypatch.setattr(angel,'call',fake)
    angel.candles('k','t','1',date(2018,1,1),date(2024,12,31))
    assert len(calls)==2
    assert calls[0]['todate'][:10]<calls[1]['fromdate'][:10]
    monkeypatch.setattr(angel,'call',lambda *a,**kw:[['2022-01-03',100,90,80,110,10]])
    with pytest.raises(angel.BrokerError,match='Invalid historical'):
        angel.candles('k','t','1',date(2022,1,1),date(2022,1,5))
