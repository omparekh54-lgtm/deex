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
    assert client.post('/api/demo',json={},headers={'Origin':'https://evil.example'}).status_code==403
    result=client.post('/api/runs',json={'symbols':['DIXON'],'start':'2022-01-03','end':'2024-12-31'})
    assert result.status_code==401

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
    for mode in ['FULL','BA','BAC','SETUP2']:
        result=client.post('/api/runs',json={'mode':mode,'symbols':['DIXON'],'start':'2022-01-03','end':'2024-12-31'})
        assert result.status_code==409
    assert not main.JOBS

def test_demo_downloads_complete_cycles_and_session_isolation(client):
    result=client.post('/api/demo',json={});assert result.status_code==202
    job_id=result.json()['id'];job=wait(client,job_id)
    assert job['status']=='COMPLETED'
    report=job['report'];assert report['demo'] and report['acceptance']=='SYNTHETIC_DEMO'
    assert report['metrics']['closed_trades']>0
    assert report['metrics']['total_costs']>0
    assert len(report['report_fingerprint'])==64
    assert report['metrics']['ending_equity']==pytest.approx(report['equity_curve'][-1]['equity'])
    assert all(r['exit_date']>=r['entry_date'] for r in report['trades'])
    assert report['metrics']['closed_trades']<len(report['fills'])
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
