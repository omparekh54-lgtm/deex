import asyncio
import json
import threading
import time
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient
from app import main,security,angel

@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    monkeypatch.delenv('RENDER',raising=False)
    monkeypatch.delenv('PMS_DATABASE_URL',raising=False)
    with main.LOCK:
        main.SESSIONS.clear();main.JOBS.clear();security.LIMITER.reset()
    yield

def session(client):
    response=client.get('/api/status')
    token=response.json()['csrf_token']
    return {'X-CSRF-Token':token,'Origin':str(client.base_url).rstrip('/')}

def credentials():
    return {'api_key':'private_api_key_123','client_code':'ABC123','pin':'sensitive-pin','totp':'123456'}

def test_csrf_is_required_even_if_origin_is_absent(monkeypatch):
    called=[];monkeypatch.setattr(angel,'authenticate',lambda *a:called.append(a))
    with TestClient(main.app) as client:
        auth=session(client)
        for headers in ({},{'X-CSRF-Token':'wrong-token'}):
            r=client.post('/api/connect',json=credentials(),headers=headers)
            assert r.status_code==403
        assert called==[]
        assert client.post('/api/connect',json=credentials(),headers={**auth,'Origin':'https://evil.example'}).status_code==403
        assert client.post('/api/connect',json=credentials(),headers={**auth,'Sec-Fetch-Site':'same-site'}).status_code==403

def test_no_credentials_retained_and_session_cookie_rotates(monkeypatch):
    monkeypatch.setattr(angel,'authenticate',lambda *a:'sensitive-token')
    with TestClient(main.app) as client:
        auth=session(client);old=client.cookies.get('deex_session')
        r=client.post('/api/connect',json=credentials(),headers=auth)
        assert r.status_code==200 and r.json()['connected']
        assert old not in main.SESSIONS and client.cookies.get('deex_session')!=old
        saved=json.dumps(main.SESSIONS)
        assert all(s not in saved for s in ('private_api_key_123','sensitive-pin','sensitive-token','123456'))
        current=main.SESSIONS[client.cookies.get('deex_session')]
        assert set(current['broker'])=={'verified','expires'}
        assert client.get('/api/status').json()['broker_credentials_retained'] is False
        assert client.post('/api/disconnect',json={},headers=auth).status_code==403
        assert client.post('/api/disconnect',json={},headers={'X-CSRF-Token':r.json()['csrf_token']}).status_code==200
        assert not client.get('/api/status').json()['broker_connected']

def test_login_limit_survives_new_browser_sessions(monkeypatch):
    seen=[]
    def reject(*args):seen.append(True);raise angel.BrokerError('Broker rejected verification.')
    monkeypatch.setattr(angel,'authenticate',reject)
    for i in range(6):
        with TestClient(main.app) as client:
            headers=session(client)
            r=client.post('/api/connect',json=credentials(),headers=headers)
            assert r.status_code==(400 if i<5 else 429)
    assert len(seen)==5
    encoded=json.dumps([(list(k),list(v)) for k,v in security.LIMITER.buckets.items()])
    assert 'ABC123' not in encoded

def test_broker_rejects_order_paths_before_transmission(monkeypatch):
    monkeypatch.setattr(angel.httpx,'post',lambda *a,**kw:pytest.fail('Must not transmit'))
    for path in ('/rest/secure/angelbroking/order/v1/placeOrder','https://evil.example','/rest/auth/angelbroking/user/v1/loginByPassword?next=evil'):
        with pytest.raises(angel.BrokerError,match='not permitted'):angel.call(path,{},'secret')

def test_broker_tls_proxy_redirect_settings_are_explicit(monkeypatch):
    captured={}
    class Result:
        status_code=200
        def json(self):return {'status':True,'data':{'jwtToken':'test-token'}}
    def post(*args,**kwargs):captured.update(kwargs);return Result()
    monkeypatch.setattr(angel.httpx,'post',post)
    assert angel.authenticate('key123','A1','pin','123456')=='test-token'
    assert captured['verify'] is True and captured['trust_env'] is False
    assert captured['follow_redirects'] is False

def test_errors_do_not_reflect_secrets_even_on_unexpected_broker_failure(monkeypatch):
    def fail(*args):raise RuntimeError('sensitive-pin private_api_key_123')
    monkeypatch.setattr(angel,'authenticate',fail)
    with TestClient(main.app) as client:
        r=client.post('/api/connect',json=credentials(),headers=session(client))
        assert r.status_code==502 and 'sensitive-pin' not in r.text and 'private_api_key_123' not in r.text

def test_session_expiry_removes_access_and_cancels_active_job():
    with TestClient(main.app) as client:
        auth=session(client);sid=client.cookies.get('deex_session')
        event=threading.Event()
        main.JOBS['test']={'owner':sid,'cancel':event,'created':time.monotonic(),'status':'RUNNING'}
        main.SESSIONS[sid]['expires']=time.monotonic()-1
        r=client.post('/api/disconnect',json={},headers=auth)
        assert r.status_code==401 and sid not in main.SESSIONS and event.is_set()

def test_idle_activity_cannot_extend_absolute_lifetime():
    s=security.new_session(1)
    s['expires']=1e10
    sessions={'s':s};security.purge(sessions,{},1+security.ABSOLUTE_TTL)
    assert not sessions

def test_oversized_stream_without_content_length_is_rejected():
    messages=[{'type':'http.request','body':b'x'*9000,'more_body':True},
              {'type':'http.request','body':b'x'*9000,'more_body':False}]
    sent=[]
    async def downstream(*args):pytest.fail('Must not decode the body')
    async def receive():return messages.pop(0)
    async def send(msg):sent.append(msg)
    scope={'type':'http','method':'POST','path':'/api/connect','headers':[(b'content-type',b'application/json')]}
    asyncio.run(security.BoundedBodyMiddleware(downstream)(scope,receive,send))
    assert sent[0]['status']==413
    assert b'x'*100 not in sent[-1].get('body',b'')

def test_json_only_query_strings_and_ranges_blocked_before_routes():
    with TestClient(main.app) as client:
        auth=session(client)
        assert client.post('/api/connect',content='pin=secret',headers={**auth,'Content-Type':'text/plain'}).status_code==415
        r=client.post('/api/connect?api_key=secret',json={},headers=auth)
        assert r.status_code==400 and 'secret' not in r.text
        assert client.get('/static/style.css',headers={'Range':'bytes=0-1,2-3'}).status_code==416
        r=client.post('/api/connect',content=b'x'*17000,headers={**auth,'Content-Type':'application/json'})
        assert r.status_code==413 and r.headers['x-frame-options']=='DENY'

def test_secure_production_cookie_https_host_and_headers(monkeypatch):
    monkeypatch.setenv('RENDER','true')
    monkeypatch.setenv('RENDER_EXTERNAL_URL','https://deex-research.onrender.com')
    with TestClient(main.app,base_url='https://deex-research.onrender.com') as client:
        r=client.get('/api/status')
        assert r.status_code==200
        cookie=r.headers['set-cookie']
        assert '__Host-deex_session=' in cookie and 'Secure' in cookie and 'HttpOnly' in cookie
        assert 'SameSite=strict' in cookie and 'Domain=' not in cookie
        assert r.headers['strict-transport-security']=='max-age=31536000'
        assert 'payment=()' in r.headers['permissions-policy']
        assert client.get('/health',headers={'Host':'evil.example'}).status_code==403
        auth={'X-CSRF-Token':r.json()['csrf_token'],'Origin':'https://deex-research.onrender.com'}
        assert client.post('/api/runs',json={},headers=auth).status_code==422
    with TestClient(main.app,base_url='http://deex-research.onrender.com') as client:
        assert client.get('/api/status').status_code==400

def test_static_assets_do_not_create_sessions():
    with TestClient(main.app) as client:
        assert client.get('/static/style.css').status_code==200
        assert not main.SESSIONS

def test_clear_removes_report_access(monkeypatch):
    with TestClient(main.app) as client:
        auth=session(client);sid=client.cookies.get('deex_session')
        main.JOBS['report']={'owner':sid,'cancel':threading.Event(),'created':time.monotonic(),'status':'COMPLETED','report':{}}
        r=client.post('/api/disconnect',json={},headers=auth)
        assert r.status_code==200
        assert 'report' not in main.JOBS
        assert client.get('/api/runs/report').status_code==404


def test_slow_body_cannot_hold_a_connection_forever(monkeypatch):
    monkeypatch.setattr(security,'BODY_TIMEOUT',.001)
    sent=[]
    async def downstream(*args):pytest.fail('Must reject before parsing')
    async def receive():
        await asyncio.sleep(.05)
        return {'type':'http.request','body':b'{}'}
    async def send(msg):sent.append(msg)
    scope={'type':'http','method':'POST','path':'/api/connect','headers':[(b'content-type',b'application/json')]}
    asyncio.run(security.BoundedBodyMiddleware(downstream)(scope,receive,send))
    assert sent[0]['status']==408
