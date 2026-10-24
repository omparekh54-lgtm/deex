from __future__ import annotations
import asyncio
import csv
import io
import json
import os
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from app import angel
from app.engine import Cancelled, DataGap, database
from app import security
from packages.pms.config import load_strategy_config
from app.universe import UNIVERSE_SIZE

ROOT=Path(__file__).resolve().parents[1]
@asynccontextmanager
async def lifespan(app):
    async def cleanup():
        while True:
            await asyncio.sleep(60)
            with LOCK:security.purge(SESSIONS,JOBS,time.monotonic())
    task=asyncio.create_task(cleanup())
    try:yield
    finally:
        task.cancel()
        try:await task
        except asyncio.CancelledError:pass

app=FastAPI(title='DEEX Research API',docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
SESSIONS={}; JOBS={}; LOCK=threading.RLock(); POOL=ThreadPoolExecutor(max_workers=1)
TTL=security.ABSOLUTE_TTL

def today(): return datetime.now(ZoneInfo('Asia/Kolkata')).date()

class Login(BaseModel):
    model_config=ConfigDict(extra="forbid")
    api_key: SecretStr=Field(max_length=256)
    method: Literal['totp','jwt']='totp'
    client_code: str=Field(default='',max_length=40)
    pin: SecretStr=Field(default=SecretStr(''),max_length=32)
    totp: SecretStr=Field(default=SecretStr(''),max_length=6)
    jwt: SecretStr=Field(default=SecretStr(''),max_length=8192)

class Run(BaseModel):
    mode: Literal['TECHNICAL','FULL','BA','BAC','SETUP2']='TECHNICAL'
    symbols: list[str]=Field(min_length=0,max_length=100)
    start: date
    end: date
    capital: float=Field(default=1000000,ge=10000,le=1e10,allow_inf_nan=False)
    reentry: bool=True
    always_armed: bool=False
    adr_min: float=Field(default=3,ge=2,le=10,allow_inf_nan=False)
    adr_window: Literal[20,50]=20
    adr_cap: float | None=Field(default=None,ge=3,le=30,allow_inf_nan=False)
    slippage_bps: float=Field(default=10,ge=0,le=100,allow_inf_nan=False)
    impact_bps: float=Field(default=5,ge=0,le=100,allow_inf_nan=False)
    brokerage_bps: float=Field(default=0,ge=0,le=100,allow_inf_nan=False)
    @field_validator('symbols')
    @classmethod
    def validate_symbols(cls,values):
        values=list(dict.fromkeys(s.strip().upper().removeprefix('NSE:').removesuffix('-EQ') for s in values))
        if any(not re.fullmatch(r'[A-Z0-9&_.-]{1,30}',s) for s in values):
            raise ValueError('Use NSE equity symbols separated by commas.')
        return values
    @model_validator(mode='after')
    def dates(self):
        if self.start < date(2010,1,1) or self.end < self.start or self.end>=today():
            raise ValueError('Choose ordered historical dates, ending before today in India.')
        if (self.end-self.start).days>3653:
            raise ValueError('Limit a run to ten years.')
        if self.adr_cap is not None and self.adr_cap<self.adr_min:
            raise ValueError('ADR cap must be at least the ADR minimum.')
        return self

class RunRequest(BaseModel):
    """Only the account and dates are user inputs. Strategy policy is immutable."""
    model_config = ConfigDict(extra='forbid')
    start: date
    end: date
    capital: float=Field(default=1000000,ge=10000,le=1e10,allow_inf_nan=False)
    @model_validator(mode='after')
    def dates(self):
        Run(symbols=[],mode='FULL',start=self.start,end=self.end,capital=self.capital)
        return self
    def replay(self):
        return Run(symbols=[],mode='FULL',start=self.start,end=self.end,capital=self.capital)

@app.exception_handler(RequestValidationError)
async def validation_error(request,exc):
    # Pydantic's default errors include raw inputs, which can contain passwords.
    return JSONResponse({'detail':'Invalid input. Check dates, symbol list, capital and numeric limits.'},status_code=422)

@app.middleware('http')
async def safety(request:Request,call_next):
    now=time.monotonic()
    is_mutation=request.method not in ('GET','HEAD','OPTIONS')
    if not security.valid_origin(request):
        return security.error('Request origin or host is not allowed.',403)
    if security.production() and request.url.scheme!='https':
        return security.error('HTTPS is required.',400)
    if is_mutation and request.headers.get('sec-fetch-site') in ('cross-site','same-site'):
        return security.error('Cross-site requests are disabled.',403)
    with LOCK:
        security.purge(SESSIONS,JOBS,now)
        sid=request.cookies.get(security.cookie_name())
        creates_session=request.method=='GET' and request.url.path in ('/','/api/status')
        if sid not in SESSIONS:
            if request.url.path.startswith('/api/') and not creates_session:
                return security.error('Session expired. Reload the page.',404 if request.method=='GET' else 401)
            sid=None
            if creates_session:
                if len(SESSIONS)>=300:return security.error('Session capacity reached. Try again later.',503)
                address=request.client.host if request.client else 'unknown'
                if not security.LIMITER.allow('new_session',address,30,60):
                    return security.error('Too many new sessions. Try again shortly.',429)
                sid=secrets.token_urlsafe(32)
                SESSIONS[sid]=security.new_session(now)
                request.state.set_cookie=True
        if sid:
            session=SESSIONS[sid]
            if is_mutation:
                token=request.headers.get('x-csrf-token','')
                if not token or not secrets.compare_digest(token,session['csrf']):
                    return security.error('Session verification failed. Reload the page.',403)
            session['expires']=min(now+security.IDLE_TTL,session['absolute_expires'])
        request.state.sid=sid
    response=await call_next(request)
    if getattr(request.state,'set_cookie',False):
        response.set_cookie(security.cookie_name(),request.state.sid,httponly=True,
            secure=security.production(),samesite='strict',path='/',max_age=TTL)
    return response

app.add_middleware(security.BoundedBodyMiddleware)

@app.get('/')
def index(): return FileResponse(ROOT/'static/index.html')

@app.get('/health')
def health(): return {'status':'ok','execution_enabled':False,'service':'deex','version':'1.2.0'}

@app.get('/api/status')
def status(request:Request):
    session=SESSIONS[request.state.sid]
    broker=session['broker']
    connected=bool(broker and broker['expires']>time.monotonic())
    return {'broker_connected':connected,'database_configured':bool(os.getenv('PMS_DATABASE_URL')),
        'execution_enabled':False,'stock_count':UNIVERSE_SIZE,'strategy':'FULL_PMS_SETUP1','session_lifetime_minutes':15,
        'csrf_token':session['csrf'],'broker_credentials_retained':False,
        'strategy_version':load_strategy_config()['version']}

@app.post('/api/connect')
def connect(request:Request,body:Login):
    session=SESSIONS[request.state.sid]
    address=request.client.host if request.client else 'unknown'
    with LOCK:
        if (not security.LIMITER.allow('login_ip',address,5,300)
            or not security.LIMITER.allow('login_account',body.client_code.strip().upper(),5,300)
            or not security.LIMITER.allow('login_global','all',30,300)):
            raise HTTPException(429,'Too many login attempts. Wait five minutes.')
    key=body.api_key.get_secret_value().strip()
    if not re.fullmatch(r'[A-Za-z0-9_-]{5,256}',key): raise HTTPException(422,'Enter a valid SmartAPI key.')
    try:
        if body.method=='totp':
            if not body.client_code or not body.pin.get_secret_value() or not re.fullmatch(r'\d{6}',body.totp.get_secret_value()):
                raise HTTPException(422,'Enter client code, PIN and the current six-digit TOTP.')
            token=angel.authenticate(key,body.client_code.strip(),body.pin.get_secret_value(),body.totp.get_secret_value())
        else:
            token=body.jwt.get_secret_value().strip().removeprefix('Bearer ')
            if not re.fullmatch(r'[A-Za-z0-9_.-]{30,8192}',token): raise HTTPException(422,'Enter a valid JWT session token.')
            angel.candles(key,token,'99926000',today()-timedelta(days=12),today()-timedelta(days=1))
        # The full PMS replay consumes the historical database. It does not
        # need an API key/JWT after verification, so retain neither credential.
        with LOCK:
            if request.state.sid not in SESSIONS:
                raise HTTPException(401,'Session expired during verification. Reload the page.')
            old=request.state.sid
            sid=secrets.token_urlsafe(32)
            SESSIONS[sid]=SESSIONS.pop(old)
            for job in JOBS.values():
                if job['owner']==old:job['owner']=sid
            request.state.sid=sid;request.state.set_cookie=True
            session['broker']={'verified':True,'expires':time.monotonic()+security.VERIFICATION_TTL}
            session['csrf']=secrets.token_urlsafe(32)
        return {'connected':True,'csrf_token':session['csrf'],
                'message':'Connection verified. API key, PIN, TOTP and JWT are not retained.'}
    except angel.BrokerError as e:
        raise HTTPException(400,str(e)) from None
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502,'Broker verification could not complete. No credentials were retained.') from None
    finally:
        key=token=''
        body.api_key=SecretStr('');body.pin=SecretStr('');body.totp=SecretStr('');body.jwt=SecretStr('')

@app.post('/api/disconnect')
def disconnect(request:Request):
    with LOCK:
        old=request.state.sid
        SESSIONS.pop(old,None)
        for job_id,job in list(JOBS.items()):
            if job['owner']==old:
                job['cancel'].set()
                if job['status'] not in ('RUNNING','QUEUED'):del JOBS[job_id]
        sid=secrets.token_urlsafe(32)
        SESSIONS[sid]=security.new_session(time.monotonic())
        request.state.sid=sid;request.state.set_cookie=True
    return {'connected':False,'csrf_token':SESSIONS[sid]['csrf']}

def owned(request,job_id):
    with LOCK:
        job=JOBS.get(job_id)
        if not job or job['owner']!=request.state.sid:
            raise HTTPException(404,'Report not found or session expired.')
        return job

def submit(request,body):
    sid=request.state.sid
    with LOCK:
        session=SESSIONS[sid]
        if any(j['owner']==sid and j['status'] in ('QUEUED','RUNNING') for j in JOBS.values()):
            raise HTTPException(409,'A backtest is already running in this session.')
        if sum(j['status'] in ('QUEUED','RUNNING') for j in JOBS.values())>=3:
            raise HTTPException(429,'The backtest queue is full. Try again shortly.')
        if len(JOBS)>150: raise HTTPException(503,'Report capacity reached. Retry after older sessions expire.')
        if not os.getenv('PMS_DATABASE_URL'):
            raise HTTPException(409,'The full PMS historical dataset is not connected. Connect the dataset and restore database availability before running the strategy across 100 stocks.')
        job_id=secrets.token_urlsafe(24)
        job={'id':job_id,'owner':sid,'status':'QUEUED','progress':0,'message':'Waiting for backtest worker',
             'created':time.monotonic(),'cancel':threading.Event(),'report':None,'error':None}
        JOBS[job_id]=job
    POOL.submit(work,job,body.replay())
    return {'id':job_id,'status':'QUEUED','stock_count':UNIVERSE_SIZE}

@app.post('/api/runs',status_code=202)
def run(request:Request,body:RunRequest): return submit(request,body)

def work(job,body):
    began=time.monotonic()
    def check():
        if job['cancel'].is_set(): raise Cancelled()
        if time.monotonic()-began>3600: raise DataGap('Run exceeded the one-hour limit. Use a shorter date range.')
    def progress(value,message):
        check()
        with LOCK: job.update(status='RUNNING',progress=value,message=message)
    try:
        progress(3,'Preparing your full PMS strategy across 100 stocks')
        report=database(body,os.environ['PMS_DATABASE_URL'],progress,check)
        check()
        with LOCK: job.update(status=report['status'],progress=100,message='Report ready',report=report)
    except Cancelled:
        with LOCK: job.update(status='CANCELLED',message='Run cancelled. No broker orders were placed.')
    except DataGap as e:
        with LOCK: job.update(status='BLOCKED',message='Historical data checks blocked this run',error=str(e)[:2500])
    except Exception:
        with LOCK: job.update(status='FAILED',message='Backtest could not complete',error='The historical data source or engine returned an error. Check database availability and coverage. No performance report was produced.')
    finally:
        with LOCK: job['elapsed_seconds']=round(time.monotonic()-began,1)

@app.get('/api/runs/{job_id}')
def poll(request:Request,job_id:str):
    job=owned(request,job_id)
    with LOCK:
        return {k:v for k,v in job.items() if k in ('id','status','progress','message','error','report','elapsed_seconds')}

@app.post('/api/runs/{job_id}/cancel')
def cancel(request:Request,job_id:str):
    job=owned(request,job_id);job['cancel'].set()
    return {'status':'CANCELLATION_REQUESTED'}

@app.get('/api/runs/{job_id}/download/{kind}')
def download(request:Request,job_id:str,kind:Literal['json','csv','html']):
    job=owned(request,job_id);report=job['report']
    if not report: raise HTTPException(409,'The run has no completed report.')
    headers={'Content-Disposition':f'attachment; filename="deex_backtest.{kind}"'}
    if kind=='json': return Response(json.dumps(report,indent=2),media_type='application/json',headers=headers)
    if kind=='csv':
        stream=io.StringIO(); fields=['instrument_id','trade_date','side','price','quantity','reason','costs']
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
        for fill in report['fills']:
            writer.writerow({**fill,'costs':fill['costs']['total']})
        return Response(stream.getvalue(),media_type='text/csv',headers=headers)
    # A standalone print-ready report, with no external services or scripts.
    sections='<h2>Performance</h2><table>'+''.join('<tr><td>'+escape(k.replace('_',' '))+'</td><td>'+escape(str(v))+'</td></tr>' for k,v in report['metrics'].items() if not isinstance(v,dict))+'</table>'
    sections+='<h2>Limitations</h2><ul>'+''.join('<li>'+escape(v)+'</li>' for v in report['diagnostics'].get('limitations',[]))+'</ul>'
    for title,key in [('Completed position cycles','trades'),('All fills','fills')]:
        rows=report[key]
        sections+='<h2>'+title+'</h2>'
        if not rows: sections+='<p>None</p>';continue
        fields=[k for k,v in rows[0].items() if not isinstance(v,(dict,list))]
        sections+='<table><tr>'+''.join('<th>'+escape(k)+'</th>' for k in fields)+'</tr>'
        sections+=''.join('<tr>'+''.join('<td>'+escape(str(row.get(k,'')))+'</td>' for k in fields)+'</tr>' for row in rows)+'</table>'
    content='<!doctype html><html><meta charset="utf-8"><title>DEEX Backtest Report</title><style>body{font:14px system-ui;max-width:1000px;margin:40px auto;color:#182438}h1{font-size:30px}table{border-collapse:collapse;width:100%;margin:15px 0}td,th{border:1px solid #dae1e8;padding:8px;text-align:left}th{background:#edf2f6}li{margin:8px 0}@media print{body{margin:15mm}tr{break-inside:avoid}}</style><h1>DEEX PMS Backtest Report</h1><p>'+escape(report['mode_label'])+' | '+escape(report['acceptance'])+'</p><p>'+escape(str(report['parameters']))+'</p>'+sections+'<h2>Reproducibility</h2><p>Report SHA256 '+escape(report['report_fingerprint'])+'</p><p>Engine source '+escape(report['engine_source_commit'])+'</p><p>Download JSON for configuration, daily NAV, events, data identity and full diagnostics.</p></html>'
    return HTMLResponse(content,headers=headers)


# ---------------------------------------------------------------------------------------------
# Strategy library (S1-S10). Added alongside the original PMS Setup 1 workflow above, which is
# unchanged: /api/runs still accepts only start, end and capital and still runs Setup 1.
# Library runs use NSE bhavcopy data and share the job queue, polling and report downloads.
# ---------------------------------------------------------------------------------------------
from packages.strategies.catalog import STRATEGY_IDS, public_catalog

class StrategyRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    strategy: Literal[STRATEGY_IDS]
    start: date
    end: date
    capital: float=Field(default=1000000,ge=10000,le=1e10,allow_inf_nan=False)
    @model_validator(mode='after')
    def dates(self):
        if self.start < date(2011,1,1) or self.end < self.start or self.end>=today():
            raise ValueError('Choose ordered dates from 2011 onward, ending before today in India.')
        if (self.end-self.start).days>3653:
            raise ValueError('Limit a run to ten years.')
        return self

@app.get('/api/strategies')
def strategies():
    return {'strategies':public_catalog()}

@app.post('/api/strategy-runs',status_code=202)
def strategy_run(request:Request,body:StrategyRunRequest):
    sid=request.state.sid
    with LOCK:
        if any(j['owner']==sid and j['status'] in ('QUEUED','RUNNING') for j in JOBS.values()):
            raise HTTPException(409,'A backtest is already running in this session.')
        if sum(j['status'] in ('QUEUED','RUNNING') for j in JOBS.values())>=3:
            raise HTTPException(429,'The backtest queue is full. Try again shortly.')
        if len(JOBS)>150: raise HTTPException(503,'Report capacity reached. Retry after older sessions expire.')
        job_id=secrets.token_urlsafe(24)
        job={'id':job_id,'owner':sid,'status':'QUEUED','progress':0,'message':'Waiting for backtest worker',
             'created':time.monotonic(),'cancel':threading.Event(),'report':None,'error':None}
        JOBS[job_id]=job
    POOL.submit(strategy_work,job,body)
    return {'id':job_id,'status':'QUEUED','strategy':body.strategy}

def strategy_work(job,body):
    from packages.strategies.runner import run_strategy
    from packages.strategies.bhavcopy import BhavcopyUnavailable
    began=time.monotonic()
    def check():
        if job['cancel'].is_set(): raise Cancelled()
        if time.monotonic()-began>3600: raise DataGap('Run exceeded the one-hour limit. Use a shorter date range.')
    def progress(value,message):
        check()
        with LOCK: job.update(status='RUNNING',progress=int(value),message=message)
    try:
        progress(2,'Preparing strategy '+body.strategy)
        report=run_strategy(body.strategy,body.start,body.end,body.capital,progress,check)
        check()
        with LOCK: job.update(status=report['status'],progress=100,message='Report ready',report=report)
    except Cancelled:
        with LOCK: job.update(status='CANCELLED',message='Run cancelled. No broker orders were placed.')
    except (DataGap,BhavcopyUnavailable) as e:
        with LOCK: job.update(status='BLOCKED',message='Historical data checks blocked this run',error=str(e)[:2500])
    except MemoryError:
        with LOCK: job.update(status='FAILED',message='Backtest could not complete',error='The server ran out of memory. Use a shorter date range or a larger instance.')
    except Exception:
        with LOCK: job.update(status='FAILED',message='Backtest could not complete',error='The strategy engine or data loader returned an error. No performance report was produced.')
    finally:
        with LOCK: job['elapsed_seconds']=round(time.monotonic()-began,1)
