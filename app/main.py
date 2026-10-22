from __future__ import annotations
import csv
import io
import json
import os
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from app import angel
from app.engine import Cancelled, DataGap, configuration, database, demo_data, technical

ROOT=Path(__file__).resolve().parents[1]
app=FastAPI(title='DEEX Research API',docs_url=None,redoc_url=None,openapi_url=None)
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
SESSIONS={}; JOBS={}; LOCK=threading.RLock(); POOL=ThreadPoolExecutor(max_workers=1)
TTL=7200

def today(): return datetime.now(ZoneInfo('Asia/Kolkata')).date()

class Login(BaseModel):
    api_key: SecretStr
    method: Literal['totp','jwt']='totp'
    client_code: str=Field(default='',max_length=40)
    pin: SecretStr=SecretStr('')
    totp: SecretStr=SecretStr('')
    jwt: SecretStr=SecretStr('')

class Run(BaseModel):
    mode: Literal['TECHNICAL','FULL','BA','BAC','SETUP2']='TECHNICAL'
    symbols: list[str]=Field(min_length=1,max_length=50)
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

@app.exception_handler(RequestValidationError)
async def validation_error(request,exc):
    # Pydantic's default errors include raw inputs, which can contain passwords.
    return JSONResponse({'detail':'Invalid input. Check dates, symbol list, capital and numeric limits.'},status_code=422)

@app.middleware('http')
async def safety(request:Request,call_next):
    if request.headers.get('content-length','').isdigit() and int(request.headers['content-length'])>65536:
        return JSONResponse({'detail':'Request is too large.'},status_code=413)
    if request.method not in ('GET','HEAD','OPTIONS'):
        origin=request.headers.get('origin')
        if origin and origin.rstrip('/') != str(request.base_url).rstrip('/'):
            return JSONResponse({'detail':'Cross-origin requests are disabled.'},status_code=403)
    now=time.monotonic()
    with LOCK:
        for key in list(SESSIONS):
            if SESSIONS[key]['expires']<now:
                del SESSIONS[key]
        for key in list(JOBS):
            job=JOBS[key]
            if now-job['created']>TTL and job['status'] not in ('QUEUED','RUNNING'):
                del JOBS[key]
        sid=request.cookies.get('deex_session')
        new_session=sid not in SESSIONS
        if new_session:
            if len(SESSIONS)>=300:
                return JSONResponse({'detail':'Session capacity reached. Try again later.'},status_code=503)
            sid=secrets.token_urlsafe(32)
            SESSIONS[sid]={'expires':now+TTL,'broker':None,'login_times':[],'last_demo':0}
        else:
            SESSIONS[sid]['expires']=now+TTL
        request.state.sid=sid
    response=await call_next(request)
    response.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',
        'Referrer-Policy':'no-referrer','X-Frame-Options':'DENY',
        'Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"})
    if new_session:
        response.set_cookie('deex_session',sid,httponly=True,secure=bool(os.getenv('RENDER')),samesite='strict',max_age=TTL)
    return response

@app.get('/')
def index(): return FileResponse(ROOT/'static/index.html')

@app.get('/health')
def health(): return {'status':'ok','execution_enabled':False,'service':'deex','version':'1.0.0'}

@app.get('/api/status')
def status(request:Request):
    session=SESSIONS[request.state.sid]
    broker=session['broker']
    connected=bool(broker and broker['expires']>time.monotonic())
    return {'broker_connected':connected,'database_configured':bool(os.getenv('PMS_DATABASE_URL')),
        'execution_enabled':False,'max_symbols':50,'session_lifetime_minutes':120,
        'strategy_version':configuration(Run(symbols=['EXAMPLE'],start=date(2023,1,1),end=date(2024,12,31)))['version']}

@app.post('/api/connect')
def connect(request:Request,body:Login):
    session=SESSIONS[request.state.sid]
    now=time.monotonic()
    with LOCK:
        session['login_times']=[t for t in session['login_times'] if now-t<300]
        if len(session['login_times'])>=5:
            raise HTTPException(429,'Too many login attempts. Wait five minutes.')
        session['login_times'].append(now)
    key=body.api_key.get_secret_value().strip()
    if not 5<=len(key)<=256: raise HTTPException(422,'Enter a valid SmartAPI key.')
    try:
        if body.method=='totp':
            if not body.client_code or not body.pin.get_secret_value() or not re.fullmatch(r'\d{6}',body.totp.get_secret_value()):
                raise HTTPException(422,'Enter client code, PIN and the current six-digit TOTP.')
            token=angel.authenticate(key,body.client_code.strip(),body.pin.get_secret_value(),body.totp.get_secret_value())
        else:
            token=body.jwt.get_secret_value().strip().removeprefix('Bearer ')
            if len(token)<30 or len(token)>8192: raise HTTPException(422,'Enter a valid JWT session token.')
            angel.candles(key,token,'99926000',today()-timedelta(days=12),today()-timedelta(days=1))
        with LOCK:
            session['broker']={'key':key,'token':token,'expires':time.monotonic()+5400}
        return {'connected':True,'message':'Angel One connected. Credentials are held in memory only.'}
    except angel.BrokerError as e:
        raise HTTPException(400,str(e)) from None

@app.post('/api/disconnect')
def disconnect(request:Request):
    with LOCK:
        SESSIONS[request.state.sid]['broker']=None
        for job in JOBS.values():
            if job['owner']==request.state.sid and job['status'] in ('RUNNING','QUEUED'):
                job['cancel'].set()
    return {'connected':False}

def owned(request,job_id):
    with LOCK:
        job=JOBS.get(job_id)
        if not job or job['owner']!=request.state.sid:
            raise HTTPException(404,'Report not found or session expired.')
        return job

def submit(request,body,*,demo=False):
    sid=request.state.sid
    with LOCK:
        session=SESSIONS[sid]
        if any(j['owner']==sid and j['status'] in ('QUEUED','RUNNING') for j in JOBS.values()):
            raise HTTPException(409,'A backtest is already running in this session.')
        if sum(j['status'] in ('QUEUED','RUNNING') for j in JOBS.values())>=3:
            raise HTTPException(429,'The research queue is full. Try again shortly.')
        if len(JOBS)>150: raise HTTPException(503,'Report capacity reached. Retry after older sessions expire.')
        broker=session['broker']
        if not demo and body.mode=='TECHNICAL' and (not broker or broker['expires']<=time.monotonic()):
            raise HTTPException(401,'Connect your Angel One account first.')
        if not demo and body.mode!='TECHNICAL' and not os.getenv('PMS_DATABASE_URL'):
            raise HTTPException(409,'Full PMS needs the historical dataset. Set PMS_DATABASE_URL in the hosting environment. Angel One candles alone cannot supply Module A or market breadth.')
        if demo and time.monotonic()-session['last_demo']<15:
            raise HTTPException(429,'Wait fifteen seconds before another demo.')
        if demo: session['last_demo']=time.monotonic()
        job_id=secrets.token_urlsafe(24)
        job={'id':job_id,'owner':sid,'status':'QUEUED','progress':0,'message':'Waiting for research worker',
             'created':time.monotonic(),'cancel':threading.Event(),'report':None,'error':None}
        JOBS[job_id]=job
    POOL.submit(work,job,body,dict(broker) if broker and body.mode=='TECHNICAL' and not demo else None,demo)
    return {'id':job_id,'status':'QUEUED'}

@app.post('/api/runs',status_code=202)
def run(request:Request,body:Run): return submit(request,body)

@app.post('/api/demo',status_code=202)
def demo(request:Request):
    body=Run(symbols=['DEMO_ALPHA','DEMO_BETA','DEMO_GAMMA'],start=date(2022,1,3),end=date(2023,12,29))
    return submit(request,body,demo=True)

def work(job,body,broker,demo):
    began=time.monotonic()
    def check():
        if job['cancel'].is_set(): raise Cancelled()
        if time.monotonic()-began>3600: raise DataGap('Run exceeded the one-hour limit. Reduce the symbol list or date range.')
    def progress(value,message):
        check()
        with LOCK: job.update(status='RUNNING',progress=value,message=message)
    try:
        progress(3,'Preparing isolated research account')
        if demo:
            data,calendar=demo_data()
            progress(40,'Synthetic data ready. This is a demonstration, not historical performance.')
            report=technical(body,data,calendar,progress,check,demo=True)
        elif body.mode=='TECHNICAL':
            progress(5,'Loading NSE equity instrument tokens')
            instruments=angel.master()
            unknown=[s for s in body.symbols if s not in instruments]
            if unknown: raise DataGap('Not found in current NSE EQ master: '+', '.join(unknown))
            warmup=body.start-timedelta(days=730)
            progress(8,'Downloading Nifty 50 session calendar')
            calendar=sorted(angel.candles(broker['key'],broker['token'],'99926000',warmup,body.end,check))
            if not calendar: raise DataGap('Angel One returned no Nifty 50 calendar candles.')
            data={}
            for n,symbol in enumerate(body.symbols):
                check()
                progress(10+int(30*n/len(body.symbols)),f'Downloading {symbol} ({n+1}/{len(body.symbols)})')
                data[symbol]=angel.candles(broker['key'],broker['token'],instruments[symbol],warmup,body.end,check)
                if not data[symbol]: raise DataGap(f'{symbol}: no historical candles returned.')
            report=technical(body,data,calendar,progress,check)
        else:
            report=database(body,os.environ['PMS_DATABASE_URL'],progress,check)
        check()
        with LOCK: job.update(status=report['status'],progress=100,message='Report ready',report=report)
    except Cancelled:
        with LOCK: job.update(status='CANCELLED',message='Run cancelled. No broker orders were placed.')
    except (DataGap,angel.BrokerError) as e:
        with LOCK: job.update(status='BLOCKED',message='Input checks blocked this run',error=str(e)[:2500])
    except Exception:
        # Never expose database URLs, broker responses or exception arguments.
        with LOCK: job.update(status='FAILED',message='Backtest could not complete',error='The data source or engine returned an error. Check database availability/schema and data coverage, or reduce the date range. No performance report was produced.')
    finally:
        if broker: broker.clear()
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
