"""Request, session and browser protections; never store broker credentials."""
from __future__ import annotations
import asyncio
import hashlib
import hmac
import os
import secrets
import threading
import time
from collections import OrderedDict, deque
from urllib.parse import urlsplit
from starlette.responses import JSONResponse

MAX_BODY=16_384
BODY_TIMEOUT=15
IDLE_TTL=15*60
ABSOLUTE_TTL=2*60*60
VERIFICATION_TTL=10*60


def production(): return os.getenv('RENDER','').lower()=='true'
def cookie_name(): return '__Host-deex_session' if production() else 'deex_session'

def headers():
    result={'Cache-Control':'no-store, private','Pragma':'no-cache',
        'X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer','X-Frame-Options':'DENY',
        'Permissions-Policy':'camera=(), microphone=(), geolocation=(), payment=(), usb=()',
        'Cross-Origin-Opener-Policy':'same-origin','Cross-Origin-Resource-Policy':'same-origin',
        'Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; frame-src 'none'; worker-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"}
    if production():result['Strict-Transport-Security']='max-age=31536000'
    return result


def error(detail,status): return JSONResponse({'detail':detail},status_code=status,headers=headers())

def valid_origin(request):
    # Pin the public host supplied by Render. Never trust X-Forwarded-Host/IP.
    expected=os.getenv('RENDER_EXTERNAL_URL','').rstrip('/') if production() else str(request.base_url).rstrip('/')
    if production():
        if not expected or urlsplit(expected).scheme!='https':return False
        if request.headers.get('host','').lower()!=urlsplit(expected).netloc.lower():return False
    origin=request.headers.get('origin')
    return not origin or origin.rstrip('/')==expected


def new_session(now):
    return {'created':now,'expires':now+IDLE_TTL,'absolute_expires':now+ABSOLUTE_TTL,
            'broker':None,'csrf':secrets.token_urlsafe(32)}


def purge(sessions,jobs,now):
    for sid,session in list(sessions.items()):
        if min(session['expires'],session['absolute_expires'])<=now:
            session.clear();del sessions[sid]
            for job_id,job in list(jobs.items()):
                if job['owner']==sid:
                    job['cancel'].set()
                    if job['status'] not in ('QUEUED','RUNNING'):del jobs[job_id]
        elif session.get('broker') and session['broker']['expires']<=now:
            session['broker']=None
    for job_id,job in list(jobs.items()):
        if now-job['created']>ABSOLUTE_TTL and job['status'] not in ('QUEUED','RUNNING'):
            del jobs[job_id]


class RateLimiter:
    """Bounded process-local buckets. Only opaque HMAC identifiers are retained."""
    def __init__(self):
        self.secret=secrets.token_bytes(32);self.buckets=OrderedDict();self.lock=threading.Lock()
    def identity(self,value):return hmac.new(self.secret,value.encode(),hashlib.sha256).hexdigest()
    def allow(self,category,value,limit,window,now=None):
        now=time.monotonic() if now is None else now
        key=(category,self.identity(value))
        with self.lock:
            bucket=self.buckets.pop(key,deque())
            while bucket and bucket[0]<=now-window:bucket.popleft()
            self.buckets[key]=bucket
            if len(bucket)>=limit:return False
            bucket.append(now)
            while len(self.buckets)>2048:self.buckets.popitem(last=False)
            return True
    def reset(self):
        with self.lock:self.buckets.clear()

LIMITER=RateLimiter()

class BoundedBodyMiddleware:
    """Bound streamed bodies too, before JSON decoding. Adds headers on errors."""
    def __init__(self,app):self.app=app
    async def __call__(self,scope,receive,send):
        if scope['type']!='http':return await self.app(scope,receive,send)
        request_headers={k.lower():v for k,v in scope['headers']}
        async def protected_send(message):
            if message['type']=='http.response.start':
                existing={k.lower():v for k,v in message.get('headers',[])}
                for key,value in headers().items():existing[key.lower().encode()]=value.encode()
                message={**message,'headers':list(existing.items())}
            await send(message)
        async def reject(detail,status):await error(detail,status)(scope,receive,protected_send)
        # Byte ranges are not needed by this small app. Reject them before the
        # static-file server's range parser, including pathological multi-ranges.
        if b'range' in request_headers:return await reject('Range requests are not supported.',416)
        if scope.get('query_string') and scope.get('path','').startswith('/api/'):
            return await reject('API parameters must not be sent in URLs.',400)
        if scope['method'] in ('POST','PUT','PATCH','DELETE'):
            if request_headers.get(b'content-type',b'').split(b';',1)[0].strip().lower()!=b'application/json':
                return await reject('Use an application/json request body.',415)
            length=request_headers.get(b'content-length')
            if length:
                try:count=int(length)
                except ValueError:return await reject('Invalid request size.',400)
                if count<0:return await reject('Invalid request size.',400)
                if count>MAX_BODY:return await reject('Request is too large.',413)
            body=bytearray()
            try:
                async with asyncio.timeout(BODY_TIMEOUT):
                    while True:
                        message=await receive()
                        if message['type']=='http.disconnect':return
                        body.extend(message.get('body',b''))
                        if len(body)>MAX_BODY:return await reject('Request is too large.',413)
                        if not message.get('more_body'):break
            except TimeoutError:
                return await reject('Request body timed out.',408)
            delivered=False
            async def bounded_receive():
                nonlocal delivered
                if not delivered:
                    delivered=True
                    return {'type':'http.request','body':bytes(body),'more_body':False}
                return await receive()
            return await self.app(scope,bounded_receive,protected_send)
        return await self.app(scope,receive,protected_send)
