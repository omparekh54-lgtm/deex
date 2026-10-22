"""Bounded, read-only SmartAPI adapter. No order endpoints exist here."""
from __future__ import annotations
import json
import math
import threading
import time
from datetime import date, timedelta
import httpx

BASE = 'https://apiconnect.angelone.in'
MASTER = 'https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json'
_lock = threading.Lock()
_last_request = 0.0
_master = None
_master_time = 0.0
ALLOWED_PATHS=frozenset({
    '/rest/auth/angelbroking/user/v1/loginByPassword',
    '/rest/secure/angelbroking/historical/v1/getCandleData',
})

class BrokerError(ValueError):
    pass

def call(path, payload, key, token='', *, retry=True):
    global _last_request
    if path not in ALLOWED_PATHS:
        raise BrokerError('This broker operation is not permitted.')
    if any(not isinstance(v,str) or any(ord(c)<32 or ord(c)==127 for c in v) for v in (key,token)):
        raise BrokerError('Invalid broker authentication input.')
    headers = {'X-PrivateKey': key, 'Authorization': 'Bearer '+token,
               'X-SourceID': 'WEB', 'X-UserType': 'USER',
               'X-ClientLocalIP': '127.0.0.1', 'X-ClientPublicIP': '127.0.0.1',
               'X-MACAddress': '00:00:00:00:00:00', 'Content-Type': 'application/json'}
    for attempt in range(3 if retry else 1):
        with _lock:
            time.sleep(max(0, .6 - (time.monotonic() - _last_request)))
            _last_request = time.monotonic()
        try:
            response = httpx.post(BASE+path, json=payload, headers=headers, timeout=35, verify=True, trust_env=False, follow_redirects=False)
            if response.status_code in (429, 502, 503, 504) and attempt < 2:
                time.sleep(1 + attempt * 2)
                continue
            if response.status_code != 200:
                raise BrokerError('Angel One request failed. Check your session, API app permissions and broker availability.')
            data = response.json()
        except (httpx.HTTPError, ValueError, TypeError):
            raise BrokerError('Could not reach Angel One. Retry after checking broker availability.') from None
        if not isinstance(data,dict) or not data.get('status'):
            # Never echo remote messages or responses: they may contain secrets.
            raise BrokerError('Angel One rejected the request. Check the API key, client code, PIN, current TOTP or JWT session.')
        return data.get('data')
    raise BrokerError('Angel One rate limit reached. Wait briefly and try again.')

def authenticate(key, client_code, pin, totp):
    data = call('/rest/auth/angelbroking/user/v1/loginByPassword',
                {'clientcode': client_code, 'password': pin, 'totp': totp}, key, retry=False)
    token = (data or {}).get('jwtToken')
    if not token:
        raise BrokerError('Angel One did not return an authenticated session.')
    return token

def master():
    global _master, _master_time
    if _master is None or time.monotonic() - _master_time > 21600:
        try:
            with httpx.stream('GET', MASTER, timeout=60, follow_redirects=False, verify=True, trust_env=False) as response:
                response.raise_for_status()
                content = bytearray()
                for chunk in response.iter_bytes():
                    content.extend(chunk)
                    if len(content) > 60_000_000:
                        raise BrokerError('Instrument master exceeds the download limit.')
            data = json.loads(content)
            _master = {r['symbol'][:-3]: r['token'] for r in data
                       if r.get('exch_seg') == 'NSE' and r.get('symbol', '').endswith('-EQ')}
            _master_time = time.monotonic()
        except (httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError):
            raise BrokerError('Angel One instrument list is unavailable.') from None
    return _master

def candles(key, token, symbol_token, start, end, check=lambda: None):
    rows = {}
    cursor = start
    while cursor <= end:
        check()
        stop = min(end, cursor + timedelta(days=1899))
        data = call('/rest/secure/angelbroking/historical/v1/getCandleData',
                    {'exchange': 'NSE', 'symboltoken': str(symbol_token), 'interval': 'ONE_DAY',
                     'fromdate': cursor.isoformat()+' 00:00', 'todate': stop.isoformat()+' 23:59'}, key, token)
        for record in data or []:
            if not isinstance(record, list) or len(record) < 6:
                raise BrokerError('Malformed historical candle returned by Angel One.')
            day = date.fromisoformat(str(record[0])[:10])
            values = [float(v) for v in record[1:6]]
            o,h,l,c,v = values
            if not all(math.isfinite(n) for n in values) or min(o,h,l,c) <= 0 or v < 0 or not l <= min(o,c) <= max(o,c) <= h:
                raise BrokerError('Invalid historical OHLCV data returned by Angel One.')
            if v != int(v):
                raise BrokerError('Invalid fractional share volume in historical data.')
            if day in rows and rows[day] != values:
                raise BrokerError('Conflicting historical candles returned by Angel One.')
            if start <= day <= end:
                rows[day] = values
        cursor = stop + timedelta(days=1)
    return rows
