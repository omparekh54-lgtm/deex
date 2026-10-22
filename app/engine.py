"""Website adapters around the versioned PMS strategy and event-driven engine."""
from __future__ import annotations
import hashlib
import json
import math
import random
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import pandas as pd
from packages.pms.config import load_strategy_config
from packages.pms.module_b import Bar, RallyState, RallyTracker, advance_rally
from packages.pms.backtest import EventDrivenBacktester, MarketBar, Signal
from packages.pms.backtest_matrix import Case, metrics, plain, round_trips, fingerprint
from packages.pms.trade_engine import TradeState
from packages.pms.research import ResearchVariant

SOURCE_COMMIT = '55495ec7bc1ea2e18a80fedb22158c0b0cf56ce2'
MODES = {
    'TECHNICAL': 'Angel One technical study',
    'FULL': 'Full PMS A + B + C + E',
    'BA': 'Fundamentals + technical',
    'BAC': 'Fundamentals + technical + ranking',
    'SETUP2': 'Earnings anchored Setup 2',
}
class DataGap(ValueError):
    pass
class Cancelled(Exception):
    pass

def configuration(request):
    cfg = load_strategy_config()
    cfg['backtest'].update(initial_capital=request.capital, slippage_bps=request.slippage_bps,
        impact_bps=request.impact_bps, brokerage_bps=request.brokerage_bps,
        research_leg1_always_armed=request.always_armed)
    cfg['technical']['min_adr_20d_pct'] = request.adr_min
    cfg['backtest']['research_adr_window'] = request.adr_window
    cfg['backtest']['research_adr_upper_cap'] = request.adr_cap
    return cfg

def frame_for(rows, adr_window=20):
    frame = pd.DataFrame([{'day':d,'open':v[0],'high':v[1],'low':v[2],'close':v[3],'volume':v[4]} for d,v in sorted(rows.items())]).set_index('day')
    for n in (9,21):
        frame['ema'+str(n)] = frame.close.ewm(span=n, adjust=False, min_periods=n).mean()
    frame['baseline'] = frame.volume.shift(1).rolling(50).mean()
    frame['avg_volume'] = frame.volume.rolling(20).mean()
    frame['min_volume'] = frame.volume.rolling(20).min()
    frame['adtv'] = (frame.close*frame.volume).rolling(20).mean()
    frame['adr'] = ((frame.high/frame.low).rolling(adr_window).mean()-1)*100
    frame['high52'] = frame.high.rolling(252).max()
    return frame

def technical(request, data, calendar, progress, check, *, demo=False):
    cfg = configuration(request)
    bars, signals, frames, rejection = [], {}, {}, Counter()
    window = sorted(d for d in calendar if request.start <= d <= request.end)
    if not window:
        raise DataGap('No exchange sessions found in the selected period.')
    for number, (symbol, rows) in enumerate(sorted(data.items())):
        check()
        frame = frame_for(rows, request.adr_window)
        frames[symbol] = frame
        history = [d for d in calendar if d < request.start]
        if len(frame[frame.index < request.start]) < 252:
            raise DataGap(f'{symbol}: fewer than 252 warmup sessions. Choose a later start or another symbol.')
        needed = set(history[-252:]) | set(window)
        if not needed.issubset(rows):
            missing = len(needed-set(rows))
            raise DataGap(f'{symbol}: {missing} required NSE sessions are missing. Repair the history before running.')
        tracker = RallyTracker()
        for i,(day,row) in enumerate(frame.iterrows()):
            if i < 50 or day > request.end:
                continue
            prev = frame.iloc[i-1]
            if not math.isfinite(row.ema21) or not math.isfinite(prev.ema21):
                continue
            if request.start <= day <= request.end:
                bars.append(MarketBar(symbol,day,row.open,row.high,row.low,row.close,row.ema9,prev.ema9,prev.ema21))
            tracker = advance_rally(tracker, Bar(str(day),row.open,row.high,row.low,row.close,row.volume,row.ema21),prev.close,prev.ema21,row.baseline,cfg)
            if tracker.state == RallyState.SIGNAL_CREATED:
                if request.start <= day <= request.end:
                    reasons = []
                    if i+1 < 252: reasons.append('history')
                    if row.close <= 30: reasons.append('price')
                    if row.avg_volume <= 100000: reasons.append('average_volume')
                    if row.min_volume <= 75000: reasons.append('minimum_volume')
                    if not 1e7 <= row.adtv <= 5e9: reasons.append('turnover')
                    if row.adr < request.adr_min: reasons.append('adr')
                    if request.adr_cap is not None and row.adr > request.adr_cap: reasons.append('adr_cap')
                    if (1-row.close/row.high52)*100 > 50: reasons.append('distance_to_high')
                    if not reasons:
                        signals.setdefault(day,[]).append(Signal(symbol,day,tracker.signal_high,tracker.signal_low,adtv=row.adtv))
                    else:
                        rejection.update(reasons)
                tracker = RallyTracker(last_date=str(day))
        progress(45 + int(25*(number+1)/len(data)), f'Calculated rally signals for {symbol}')
    broker = EventDrivenBacktester(cfg=cfg)
    def eligible(symbol, day):
        check()
        # Pending entry checks use the last close strictly BEFORE this session.
        previous = frames[symbol][frames[symbol].index < day]
        if previous.empty: return False
        adr = float(previous.iloc[-1].adr)
        return math.isfinite(adr) and adr >= request.adr_min and (request.adr_cap is None or adr <= request.adr_cap)
    def guard(position,bar,quantity):
        # Unknown sector is intentionally not invented. Full mode enforces sectors.
        heat = sum(max(0,(p.entry-p.stop))*p.remaining_shares for key,p in broker.positions.items()
                   if key != position.instrument_id and p.entry and p.stop)
        equity = broker._equity_at_open()
        budget = max(0,equity*.035-heat)
        return max(0,min(quantity,int(budget/(position.entry-position.stop))))
    def close(day, account):
        check()
        if day in window:
            progress(70+int(25*(window.index(day)+1)/len(window)), 'Replaying '+str(day))
    result = broker.run(bars,lambda day: sorted(signals.get(day,[]),key=lambda s:s.instrument_id),
        eligibility_provider=eligible, signal_eligibility_provider=lambda i,d: True,
        allow_reentry=request.reentry, trading_sessions=sorted(calendar),entry_guard=guard,on_session_close=close)
    limits = [
        'Technical-only exploratory study. Modules A, C and E are not applied.',
        'Historical Nifty 50 membership, earnings blackout and sector limits are unavailable from candles and are not applied.',
        'Current NSE EQ instrument selection has survivorship bias. Delisted securities are not reconstructed.',
        'Corporate actions and Angel candle adjustment basis are unverified. Splits, bonuses and dividends may distort results.',
        'Turnover is estimated as close times volume. Symbol order is alphabetical rather than Module C ranking.',
        'Daily-bar stops use conservative same-day entry/stop handling. Circuit limits and intraday liquidity are not modeled.',
        'Delivery cost rates are configurable assumptions; pre-2026 historical charges are not a complete dated tariff archive.',
        'Universe price/liquidity filters are checked on each signal, rather than every four sessions.',
        'Open positions remain marked to the last close and are not forced closed at the period end.',
    ]
    return report(request,cfg,result,{'rejections':dict(rejection),'signals':sum(map(len,signals.values())),
        'limitations':limits,'source':'SYNTHETIC_DEMO' if demo else 'ANGEL_ONE_ONE_DAY',
        'dataset_fingerprint': fingerprint({s:{str(d):v for d,v in sorted(r.items())} for s,r in data.items()}),
        'calendar_source':'synthetic demo sessions' if demo else 'Angel One Nifty 50 daily candles',
        'raw_price_adjustment':'UNVERIFIED','scope':'EXPLICIT_SYMBOL_SAMPLE'}, demo=demo)

def database(request, url, progress, check):
    from sqlalchemy import create_engine
    from packages.pms.replay_service import prepared_replay
    from packages.pms.matrix_replay import ResearchDataGap
    variant = ResearchVariant(request.mode, use_module_c=request.mode!='BA', use_module_e=request.mode in ('FULL','SETUP2'),
                              setup_2=request.mode=='SETUP2',reentry=request.reentry)
    case=Case('DEEX_'+request.mode,variant)
    cfg=configuration(request)
    if url.startswith('postgres://'): url=url.replace('postgres://','postgresql+psycopg2://',1)
    elif url.startswith('postgresql://'): url=url.replace('postgresql://','postgresql+psycopg2://',1)
    engine=create_engine(url,pool_pre_ping=True,pool_size=1,max_overflow=0,
        connect_args={'connect_timeout':15,'options':'-c default_transaction_read_only=on -c statement_timeout=120000'})
    try:
        progress(15,'Checking point-in-time dataset and coverage gates')
        with prepared_replay(engine,['NSE:'+s for s in request.symbols],request.start,request.end,[case]) as prepared:
            check()
            progress(35,'Replaying fresh fundamental, technical, ranking and regime decisions')
            # Cooperative cancel and real session progress without changing strategy logic.
            from packages.pms.replay_service import execution_identity, ENGINE_VERSION
            from packages.pms.backtest_matrix import Cell
            from packages.pms.matrix_replay import MatrixReplay
            blockers=prepared.gate(Cell(prepared.period,case),cfg)
            if blockers: raise DataGap('; '.join(blockers))
            cfg['backtest']['matrix_research_only']=True
            replay=MatrixReplay(prepared.snapshot,prepared.symbols,cfg,case,request.start,request.end)
            original=replay.on_close
            days=sorted(d for d in replay.calendar.sessions if request.start<=d<=request.end)
            def on_close(day,account):
                check()
                original(day,account)
                if day in days: progress(35+int(60*(days.index(day)+1)/max(1,len(days))), 'Replayed '+str(day))
            replay.on_close=on_close
            result,diag=replay.run()
            diag.update(execution_identity(cfg,case),dataset_fingerprint=prepared.manifest['fingerprint'],
                        code_version=prepared.code_version,scope=prepared.manifest['scope'],acceptance='NOT_CERTIFIED')
            diag.setdefault('limitations',[]).extend([
                'Module D is parked. Setup 2 is research only.',
                'Draft policy normalizations remain provisional. Benchmark resolved to Nifty MidSmallcap 400 in the inherited configuration.',
                'Single-run inspection of 2025 onward is exploratory holdout use, not certified out-of-sample performance.',
                'Full PMS runs use the configured historical database, not the Angel candle download. No database rows are changed.',
                'Inherited transaction-cost assumptions are not a complete dated historical tariff archive.'
            ])
            return report(request,cfg,result,diag)
    except ResearchDataGap as e:
        raise DataGap(str(e)) from None
    finally:
        engine.dispose()

def report(request,cfg,result,diag,*,demo=False):
    closed,active=round_trips(result)
    period=SimpleNamespace(start=request.start,end=request.end)
    performance=metrics(result,period)
    curve=[];peak=result.initial_capital
    for day,equity in result.equity_curve:
        peak=max(peak,equity)
        curve.append({'date':day.isoformat(),'equity':equity,'drawdown_pct':(1-equity/peak)*100})
    payload={'title':'DEEX PMS Backtest Report','created_at':datetime.now(timezone.utc).isoformat(),
        'status':'COMPLETED' if closed else 'NO_TRADES','demo':demo,
        'acceptance':'SYNTHETIC_DEMO' if demo else 'EXPLORATORY_NOT_CERTIFIED',
        'mode':request.mode,'mode_label':MODES[request.mode],
        'parameters':{'symbols':request.symbols,'start':str(request.start),'end':str(request.end),'capital':request.capital,
            'reentry':request.reentry,'adr_min':request.adr_min,'adr_window':request.adr_window,'adr_cap':request.adr_cap},
        'metrics':performance,'equity_curve':curve,'trades':closed,'open_positions':list(active.values()),
        'fills':[asdict(f) for f in result.fills], 'events':[asdict(e) for e in result.events],
        'diagnostics':diag,'configuration':cfg,'engine_source_commit':SOURCE_COMMIT,
        'rules_source':'PMS_Rules_v1(3).docx','execution_enabled':False}
    payload=plain(payload)
    payload['report_fingerprint']=fingerprint(payload)
    return payload

def demo_data():
    rng=random.Random(17)
    days=list(pd.bdate_range('2020-01-01','2023-12-29').date)
    data={}
    for symbol,offset in [('DEMO_ALPHA',0),('DEMO_BETA',25),('DEMO_GAMMA',50)]:
        price=100.;rows={}
        for i,day in enumerate(days):
            phase=(i+offset)%110
            change=(.025 if 55<=phase<67 else -.022 if 67<=phase<73 else .05 if phase==73 else -.008 if 74<=phase<82 else rng.uniform(-.009,.006))
            open_=price*(1.045 if phase==73 else 1+rng.uniform(-.003,.003));price=max(35,price*(1+change))
            high=max(open_,price)*1.021;low=min(open_,price)*(.99 if phase==73 else .979)
            volume=650000 if 55<=phase<67 else 220000
            rows[day]=[round(open_,4),round(high,4),round(low,4),round(price,4),volume]
        data[symbol]=rows
    return data,days
