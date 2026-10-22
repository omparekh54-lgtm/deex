"""A reproducible 100-stock sample chosen using information before the replay."""
from __future__ import annotations
import math
from datetime import timedelta
from sqlalchemy import text

UNIVERSE_SIZE = 100
UNIVERSE_POLICY = 'TOP_100_PRE_START_63_SESSION_ADTV_V1'
# This samples the historical base universe, not the future strategy winners.
# All A/B/C/E admission rules are still evaluated on every replay date.
UNIVERSE_SQL = """
with calendar as (
 select distinct trade_date from pms.module_e_benchmark_bars
 where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX')
 and trade_date<:start and trade_date>=:warmup
 order by trade_date desc limit 252
), last_session as (select max(trade_date) d from calendar),
prices as (
 select distinct on(p.instrument_id,p.trade_date) p.instrument_id,p.trade_date,p.close,p.volume,p.turnover
 from pms.daily_prices p join calendar c using(trade_date)
 where lower(p.source) like 'nse%'
 order by p.instrument_id,p.trade_date,lower(p.source)
), ranked as (
 select *,row_number() over(partition by instrument_id order by trade_date desc) n from prices
), liquidity as (
 select instrument_id,count(*) history_sessions,
 avg(coalesce(turnover,close*volume)) filter(where n<=63) adtv,
 count(*) filter(where n<=63) liquidity_sessions,
 max(trade_date) last_price_date
 from ranked group by instrument_id
), sectors as (
 select distinct on(s.instrument_id) s.instrument_id,s.sector,s.industry
 from pms.sector_history s cross join last_session l
 where s.effective_from<=l.d and (s.effective_to is null or s.effective_to>l.d)
 order by s.instrument_id,s.effective_from desc,s.id desc
)
select 'NSE:'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) instrument_id,
 l.adtv,s.sector,s.industry,l.last_price_date
from pms.instruments i join liquidity l on l.instrument_id=i.id
join sectors s on s.instrument_id=i.id cross join last_session d
where i.exchange='NSE' and i.trading_symbol like '%-EQ'
 and i.metadata->>'ingestion_duplicate_of' is null
 and i.listed_from<=d.d and (i.delisted_on is null or i.delisted_on>d.d)
 and l.history_sessions=252 and l.liquidity_sessions=63 and l.last_price_date=d.d
 and l.adtv>=20000000
 and coalesce(s.sector,'')<>''
 and lower(concat(s.sector,' ',s.industry)) !~ '(financial|bank|insurance|insurer|nbfc)'
 and not exists(select 1 from pms.index_constituents_history x
   where x.instrument_id=i.id and x.index_code='NIFTY50'
   and x.effective_from<=d.d and (x.effective_to is null or x.effective_to>d.d))
order by l.adtv desc,instrument_id
limit :limit
"""

def automatic_universe(snapshot,start):
    from app.engine import DataGap
    params={'start':start,'warmup':start-timedelta(days=1100),'limit':UNIVERSE_SIZE}
    rows=[dict(r) for r in snapshot.execute(text(UNIVERSE_SQL),params).mappings().all()]
    if len(rows)!=UNIVERSE_SIZE:
        raise DataGap(f'The historical dataset supports {len(rows)} of the required 100 stocks before the start date. Complete the historical universe and reference data before running. No smaller sample was substituted.')
    symbols=[r['instrument_id'] for r in rows]
    if len(set(symbols))!=UNIVERSE_SIZE or any(not s.startswith('NSE:') for s in symbols):
        raise DataGap('Historical stock identities are ambiguous. Repair the instrument dataset before running.')
    if any(not math.isfinite(float(r['adtv'])) or float(r['adtv'])<2e7 for r in rows):
        raise DataGap('Historical stock liquidity is invalid.')
    for row in rows:
        row['adtv']=float(row['adtv'])
    return symbols,{
        'policy':UNIVERSE_POLICY,'stock_count':UNIVERSE_SIZE,'selection_cutoff':'strictly before '+start.isoformat(),
        'selection':'Top 100 historically listed NSE EQ non-financial stocks outside Nifty 50, ordered by preceding 63-session average traded value; 252 official price sessions required.',
        'ranking_tie_breaker':'canonical symbol ascending',
        'membership':'Frozen at the start; no future winners, current constituents or end-date coverage used to choose names.',
        'scope':'A 100-stock historical sample, not the full NSE universe. Not all scanned stocks will qualify or trade.',
        'stocks':rows,
    }
