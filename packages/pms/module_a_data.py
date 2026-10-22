"""Read-only, point-in-time reference inputs for the fundamental scanner."""
from __future__ import annotations

from datetime import datetime, time, timedelta
from sqlalchemy import text
from packages.pms.module_a_context import as_ist

# One row per instrument. Deduplicate market sources BEFORE the 63-session limit.
# Historical matrix runs set :historical_research=true and therefore use only
# official NSE rows. Production keeps its existing Angel-One-first behaviour.
CONTEXT_SQL = """
with calendar as (
 select distinct trade_date from module_e_benchmark_bars where benchmark_code='NIFTY50'
 and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date<=cast(:market_date as date)
), price_daily as (
 select distinct on (p.instrument_id,p.trade_date) p.instrument_id,p.trade_date,p.close,p.volume,p.turnover
 from daily_prices p join calendar c on c.trade_date=p.trade_date
 where (not cast(:historical_research as boolean) or lower(p.source) like 'nse%')
 and (cast(:instrument_pk as bigint) is null or p.instrument_id=cast(:instrument_pk as bigint))
 order by p.instrument_id,p.trade_date desc,
 case when cast(:historical_research as boolean) and lower(p.source) like 'nse%' then 0
      when p.source='angel_one' then 0 when p.source like 'NSE%' then 1 else 2 end,p.source
), price_sessions as (
 select *,row_number() over(partition by instrument_id order by trade_date desc) session_rank from price_daily
), prices as (
 select instrument_id,max(trade_date) price_date,(array_agg(close order by trade_date desc))[1] close,
 avg(coalesce(turnover,close*volume))/10000000.0 avg_traded_value_cr,count(*) liquidity_sessions
 from price_sessions where session_rank<=:sessions group by instrument_id
), shareholding as (
 select distinct on (instrument_id) instrument_id,period_end,promoter_pledge_pct,total_shares
 from shareholding_history where filed_at<=:as_of and period_end<=cast(:market_date as date)
 order by instrument_id,period_end desc,filed_at desc,revision_number desc,source
), shares as (
 select distinct on (instrument_id) instrument_id,shares_outstanding from shares_outstanding_history
 where effective_from<=cast(:market_date as date) and (effective_to is null or effective_to>cast(:market_date as date))
 and knowable_at<=:as_of order by instrument_id,effective_from desc,knowable_at desc,source
), sectors as (
 select distinct on (instrument_id) instrument_id,sector,industry from sector_history
 where effective_from<=cast(:reference_date as date) and (effective_to is null or effective_to>cast(:reference_date as date))
 order by instrument_id,effective_from desc,id desc
), surveillance_coverage as (
 select surveillance_type,max(latest) latest from (
 select surveillance_type,max(effective_from) latest from surveillance_history
 where effective_from<=:as_of and source like 'NSE_%' group by surveillance_type
 union all
 select t.kind, max((metadata->>'date')::date::timestamp at time zone 'Asia/Kolkata') latest
 from provider_snapshots p cross join (values ('ASM'),('GSM')) t(kind)
 where p.provider='NSE' and p.dataset='CLOUD_SURVEILLANCE' and p.observed_at<=:as_of
 and p.metadata->>'complete'='true' group by t.kind
 ) verified group by surveillance_type
), active_surveillance as (
 select distinct instrument_id from surveillance_history h where surveillance_type in ('ASM','GSM')
 and effective_from<=:as_of and (effective_to is null or
   case when source='NSE_SURVEILLANCE' then effective_to>:as_of
        else (effective_to at time zone 'Asia/Kolkata')::date>=cast(:market_date as date) end)
), data_errors as (
 select instrument_id,count(*) n from data_quality_issues where severity='ERROR' and resolved_at is null group by instrument_id
)
select i.id,i.exchange,i.nse_symbol,i.trading_symbol,i.sector current_sector,i.listed_from,i.delisted_on,i.metadata,
 sh.period_end shareholding_period,sh.promoter_pledge_pct,coalesce(so.shares_outstanding,sh.total_shares) total_shares,
 px.price_date,px.close,px.avg_traded_value_cr,coalesce(px.liquidity_sessions,0) liquidity_sessions,
 sec.sector,sec.industry,(sv.instrument_id is not null) asm_gsm,
 (select count(*) from surveillance_coverage where latest>=:surveillance_since and surveillance_type in ('ASM','GSM')) surveillance_types_ready,
 coalesce(dq.n,0)+coalesce((select n from data_errors where instrument_id is null),0) open_data_errors
from instruments i left join shareholding sh on sh.instrument_id=i.id
left join shares so on so.instrument_id=i.id left join prices px on px.instrument_id=i.id
left join sectors sec on sec.instrument_id=i.id left join active_surveillance sv on sv.instrument_id=i.id
left join data_errors dq on dq.instrument_id=i.id
where i.exchange='NSE' and i.active=true and (cast(:instrument_pk as bigint) is null or i.id=cast(:instrument_pk as bigint))
order by i.nse_symbol
"""


def context_params(as_of: datetime, cfg: dict, instrument_pk=None):
    stamp = as_ist(as_of)
    market_date = stamp.date() if stamp.time() >= time(15, 30) else stamp.date()-timedelta(days=1)
    historical_research = bool(
        cfg.get('backtest', {}).get('matrix_research_only')
        or cfg.get('backtest', {}).get('final_acceptance_scope')
    )
    return {'as_of': stamp, 'market_date': market_date, 'reference_date': stamp.date(), 'instrument_pk': instrument_pk,
            'sessions': int(cfg['fundamental'].get('liquidity_sessions',63)),
            'surveillance_since': stamp-timedelta(days=int(cfg['fundamental'].get('max_surveillance_age_days',7)),
            ), 'historical_research': historical_research}


def normalize_context(row: dict, as_of: datetime, cfg: dict) -> dict:
    stamp = as_ist(as_of)
    context = dict(row)
    metadata = context.get('metadata') or {}
    observation = metadata.get('sector_classification') or {}
    observed = observation.get('observed_at') or metadata.get('classification_updated_at')
    knowable = bool(observed and as_ist(datetime.fromisoformat(observed))<=stamp)
    if not context.get('sector') and knowable:
        context['sector'] = context.get('current_sector')
    if not context.get('industry') and knowable:
        context['industry'] = observation.get('industry') or metadata.get('industry')
    context['suspended'] = str(observation.get('is_suspended')).lower()=='true' if knowable else None
    cloud_status=metadata.get('cloud_status') or {}
    if cloud_status.get('observed_at') and as_ist(datetime.fromisoformat(cloud_status['observed_at']))<=stamp:
        context['suspended']=cloud_status.get('suspended')
    context['listing_known'] = bool(context.get('listed_from') and context['listed_from']<=stamp.date()
                                    and (context.get('delisted_on') is None or context['delisted_on']>stamp.date()))
    context['surveillance_known'] = context.get('surveillance_types_ready',0)==2 and (
        stamp.date().isoformat()>='2024-01-23' or cfg['fundamental'].get('legacy_surveillance_verified',False))
    close, shares = context.get('close'), context.get('total_shares')
    context['market_cap_cr'] = float(close)*float(shares)/10_000_000 if close is not None and shares is not None else None
    context['instrument_id'] = 'NSE:'+str(context.get('nse_symbol') or str(context['trading_symbol']).removesuffix('-EQ'))
    return context


class ModuleAReferenceInputs:
    def __init__(self, engine, cfg):
        self.engine, self.cfg = engine, cfg

    def contexts(self, as_of: datetime, instrument_pk=None) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(text(CONTEXT_SQL),context_params(as_of,self.cfg,instrument_pk)).mappings().all()
        return [normalize_context(dict(row),as_of,self.cfg) for row in rows]
