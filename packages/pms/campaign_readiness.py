"""Read-only forward PAPER launch checks; failed checks never enable entries."""
from __future__ import annotations

from datetime import date, datetime, time
import json

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from packages.pms.backtest_matrix import fingerprint, plain
from packages.pms.module_a_context import IST

READINESS_SQL = """select jsonb_build_object(
 'checked_at',transaction_timestamp(),
 'safety',(select to_jsonb(s)-'updated_by' from pms.safety_state s where singleton=true),
 'active_instruments',(select count(*) from pms.instruments where exchange='NSE' and active),
 'missing_sectors',(select count(*) from pms.instruments i where i.exchange='NSE' and i.active
    and not exists(select 1 from pms.sector_history h where h.instrument_id=i.id
    and h.effective_from<=:day and (h.effective_to is null or h.effective_to>:day) and btrim(h.sector)<>'')),
 'v2_reprocessing_pending',(select count(*) from pms.financial_filings f where f.filing_timestamp<=least(:stamp,transaction_timestamp())
    and f.xbrl_uri is not null and coalesce(f.raw_payload->'financial_extraction'->>'version','')<>'FINANCIAL_XBRL_v2'),
 'annual_cash_flow_companies',(select count(distinct f.instrument_id) from pms.financial_filings f
    join pms.financial_facts v on v.filing_id=f.id and v.fact_name='operating_cash_flow'
    where lower(f.filing_kind) in ('annual','year','yearly') and f.filing_timestamp<=least(:stamp,transaction_timestamp())
    and v.value is not null and v.value::text not in ('NaN','Infinity','-Infinity')),
 'critical_dq_errors',(select count(*) from pms.data_quality_issues where severity='ERROR' and resolved_at is null),
 'benchmark_rows',(select count(distinct benchmark_code) from pms.module_e_benchmark_bars
    where trade_date=:day and benchmark_code in ('NIFTY50','NIFTY500','NIFTYMIDSMALLCAP400','INDIAVIX')
    and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and close>0 and close::text not in ('NaN','Infinity','-Infinity')),
 'price_instruments',(select count(distinct instrument_id) from pms.daily_prices where trade_date=:day),
 'latest_collection',(select jsonb_build_object('status',status,'started_at',started_at,'finished_at',finished_at,
    'details',details-'documents') from pms.worker_runs where job_name='cloud_collection'
    and details->>'trade_date'=cast(:day as text) order by started_at desc,id desc limit 1),
 'latest_collection_attempt',(select jsonb_build_object('status',status,'trade_date',details->>'trade_date',
    'error_code',details->>'error_code','started_at',started_at) from pms.worker_runs
    where job_name='cloud_collection' order by started_at desc,id desc limit 1),
 'paper_snapshots',(select count(*) from pms.portfolio_snapshots where account_mode='PAPER'),
 'period_identity_ready',exists(select 1 from pg_index ix where ix.indisunique and ix.indisvalid
    and ix.indrelid=coalesce(to_regclass('pms.financial_filings_local'),to_regclass('pms.financial_filings'))
    and (select array_agg(a.attname::text order by k.ordinality)
    from unnest(ix.indkey) with ordinality k(attnum,ordinality)
    join pg_attribute a on a.attrelid=ix.indrelid and a.attnum=k.attnum)
    =array['instrument_id','period_end','filing_timestamp','source','consolidated','revision_number','filing_kind'])
) facts"""


def readiness_query(day):
    return text(READINESS_SQL).bindparams(day=day,stamp=datetime.combine(day,time(23,59,59),tzinfo=IST))


def connector_readiness_sql(day):
    return str(readiness_query(day).compile(dialect=postgresql.dialect(),compile_kwargs={'literal_binds':True}))


def assess_readiness(facts, cfg, *, day, code_version, operational_proofs=None):
    proofs = operational_proofs or {}
    checks=[]
    def check(code, passed, detail):
        checks.append(dict(code=code,status='PASS' if passed else 'BLOCKED',detail=detail))
    state=facts.get('safety') or {}
    check('PAPER_ONLY',state.get('execution_mode')=='PAPER','PAPER mode required; this check cannot enable live trading')
    check('SAFETY_UNLOCKED',not state.get('emergency_stop',True) and not state.get('locked_reason'),
          dict(emergency_stop=bool(state.get('emergency_stop')),lock_reason=state.get('locked_reason')))
    check('FINANCIAL_PERIOD_IDENTITY',facts.get('period_identity_ready') is True,'Migration 012 must precede V2 writers')
    check('REFERENCE_COVERAGE',facts.get('active_instruments',0)>0 and facts.get('missing_sectors')==0,
          dict(active_instruments=facts.get('active_instruments'),missing_dated_sectors=facts.get('missing_sectors')))
    check('FINANCIAL_V2',facts.get('v2_reprocessing_pending')==0 and facts.get('annual_cash_flow_companies',0)>0,
          dict(pending=facts.get('v2_reprocessing_pending'),annual_cash_flow_companies=facts.get('annual_cash_flow_companies')))
    check('CURRENT_MARKET_DATA',facts.get('benchmark_rows')==4 and facts.get('price_instruments',0)>0,
          dict(requested_session=day,benchmark_series=facts.get('benchmark_rows'),price_instruments=facts.get('price_instruments')))
    collection=facts.get('latest_collection') or {}
    details=collection.get('details') or {}
    archive=details.get('raw_archive') or {}
    check('COMPLETE_COLLECTION',collection.get('status')=='SUCCESS' and details.get('complete') is True
          and archive.get('status')=='VERIFIED' and archive.get('anonymous_read_denied') is True,
          dict(requested_session=day,latest_attempt=facts.get('latest_collection_attempt')))
    check('CRITICAL_DQ',facts.get('critical_dq_errors')==0,dict(open_errors=facts.get('critical_dq_errors')))
    for code,key in [('DEPLOYED_CODE','deployed_code_verified'),('SCHEDULER_AND_ARCHIVE','scheduler_verified'),
                     ('RECOVERY_AND_REHEARSAL','rehearsal_verified')]:
        check(code,proofs.get(key) is True,'Must be verified from current deployment/operational receipts, never inferred from code presence')
    blocked=[r['code'] for r in checks if r['status']=='BLOCKED']
    return plain(dict(version='PAPER_CAMPAIGN_READINESS_v1',status='BLOCKED' if blocked else 'READY_FOR_ACTIVATION',
        checked_at=facts.get('checked_at'),as_of=day,code_version=code_version,
        strategy_version=cfg['version'],config_fingerprint=fingerprint(cfg),initial_capital=cfg['backtest']['initial_capital'],
        checks=checks,blockers=blocked,campaign_activated=False,activated_at=None,
        elapsed_campaign_sessions=0,activation_manifest_frozen=False,
        rule='Freeze activation time/config/code/universe only after all checks pass. Count distinct same-day verified exchange-session journals created after activation; historical and catch-up runs never count. The rule document requires 2–3 months of paper observation.'))


def qualifies_elapsed_session(run, *, activated_at, config_hash, code_version, official_sessions):
    """Checks real journal timing, rather than the dates asserted in its payload."""
    try:
        day=date.fromisoformat(run['trade_date'])
        start=datetime.fromisoformat(run['started_at'].replace('Z','+00:00'))
        finish=datetime.fromisoformat(run['finished_at'].replace('Z','+00:00'))
        activation=datetime.fromisoformat(activated_at.replace('Z','+00:00'))
        if any(t.tzinfo is None for t in (start,finish,activation)):
            return False
        return (day in official_sessions
            and activation<=datetime.combine(day,time(9,15),tzinfo=IST)
            and activation<=start<=finish
            and start.astimezone(IST).time()>=time(15,30)
            and start.astimezone(IST).date()==finish.astimezone(IST).date()==day
            and finish.astimezone(IST).time()>=time(15,30)
            and run['status']=='SUCCESS' and run.get('mode')=='PAPER'
            and run.get('config_hash')==config_hash and run.get('code_version')==code_version
            and run.get('paper_execution_status')=='SUCCESS'
            and run.get('collection_complete') is True and run.get('reconciliation_ok') is True)
    except (ValueError,KeyError,TypeError,AttributeError):
        return False
