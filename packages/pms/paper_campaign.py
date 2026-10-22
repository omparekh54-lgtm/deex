"""Frozen forward-campaign identities and evidence; preparation never enables orders."""
from __future__ import annotations
from datetime import datetime, timedelta
from uuid import uuid4
from sqlalchemy import text
from packages.pms.backtest_matrix import fingerprint, plain
from packages.pms.campaign_readiness import qualifies_elapsed_session
from packages.pms.module_a_context import IST


def operational_proofs(receipt, *, now, code_version):
    """Accept only fresh, matching, successfully measured operational receipts."""
    empty=dict(deployed_code_verified=False,scheduler_verified=False,rehearsal_verified=False)
    try:
        stamp=datetime.fromisoformat(receipt['checked_at'])
        if stamp.tzinfo is None or not timedelta(0)<=now-stamp<=timedelta(hours=24):return empty
        if receipt['code_version']!=code_version or receipt['version']!='PAPER_OPERATIONS_v1':return empty
        checks=receipt['checks']
        return dict(deployed_code_verified=checks.get('deployed_code') is True,
                    scheduler_verified=checks.get('scheduled_delivery') is True and checks.get('archive') is True,
                    rehearsal_verified=checks.get('restore') is True and checks.get('read_only_rehearsal') is True)
    except (ValueError,KeyError,TypeError,AttributeError):return empty


def universe(conn):
    return [dict(r) for r in conn.execute(text("select id,nse_symbol,isin from pms.instruments where exchange='NSE' and active order by id")).mappings()]


def draft_manifest(readiness, cfg, identities, *, code_version):
    if not identities:raise ValueError('campaign_universe_empty')
    return plain(dict(version='PAPER_CAMPAIGN_v1',status='PREPARED',campaign_activated=False,activated_at=None,
        code_version=code_version,config_hash=fingerprint(cfg),config=cfg,
        universe=identities,universe_hash=fingerprint(identities),
        initial_capital=cfg['backtest']['initial_capital'],readiness=readiness,
        elapsed_campaign_sessions=0,blockers=readiness['blockers']))


def active_campaign(conn):
    row=conn.execute(text('select id,activated_at,manifest from pms.paper_campaigns where closed_at is null')).mappings().one_or_none()
    return dict(row) if row else None


def campaign_context(conn,cfg,code_version):
    campaign=active_campaign(conn)
    if not campaign:return dict(campaign_id=None,campaign_guard_ok=True)
    m=campaign['manifest']
    okay=(m['config_hash']==fingerprint(cfg) and m['code_version']==code_version
          and m['universe_hash']==fingerprint(universe(conn)))
    return dict(campaign_id=str(campaign['id']),campaign_guard_ok=okay)


def freeze_campaign(conn, manifest, *, readiness, now):
    """Called only in a transaction after fresh launch checks; never changes safety."""
    conn.execute(text("select pg_advisory_xact_lock(hashtext('pms_campaign_activation'))"))
    state=conn.execute(text('select execution_mode,new_orders_enabled,emergency_stop,locked_reason from pms.safety_state where singleton=true for update')).mappings().one()
    if dict(state)!=dict(execution_mode='PAPER',new_orders_enabled=False,emergency_stop=False,locked_reason=None):
        raise ValueError('campaign_requires_resolved_paper_lock_and_entries_disabled')
    stamp=datetime.fromisoformat(readiness['checked_at'])
    from packages.pms.matrix_data import code_fingerprint
    from packages.pms.config import load_strategy_config
    if stamp.tzinfo is None or not timedelta(0)<=now-stamp<=timedelta(minutes=5):
        raise ValueError('campaign_readiness_stale')
    if manifest['code_version']!=code_fingerprint() or manifest['config_hash']!=fingerprint(load_strategy_config()):
        raise ValueError('campaign_code_or_configuration_changed')
    if readiness['status']!='READY_FOR_ACTIVATION' or readiness['blockers']:
        raise ValueError('campaign_readiness_blocked')
    if active_campaign(conn):raise ValueError('campaign_already_active')
    if manifest['universe_hash']!=fingerprint(universe(conn)) or manifest['universe_hash']!=fingerprint(manifest['universe']):
        raise ValueError('campaign_universe_changed')
    if (manifest['code_version']!=readiness['code_version'] or manifest['config_hash']!=readiness['config_fingerprint']
            or fingerprint(manifest['config'])!=manifest['config_hash']
            or manifest['initial_capital']!=manifest['config']['backtest']['initial_capital']
            or manifest['initial_capital']!=readiness['initial_capital']):
        raise ValueError('campaign_manifest_changed')
    if now.tzinfo is None:raise ValueError('campaign_timestamp_required')
    # Clock is read inside the transaction; caller cannot backdate activation.
    actual=conn.execute(text('select transaction_timestamp()')).scalar_one()
    if abs((actual-now).total_seconds())>60:raise ValueError('campaign_timestamp_stale')
    frozen={**manifest,'campaign_activated':True,'status':'ACTIVE','activated_at':actual.isoformat(),'readiness':readiness}
    cid=str(uuid4())
    import json
    conn.execute(text('insert into pms.paper_campaigns(id,activated_at,manifest) values(:id,:stamp,cast(:manifest as jsonb))'),
                 dict(id=cid,stamp=actual,manifest=json.dumps(frozen,default=str,allow_nan=False)))
    return {**frozen,'campaign_id':cid}


def campaign_evidence(campaign, runs, official_sessions, *, now):
    """Re-evaluate actual journal rows every time; duplicate dates count once."""
    m=campaign['manifest'];cid=str(campaign['id']);eligible={}
    for row in runs:
        r=dict(row.get('details') or {})
        # Database timestamps/status are authoritative, never user-supplied payload clocks.
        r.update(started_at=row['started_at'].isoformat(),finished_at=row['finished_at'].isoformat() if row.get('finished_at') else '',status=row['status'])
        if (r.get('campaign_id')!=cid or not r.get('campaign_guard_ok') or row.get('finished_at') is None
                or row['finished_at']>now):continue
        if qualifies_elapsed_session(r,activated_at=campaign['activated_at'].isoformat(),
                config_hash=m['config_hash'],code_version=m['code_version'],official_sessions=official_sessions):
            eligible[r['trade_date']]=str(row['id'])
    elapsed=max(0,(now-campaign['activated_at']).days)
    return dict(campaign_id=cid,activated_at=campaign['activated_at'].isoformat(),
        elapsed_calendar_days=elapsed,elapsed_campaign_sessions=len(eligible),
        qualifying_sessions=sorted(eligible),journal_ids=[eligible[d] for d in sorted(eligible)],
        duration_gate_passed=elapsed>=60 and bool(eligible),final_acceptance=False,live_promotion=False)


def load_evidence(conn, *, now):
    campaign=active_campaign(conn)
    if not campaign:return None
    sessions=set(conn.execute(text("select distinct trade_date from pms.module_e_benchmark_bars where source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and benchmark_code='NIFTY50'")).scalars())
    rows=[dict(r) for r in conn.execute(text("select id,status,started_at,finished_at,details from pms.worker_runs where job_name='daily_orchestrator' and started_at>=:stamp order by started_at,id"),{'stamp':campaign['activated_at']}).mappings()]
    return campaign_evidence(campaign,rows,sessions,now=now)
