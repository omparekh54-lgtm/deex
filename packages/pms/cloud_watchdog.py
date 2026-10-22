"""Read-only deployment checks; no recovery action or trading permission changes."""
from datetime import datetime, timedelta, time
from sqlalchemy import text
from packages.data_layer.cloud_sources import IST, SourceError

ROLE_SQL = """SELECT r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
 OR r.rolbypassrls OR pg_has_role(r.rolname,'pms_owner','MEMBER') AS elevated,
 EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname IN ('pms','public')
 AND has_schema_privilege(r.oid,n.oid,'CREATE')) AS schema_create,
 EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname IN ('pms','public') AND c.relkind IN ('r','p','v','m') AND
 (has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES'))) AS writes,
 EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='pms' AND has_function_privilege(r.oid,p.oid,'EXECUTE')) AS function_execute
 FROM pg_roles r WHERE r.rolname='pms_status'"""


def expected_session(now, years, special=(), unverified=()):
    now = now.astimezone(IST)
    day = now.date() if now.time() >= time(20,15) else now.date()-timedelta(days=1)
    for _ in range(14):
        if str(day.year) not in years or day.isoformat() in unverified:
            raise SourceError('MONITOR_CALENDAR_UNVERIFIED')
        if day.isoformat() in special or (day.weekday()<5 and day.isoformat() not in years[str(day.year)]):
            return day
        day -= timedelta(days=1)
    raise SourceError('MONITOR_SESSION_UNAVAILABLE')


def assess_collection(run, now):
    if run is None:
        return 'COLLECTION_MISSED'
    stamp = run['started_at']
    if stamp.tzinfo is None or stamp > now + timedelta(minutes=1):
        return 'COLLECTION_TIMESTAMP_INVALID'
    if run['status']=='RUNNING':
        return 'COLLECTION_STALE' if now-stamp>timedelta(minutes=32) else 'COLLECTION_IN_PROGRESS'
    d = run.get('details') or {}
    if (run['status']!='SUCCESS' or d.get('complete') is not True
            or d.get('version')!='CLOUD_COLLECTION_v1'
            or run.get('finished_at') is None or run['finished_at']>now
            or (d.get('raw_archive') or {}).get('status')!='VERIFIED'
            or (d.get('raw_archive') or {}).get('anonymous_read_denied') is not True):
        return 'COLLECTION_INCOMPLETE'
    return None


def database_checks(engine, now):
    codes = []
    with engine.connect() as c, c.begin():
        c.execute(text('SET TRANSACTION READ ONLY'))
        c.execute(text("SET LOCAL statement_timeout='20000'"))
        state = dict(c.execute(text('SELECT execution_mode,new_orders_enabled FROM pms.safety_state')).mappings().one())
        if state != {'execution_mode':'PAPER','new_orders_enabled':False}:
            codes.append('PAPER_SAFETY_CHANGED')
        size = c.execute(text('SELECT pg_database_size(current_database())')).scalar_one()
        if size >= 900*1024*1024:
            codes.append('DATABASE_CAPACITY_RESERVE')
        role = c.execute(text(ROLE_SQL)).mappings().one_or_none()
        if role is None or any(role.values()):
            codes.append('STATUS_ROLE_ACCESS_CHANGED')
        exposed = c.execute(text("""SELECT count(*) FROM pg_namespace n,
          LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
          WHERE n.nspname='pms' AND a.grantee=0 AND a.privilege_type IN ('USAGE','CREATE')""")).scalar_one()
        if exposed:
            codes.append('PUBLIC_PMS_SCHEMA_ACCESS')
        dq = c.execute(text("SELECT count(*) FROM pms.data_quality_issues WHERE severity='ERROR' AND resolved_at IS NULL")).scalar_one()
        if dq:
            codes.append('CRITICAL_DATA_QUALITY_ERRORS')
        snapshots = c.execute(text("""SELECT metadata FROM pms.provider_snapshots
          WHERE dataset='NSE_TRADING_CALENDAR' AND provider='NSE' AND observed_at<=:now
          ORDER BY observed_at DESC LIMIT 100"""), {'now':now}).scalars().all()
        years, special, unverified = {}, set(), set()
        for d in snapshots:
            for y, days in d.get('holiday_years',{}).items():
                years.setdefault(y, days)
            special.update(d.get('special_sessions',[]))
            unverified.update(d.get('unverified_special_session_dates',[]))
        day = expected_session(now, years, special, unverified-special)
        run = c.execute(text("""SELECT status,started_at,finished_at,details FROM pms.worker_runs
          WHERE job_name='cloud_collection' AND details->>'trade_date'=:day
          ORDER BY started_at DESC,id DESC LIMIT 1"""), {'day':str(day)}).mappings().first()
        code = assess_collection(run, now)
        if code:
            codes.append(code)
    return {'codes':codes,'expected_session':str(day),'database_bytes':size,
            'database_read_only':True,'status_role_verified':bool(role) and not any(role.values())}
