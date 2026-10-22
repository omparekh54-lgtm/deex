"""Sanitized read-only operational evidence; availability is not trading readiness."""

import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from sqlalchemy.engine import make_url

STALE_AFTER = timedelta(minutes=32)  # Beyond the worker hard limit plus journal slack.


def runtime_posture():
    key = os.getenv("OPS_API_KEY", "")
    redis = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        from urllib.parse import urlsplit

        endpoint = urlsplit(redis)
        broker_transport = (
            "TLS"
            if endpoint.scheme == "rediss"
            else (
                "LOCAL"
                if endpoint.hostname in {"localhost", "127.0.0.1", "::1"}
                else "REVIEW_PRIVATE_NETWORK"
            )
        )
    except ValueError:
        broker_transport = "INVALID"
    return dict(
        operator_key_configured=bool(key),
        operator_key_accepted=bool(key) and len(key) <= 256,
        operator_key_strength_review=len(key) < 32,
        mutation_auth="FAIL_CLOSED",
        broker_transport=broker_transport,
        serializer="JSON_ONLY",
        real_money_broker="DISABLED",
        startup_backfill_enabled=os.getenv("RUN_REFERENCE_GAP_BACKFILL", "0").lower()
        in {"1", "true", "yes", "on"},
        backup_restore_evidence="NOT_VERIFIED",
        execution_readiness=False,
    )


WORKERS_SQL = """select id,worker_name,job_name,status,started_at,finished_at from pms.worker_runs
where (status='RUNNING' and started_at<:stale) or started_at>=:recent order by started_at desc limit 100"""
ACCESS_SQL = """select r.rolname role,n.nspname schema,c.relname relation,c.relkind,c.relrowsecurity,
has_schema_privilege(r.oid,n.oid,'USAGE') schema_usage,
has_table_privilege(r.oid,c.oid,'SELECT') can_read,
(has_table_privilege(r.oid,c.oid,'INSERT') or has_table_privilege(r.oid,c.oid,'UPDATE') or has_table_privilege(r.oid,c.oid,'DELETE') or has_table_privilege(r.oid,c.oid,'TRUNCATE')) can_write
from pg_class c join pg_namespace n on n.oid=c.relnamespace cross join pg_roles r
where n.nspname='pms' and c.relkind in ('r','p','v','m') and r.rolname in ('anon','authenticated')
and has_schema_privilege(r.oid,n.oid,'USAGE') and
(has_table_privilege(r.oid,c.oid,'SELECT') or has_table_privilege(r.oid,c.oid,'INSERT') or has_table_privilege(r.oid,c.oid,'UPDATE') or has_table_privilege(r.oid,c.oid,'DELETE') or has_table_privilege(r.oid,c.oid,'TRUNCATE')) order by r.rolname,c.relname"""


def worker_health(rows, now):
    stale = []
    failed = []
    for row in rows:
        stamp = row["started_at"]
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        if row["status"] == "RUNNING" and now - stamp > STALE_AFTER:
            stale.append(str(row["id"]))
        if row["status"] in {"FAILED", "PARTIAL", "MISMATCH", "INTERRUPTED"}:
            failed.append(str(row["id"]))
    return dict(
        status=(
            "NO_EVIDENCE"
            if not rows
            else "ATTENTION" if stale or failed else "OBSERVED"
        ),
        stale_running_ids=stale,
        failed_or_partial_ids=failed,
        returned_runs=len(rows),
        note="Bounded recent journal window; OBSERVED does not prove that every expected job ran or that a worker is currently alive.",
    )


def reliability_report(engine, *, now=None):
    now = now or datetime.now(timezone.utc)
    with engine.connect() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                text(WORKERS_SQL),
                {"stale": now - STALE_AFTER, "recent": now - timedelta(days=7)},
            )
            .mappings()
            .all()
        ]
    return dict(
        posture=runtime_posture(),
        workers=worker_health(rows, now),
        runs=rows,
        acceptance="NOT_CERTIFIED",
    )


def safe_details(value):
    # Historical journals may contain old raw errors; do not return those through status APIs.
    import re

    if isinstance(value, dict):
        return {
            k: (
                "[withheld]"
                if k.lower()
                in {
                    "error",
                    "exception",
                    "password",
                    "token",
                    "secret",
                    "authorization",
                    "ops_api_key",
                    "api_key",
                }
                else safe_details(v)
            )
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [safe_details(v) for v in value]
    if isinstance(value, str):
        return re.sub(
            r"postgres(?:ql)?(?:\+\w+)?://[^\s\x27\x22]+",
            "[database connection withheld]",
            value,
        )
    return value
