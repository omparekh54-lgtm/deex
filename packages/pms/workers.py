from __future__ import annotations
import json
import os
from datetime import date, datetime, timezone
from uuid import uuid4
from celery import Celery
from celery.schedules import crontab
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, InterfaceError, TimeoutError as DBTimeout
from apps.api.main import _render_safe_database_url
from packages.pms.database import online_engine
from packages.pms.brokers import PaperBroker
from packages.pms.reconciliation import ReconciliationService
from packages.pms.safety import SafetyController
from packages.pms.module_a_context import IST

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
celery_app = Celery(
    "pms_workers",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["packages.pms.worker_tasks_extended"],
)
celery_app.conf.update(
    timezone="Asia/Kolkata",
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_accept_content=["json"],
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=3,
    result_expires=86400,
    task_soft_time_limit=1800,
    task_time_limit=1860,
    task_acks_late=False,
    task_reject_on_worker_lost=False,
    task_acks_on_failure_or_timeout=True,
)
celery_app.conf.beat_schedule = {
    "market-close-pipeline": {
        "task": "pms.market_close",
        "schedule": crontab(hour=19, minute=0),
    },
    "market-close-retry-1": {
        "task": "pms.market_close",
        "schedule": crontab(hour=19, minute=15),
    },
    "market-close-retry-2": {
        "task": "pms.market_close",
        "schedule": crontab(hour=19, minute=30),
    },
    "data-health-check": {
        "task": "pms.data_health",
        "schedule": crontab(hour=18, minute=40, day_of_week="1-5"),
    },
    "surveillance-guard": {
        "task": "pms.surveillance_guard",
        "schedule": crontab(hour=18, minute=45, day_of_week="1-5"),
    },
    "readiness-refresh": {
        "task": "pms.readiness_refresh",
        "schedule": crontab(hour=19, minute=0, day_of_week="1-5"),
    },
    "weekly-paper-validation": {
        "task": "pms.paper_validation",
        "schedule": crontab(hour=19, minute=10, day_of_week="5"),
        "args": (60,),
    },
}


def _engine():
    url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required for workers")
    return online_engine(_render_safe_database_url(url), worker=True)


def _record(worker, job):
    engine = _engine()
    run_id = str(uuid4())
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "insert into worker_runs(id,worker_name,job_name,status) values (:id,:w,:j,'RUNNING')"
                ),
                dict(id=run_id, w=worker, j=job),
            )
    except Exception:
        engine.dispose()
        raise RuntimeError("Worker journal unavailable") from None
    return engine, run_id


def _finish(engine, run_id, status, details):
    with engine.begin() as conn:
        conn.execute(
            text(
                "update worker_runs set status=:s,finished_at=:t,details=cast(:d as jsonb) where id=:id"
            ),
            dict(
                s=status,
                t=datetime.now(timezone.utc),
                d=json.dumps(details, default=str, allow_nan=False),
                id=run_id,
            ),
        )


def _fail(engine, run_id, exc, lock=None):
    # Journal/safety storage may itself be unreachable; never expose either exception string.
    if lock:
        try:
            SafetyController(engine).lock(lock)
        except Exception:
            pass
    try:
        _finish(engine, run_id, "FAILED", {"error_type": type(exc).__name__})
    except Exception:
        pass


@celery_app.task(bind=True, name="pms.market_close", acks_late=True, max_retries=3)
def market_close_pipeline(self, trade_date=None):
    from packages.pms.daily_orchestrator import DailyOrchestrator

    target = trade_date or datetime.now(IST).date().isoformat()
    engine = None
    try:
        engine = _engine()
        result = DailyOrchestrator(engine).run(date.fromisoformat(target))
        if result["status"] == "FAILED" and result.get("error_type") in {
            "OperationalError",
            "InterfaceError",
            "TimeoutError",
        }:
            raise OperationalError("retryable daily database failure", None, None)
        if result["status"] in {"FAILED", "BLOCKED"}:
            raise RuntimeError(
                "Daily orchestrator " + result["status"] + "; inspect worker_runs"
            )
        return result
    except (OperationalError, InterfaceError, DBTimeout):
        # Freeze the date across retries. Never silently switch to another session at midnight.
        raise self.retry(
            exc=RuntimeError("Daily database temporarily unavailable"),
            args=(),
            kwargs={"trade_date": target},
            countdown=min(60, 5 * 2**self.request.retries),
            max_retries=3,
        ) from None
    except Exception as exc:
        raise RuntimeError(
            "Daily worker failed (" + type(exc).__name__ + "); inspect journal"
        ) from None
    finally:
        if engine is not None:
            engine.dispose()


@celery_app.task(name="pms.reconcile")
def reconcile(account_mode="PAPER"):
    if account_mode != "PAPER":
        raise ValueError("This worker reconciles the formal PAPER account only")
    engine, run_id = _record("execution_worker", "reconcile")
    try:
        result = ReconciliationService(engine, PaperBroker(engine)).run("PAPER")
        payload = dict(
            reconciliation_id=result.run_id,
            ok=result.ok,
            discrepancies=list(result.discrepancies),
        )
        _finish(engine, run_id, "SUCCESS" if result.ok else "MISMATCH", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc, "reconciliation_error:" + type(exc).__name__)
        raise RuntimeError(
            "Reconciliation worker failed (" + type(exc).__name__ + ")"
        ) from None
    finally:
        engine.dispose()


@celery_app.task(name="pms.data_health")
def data_health_check():
    engine, run_id = _record("market_worker", "data_health")
    try:
        with engine.connect() as conn:
            latest = conn.execute(
                text("select max(trade_date) from daily_prices")
            ).scalar_one_or_none()
            errors = int(
                conn.execute(
                    text(
                        "select count(*) from data_quality_issues where severity='ERROR' and resolved_at is null"
                    )
                ).scalar_one()
            )
        stale = latest is None or (datetime.now(IST).date() - latest).days > 5
        if stale or errors:
            SafetyController(engine).lock("data_health_failed")
        payload = dict(
            latest_price=str(latest) if latest else None, errors=errors, stale=stale
        )
        _finish(engine, run_id, "DEGRADED" if stale or errors else "SUCCESS", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc, "data_health_exception")
        raise RuntimeError(
            "Data health worker failed (" + type(exc).__name__ + ")"
        ) from None
    finally:
        engine.dispose()
