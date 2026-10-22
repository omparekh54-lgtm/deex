"""One restartable PAPER after-close decision workflow for every entry point."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime
import hashlib
import json
import os
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import make_url

from packages.pms.config import load_strategy_config
from packages.pms.market_calendar import MarketCalendar
from packages.pms.module_a_context import IST
from packages.pms.module_c_pipeline import ModuleCPipeline
from packages.pms.module_e_pipeline import ModuleEPipeline, compact_quality
from packages.pms.portfolio_accounting import PortfolioAccountingService
from packages.pms.brokers import PaperBroker
from packages.pms.reconciliation import ReconciliationService

VERSION = "DAILY_ORCHESTRATOR_v1.1"
JOB = "daily_orchestrator"
LOCK = "pms_daily_orchestrator:PAPER"


def encode(value):
    return json.dumps(value, default=str, allow_nan=False, sort_keys=True)


class RunJournal:
    """Uses the existing worker_runs table; no new schema is required.

    The dedicated connection must be direct PostgreSQL or a SESSION pooler.
    A transaction pooler cannot hold a session advisory lock safely.
    """

    def __init__(self, engine):
        if make_url(str(engine.url)).port == 6543:
            raise ValueError(
                "Daily orchestrator requires direct PostgreSQL or a session pooler, not port 6543"
            )
        self.engine = engine
        self._connection = None
        self._backend_pid = None

    @contextmanager
    def exclusive(self):
        with self.engine.connect() as conn:
            acquired = False
            try:
                acquired = bool(
                    conn.execute(
                        text("select pg_try_advisory_lock(hashtext(:key))"),
                        {"key": LOCK},
                    ).scalar_one()
                )
                conn.commit()
                if acquired:
                    self._connection = conn
                    self._backend_pid = conn.execute(
                        text("select pg_backend_pid()")
                    ).scalar_one()
                    conn.commit()
                yield acquired
            finally:
                self._connection = None
                if acquired:
                    try:
                        conn.rollback()
                        released = conn.execute(
                            text("select pg_advisory_unlock(hashtext(:key))"),
                            {"key": LOCK},
                        ).scalar_one()
                        conn.commit()
                        if not released:
                            conn.invalidate()
                            raise RuntimeError("Daily orchestrator lock was lost")
                    except BaseException:
                        conn.invalidate()
                        raise

    def latest(self, key):
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("""select id,status,details from worker_runs
                where job_name=:job and details->>'run_key'=:key
                order by started_at desc,id desc limit 1"""),
                    {"job": JOB, "key": key},
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def save(self, report):
        if self._connection is None or self._connection.invalidated:
            raise RuntimeError("Daily orchestrator lock connection unavailable")
        pid = self._connection.execute(text("select pg_backend_pid()")).scalar_one()
        self._connection.commit()
        if pid != self._backend_pid:
            raise RuntimeError("Daily orchestrator database session changed")
        with self.engine.begin() as conn:
            if (report["status"] == "RUNNING" and not report.get("stages")) or report[
                "status"
            ] in {"BLOCKED", "FAILED", "SKIPPED"}:
                # Withdraw old proposals before any retry, and on a failed finish.
                # Broker orders and filled holdings are deliberately separate.
                params = {
                    "d": report["trade_date"],
                    "pattern": "%:MODULE_C_%:PAPER",
                    "stale": encode(
                        {
                            "selected": False,
                            "qualified": False,
                            "kind": "REJECTED",
                            "rejected_reasons": ["daily_run_not_ready"],
                        }
                    ),
                }
                conn.execute(
                    text(
                        """update module_c_rankings set selected=false,qualified=false,
                    rejected_reasons='["daily_run_not_ready"]'::jsonb
                    where trade_date=:d and strategy_version like :pattern and selected"""
                    ),
                    params,
                )
                conn.execute(
                    text(
                        """update strategy_decisions set decision=decision || cast(:stale as jsonb)
                    where module='C' and strategy_version like :pattern
                    and (as_of at time zone 'Asia/Kolkata')::date=:d
                    and decision->>'selected'='true'"""
                    ),
                    params,
                )
            conn.execute(
                text(
                    """insert into worker_runs(id,worker_name,job_name,status,started_at,finished_at,details)
                values(:id,'daily_orchestrator',:job,:status,:started,:finished,cast(:details as jsonb))
                on conflict(id) do update set status=excluded.status,finished_at=excluded.finished_at,details=excluded.details"""
                ),
                {
                    "id": report["run_id"],
                    "job": JOB,
                    "status": report["status"],
                    "started": report["started_at"],
                    "finished": report.get("finished_at"),
                    "details": encode(report),
                },
            )


class DailyServices:
    def __init__(self, engine, cfg):
        self.engine, self.cfg = engine, cfg

    def campaign_context(self, code_version):
        from packages.pms.paper_campaign import campaign_context
        with self.engine.connect() as c:
            return campaign_context(c,self.cfg,code_version)

    def settle(self, day):
        from packages.pms.formal_paper import FormalPaperExecution

        return FormalPaperExecution(self.engine, self.cfg).settle(day)

    def admit(self, day):
        from packages.pms.formal_paper import FormalPaperExecution

        from packages.pms.matrix_data import code_fingerprint
        if not self.campaign_context(code_fingerprint())['campaign_guard_ok']:
            return dict(status='BLOCKED',reasons=['campaign_frozen_inputs_changed'])
        return FormalPaperExecution(self.engine, self.cfg).admit(day)

    def preflight(self, day, now):
        source_complete=False
        calendar = MarketCalendar.from_database(self.engine, now)
        forward = calendar.forward(day, 1)
        if not forward:
            return {
                "status": "BLOCKED",
                "reasons": ["exchange_calendar_unavailable_or_unverified"],
            }
        if forward[0] != day:
            return {"status": "SKIPPED", "reasons": ["exchange_non_session"]}
        if now.date() == day and (now.hour, now.minute) < (15, 30):
            return {"status": "SKIPPED", "reasons": ["market_not_closed"]}
        if os.getenv('PMS_CLOUD_COLLECTION_REQUIRED', '0') == '1':
            from packages.pms.cloud_collection import collection_gate
            with self.engine.connect() as conn:
                source_complete=collection_gate(conn,day,now)
                if not source_complete:
                    return {'status':'BLOCKED','reasons':['cloud_collection_missing_partial_failed_or_stale']}
        with self.engine.connect() as conn:
            counts = dict(
                conn.execute(
                    text("""select benchmark_code,count(distinct trade_date) from module_e_benchmark_bars
                where trade_date=:d and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and close>0
                and close::text not in ('NaN','Infinity','-Infinity')
                and benchmark_code in ('NIFTY50','NIFTY500','NIFTYMIDSMALLCAP400','INDIAVIX')
                group by benchmark_code"""),
                    {"d": day},
                ).all()
            )
            prices = int(
                conn.execute(
                    text(
                        "select count(distinct instrument_id) from daily_prices where trade_date=:d"
                    ),
                    {"d": day},
                ).scalar_one()
            )
            errors = int(
                conn.execute(
                    text(
                        "select count(*) from data_quality_issues where severity='ERROR' and resolved_at is null"
                    )
                ).scalar_one()
            )
            safety = (
                conn.execute(
                    text(
                        "select emergency_stop,locked_reason from safety_state where singleton=true"
                    )
                )
                .mappings()
                .first()
            )
        reasons = [
            code + "_session_missing_or_duplicate"
            for code in ("NIFTY50", "NIFTY500", "NIFTYMIDSMALLCAP400", "INDIAVIX")
            if counts.get(code) != 1
        ]
        if not prices:
            reasons.append("equity_prices_not_ready")
        if errors:
            reasons.append("unresolved_data_quality_errors")
        if not safety or safety["emergency_stop"] or safety["locked_reason"]:
            reasons.append("execution_safety_locked_or_unavailable")
        return {
            "status": "BLOCKED" if reasons else "READY",
            "reasons": reasons,
            "collection_complete":source_complete,
            "benchmark_session_rows": counts,
            "equity_instruments_with_bars": prices,
            "critical_dq_errors": errors,
            "coverage_note": "Presence is not completeness; Module E and A/B verify usable histories.",
        }

    def reconcile(self, day):
        result = ReconciliationService(
            self.engine,
            PaperBroker(
                self.engine, initial_cash=float(self.cfg["backtest"]["initial_capital"])
            ),
        ).run("PAPER")
        return {
            "ok": result.ok,
            "run_id": result.run_id,
            "discrepancies": list(result.discrepancies),
            "scope": "Internal PAPER ledger consistency; not independent live broker evidence.",
        }

    def snapshot(self, day):
        service = PortfolioAccountingService(
            self.engine, initial_capital=float(self.cfg["backtest"]["initial_capital"])
        )
        snapshot = service.snapshot(day, "PAPER")
        service.persist_snapshot(snapshot)
        return {
            "date": str(day),
            "mode": "PAPER",
            "nav": snapshot.nav,
            "cash": snapshot.cash,
            "positions": len(snapshot.positions),
            "portfolio_heat_pct": snapshot.portfolio_heat_pct,
        }

    def regime(self, day):
        service = ModuleEPipeline(self.engine, self.cfg)
        _, decision = service.evaluate(day, persist=True)
        return {
            "band": decision.band,
            "capacity": decision.max_open_trades,
            "data_ready": decision.data_ready,
            "reason": decision.reason,
            "data_quality": compact_quality(
                service.last_report.get("data_quality", {})
            ),
        }

    def allocation(self, day):
        from packages.pms.formal_paper import FormalPaperExecution

        reentries = FormalPaperExecution(self.engine, self.cfg).reentry_signals(day)
        report = ModuleCPipeline(self.engine, self.cfg).generate(
            day, persist=True, persist_upstream=True, reentry_signals=reentries
        )
        values = asdict(report)
        return {
            name: values[name]
            for name in (
                "universe_size",
                "a_eligible",
                "b_signals",
                "ranked",
                "qualified",
                "selected",
                "rejected",
                "blockers",
                "rejection_counts",
                "upstream",
                "strategy_version",
            )
        }


class DailyOrchestrator:
    def __init__(self, engine, cfg=None, *, journal=None, services=None, clock=None):
        self.cfg = cfg or load_strategy_config()
        self.journal = journal or RunJournal(engine)
        self.services = services or DailyServices(engine, self.cfg)
        self.clock = clock or (lambda: datetime.now(IST))

    def _now(self):
        value = self.clock()
        if value.tzinfo is None:
            raise ValueError("Orchestrator clock must be timezone aware")
        return value.astimezone(IST)

    def run(self, day: date | None = None, *, check_only=False, rerun=False):
        now = self._now()
        day = day or now.date()
        if day > now.date():
            raise ValueError("Future sessions are not supported")
        if not check_only and day != now.date():
            raise ValueError(
                "Persisted daily runs require today's IST date; use --check for past dates"
            )
        from packages.pms.backtest_matrix import fingerprint as config_fingerprint
        from packages.pms.matrix_data import code_fingerprint
        fingerprint = config_fingerprint(self.cfg)
        key = f"{day}:PAPER:{VERSION}:{fingerprint}"
        base = {
            "trade_date": str(day),
            "market_data_session": str(day),
            "mode": "PAPER",
            "implementation": VERSION,
            "config_hash": fingerprint,
            "code_version": code_fingerprint(),
            "run_key": key,
            "broker_orders_submitted": 0,
            "proposal_only": False,
            "decision_outputs_are_proposals": True,
            "external_broker_orders_submitted": 0,
        }
        if check_only:
            preflight = self.services.preflight(day, now)
            return {
                **base,
                "status": preflight["status"],
                "check_only": True,
                "preflight": preflight,
            }
        if hasattr(self.services,'campaign_context'):
            base.update(self.services.campaign_context(base['code_version']))
        else:
            base.update(campaign_id=None,campaign_guard_ok=True)
        with self.journal.exclusive() as acquired:
            if not acquired:
                return {
                    **base,
                    "status": "BUSY",
                    "reason": "another_daily_run_holds_lock",
                }
            prior = self.journal.latest(key)
            if base["campaign_guard_ok"] and prior and prior["details"].get("campaign_id")==base["campaign_id"] and prior["status"] == "SUCCESS" and not rerun and os.getenv('PMS_CLOUD_COLLECTION_REQUIRED','0')!='1':
                replay = {**prior["details"], "replayed": True}
                try:
                    replay["paper_admission"] = self.services.admit(day)
                    replay["paper_execution_status"] = replay["paper_admission"][
                        "status"
                    ]
                except Exception as exc:
                    replay.update(
                        status="FAILED",
                        reasons=["paper_admission_failed"],
                        error_type=type(exc).__name__,
                    )
                self.journal.save(replay)
                return replay
            if prior and prior["status"] == "RUNNING":
                interrupted = {
                    **prior["details"],
                    "status": "INTERRUPTED",
                    "finished_at": now.isoformat(),
                    "reasons": ["previous_process_lost_daily_lock"],
                }
                self.journal.save(interrupted)
            report = {
                **base,
                "run_id": str(uuid4()),
                "status": "RUNNING",
                "started_at": now.isoformat(),
                "attempt": int(prior["details"].get("attempt", 0)) + 1 if prior else 1,
                "previous_run_id": str(prior["id"]) if prior else None,
                "stages": {},
                "reasons": [],
            }
            self.journal.save(report)
            active = None

            def stage(name, operation):
                nonlocal active
                active = name
                item = {"status": "RUNNING", "started_at": self._now().isoformat()}
                report["stages"][name] = item
                self.journal.save(report)
                item["result"] = operation()
                item.update(status="SUCCESS", finished_at=self._now().isoformat())
                self.journal.save(report)
                return item["result"]

            try:
                settlement = stage(
                    "paper_settlement", lambda: self.services.settle(day)
                )
                preflight = stage(
                    "preflight", lambda: self.services.preflight(day, now)
                )
                report['collection_complete']=bool(preflight.get('collection_complete',False))
                if not report['campaign_guard_ok']:
                    report.update(status='BLOCKED',reasons=['campaign_frozen_inputs_changed'])
                elif settlement["status"] in {"BLOCKED", "FAILED"}:
                    report.update(
                        status="BLOCKED", reasons=["paper_settlement_requires_review"]
                    )
                elif preflight["status"] != "READY":
                    report.update(
                        status=preflight["status"], reasons=preflight["reasons"]
                    )
                else:
                    reconciliation = stage(
                        "paper_reconciliation", lambda: self.services.reconcile(day)
                    )
                    report['reconciliation_ok']=reconciliation['ok'] is True
                    if not reconciliation["ok"]:
                        report.update(
                            status="BLOCKED", reasons=["paper_reconciliation_mismatch"]
                        )
                    else:
                        stage("paper_snapshot", lambda: self.services.snapshot(day))
                        regime = stage("module_e", lambda: self.services.regime(day))
                        allocation = stage(
                            "modules_a_b_c", lambda: self.services.allocation(day)
                        )
                        reasons = list(allocation["blockers"])
                        if not regime["data_ready"]:
                            reasons.append("module_e_data_unavailable")
                        if not allocation.get("upstream", {}).get(
                            "configuration_ready", False
                        ):
                            reasons.append("module_a_configuration_not_ready")
                        report.update(
                            status="BLOCKED" if reasons else "SUCCESS",
                            reasons=list(dict.fromkeys(reasons)),
                            capacity=regime["capacity"],
                            regime=regime["band"],
                            selected=allocation["selected"],
                            paper_nav=report["stages"]["paper_snapshot"]["result"][
                                "nav"
                            ],
                        )
                        if reasons and allocation["selected"]:
                            raise RuntimeError(
                                "Blocked daily run unexpectedly contains selected proposals"
                            )
            except Exception as exc:
                # Persist a bounded type-only error: exception strings can expose DB URLs/parameters.
                report.update(
                    status="FAILED",
                    reasons=["stage_failed:" + str(active)],
                    error_type=type(exc).__name__,
                )
                if active:
                    report["stages"][active].update(
                        status="FAILED", error_type=type(exc).__name__
                    )
            report["finished_at"] = self._now().isoformat()
            self.journal.save(report)
            if report["status"] == "SUCCESS":
                try:
                    report["paper_admission"] = self.services.admit(day)
                    report["paper_execution_status"] = report["paper_admission"][
                        "status"
                    ]
                    self.journal.save(report)
                except Exception as exc:
                    report.update(
                        status="FAILED",
                        reasons=["paper_admission_failed"],
                        error_type=type(exc).__name__,
                    )
                    self.journal.save(report)
            report['finished_at']=self._now().isoformat()
            self.journal.save(report)
            return report
