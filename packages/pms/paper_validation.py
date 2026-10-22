from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import date, datetime, time
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, text

from packages.pms.audit import AuditEvent, AuditTrail


@dataclass(frozen=True)
class PaperValidationCriteria:
    min_calendar_days: int = 60
    min_fills: int = 20
    min_snapshots: int = 20
    max_reconciliation_mismatches: int = 0
    max_worker_failures: int = 0
    max_data_quality_errors: int = 0
    max_drawdown_pct: float = 20.0


@dataclass(frozen=True)
class PaperValidationResult:
    run_id: str
    passed: bool
    metrics: dict[str, Any]
    gates: dict[str, bool]
    criteria: PaperValidationCriteria


class PaperValidationService:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.audit = AuditTrail(engine)

    def evaluate(
        self,
        started_on: date,
        ended_on: date,
        *,
        criteria: PaperValidationCriteria | None = None,
        actor: str = "system",
        persist: bool = True,
    ) -> PaperValidationResult:
        criteria = criteria or PaperValidationCriteria()
        if ended_on < started_on:
            raise ValueError("ended_on must be on or after started_on")
        from packages.pms.paper_campaign import load_evidence
        from packages.pms.module_a_context import IST
        now=datetime.now(IST)
        with self.engine.connect() as conn:
            evidence=load_evidence(conn,now=min(now,datetime.combine(ended_on,time.max,tzinfo=IST)))
            qualified=[date.fromisoformat(d) for d in (evidence or {}).get('qualifying_sessions',[]) if started_on<=date.fromisoformat(d)<=ended_on]
            fills = int(
                conn.execute(
                    text("""
                select count(*) from broker_fills f join broker_orders o on o.id=f.order_id
                where o.mode='PAPER' and f.raw_response->>'implementation'='PAPER_EXECUTION_v1.0' and (f.fill_time at time zone 'Asia/Kolkata')::date between :s and :e
                and (f.fill_time at time zone 'Asia/Kolkata')::date=any(cast(:qualified as date[]))
            """),
                    {"s": started_on, "e": ended_on,"qualified":qualified},
                ).scalar_one()
            )
            snapshots = [
                dict(r)
                for r in conn.execute(
                    text("""
                select snapshot_date,nav from portfolio_snapshots
                where account_mode='PAPER' and snapshot_date between :s and :e and snapshot_date=any(cast(:qualified as date[])) order by snapshot_date
            """),
                    {"s": started_on, "e": ended_on,"qualified":qualified},
                )
                .mappings()
                .all()
            ]
            recon_mismatch = int(
                conn.execute(
                    text("""
                select count(*) from reconciliation_runs where account_mode='PAPER'
                and (started_at at time zone 'Asia/Kolkata')::date between :s and :e and status not in ('SUCCESS','MATCHED')
            """),
                    {"s": started_on, "e": ended_on,"qualified":qualified},
                ).scalar_one()
            )
            worker_failures = int(
                conn.execute(
                    text("""
                select count(*) from worker_runs where (started_at at time zone 'Asia/Kolkata')::date between :s and :e and status in ('FAILED','BLOCKED','INTERRUPTED')
            """),
                    {"s": started_on, "e": ended_on,"qualified":qualified},
                ).scalar_one()
            )
            dq_errors = int(
                conn.execute(
                    text(
                        "select count(*) from data_quality_issues where severity='ERROR' and resolved_at is null"
                    )
                ).scalar_one()
            )
        max_dd = 0.0
        peak = None
        for row in snapshots:
            nav = float(row["nav"])
            peak = nav if peak is None else max(peak, nav)
            if peak and peak > 0:
                max_dd = max(max_dd, (peak - nav) / peak * 100.0)
        calendar_days = (ended_on - started_on).days + 1
        metrics = {
            "calendar_days": calendar_days,
            "campaign_evidence":evidence,
            "qualified_campaign_sessions":len(qualified),
            "fills": fills,
            "snapshots": len(snapshots),
            "reconciliation_mismatches": recon_mismatch,
            "worker_failures": worker_failures,
            "data_quality_errors": dq_errors,
            "max_drawdown_pct": round(max_dd, 4),
            "ending_nav": float(snapshots[-1]["nav"]) if snapshots else None,
        }
        gates = {
            "campaign_bound":bool(evidence and started_on>=datetime.fromisoformat(evidence['activated_at']).astimezone(IST).date() and ended_on<=now.date()),
            "duration":bool(evidence and evidence['elapsed_calendar_days']>=criteria.min_calendar_days),
            "elapsed_sessions":len(qualified)>=criteria.min_snapshots,
            "fills": fills >= criteria.min_fills,
            "snapshots": len(snapshots) >= criteria.min_snapshots,
            "reconciliation": recon_mismatch <= criteria.max_reconciliation_mismatches,
            "workers": worker_failures <= criteria.max_worker_failures,
            "data_quality": dq_errors <= criteria.max_data_quality_errors,
            "drawdown": max_dd <= criteria.max_drawdown_pct,
        }
        passed = all(gates.values())
        run_id = str(uuid4())
        if persist:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                    insert into paper_validation_runs(id,started_on,ended_on,criteria,metrics,gates,passed,created_by)
                    values (:id,:s,:e,cast(:criteria as jsonb),cast(:metrics as jsonb),cast(:gates as jsonb),:passed,:actor)
                """),
                    {
                        "id": run_id,
                        "s": started_on,
                        "e": ended_on,
                        "criteria": json.dumps(asdict(criteria)),
                        "metrics": json.dumps(metrics),
                        "gates": json.dumps(gates),
                        "passed": passed,
                        "actor": actor,
                    },
                )
            self.audit.record(
                AuditEvent(
                    "PAPER_VALIDATION_COMPLETED",
                    actor,
                    {"passed": passed, "metrics": metrics, "gates": gates},
                    entity_type="paper_validation",
                    entity_id=run_id,
                )
            )
        return PaperValidationResult(run_id, passed, metrics, gates, criteria)
