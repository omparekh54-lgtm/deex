from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, text

from packages.pms.audit import AuditEvent, AuditTrail
from packages.pms.brokers import BrokerOrderRequest
from packages.pms.safety import SafetyController
from packages.pms.module_a_context import IST


@dataclass(frozen=True)
class LiveValidationLimits:
    max_capital: float
    max_order_value: float
    max_open_positions: int
    max_daily_loss: float
    allowed_symbols: tuple[str, ...] = ()


class LiveValidationService:
    """Creates tightly bounded live-validation proposals and evidence records.

    This service never sends an order to a broker. Approval records are an
    operator decision artifact; separately executed real-world orders can be
    recorded as observations for validation evidence.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.audit = AuditTrail(engine)
        self.safety = SafetyController(engine)

    def create_session(
        self, limits: LiveValidationLimits, *, actor: str, notes: str | None = None
    ) -> str:
        if (
            any(
                not math.isfinite(float(v)) or v <= 0
                for v in (
                    limits.max_capital,
                    limits.max_order_value,
                    limits.max_daily_loss,
                )
            )
            or type(limits.max_open_positions) is not int
            or limits.max_open_positions <= 0
        ):
            raise ValueError("live-validation limits must be positive")
        if limits.max_order_value > limits.max_capital:
            raise ValueError("max_order_value cannot exceed max_capital")
        session_id = str(uuid4())
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                insert into live_validation_sessions
                (id,status,max_capital,max_order_value,max_open_positions,max_daily_loss,allowed_symbols,notes,created_by)
                values (:id,'DRAFT',:capital,:order_value,:positions,:daily_loss,cast(:symbols as jsonb),:notes,:actor)
            """),
                {
                    "id": session_id,
                    "capital": limits.max_capital,
                    "order_value": limits.max_order_value,
                    "positions": limits.max_open_positions,
                    "daily_loss": limits.max_daily_loss,
                    "symbols": json.dumps(list(limits.allowed_symbols)),
                    "notes": notes,
                    "actor": actor,
                },
            )
        self.audit.record(
            AuditEvent(
                "LIVE_VALIDATION_SESSION_CREATED",
                actor,
                {"limits": limits.__dict__},
                entity_type="live_validation_session",
                entity_id=session_id,
            )
        )
        return session_id

    def activate_session(self, session_id: str, *, actor: str) -> None:
        status = self.safety.status()
        if status.execution_mode != "LIVE_VALIDATION":
            raise RuntimeError("Safety mode must be LIVE_VALIDATION before activation")
        if status.emergency_stop or status.locked_reason:
            raise RuntimeError("Execution safety state is locked")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "select pg_advisory_xact_lock(hashtext('pms_live_validation_session'))"
                )
            )
            current = (
                conn.execute(
                    text(
                        "select execution_mode,emergency_stop,locked_reason from safety_state where singleton=true for update"
                    )
                )
                .mappings()
                .one()
            )
            if (
                current["execution_mode"] != "LIVE_VALIDATION"
                or current["emergency_stop"]
                or current["locked_reason"]
            ):
                raise RuntimeError("Execution safety state changed")
            active = conn.execute(
                text(
                    "select count(*) from live_validation_sessions where status='ACTIVE' and id<>:id"
                ),
                {"id": session_id},
            ).scalar_one()
            if active:
                raise RuntimeError("another live-validation session is already active")
            updated = conn.execute(
                text(
                    "update live_validation_sessions set status='ACTIVE',started_at=:now,activated_by=:actor where id=:id and status='DRAFT'"
                ),
                {"id": session_id, "now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if not updated:
                raise RuntimeError("session is not in DRAFT state")
        self.audit.record(
            AuditEvent(
                "LIVE_VALIDATION_SESSION_ACTIVATED",
                actor,
                {},
                entity_type="live_validation_session",
                entity_id=session_id,
            )
        )

    def _session(self, session_id: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("select * from live_validation_sessions where id=:id"),
                    {"id": session_id},
                )
                .mappings()
                .first()
            )
        if not row:
            raise KeyError(session_id)
        return dict(row)

    def propose_order(
        self, session_id, request, *, estimated_price, actor="strategy", reason=None
    ):
        if (
            not math.isfinite(float(estimated_price))
            or estimated_price <= 0
            or type(request.quantity) is not int
            or request.quantity <= 0
        ):
            raise ValueError("Finite positive order inputs required")
        side, kind = request.side.upper(), request.order_type.upper()
        if side not in {"BUY", "SELL"} or kind not in {
            "MARKET",
            "LIMIT",
            "STOP",
            "STOP_MARKET",
            "SL-M",
        }:
            raise ValueError("Invalid side/type")
        if any(
            v is not None and (not math.isfinite(float(v)) or v <= 0)
            for v in (request.limit_price, request.trigger_price)
        ):
            raise ValueError("Invalid order prices")
        if kind == "LIMIT" and request.limit_price is None:
            raise ValueError("LIMIT requires a price")
        if kind in {"STOP", "STOP_MARKET", "SL-M"} and request.trigger_price is None:
            raise ValueError("Stop requires a trigger")
        value = float(estimated_price) * request.quantity
        if not math.isfinite(value):
            raise ValueError("Invalid order notional")
        symbol = request.instrument_id.upper()
        exchange, raw_symbol = symbol.split(":", 1)
        approval_id = str(uuid4())
        with self.engine.begin() as conn:
            session = (
                conn.execute(
                    text(
                        "select * from live_validation_sessions where id=:id for update"
                    ),
                    {"id": session_id},
                )
                .mappings()
                .first()
            )
            if not session or session["status"] != "ACTIVE":
                raise RuntimeError("Live-validation session is not active")
            if value > float(session["max_order_value"]):
                raise RuntimeError("Order exceeds session limit")
            if session.get("allowed_symbols") and symbol not in {
                str(x).upper() for x in session["allowed_symbols"]
            }:
                raise RuntimeError("Instrument outside session allowlist")
            committed = float(
                conn.execute(
                    text(
                        "select coalesce(sum(estimated_order_value),0) from live_order_approvals where session_id=:id and status in ('PROPOSED','APPROVED') and side='BUY'"
                    ),
                    {"id": session_id},
                ).scalar_one()
            )
            if side == "BUY" and committed + value > float(session["max_capital"]):
                raise RuntimeError("Session committed notional exceeds capital")
            latest = (
                conn.execute(
                    text(
                        "select realized_pnl,unrealized_pnl from portfolio_snapshots where account_mode in ('LIVE_VALIDATION','LIVE_APPROVAL') and snapshot_date=:d order by created_at desc limit 1"
                    ),
                    {"d": datetime.now(IST).date()},
                )
                .mappings()
                .first()
            )
            if latest and float(latest["realized_pnl"] or 0) + float(
                latest["unrealized_pnl"] or 0
            ) <= -float(session["max_daily_loss"]):
                raise RuntimeError("Session daily loss limit reached")
            instrument = conn.execute(
                text(
                    "select id from instruments where exchange=:e and trading_symbol=:s order by valid_from desc limit 1"
                ),
                {
                    "e": exchange,
                    "s": raw_symbol + "-EQ" if exchange == "NSE" else raw_symbol,
                },
            ).scalar_one_or_none()
            if instrument is None:
                raise KeyError(symbol)
            opened = int(
                conn.execute(
                    text(
                        "select count(*) from trades where state not in ('CLOSED','CANCELLED')"
                    )
                ).scalar_one()
            )
            if side == "BUY" and opened >= int(session["max_open_positions"]):
                raise RuntimeError("Session position limit reached")
            conn.execute(
                text(
                    """insert into live_order_approvals(id,session_id,instrument_id,side,quantity,order_type,limit_price,trigger_price,estimated_order_value,proposal_reason,proposed_by)
                values(:id,:session,:instrument,:side,:qty,:type,:limit,:trigger,:value,:reason,:actor)"""
                ),
                dict(
                    id=approval_id,
                    session=session_id,
                    instrument=instrument,
                    side=side,
                    qty=request.quantity,
                    type=kind,
                    limit=request.limit_price,
                    trigger=request.trigger_price,
                    value=value,
                    reason=reason,
                    actor=actor,
                ),
            )
        self.audit.record(
            AuditEvent(
                "LIVE_ORDER_PROPOSED",
                actor,
                {
                    "instrument": symbol,
                    "quantity": request.quantity,
                    "estimated_order_value": value,
                },
                entity_type="live_order_approval",
                entity_id=approval_id,
            )
        )
        return approval_id

    def approve(self, approval_id: str, *, actor: str) -> None:
        status = self.safety.status()
        if not status.can_place_validation_order:
            raise PermissionError("live-validation approval is blocked by safety state")
        with self.engine.begin() as conn:
            current = (
                conn.execute(
                    text(
                        "select execution_mode,new_orders_enabled,emergency_stop,locked_reason from safety_state where singleton=true for update"
                    )
                )
                .mappings()
                .one()
            )
            if (
                current["execution_mode"] != "LIVE_VALIDATION"
                or not current["new_orders_enabled"]
                or current["emergency_stop"]
                or current["locked_reason"]
            ):
                raise PermissionError(
                    "Live-validation approval blocked by current safety state"
                )
            session = conn.execute(
                text(
                    "select s.id from live_validation_sessions s join live_order_approvals a on a.session_id=s.id where a.id=:id and s.status='ACTIVE' for update of s"
                ),
                {"id": approval_id},
            ).scalar_one_or_none()
            if session is None:
                raise RuntimeError("Session is no longer active")
            updated = conn.execute(
                text("""
                update live_order_approvals set status='APPROVED',approved_at=:now,approved_by=:actor
                where id=:id and status='PROPOSED'
                and exists(select 1 from safety_state where singleton=true and execution_mode='LIVE_VALIDATION' and new_orders_enabled=true and emergency_stop=false and locked_reason is null)
                and exists(select 1 from live_validation_sessions s where s.id=live_order_approvals.session_id and s.status='ACTIVE')
            """),
                {"id": approval_id, "now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if not updated:
                raise RuntimeError("approval is not in PROPOSED state")
        self.audit.record(
            AuditEvent(
                "LIVE_ORDER_APPROVED",
                actor,
                {},
                entity_type="live_order_approval",
                entity_id=approval_id,
            )
        )

    def reject(self, approval_id: str, *, actor: str, reason: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "update live_order_approvals set status='REJECTED',rejection_reason=:reason,approved_by=:actor,approved_at=:now where id=:id and status='PROPOSED'"
                ),
                {
                    "id": approval_id,
                    "reason": reason,
                    "actor": actor,
                    "now": datetime.now(timezone.utc),
                },
            )
        self.audit.record(
            AuditEvent(
                "LIVE_ORDER_REJECTED",
                actor,
                {"reason": reason},
                entity_type="live_order_approval",
                entity_id=approval_id,
            )
        )

    def record_observation(
        self,
        approval_id: str,
        *,
        expected_price: float,
        fill_price: float,
        quantity: int,
        executed_at: datetime,
        reconciliation_ok: bool,
        actor: str,
        fees: float = 0.0,
        external_order_reference: str | None = None,
        notes: str | None = None,
    ) -> str:
        if (
            any(not math.isfinite(float(v)) for v in (expected_price, fill_price, fees))
            or expected_price <= 0
            or fill_price <= 0
            or type(quantity) is not int
            or quantity <= 0
            or fees < 0
        ):
            raise ValueError("invalid live-validation observation")
        if executed_at.tzinfo is None or executed_at > datetime.now(timezone.utc):
            raise ValueError("Observed execution time must be aware and historical")
        slippage_bps = (float(fill_price) / float(expected_price) - 1.0) * 10000.0
        observation_id = str(uuid4())
        with self.engine.begin() as conn:
            approval = (
                conn.execute(
                    text(
                        "select status,quantity from live_order_approvals where id=:id for update"
                    ),
                    {"id": approval_id},
                )
                .mappings()
                .first()
            )
            if not approval or approval["status"] != "APPROVED":
                raise RuntimeError("Observation requires an approved proposal")
            used = conn.execute(
                text(
                    "select coalesce(sum(quantity),0) from live_validation_observations where approval_id=:id"
                ),
                {"id": approval_id},
            ).scalar_one()
            if used + quantity > int(approval["quantity"]):
                raise RuntimeError("Cumulative observations exceed approved quantity")
            conn.execute(
                text("""
                insert into live_validation_observations
                (id,approval_id,external_order_reference,executed_at,expected_price,fill_price,quantity,fees,slippage_bps,reconciliation_ok,notes,recorded_by)
                values (:id,:approval,:external,:executed,:expected,:fill,:qty,:fees,:slippage,:recon,:notes,:actor)
            """),
                {
                    "id": observation_id,
                    "approval": approval_id,
                    "external": external_order_reference,
                    "executed": executed_at,
                    "expected": expected_price,
                    "fill": fill_price,
                    "qty": quantity,
                    "fees": fees,
                    "slippage": slippage_bps,
                    "recon": reconciliation_ok,
                    "notes": notes,
                    "actor": actor,
                },
            )
        self.audit.record(
            AuditEvent(
                "LIVE_VALIDATION_OBSERVATION_RECORDED",
                actor,
                {
                    "approval_id": approval_id,
                    "slippage_bps": slippage_bps,
                    "reconciliation_ok": reconciliation_ok,
                },
                entity_type="live_validation_observation",
                entity_id=observation_id,
            )
        )
        return observation_id

    def close_session(
        self, session_id: str, *, actor: str, completed: bool, notes: str | None = None
    ) -> None:
        session = self._session(session_id)
        if session["status"] != "ACTIVE":
            raise RuntimeError("only ACTIVE sessions can be closed")
        with self.engine.connect() as conn:
            unresolved = int(
                conn.execute(
                    text(
                        "select count(*) from live_order_approvals where session_id=:id and status='PROPOSED'"
                    ),
                    {"id": session_id},
                ).scalar_one()
            )
            approved_without_evidence = int(
                conn.execute(
                    text("""
                select count(*) from live_order_approvals a
                left join live_validation_observations o on o.approval_id=a.id
                where a.session_id=:id and a.status='APPROVED' and o.id is null
            """),
                    {"id": session_id},
                ).scalar_one()
            )
            bad_reconciliation = int(
                conn.execute(
                    text("""
                select count(*) from live_validation_observations o
                join live_order_approvals a on a.id=o.approval_id
                where a.session_id=:id and o.reconciliation_ok=false
            """),
                    {"id": session_id},
                ).scalar_one()
            )
        if completed and unresolved:
            raise RuntimeError(
                "cannot complete a session with unresolved order proposals"
            )
        if completed and approved_without_evidence:
            raise RuntimeError(
                "cannot complete a session until every approved proposal has execution evidence"
            )
        if completed and bad_reconciliation:
            raise RuntimeError(
                "cannot complete a session with unreconciled validation observations"
            )
        status = "COMPLETED" if completed else "ABORTED"
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "update live_validation_sessions set status=:status,ended_at=:now,closed_by=:actor,notes=coalesce(:notes,notes) where id=:id"
                ),
                {
                    "status": status,
                    "now": datetime.now(timezone.utc),
                    "actor": actor,
                    "notes": notes,
                    "id": session_id,
                },
            )
        self.audit.record(
            AuditEvent(
                "LIVE_VALIDATION_SESSION_CLOSED",
                actor,
                {
                    "status": status,
                    "unresolved_proposals": unresolved,
                    "approved_without_evidence": approved_without_evidence,
                    "unreconciled_observations": bad_reconciliation,
                },
                entity_type="live_validation_session",
                entity_id=session_id,
            )
        )
