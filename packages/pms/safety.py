from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class SafetyStatus:
    execution_mode: str
    new_orders_enabled: bool
    emergency_stop: bool
    locked_reason: str | None

    @property
    def can_place_live_order(self) -> bool:
        return (
            self.execution_mode == "LIVE_APPROVAL"
            and self.new_orders_enabled
            and not self.emergency_stop
            and not self.locked_reason
        )

    @property
    def can_place_validation_order(self) -> bool:
        return (
            self.execution_mode == "LIVE_VALIDATION"
            and self.new_orders_enabled
            and not self.emergency_stop
            and not self.locked_reason
        )


class SafetyController:
    def __init__(self, engine: Engine):
        self.engine = engine

    def status(self) -> SafetyStatus:
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "select execution_mode,new_orders_enabled,emergency_stop,locked_reason from safety_state where singleton=true"
                    )
                )
                .mappings()
                .one()
            )
        return SafetyStatus(
            str(row["execution_mode"]),
            bool(row["new_orders_enabled"]),
            bool(row["emergency_stop"]),
            row["locked_reason"],
        )

    def lock(self, reason: str, actor: str = "system") -> None:
        with self.engine.begin() as conn:
            changed = conn.execute(
                text("""
                update safety_state set new_orders_enabled=false, locked_reason=:reason,
                    updated_at=:now, updated_by=:actor where singleton=true
            """),
                {"reason": reason, "now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if changed != 1:
                raise RuntimeError("Safety state unavailable")
            conn.execute(
                text(
                    "insert into audit_log(event_type,actor,payload) values ('EXECUTION_LOCKED',:actor,jsonb_build_object('reason',:reason))"
                ),
                {"actor": actor, "reason": reason},
            )

    def emergency_stop(self, reason: str, actor: str = "operator") -> None:
        with self.engine.begin() as conn:
            changed = conn.execute(
                text("""
                update safety_state set new_orders_enabled=false, emergency_stop=true,
                    locked_reason=:reason, updated_at=:now, updated_by=:actor where singleton=true
            """),
                {"reason": reason, "now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if changed != 1:
                raise RuntimeError("Safety state unavailable")
            conn.execute(
                text(
                    "insert into audit_log(event_type,actor,payload) values ('EMERGENCY_STOP',:actor,jsonb_build_object('reason',:reason))"
                ),
                {"actor": actor, "reason": reason},
            )

    def set_mode(self, mode: str, actor: str = "operator") -> None:
        if mode not in {"PAPER", "LIVE_VALIDATION", "LIVE_APPROVAL"}:
            raise ValueError(
                "Only PAPER, LIVE_VALIDATION and LIVE_APPROVAL are supported; LIVE_AUTO remains disabled"
            )
        with self.engine.begin() as conn:
            changed = conn.execute(
                text(
                    """update safety_state set execution_mode=:mode,new_orders_enabled=false,
                updated_at=:now,updated_by=:actor where singleton=true"""
                ),
                {"mode": mode, "now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if changed != 1:
                raise RuntimeError("Safety state unavailable")
            conn.execute(
                text(
                    "insert into audit_log(event_type,actor,payload) values ('EXECUTION_MODE_SET',:actor,jsonb_build_object('mode',:mode))"
                ),
                {"actor": actor, "mode": mode},
            )

    def enable_new_orders(self, actor: str = "operator") -> None:
        # Check and update in one statement: a concurrent stop cannot be overwritten.
        with self.engine.begin() as conn:
            changed = conn.execute(
                text(
                    """update safety_state set new_orders_enabled=true,updated_at=:now,updated_by=:actor
                where singleton=true and not emergency_stop and locked_reason is null
                and execution_mode in ('PAPER','LIVE_VALIDATION','LIVE_APPROVAL')"""
                ),
                {"now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if changed != 1:
                raise RuntimeError(
                    "Execution is locked or unavailable; investigate the cause before enabling orders"
                )
            conn.execute(
                text(
                    "insert into audit_log(event_type,actor,payload) values ('NEW_ORDERS_ENABLED',:actor,jsonb_build_object('enabled',true))"
                ),
                {"actor": actor},
            )

    def disable_new_orders(self, actor: str = "operator") -> None:
        with self.engine.begin() as conn:
            changed = conn.execute(
                text(
                    "update safety_state set new_orders_enabled=false,updated_at=:now,updated_by=:actor where singleton=true"
                ),
                {"now": datetime.now(timezone.utc), "actor": actor},
            ).rowcount
            if changed != 1:
                raise RuntimeError("Safety state unavailable")
            conn.execute(
                text(
                    "insert into audit_log(event_type,actor,payload) values ('NEW_ORDERS_DISABLED',:actor,jsonb_build_object('enabled',false))"
                ),
                {"actor": actor},
            )
