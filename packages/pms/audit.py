from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class AuditEvent:
    event_type: str
    actor: str
    payload: dict[str, Any]
    entity_type: str | None = None
    entity_id: str | None = None
    strategy_version: str | None = None
    data_version: str | None = None
    correlation_id: UUID | None = None
    before_state: dict[str, Any] | None = None
    after_state: dict[str, Any] | None = None


class AuditTrail:
    def __init__(self, engine: Engine):
        self.engine = engine

    def record(self, event: AuditEvent) -> str:
        correlation_id = str(event.correlation_id or uuid4())
        with self.engine.begin() as conn:
            row_id = conn.execute(text("""
                insert into audit_log
                (event_type, actor, payload, correlation_id, entity_type, entity_id,
                 strategy_version, data_version, before_state, after_state, created_at)
                values (:event_type,:actor,cast(:payload as jsonb),:correlation_id,:entity_type,:entity_id,
                        :strategy_version,:data_version,cast(:before_state as jsonb),cast(:after_state as jsonb),:created_at)
                returning id
            """), {
                "event_type": event.event_type,
                "actor": event.actor,
                "payload": json.dumps(event.payload, default=str),
                "correlation_id": correlation_id,
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "strategy_version": event.strategy_version,
                "data_version": event.data_version,
                "before_state": json.dumps(event.before_state, default=str) if event.before_state is not None else None,
                "after_state": json.dumps(event.after_state, default=str) if event.after_state is not None else None,
                "created_at": datetime.now(timezone.utc),
            }).scalar_one()
        return str(row_id)

    def history(self, *, entity_type: str | None = None, entity_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        where = []
        params: dict[str, Any] = {"limit": limit}
        if entity_type:
            where.append("entity_type=:entity_type")
            params["entity_type"] = entity_type
        if entity_id:
            where.append("entity_id=:entity_id")
            params["entity_id"] = entity_id
        clause = "where " + " and ".join(where) if where else ""
        with self.engine.connect() as conn:
            rows = conn.execute(text(f"select * from audit_log {clause} order by created_at desc limit :limit"), params).mappings().all()
        return [dict(row) for row in rows]
