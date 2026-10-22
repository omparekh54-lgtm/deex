from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import Engine, text

from .reference_data import CorporateAction, ShareholdingRecord


def parse_nse_shareholding_row(instrument_id: str, row: dict[str, Any]) -> ShareholdingRecord:
    def pick(*keys: str):
        lowered = {str(k).lower().replace(" ", "").replace("_", ""): v for k, v in row.items()}
        for key in keys:
            k = key.lower().replace(" ", "").replace("_", "")
            if k in lowered and lowered[k] not in (None, "", "-"):
                return lowered[k]
        return None

    def num(value):
        if value in (None, "", "-"):
            return None
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            return None

    def d(value):
        if isinstance(value, date):
            return value
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(str(value), fmt).date()
            except ValueError:
                pass
        raise ValueError(f"unparseable date: {value}")

    def dt(value):
        if isinstance(value, datetime):
            return value
        text_value = str(value)
        for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text_value, fmt)
            except ValueError:
                pass
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            raise ValueError(f"unparseable datetime: {value}")

    period_end = d(pick("asOnDate", "as on date", "periodEnd", "date"))
    filed_at = dt(pick("broadcastDateTime", "broadcast date/time", "submissionDateTime", "filedAt"))
    promoter = num(pick("promoterHoldingPct", "promoter & promoter group (a)", "promoterHolding"))
    pledge = num(pick("pledgedPct", "% of promoter shares (x/a)", "promoterPledgePct", "encumberedPct"))
    total_shares = num(pick("totalIssuedShares", "total no. of issued shares a+b+c", "totalShares"))
    revision = int(num(pick("revisionNumber", "revision")) or 1)
    return ShareholdingRecord(
        instrument_id=instrument_id,
        period_end=period_end,
        filed_at=filed_at,
        promoter_holding_pct=promoter,
        promoter_pledge_pct=pledge,
        total_shares=total_shares,
        source="NSE",
        revision_number=revision,
        raw_payload=row,
    )


class ReferenceDataStorage:
    def __init__(self, engine: Engine):
        self.engine = engine

    @staticmethod
    def _parts(instrument_id: str) -> tuple[str, str]:
        exchange, symbol = instrument_id.split(":", 1)
        return exchange, f"{symbol}-EQ" if exchange == "NSE" else symbol

    def _pk(self, conn, instrument_id: str) -> int:
        e, s = self._parts(instrument_id)
        value = conn.execute(text("select id from instruments where exchange=:e and trading_symbol=:s order by id desc limit 1"), {"e": e, "s": s}).scalar_one()
        return int(value)

    def upsert_shareholding(self, records: Iterable[ShareholdingRecord]) -> int:
        rows = list(records)
        with self.engine.begin() as conn:
            for r in rows:
                pk = self._pk(conn, r.instrument_id)
                conn.execute(text("""
                    insert into shareholding_history(
                      instrument_id, period_end, filed_at, promoter_holding_pct,
                      promoter_pledge_pct, total_shares, source, revision_number, raw_payload
                    ) values (:id,:period_end,:filed_at,:promoter,:pledge,:shares,:source,:revision,cast(:raw as jsonb))
                    on conflict (instrument_id, period_end, filed_at, source, revision_number)
                    do update set promoter_holding_pct=excluded.promoter_holding_pct,
                                  promoter_pledge_pct=excluded.promoter_pledge_pct,
                                  total_shares=excluded.total_shares,
                                  raw_payload=excluded.raw_payload
                """), {
                    "id": pk, "period_end": r.period_end, "filed_at": r.filed_at,
                    "promoter": r.promoter_holding_pct, "pledge": r.promoter_pledge_pct,
                    "shares": r.total_shares, "source": r.source, "revision": r.revision_number,
                    "raw": json.dumps(r.raw_payload or {}),
                })
                if r.total_shares:
                    conn.execute(text("""
                        insert into shares_outstanding_history(
                          instrument_id,effective_from,shares_outstanding,source,knowable_at,raw_payload
                        ) values (:id,:d,:shares,:source,:knowable,cast(:raw as jsonb))
                        on conflict (instrument_id,effective_from,source,knowable_at) do update
                        set shares_outstanding=excluded.shares_outstanding, raw_payload=excluded.raw_payload
                    """), {"id": pk, "d": r.period_end, "shares": r.total_shares,
                           "source": r.source, "knowable": r.filed_at, "raw": json.dumps(r.raw_payload or {})})
        return len(rows)

    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> int:
        rows = list(actions)
        with self.engine.begin() as conn:
            for a in rows:
                pk = self._pk(conn, a.instrument_id)
                conn.execute(text("""
                    insert into corporate_actions(instrument_id,action_type,ex_date,ratio_from,ratio_to,cash_amount,source,announced_at)
                    values (:id,:kind,:ex,:rf,:rt,:cash,:source,:announced)
                    on conflict (instrument_id,action_type,ex_date,source) do update
                    set ratio_from=excluded.ratio_from,ratio_to=excluded.ratio_to,cash_amount=excluded.cash_amount,announced_at=excluded.announced_at
                """), {"id": pk, "kind": a.action_type.upper(), "ex": a.ex_date, "rf": a.ratio_from,
                       "rt": a.ratio_to, "cash": a.cash_amount, "source": a.source, "announced": a.announced_at})
        return len(rows)

    def replace_index_membership(self, index_code: str, effective_from: date, instrument_ids: Iterable[str], source: str = "NSE") -> int:
        members = list(dict.fromkeys(instrument_ids))
        with self.engine.begin() as conn:
            conn.execute(text("update index_constituents_history set effective_to=:d where index_code=:idx and effective_to is null and effective_from<:d"), {"idx": index_code, "d": effective_from})
            for instrument_id in members:
                pk = self._pk(conn, instrument_id)
                conn.execute(text("""
                    insert into index_constituents_history(index_code,instrument_id,effective_from,source)
                    values (:idx,:id,:d,:source)
                    on conflict (index_code,instrument_id,effective_from,source) do nothing
                """), {"idx": index_code, "id": pk, "d": effective_from, "source": source})
        return len(members)
