from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import Engine, text

IST = ZoneInfo("Asia/Kolkata")


class ReferenceGapIngestor:
    """Populate mandatory point-in-time reference datasets used by PMS hard filters."""

    def __init__(self, engine: Engine):
        self.engine = engine

    @staticmethod
    def _finite(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _first(row: dict[str, Any], *names: str):
        lower = {str(k).lower(): v for k, v in row.items()}
        for name in names:
            if name.lower() in lower and lower[name.lower()] not in (None, ""):
                return lower[name.lower()]
        return None

    @staticmethod
    def _date(value):
        if value is None or value == "":
            return None
        ts = pd.to_datetime(value, errors="coerce", dayfirst=True)
        return None if pd.isna(ts) else ts.date()

    def _instrument_maps(self) -> tuple[dict[str, int], dict[str, int]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                select id, upper(coalesce(nse_symbol, replace(trading_symbol,'-EQ',''))) symbol,
                       upper(coalesce(isin,'')) isin
                from instruments where exchange='NSE' and active=true
            """)).mappings().all()
        by_symbol = {str(r["symbol"]): int(r["id"]) for r in rows if r["symbol"]}
        by_isin = {str(r["isin"]): int(r["id"]) for r in rows if r["isin"]}
        return by_symbol, by_isin

    def ingest_shareholding(self, *, symbol: str | None = None) -> dict[str, int]:
        from finstack import nse_shareholding_history

        df = nse_shareholding_history(symbol)
        if df.empty:
            return {"source_rows": 0, "matched": 0, "unmatched": 0, "written": 0}
        by_symbol, by_isin = self._instrument_maps()
        payloads: list[dict[str, Any]] = []
        unmatched = 0
        for _, row in df.iterrows():
            ticker = str(row.get("ticker") or "").upper().strip()
            isin = str(row.get("isin") or "").upper().strip()
            instrument_id = by_isin.get(isin) or by_symbol.get(ticker)
            filed_at = row.get("filed_at")
            period_end = row.get("period_end")
            if instrument_id is None or pd.isna(filed_at) or pd.isna(period_end):
                unmatched += 1
                continue
            filed_ts = pd.Timestamp(filed_at).to_pydatetime()
            if filed_ts.tzinfo is None:
                filed_ts = filed_ts.replace(tzinfo=IST)
            promoter_pct = self._finite(row.get("promoter_pct"))
            pledge_of_promoter = self._finite(row.get("pledge_pct_of_promoter"))
            pledge_total = self._finite(row.get("pledge_pct"))
            if pledge_of_promoter is None and pledge_total is not None and promoter_pct and promoter_pct > 0:
                pledge_of_promoter = pledge_total / promoter_pct * 100.0
            revised = str(row.get("revised") or "N").upper() == "Y"
            raw = {
                "ticker": ticker, "isin": isin or None,
                "record_id": None if pd.isna(row.get("record_id")) else str(row.get("record_id")),
                "source_filename": row.get("source_filename"),
                "pledge_pct_total_equity": pledge_total,
                "pledge_pct_of_promoter": pledge_of_promoter,
            }
            payloads.append({
                "instrument_id": instrument_id,
                "period_end": pd.Timestamp(period_end).date(), "filed_at": filed_ts,
                "promoter_holding_pct": promoter_pct, "promoter_pledge_pct": pledge_of_promoter,
                "total_shares": None if pd.isna(row.get("total_shares")) else float(row.get("total_shares")),
                "source": "NSE_SHP_XBRL", "revision_number": 1 if revised else 0,
                "raw_payload": json.dumps(raw, default=str),
            })
        if not payloads:
            return {"source_rows": len(df), "matched": 0, "unmatched": unmatched, "written": 0}
        sql = text("""
            insert into shareholding_history(
                instrument_id,period_end,filed_at,promoter_holding_pct,promoter_pledge_pct,
                total_shares,source,revision_number,raw_payload
            ) values (
                :instrument_id,:period_end,:filed_at,:promoter_holding_pct,:promoter_pledge_pct,
                :total_shares,:source,:revision_number,cast(:raw_payload as jsonb)
            )
            on conflict (instrument_id,period_end,filed_at,source,revision_number)
            do update set promoter_holding_pct=excluded.promoter_holding_pct,
                          promoter_pledge_pct=excluded.promoter_pledge_pct,
                          total_shares=excluded.total_shares,raw_payload=excluded.raw_payload
        """)
        with self.engine.begin() as conn:
            for start in range(0, len(payloads), 1000):
                conn.execute(sql, payloads[start:start + 1000])
        return {"source_rows": len(df), "matched": len(payloads), "unmatched": unmatched, "written": len(payloads)}

    def ingest_surveillance_day(self, as_of: date) -> dict[str, int]:
        from finstack import nse_surveillance

        df = nse_surveillance(as_of)
        if df.empty:
            return {"source_rows": 0, "active": 0, "opened": 0, "closed": 0, "stage_changes": 0}
        by_symbol, _ = self._instrument_maps()
        active: dict[tuple[int, str], dict[str, Any]] = {}
        for _, row in df.iterrows():
            instrument_id = by_symbol.get(str(row.get("symbol") or "").upper().strip())
            measure = str(row.get("surveillance_type") or "").upper().strip()
            if instrument_id is None or measure not in {"ASM", "GSM"}:
                continue
            key = (instrument_id, measure)
            stage = None if pd.isna(row.get("stage")) else str(row.get("stage"))
            prev = active.get(key)
            if prev is None or (prev.get("stage") is None and stage is not None):
                active[key] = {"stage": stage}
        effective = datetime(as_of.year, as_of.month, as_of.day, tzinfo=IST)
        opened = closed = stage_changes = 0
        with self.engine.begin() as conn:
            open_rows = conn.execute(text("""
                select id,instrument_id,surveillance_type,stage from surveillance_history
                where effective_to is null and source='NSE_SURVEILLANCE'
            """)).mappings().all()
            open_map = {(int(r["instrument_id"]), str(r["surveillance_type"]).upper()): dict(r) for r in open_rows}
            for key, old in list(open_map.items()):
                new = active.get(key)
                if new is None:
                    conn.execute(text("update surveillance_history set effective_to=:t where id=:id"), {"t": effective, "id": old["id"]})
                    closed += 1
                    open_map.pop(key, None)
                elif (old.get("stage") or None) != (new.get("stage") or None):
                    conn.execute(text("update surveillance_history set effective_to=:t where id=:id"), {"t": effective, "id": old["id"]})
                    closed += 1; stage_changes += 1; open_map.pop(key, None)
            for (instrument_id, measure), state in active.items():
                current = open_map.get((instrument_id, measure))
                if current and (current.get("stage") or None) == (state.get("stage") or None):
                    continue
                conn.execute(text("""
                    insert into surveillance_history(instrument_id,surveillance_type,stage,effective_from,effective_to,source)
                    values (:instrument_id,:measure,:stage,:effective,null,'NSE_SURVEILLANCE')
                    on conflict (instrument_id,surveillance_type,effective_from,source)
                    do update set stage=excluded.stage
                """), {"instrument_id": instrument_id, "measure": measure, "stage": state.get("stage"), "effective": effective})
                opened += 1
        return {"source_rows": len(df), "active": len(active), "opened": opened, "closed": closed, "stage_changes": stage_changes}

    def ingest_latest_surveillance(self, as_of: date | None = None, lookback_days: int = 10) -> dict[str, Any]:
        d = as_of or datetime.now(IST).date()
        for offset in range(lookback_days + 1):
            candidate = d - timedelta(days=offset)
            if candidate.weekday() >= 5:
                continue
            result = self.ingest_surveillance_day(candidate)
            if result["source_rows"]:
                return {"date": candidate.isoformat(), **result}
        return {"date": None, "source_rows": 0, "active": 0, "opened": 0, "closed": 0, "stage_changes": 0}

    def backfill_surveillance(self, start: date, end: date) -> dict[str, Any]:
        if end < start:
            raise ValueError("end must be on or after start")
        results = []
        d = start
        while d <= end:
            if d.weekday() < 5:
                r = self.ingest_surveillance_day(d)
                if r["source_rows"]:
                    results.append({"date": d.isoformat(), **r})
            d += timedelta(days=1)
        return {"start": start.isoformat(), "end": end.isoformat(), "trading_files": len(results),
                "opened": sum(r["opened"] for r in results), "closed": sum(r["closed"] for r in results),
                "stage_changes": sum(r["stage_changes"] for r in results), "last": results[-1] if results else None}

    @staticmethod
    def _index_code(name: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", str(name).upper())

    def ingest_index_membership_history(self) -> dict[str, int]:
        from finstack import nse_index_membership_history

        df = nse_index_membership_history()
        by_symbol, _ = self._instrument_maps()
        payloads = []
        unmatched = 0
        for _, row in df.iterrows():
            symbol = str(row.get("symbol") or "").upper().strip()
            instrument_id = by_symbol.get(symbol)
            start = row.get("valid_from")
            if instrument_id is None or pd.isna(start):
                unmatched += 1
                continue
            end = row.get("valid_to")
            payloads.append({"index_code": self._index_code(row.get("index_name")), "instrument_id": instrument_id,
                             "effective_from": start, "effective_to": None if pd.isna(end) else end,
                             "source": "NSE_INDEX_RELEASE_HISTORY"})
        sql = text("""
            insert into index_constituents_history_local(index_code,instrument_id,effective_from,effective_to,source)
            values (:index_code,:instrument_id,:effective_from,:effective_to,:source)
            on conflict (index_code,instrument_id,effective_from,source)
            do update set effective_to=excluded.effective_to
        """)
        with self.engine.begin() as conn:
            for start in range(0, len(payloads), 1000):
                conn.execute(sql, payloads[start:start + 1000])
        return {"source_rows": len(df), "matched": len(payloads), "unmatched": unmatched, "written": len(payloads)}

    @staticmethod
    def _action_type(purpose: str) -> str:
        p = purpose.upper()
        if "BONUS" in p: return "BONUS"
        if "SPLIT" in p or "SUB-DIV" in p or "SUB DIV" in p: return "SPLIT"
        if "DIVIDEND" in p: return "DIVIDEND"
        if "RIGHT" in p: return "RIGHTS"
        if "MERGER" in p or "AMALGAM" in p: return "MERGER"
        return "OTHER"

    @staticmethod
    def _ratio(purpose: str, kind: str) -> tuple[float | None, float | None]:
        text_value = purpose.upper()
        if kind == "BONUS":
            m = re.search(r"(?:BONUS[^0-9]*)?(\d+(?:\.\d+)?)\s*[:/]\s*(\d+(?:\.\d+)?)", text_value)
            if m: return float(m.group(2)), float(m.group(1))
        if kind == "SPLIT":
            nums = [float(x) for x in re.findall(r"(?:RS\.?|INR)?\s*(\d+(?:\.\d+)?)", text_value)]
            if len(nums) >= 2: return nums[0], nums[-1]
        return None, None

    def ingest_corporate_actions(self) -> dict[str, int]:
        from finstack import corporate_actions

        raw_rows = corporate_actions(None) or []
        if isinstance(raw_rows, pd.DataFrame):
            records = raw_rows.to_dict("records")
        elif isinstance(raw_rows, dict):
            records = raw_rows.get("data") or raw_rows.get("records") or [raw_rows]
        else:
            records = list(raw_rows)
        by_symbol, _ = self._instrument_maps()
        payloads = []
        unmatched = 0
        for raw in records:
            if not isinstance(raw, dict):
                continue
            symbol = str(self._first(raw, "symbol", "sm_symbol", "tradingSymbol", "ticker") or "").upper().strip()
            instrument_id = by_symbol.get(symbol)
            ex_date = self._date(self._first(raw, "exDate", "ex_date", "ex-date", "exdate"))
            purpose = str(self._first(raw, "purpose", "subject", "description", "action") or "")
            if instrument_id is None or ex_date is None or not purpose:
                unmatched += 1
                continue
            kind = self._action_type(purpose)
            ratio_from, ratio_to = self._ratio(purpose, kind)
            cash = None
            if kind == "DIVIDEND":
                m = re.search(r"(?:RS\.?|INR)\s*(\d+(?:\.\d+)?)", purpose.upper())
                cash = float(m.group(1)) if m else None
            announced = self._first(raw, "broadcastDate", "announced_at", "announcementDate", "announcement_date")
            announced_ts = pd.to_datetime(announced, errors="coerce", dayfirst=True) if announced else pd.NaT
            payloads.append({"instrument_id": instrument_id, "action_type": kind, "ex_date": ex_date,
                             "record_date": self._date(self._first(raw, "recordDate", "record_date")),
                             "ratio_from": ratio_from, "ratio_to": ratio_to, "cash_amount": cash,
                             "source": "NSE_CORPORATE_ACTIONS",
                             "announced_at": None if pd.isna(announced_ts) else announced_ts.to_pydatetime(),
                             "raw_payload": json.dumps(raw, default=str)})
        sql = text("""
            insert into corporate_actions(instrument_id,action_type,ex_date,record_date,ratio_from,ratio_to,cash_amount,source,announced_at,raw_payload)
            values (:instrument_id,:action_type,:ex_date,:record_date,:ratio_from,:ratio_to,:cash_amount,:source,:announced_at,cast(:raw_payload as jsonb))
            on conflict (instrument_id,action_type,ex_date,source)
            do update set record_date=excluded.record_date,ratio_from=excluded.ratio_from,ratio_to=excluded.ratio_to,
                          cash_amount=excluded.cash_amount,announced_at=coalesce(excluded.announced_at,corporate_actions.announced_at),
                          raw_payload=excluded.raw_payload
        """)
        with self.engine.begin() as conn:
            for start in range(0, len(payloads), 1000):
                conn.execute(sql, payloads[start:start + 1000])
        return {"source_rows": len(records), "matched": len(payloads), "unmatched": unmatched, "written": len(payloads)}

    def coverage(self) -> dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                select
                  (select count(*) from instruments where exchange='NSE' and active=true) active_instruments,
                  (select count(*) from shareholding_history) shareholding_rows,
                  (select count(distinct instrument_id) from shareholding_history) shareholding_instruments,
                  (select count(*) from shareholding_history where promoter_pledge_pct is not null) pledge_rows,
                  (select count(*) from surveillance_history) surveillance_rows,
                  (select count(*) from surveillance_history where effective_to is null) current_surveillance_rows,
                  (select count(*) from index_constituents_history_local) index_history_rows,
                  (select count(*) from corporate_actions) corporate_action_rows,
                  (select count(*) from earnings_calendar) earnings_rows,
                  (select count(*) from shares_outstanding_history) shares_rows
            """)).mappings().one()
        return dict(row)
