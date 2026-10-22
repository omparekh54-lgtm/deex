from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Iterable

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class ShareholdingRecord:
    instrument_id: str
    period_end: date
    filed_at: datetime
    promoter_holding_pct: float | None
    promoter_pledge_pct: float | None
    total_shares: float | None
    source: str = "NSE"
    revision_number: int = 1
    raw_payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class CorporateAction:
    instrument_id: str
    action_type: str
    ex_date: date
    ratio_from: float | None = None
    ratio_to: float | None = None
    cash_amount: float | None = None
    announced_at: datetime | None = None
    source: str = "NSE"


def market_cap_cr(close_price: float, shares_outstanding: float) -> float:
    if close_price <= 0 or shares_outstanding <= 0:
        raise ValueError("close_price and shares_outstanding must be positive")
    return close_price * shares_outstanding / 10_000_000.0


def split_adjustment_factor(action: CorporateAction) -> float:
    """Price factor applied to pre-action prices for split/bonus actions."""
    kind = action.action_type.upper()
    if kind not in {"SPLIT", "BONUS"}:
        return 1.0
    if not action.ratio_from or not action.ratio_to or action.ratio_from <= 0 or action.ratio_to <= 0:
        raise ValueError("split/bonus action requires positive ratio_from and ratio_to")
    if kind == "SPLIT":
        return action.ratio_from / action.ratio_to
    return action.ratio_from / (action.ratio_from + action.ratio_to)


class ReferenceDataQueries:
    def __init__(self, engine: Engine):
        self.engine = engine

    @staticmethod
    def _parts(instrument_id: str) -> tuple[str, str]:
        exchange, symbol = instrument_id.split(":", 1)
        return exchange, f"{symbol}-EQ" if exchange == "NSE" else symbol

    def _instrument_pk(self, conn, instrument_id: str) -> int:
        exchange, trading_symbol = self._parts(instrument_id)
        value = conn.execute(
            text("""
                select id from instruments
                where exchange=:e and trading_symbol=:s
                  and metadata->>'ingestion_duplicate_of' is null
                order by id desc limit 1
            """),
            {"e": exchange, "s": trading_symbol},
        ).scalar_one_or_none()
        if value is None:
            raise KeyError(instrument_id)
        return int(value)

    def shareholding_as_of(self, instrument_id: str, as_of: datetime) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            row = conn.execute(
                text("""
                    select period_end, filed_at, promoter_holding_pct, promoter_pledge_pct,
                           total_shares, source, revision_number
                    from shareholding_history
                    where instrument_id=:id and filed_at<=:as_of
                    order by period_end desc, filed_at desc, revision_number desc
                    limit 1
                """),
                {"id": pk, "as_of": as_of},
            ).mappings().first()
            return dict(row) if row else None

    def shares_outstanding_as_of(self, instrument_id: str, as_of: datetime) -> float | None:
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            value = conn.execute(
                text("""
                    select shares_outstanding
                    from shares_outstanding_history
                    where instrument_id=:id
                      and effective_from<=cast(:as_of as date)
                      and (effective_to is null or effective_to>cast(:as_of as date))
                      and knowable_at<=:as_of
                    order by effective_from desc, knowable_at desc
                    limit 1
                """),
                {"id": pk, "as_of": as_of},
            ).scalar_one_or_none()
            return float(value) if value is not None else None

    def market_cap_as_of(self, instrument_id: str, as_of: datetime, preferred_source: str = "angel_one") -> float | None:
        shares = self.shares_outstanding_as_of(instrument_id, as_of)
        if shares is None:
            sh = self.shareholding_as_of(instrument_id, as_of)
            shares = float(sh["total_shares"]) if sh and sh.get("total_shares") else None
        if shares is None:
            return None
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            close = conn.execute(
                text("""
                    select close from daily_prices
                    where instrument_id=:id and trade_date<=cast(:as_of as date)
                      and source=:source
                    order by trade_date desc limit 1
                """),
                {"id": pk, "as_of": as_of, "source": preferred_source},
            ).scalar_one_or_none()
            if close is None:
                close = conn.execute(
                    text("select close from daily_prices where instrument_id=:id and trade_date<=cast(:as_of as date) order by trade_date desc limit 1"),
                    {"id": pk, "as_of": as_of},
                ).scalar_one_or_none()
            return market_cap_cr(float(close), shares) if close is not None else None

    def avg_traded_value_cr(self, instrument_id: str, as_of: datetime, sessions: int = 63) -> float | None:
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            rows = conn.execute(
                text("""
                    select close, volume from daily_prices
                    where instrument_id=:id and trade_date<=cast(:as_of as date)
                    order by trade_date desc limit :n
                """),
                {"id": pk, "as_of": as_of, "n": sessions},
            ).mappings().all()
            if not rows:
                return None
            return sum(float(r["close"]) * float(r["volume"]) for r in rows) / len(rows) / 10_000_000.0

    def is_index_member(self, instrument_id: str, index_code: str, on_date: date) -> bool:
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            value = conn.execute(
                text("""
                    select 1 from index_constituents_history
                    where instrument_id=:id and index_code=:idx
                      and effective_from<=:d and (effective_to is null or effective_to>:d)
                    limit 1
                """),
                {"id": pk, "idx": index_code, "d": on_date},
            ).scalar_one_or_none()
            return value is not None

    def universe_as_of(self, on_date: date, exchange: str = "NSE") -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    select exchange, coalesce(nse_symbol, replace(trading_symbol, '-EQ', '')) as symbol
                    from instruments
                    where exchange=:e and active=true
                      and metadata->>'ingestion_duplicate_of' is null
                      and (listed_from is null or listed_from<=:d)
                      and (delisted_on is null or delisted_on>:d)
                    order by symbol
                """),
                {"e": exchange, "d": on_date},
            ).mappings().all()
            return [f"{r['exchange']}:{r['symbol']}" for r in rows]

    def earnings_within_trading_window(
        self,
        instrument_id: str,
        on_date: date,
        calendar_dates: Iterable[date],
        *,
        as_of: datetime | None = None,
    ) -> bool:
        """Return True when the entry must be earnings-blackout blocked.

        A known announced event inside the window blocks as before. In addition,
        absent or malformed historical earnings evidence now fails closed: no
        archive is never interpreted as proof that no earnings event exists.
        """
        target = set(calendar_dates)
        if not target:
            return True
        known_at = as_of or datetime.combine(on_date, time(23, 59, 59))
        window_end = max(target)
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            archive_rows = conn.execute(
                text("""
                    select event_date,announced_at
                    from earnings_calendar
                    where instrument_id=:id and event_date<=:window_end
                    order by event_date desc
                """),
                {"id": pk, "window_end": window_end},
            ).mappings().all()
            if not archive_rows:
                return True
            # A malformed/retrospectively knowable event inside the entry window
            # is uncertainty, therefore a blackout rather than a false negative.
            for row in archive_rows:
                event_date = row["event_date"]
                announced_at = row["announced_at"]
                if on_date <= event_date <= window_end and (
                    announced_at is None
                    or announced_at > datetime.combine(event_date, time(23, 59, 59), tzinfo=getattr(known_at, "tzinfo", None))
                ):
                    return True
            return any(
                on_date <= row["event_date"] <= window_end
                and row["announced_at"] is not None
                and row["announced_at"] <= known_at
                for row in archive_rows
            )

    def corporate_actions_between(self, instrument_id: str, start: date, end: date) -> list[CorporateAction]:
        with self.engine.connect() as conn:
            pk = self._instrument_pk(conn, instrument_id)
            rows = conn.execute(
                text("""
                    select action_type, ex_date, ratio_from, ratio_to, cash_amount, announced_at, source
                    from corporate_actions
                    where instrument_id=:id and ex_date between :start and :end
                    order by ex_date
                """),
                {"id": pk, "start": start, "end": end},
            ).mappings().all()
            return [
                CorporateAction(
                    instrument_id=instrument_id,
                    action_type=str(r["action_type"]),
                    ex_date=r["ex_date"],
                    ratio_from=float(r["ratio_from"]) if r["ratio_from"] is not None else None,
                    ratio_to=float(r["ratio_to"]) if r["ratio_to"] is not None else None,
                    cash_amount=float(r["cash_amount"]) if r["cash_amount"] is not None else None,
                    announced_at=r["announced_at"],
                    source=str(r["source"]),
                )
                for r in rows
            ]
