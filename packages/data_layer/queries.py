from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, text


class PointInTimeQueries:
    def __init__(self, engine: Engine, *, historical: bool = False):
        self.engine = engine
        self.historical = historical

    @staticmethod
    def _split_instrument_id(instrument_id: str) -> tuple[str, str]:
        if ":" not in instrument_id:
            raise ValueError(f"unsupported instrument id: {instrument_id}")
        exchange, symbol = instrument_id.split(":", 1)
        trading_symbol = f"{symbol}-EQ" if exchange == "NSE" else symbol
        return exchange, trading_symbol

    def _db_instrument_id(self, conn, instrument_id: str) -> int:
        exchange, trading_symbol = self._split_instrument_id(instrument_id)
        if self.historical:
            rows=conn.execute(text("""
                select id from instruments
                where exchange=:exchange
                  and metadata->>'ingestion_duplicate_of' is null
                  and (trading_symbol=:trading_symbol or nse_symbol=:symbol)
                order by id
            """),{"exchange":exchange,"trading_symbol":trading_symbol,"symbol":instrument_id.split(":",1)[1]}).scalars().all()
            if len(rows)!=1:
                raise ValueError("Historical instrument identity unavailable or ambiguous")
            return int(rows[0])
        value = conn.execute(
            text(
                """
                select id from instruments
                where exchange=:exchange and (trading_symbol=:trading_symbol or nse_symbol=:symbol
                  or metadata->'sector_classification'->>'quote_symbol'=:symbol) and active=true
                order by valid_from desc
                limit 1
                """
            ),
            {"exchange": exchange, "trading_symbol": trading_symbol, "symbol": instrument_id.split(":",1)[1]},
        ).scalar_one_or_none()
        if value is None:
            raise KeyError(f"instrument not found: {instrument_id}")
        return int(value)

    def _filing_to_dict(self, conn, instrument_id: str, filing: Any) -> dict[str, Any]:
        facts = conn.execute(
            text("select fact_name, value, unit from financial_facts where filing_id=:filing_id"),
            {"filing_id": filing["id"]},
        ).mappings().all()
        return {
            "instrument_id": instrument_id,
            "period_end": filing["period_end"],
            "filed_at": filing["filing_timestamp"],
            "source": filing["source"],
            "consolidated": filing["consolidated"],
            "revision_number": filing["revision_number"],
            "submission_type": filing["submission_type"],
            "audit_status": filing["audit_status"],
            "revised_at": filing["revised_at"],
            "revision_remarks": filing["revision_remarks"],
            "exchange_received_at": filing["exchange_received_at"],
            "exchange_disseminated_at": filing["exchange_disseminated_at"],
            "filing_kind": filing["filing_kind"],
            "external_id": filing["external_id"],
            "details_uri": filing["details_uri"],
            "xbrl_uri": filing["xbrl_uri"],
            "facts": {row["fact_name"]: row["value"] for row in facts},
            "units": {row["fact_name"]: row["unit"] for row in facts if row["unit"]},
        }

    def latest_financials_as_of(
        self,
        instrument_id: str,
        as_of: datetime,
        prefer_consolidated: bool = True,
    ) -> dict[str, Any] | None:
        """Return the newest report period that was knowable by `as_of`."""
        history = self.financial_history_as_of(
            instrument_id,
            as_of,
            prefer_consolidated=prefer_consolidated,
            limit_periods=1,
        )
        return history[0] if history else None

    def financial_history_as_of(
        self,
        instrument_id: str,
        as_of: datetime,
        *,
        prefer_consolidated: bool = True,
        limit_periods: int = 16,
    ) -> list[dict[str, Any]]:
        """Return one knowable filing per report period/kind as of a timestamp.

        For each report period the query first prefers consolidated (when requested),
        then the latest disseminated revision. NSE is only a tie-breaker versus BSE;
        both exchange observations remain stored for audit and reconciliation.
        Quarterly and annual records are ranked independently so the Module A history
        calculator can use quarterly growth plus annual cash-flow quality.
        """
        if limit_periods <= 0:
            return []
        with self.engine.connect() as conn:
            db_id = self._db_instrument_id(conn, instrument_id)
            rows = conn.execute(
                text(
                    """
                    with ranked as (
                      select ff.*,
                             row_number() over (
                               partition by ff.period_end, ff.filing_kind
                               order by
                                 case when ff.consolidated=:prefer_consolidated then 0 else 1 end,
                                 ff.filing_timestamp desc,
                                 ff.revision_number desc,
                                 case when ff.source='NSE' then 0 when ff.source='BSE' then 1 else 2 end
                             ) as rn
                      from financial_filings ff
                      where ff.instrument_id=:instrument_id
                        and ff.filing_timestamp <= :as_of
                        and ff.period_end <= cast(:as_of as date)
                    )
                    select id, period_end, filing_timestamp, source,
                           consolidated, revision_number, submission_type,
                           audit_status, revised_at, revision_remarks,
                           exchange_received_at, exchange_disseminated_at,
                           filing_kind, external_id, details_uri, xbrl_uri
                    from ranked
                    where rn=1
                    order by period_end desc,
                             case when filing_kind='annual' then 1 else 0 end,
                             filing_timestamp desc
                    limit :limit_periods
                    """
                ),
                {
                    "instrument_id": db_id,
                    "as_of": as_of,
                    "prefer_consolidated": prefer_consolidated,
                    "limit_periods": limit_periods,
                },
            ).mappings().all()
            return [self._filing_to_dict(conn, instrument_id, row) for row in rows]

    def financial_histories_as_of(self, as_of: datetime, *, limit_periods: int = 24) -> dict[str, list[dict]]:
        """Bulk equivalent of financial_history_as_of for universe scans."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                with ranked as (
                  select ff.*,row_number() over(partition by instrument_id,period_end,filing_kind
                    order by consolidated desc nulls last,filing_timestamp desc,revision_number desc,
                    case when source='NSE' then 0 when source='BSE' then 1 else 2 end,id desc) choice
                  from financial_filings ff where filing_timestamp<=:as_of and period_end<=cast(:as_of as date)
                ), limited as (
                  select *,row_number() over(partition by instrument_id order by period_end desc,
                    case when filing_kind='annual' then 1 else 0 end,filing_timestamp desc) period_rank
                  from ranked where choice=1
                )
                select i.exchange||':'||coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) instrument_id,ff.period_end,ff.filing_timestamp filed_at,
                       ff.source,ff.consolidated,ff.revision_number,ff.filing_kind,
                       coalesce((select jsonb_object_agg(f.fact_name,f.value) from financial_facts f
                                 where f.filing_id=ff.id and f.value is not null),'{}'::jsonb) facts
                from limited ff join instruments i on i.id=ff.instrument_id
                where ff.period_rank<=:limit_periods and i.exchange='NSE'
                  and i.metadata->>'ingestion_duplicate_of' is null
                  and (i.active or :historical)
                order by i.nse_symbol,ff.period_rank
            """),{'as_of':as_of,'limit_periods':limit_periods,'historical':self.historical}).mappings().all()
        result = {}
        for row in rows:
            result.setdefault(row['instrument_id'],[]).append(dict(row))
        return result

    def daily_prices(
        self,
        instrument_id: str,
        start: date,
        end: date,
        preferred_source: str | None = "angel_one",
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            db_id = self._db_instrument_id(conn, instrument_id)
            params = {"instrument_id": db_id, "start": start, "end": end}
            source_clause = ""
            if preferred_source:
                source_clause = "and source=:source"
                params["source"] = preferred_source
            elif self.historical:
                # Final historical research is certified against official NSE raw
                # history. Do not silently execute with Angel/yfinance merely because
                # those rows coexist for the same date.
                source_clause = "and lower(source) like 'nse%'"
            rows = conn.execute(
                text(
                    f"""
                    select trade_date, open, high, low, close, volume, turnover, source
                    from daily_prices
                    where instrument_id=:instrument_id
                      and trade_date between :start and :end
                      {source_clause}
                    order by trade_date
                    """
                ),
                params,
            ).mappings().all()
            return [dict(row) for row in rows]

    def surveillance_as_of(
        self,
        instrument_id: str,
        as_of: datetime,
    ) -> dict[str, Any]:
        with self.engine.connect() as conn:
            db_id = self._db_instrument_id(conn, instrument_id)
            rows = conn.execute(
                text(
                    """
                    select surveillance_type, stage, source, effective_from, effective_to
                    from surveillance_history
                    where instrument_id=:instrument_id
                      and effective_from <= :as_of
                      and (effective_to is null or effective_to > :as_of)
                    """
                ),
                {"instrument_id": db_id, "as_of": as_of},
            ).mappings().all()
            types = {str(row["surveillance_type"]).upper() for row in rows}
            return {
                "instrument_id": instrument_id,
                "as_of": as_of,
                "asm": "ASM" in types,
                "gsm": "GSM" in types,
                "rows": [dict(row) for row in rows],
            }
