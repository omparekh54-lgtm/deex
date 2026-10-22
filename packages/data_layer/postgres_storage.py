from __future__ import annotations

import json
from typing import Iterable

from sqlalchemy import Engine, create_engine, text

from .models import Candle, FinancialFiling, Instrument, SurveillanceState


class PostgresStorage:
    def __init__(self, database_url: str | None = None, engine: Engine | None = None):
        if engine is None and not database_url:
            raise ValueError("database_url or engine is required")
        self.engine = engine or create_engine(database_url, pool_pre_ping=True)

    def _instrument_id(self, conn, instrument: Instrument) -> int:
        row = conn.execute(
            text(
                """
                insert into instruments (
                    isin, nse_symbol, bse_code, angel_token, sector, active,
                    exchange, trading_symbol, name, metadata, updated_at
                ) values (
                    :isin, :nse_symbol, :bse_code, :angel_token, :sector, :active,
                    :exchange, :trading_symbol, :name, cast(:metadata as jsonb), now()
                )
                on conflict (exchange, trading_symbol)
                where exchange is not null and trading_symbol is not null
                do update set
                    isin = coalesce(excluded.isin, instruments.isin),
                    nse_symbol = coalesce(excluded.nse_symbol, instruments.nse_symbol),
                    bse_code = coalesce(excluded.bse_code, instruments.bse_code),
                    angel_token = coalesce(excluded.angel_token, instruments.angel_token),
                    sector = coalesce(excluded.sector, instruments.sector),
                    active = excluded.active,
                    name = coalesce(excluded.name, instruments.name),
                    metadata = instruments.metadata || excluded.metadata,
                    updated_at = now()
                returning id
                """
            ),
            {
                "isin": instrument.isin,
                "nse_symbol": instrument.symbol if instrument.exchange == "NSE" else None,
                "bse_code": str(instrument.metadata.get("bse_code")) if instrument.metadata.get("bse_code") else None,
                "angel_token": instrument.token,
                "sector": instrument.sector,
                "active": instrument.active,
                "exchange": instrument.exchange,
                "trading_symbol": instrument.trading_symbol,
                "name": instrument.name,
                "metadata": json.dumps(instrument.metadata),
            },
        ).first()
        if row is None:
            raise RuntimeError(f"failed to resolve instrument {instrument.instrument_id}")
        return int(row[0])

    def upsert_instruments(self, instruments: Iterable[Instrument]) -> int:
        rows = list(instruments)
        with self.engine.begin() as conn:
            for instrument in rows:
                self._instrument_id(conn, instrument)
        return len(rows)

    def _lookup_id(self, conn, instrument_id: str) -> int:
        if ":" not in instrument_id:
            raise ValueError(f"unsupported instrument id: {instrument_id}")
        exchange, symbol = instrument_id.split(":", 1)
        trading_symbol = f"{symbol}-EQ" if exchange == "NSE" else symbol
        row = conn.execute(
            text("select id from instruments where exchange=:exchange and trading_symbol=:trading_symbol"),
            {"exchange": exchange, "trading_symbol": trading_symbol},
        ).first()
        if row is None and exchange == "BSE":
            row = conn.execute(
                text("select id from instruments where bse_code=:bse_code or trading_symbol=:symbol limit 1"),
                {"bse_code": symbol, "symbol": symbol},
            ).first()
        if row is None:
            raise KeyError(f"instrument not found: {instrument_id}")
        return int(row[0])

    def upsert_candles(self, candles: Iterable[Candle]) -> int:
        rows = list(candles)
        with self.engine.begin() as conn:
            cache: dict[str, int] = {}
            for candle in rows:
                db_id = cache.get(candle.instrument_id)
                if db_id is None:
                    db_id = self._lookup_id(conn, candle.instrument_id)
                    cache[candle.instrument_id] = db_id
                conn.execute(
                    text(
                        """
                        insert into daily_prices (
                            instrument_id, trade_date, open, high, low, close, volume, source
                        ) values (:instrument_id, :trade_date, :open, :high, :low, :close, :volume, :source)
                        on conflict (instrument_id, trade_date, source)
                        do update set
                            open=excluded.open, high=excluded.high, low=excluded.low,
                            close=excluded.close, volume=excluded.volume, ingested_at=now()
                        """
                    ),
                    {
                        "instrument_id": db_id,
                        "trade_date": candle.timestamp.date(),
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                        "source": candle.source,
                    },
                )
        return len(rows)

    def append_filings(self, filings: Iterable[FinancialFiling], *, refresh_module_a: bool = True) -> int:
        rows = list(filings)
        with self.engine.begin() as conn:
            cache: dict[str, int] = {}
            for filing in rows:
                db_id = cache.get(filing.instrument_id)
                if db_id is None:
                    db_id = self._lookup_id(conn, filing.instrument_id)
                    cache[filing.instrument_id] = db_id
                filing_id = conn.execute(
                    text(
                        """
                        insert into financial_filings (
                            instrument_id, period_end, filing_timestamp, source,
                            consolidated, revision_number, submission_type, audit_status,
                            revised_at, revision_remarks, exchange_received_at,
                            exchange_disseminated_at, filing_kind, external_id,
                            details_uri, xbrl_uri, raw_payload
                        ) values (
                            :instrument_id, :period_end, :filing_timestamp, :source,
                            :consolidated, :revision_number, :submission_type, :audit_status,
                            :revised_at, :revision_remarks, :exchange_received_at,
                            :exchange_disseminated_at, :filing_kind, :external_id,
                            :details_uri, :xbrl_uri, cast(:raw_payload as jsonb)
                        )
                        on conflict (
                            instrument_id, period_end, filing_timestamp, source,
                            consolidated, revision_number, filing_kind
                        ) do update set
                            submission_type=excluded.submission_type,
                            audit_status=excluded.audit_status,
                            revised_at=excluded.revised_at,
                            revision_remarks=excluded.revision_remarks,
                            exchange_received_at=excluded.exchange_received_at,
                            exchange_disseminated_at=excluded.exchange_disseminated_at,
                            filing_kind=excluded.filing_kind,
                            external_id=coalesce(excluded.external_id, financial_filings.external_id),
                            details_uri=coalesce(excluded.details_uri, financial_filings.details_uri),
                            xbrl_uri=coalesce(excluded.xbrl_uri, financial_filings.xbrl_uri),
                            raw_payload=financial_filings.raw_payload || excluded.raw_payload,
                            ingested_at=now()
                        returning id
                        """
                    ),
                    {
                        "instrument_id": db_id,
                        "period_end": filing.period_end,
                        "filing_timestamp": filing.filed_at,
                        "source": filing.source,
                        "consolidated": filing.statement_scope.lower() == "consolidated",
                        "revision_number": filing.version,
                        "submission_type": filing.submission_type,
                        "audit_status": filing.audit_status,
                        "revised_at": filing.revised_at,
                        "revision_remarks": filing.revision_remarks,
                        "exchange_received_at": filing.exchange_received_at,
                        "exchange_disseminated_at": filing.exchange_disseminated_at,
                        "filing_kind": filing.filing_kind,
                        "external_id": filing.external_id,
                        "details_uri": filing.details_uri,
                        "xbrl_uri": filing.xbrl_uri,
                        "raw_payload": json.dumps(filing.raw_payload),
                    },
                ).scalar_one()
                facts = {
                    "sales": filing.sales,
                    "operating_profit": filing.operating_profit,
                    "pat": filing.pat,
                    "diluted_eps": filing.diluted_eps,
                    "pbt": filing.pbt,
                    "other_income": filing.other_income,
                    "exceptional_items": filing.exceptional_items,
                    "operating_cash_flow": filing.operating_cash_flow,
                    "debt": filing.debt,
                    "equity": filing.equity,
                    "interest_expense": filing.interest_expense,
                }
                if (filing.raw_payload.get("financial_extraction") or {}).get("version") == "FINANCIAL_XBRL_v2":
                    # A reparse may invalidate an earlier segment/duration value.
                    # Upserting only non-null facts would leave that bad value
                    # available to Module A indefinitely.
                    conn.execute(text("delete from financial_facts where filing_id=:filing_id and fact_name=any(cast(:names as text[]))"),
                        {"filing_id": filing_id, "names": list(facts)})
                for fact_name, value in facts.items():
                    if value is not None:
                        conn.execute(
                            text(
                                """
                                insert into financial_facts (filing_id, fact_name, value, unit)
                                values (:filing_id, :fact_name, :value, :unit)
                                on conflict (filing_id, fact_name)
                                do update set value=excluded.value, unit=excluded.unit
                                """
                            ),
                            {
                                "filing_id": filing_id,
                                "fact_name": fact_name,
                                "value": value,
                                "unit": "INR/share" if fact_name == "diluted_eps" else "INR crore",
                            },
                        )
        # Financial rows must be committed before the as-of reader can see them.
        # Replays are idempotent on (instrument, event timestamp, strategy version).
        if rows and refresh_module_a:
            from packages.pms.module_a_pipeline import ModuleAPipeline
            pipeline = ModuleAPipeline(self.engine)
            from packages.pms.module_a_context import as_ist
            events = sorted({(f.instrument_id, as_ist(f.filed_at)) for f in rows if f.instrument_id.startswith('NSE:')}, key=lambda event:event[1])
            for instrument_id, filed_at in events:
                pipeline.evaluate(instrument_id, filed_at, persist=True)
        return len(rows)

    def record_surveillance(self, states: Iterable[SurveillanceState]) -> int:
        rows = list(states)
        with self.engine.begin() as conn:
            for state in rows:
                db_id = self._lookup_id(conn, state.instrument_id)
                for surveillance_type, enabled in (("ASM", state.asm), ("GSM", state.gsm)):
                    if enabled:
                        conn.execute(
                            text(
                                """
                                insert into surveillance_history (
                                    instrument_id, surveillance_type, stage, effective_from, source
                                ) values (:instrument_id, :surveillance_type, :stage, :effective_from, :source)
                                """
                            ),
                            {
                                "instrument_id": db_id,
                                "surveillance_type": surveillance_type,
                                "stage": state.stage,
                                "effective_from": state.observed_at,
                                "source": state.source,
                            },
                        )
        return len(rows)
