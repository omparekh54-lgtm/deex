"""Read-only consistent input snapshots, coverage gates and research-only storage."""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

from sqlalchemy import text

from packages.pms.backtest_matrix import fingerprint, plain
from packages.pms.module_a_context import end_of_day

INPUTS = {
    "instruments": "true",
    "daily_prices": "trade_date between :warmup and :end",
    "module_e_benchmark_bars": "trade_date between :warmup and :end",
    "financial_filings": "filing_timestamp<=:stamp",
    "financial_facts": "filing_id in(select id from pms.financial_filings where filing_timestamp<=:stamp)",
    "shareholding_history": "filed_at<=:stamp",
    "shares_outstanding_history": "knowable_at<=:stamp",
    "sector_history": "effective_from<=:end",
    "surveillance_history": "effective_from<=:stamp",
    "index_constituents_history": "effective_from<=:end",
    "corporate_actions": "ex_date between :warmup and :end",
    "earnings_calendar": "announced_at<=:stamp",
    "provider_snapshots": "dataset='NSE_TRADING_CALENDAR' and observed_at<=:stamp",
    "data_quality_issues": "resolved_at is null and severity='ERROR'",
}


class ReadOnlySnapshot:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement, parameters=None):
        return self.connection.execute(statement, parameters or {})

    def begin(self):
        raise RuntimeError("Strategy persistence is forbidden in a matrix input snapshot")


@contextmanager
def consistent_inputs(engine):
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL search_path TO pms,public"))
            conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            yield ReadOnlySnapshot(conn)


def code_fingerprint():
    root = Path(__file__).resolve().parents[2]
    files = [
        *sorted((root / "packages").rglob("*.py")),
        root / "scripts/run_backtest_matrix.py",
        root / "scripts/run_backtest.py",
        root / "scripts/run_research_lab.py",
        root / "scripts/run_walk_forward.py",
        root / "scripts/run_data_integrity_audit.py",
    ]
    return fingerprint(
        {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    )


def input_manifest(snapshot, cells, symbols=None, *, allow_ambiguous=False):
    start, end = min(c.period.start for c in cells), max(c.period.end for c in cells)
    params = {
        "warmup": start - timedelta(days=1100),
        "end": end,
        "stamp": end_of_day(end),
        # The research identity must never read beyond the declared period.
        # In particular, Step-10 ending 2024-12-31 must not fingerprint 2025
        # calendar rows that belong to the reserved OOS period.
        "calendar_end": end,
    }
    tables = {}
    for table, where in INPUTS.items():
        rows = (
            snapshot.execute(
                text(f"""with hashes as (
          select md5(row_to_json(t)::text) h from pms.{table} t where {where}
        ) select left(h,2) bucket,count(*) rows,md5(string_agg(h,'' order by h)) digest
          from hashes group by left(h,2) order by bucket"""),
                params,
            )
            .mappings()
            .all()
        )
        tables[table] = {
            "rows": sum(int(r["rows"]) for r in rows),
            "sha256": fingerprint([dict(r) for r in rows]),
        }
    forward_calendar = (
        snapshot.execute(
            text("select distinct trade_date from pms.module_e_benchmark_bars where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date between :warmup and :calendar_end order by trade_date"),
            params,
        )
        .scalars()
        .all()
    )
    tables["forward_calendar"] = {
        "rows": len(forward_calendar),
        "sha256": fingerprint(forward_calendar),
    }
    universe = [
        dict(r)
        for r in snapshot.execute(
            text("""select id,
        exchange||':'||coalesce(nse_symbol,replace(trading_symbol,'-EQ','')) instrument_id,listed_from,delisted_on
        from pms.instruments where exchange='NSE'
        and metadata->>'ingestion_duplicate_of' is null
        and (listed_from is null or listed_from<=:end)
        and (delisted_on is null or delisted_on>:start) order by id"""),
            {"start": start, "end": end},
        )
        .mappings()
        .all()
    ]
    if symbols:
        requested = set(symbols)
        universe = [r for r in universe if r["instrument_id"] in requested]
        if {r["instrument_id"] for r in universe} != requested:
            raise ValueError("Unknown or out-of-period requested symbols")
    if not allow_ambiguous and len({r["instrument_id"] for r in universe}) != len(universe):
        raise ValueError("Canonical instrument identity is ambiguous")
    payload = dict(
        tables=tables,
        universe=plain(universe),
        scope="EXPLICIT_SYMBOL_SAMPLE" if symbols else "HISTORICAL_MASTER",
        start=start,
        end=end,
        warmup=params["warmup"],
        independent_survivorship_audit="NOT_CERTIFIED",
    )
    return {**plain(payload), "fingerprint": fingerprint(payload)}


# Final matrix acceptance is deliberately stricter than normal production data
# selection: every expected historical session must have an official NSE raw row.
# Other providers may coexist in daily_prices, but cannot substitute for NSE here.
COVERAGE = """with calendar as (
  select distinct trade_date from pms.module_e_benchmark_bars
  where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and
  trade_date between coalesce((select min(trade_date) from (
    select distinct trade_date from pms.module_e_benchmark_bars where benchmark_code='NIFTY50'
    and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date<:start order by trade_date desc limit 257) h),:start) and :end
), chosen as (
  select distinct on(p.instrument_id,p.trade_date) p.* from pms.daily_prices p
  join calendar c using(trade_date) where p.instrument_id in(select value::bigint from jsonb_array_elements_text(cast(:ids as jsonb)))
  order by p.instrument_id,p.trade_date,case when lower(source) like 'nse%' then 0 when lower(source)='angel_one' then 1 else 2 end,lower(source)
) select i.id,count(c.trade_date) expected,count(p.trade_date) observed,
 count(p.trade_date) filter(where open<=0 or low<=0 or close<=0 or high<greatest(open,close)
 or low>least(open,close) or volume<0 or open::text in ('NaN','Infinity','-Infinity')
 or high::text in ('NaN','Infinity','-Infinity') or low::text in ('NaN','Infinity','-Infinity')
 or close::text in ('NaN','Infinity','-Infinity') or volume::text in ('NaN','Infinity','-Infinity')) invalid,
 count(p.trade_date) filter(where lower(source) not like 'nse%' or lower(source) in(select jsonb_array_elements_text(cast(:adjusted as jsonb)))) unverified_raw_source
 from pms.instruments i join calendar c on c.trade_date>=i.listed_from and (i.delisted_on is null or c.trade_date<i.delisted_on)
 left join chosen p on p.instrument_id=i.id and p.trade_date=c.trade_date
 where i.id in(select value::bigint from jsonb_array_elements_text(cast(:ids as jsonb))) group by i.id order by i.id"""


class MatrixGates:
    def __init__(self, snapshot, manifest):
        self.snapshot, self.manifest, self.coverage = snapshot, manifest, {}
        self.samples = {}

    def __call__(self, cell, cfg):
        reasons = []
        universe = self.manifest["universe"]
        if not universe:
            reasons.append("historical_universe_empty")
        if any(not r["listed_from"] for r in universe):
            reasons.append("historical_listing_dates_unavailable")
        if self.manifest["tables"]["data_quality_issues"]["rows"]:
            reasons.append("unresolved_critical_data_quality_issues")
        if cell.case.variant.use_module_a:
            default = cfg["fundamental"].get("leverage_limits", {}).get("default", {})
            if any(default.get(k) is None for k in ("max_debt_to_equity", "min_interest_coverage")):
                reasons.append("module_a_default_leverage_policy_unconfigured")
            if cell.period.start.isoformat() < "2024-01-23" and not cfg["fundamental"].get("legacy_surveillance_verified"):
                reasons.append("historical_surveillance_not_verified")
            for table in ("financial_filings", "financial_facts", "shareholding_history", "sector_history"):
                if not self.manifest["tables"][table]["rows"]:
                    reasons.append(table + "_unavailable")
        if cell.period.name not in self.coverage:
            rows = [
                dict(r)
                for r in self.snapshot.execute(
                    text(COVERAGE),
                    {
                        "start": cell.period.start,
                        "end": cell.period.end,
                        "ids": json.dumps([r["id"] for r in universe]),
                        "adjusted": json.dumps([str(s).lower() for s in cfg["technical"].get("adjusted_price_sources", [])]),
                    },
                )
                .mappings()
                .all()
            ]
            bad = [r for r in rows if r["expected"] != r["observed"] or r["invalid"] or r["unverified_raw_source"]]
            self.samples[cell.period.name] = bad[:25]
            self.coverage[cell.period.name] = []
            if bad:
                self.coverage[cell.period.name].append("historical_price_sessions_missing_invalid_or_adjustment_unverified")
            params = {"s": cell.period.start, "e": cell.period.end}
            sessions = (
                self.snapshot.execute(
                    text("select distinct trade_date from pms.module_e_benchmark_bars where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date between :s and :e order by trade_date"),
                    params,
                )
                .scalars()
                .all()
            )
            if not sessions or (cell.period.end - sessions[-1]).days > 7 or (sessions[0] - cell.period.start).days > 7:
                self.coverage[cell.period.name].append("official_calendar_window_missing_or_truncated")
            prior = self.snapshot.execute(
                text("select count(distinct trade_date) from pms.module_e_benchmark_bars where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date<:s"),
                params,
            ).scalar_one()
            if prior < 257:
                self.coverage[cell.period.name].append("official_warmup_calendar_fewer_than_257_sessions")
        return sorted(set(reasons + self.coverage[cell.period.name]))


def persist_report(engine, report):
    from copy import deepcopy

    compact = deepcopy(report["cells"])
    for cell in compact:
        diagnostics = cell.get("diagnostics", {})
        diagnostics.pop("regime", None)
        diagnostics.pop("allocation", None)
    params = {
        "id": report["run_id"],
        "s": report["dataset"]["start"],
        "e": report["dataset"]["end"],
        "cfg": json.dumps(plain({"identity": report["identity"], "config": report["config"], "dataset": report["dataset"], "code_version": report["code_version"]}), allow_nan=False),
        "metrics": json.dumps(plain({"summary": report["summary"], "cells": compact, "scope": report["scope"], "holdout": report["holdout"]}), allow_nan=False),
        "version": report["config"]["version"],
    }
    with engine.begin() as conn:
        changed = conn.execute(
            text("""insert into pms.research_runs(id,run_type,variant,start_date,end_date,config,metrics,strategy_version)
          values(:id,'BACKTEST_MATRIX','PREDECLARED_MATRIX',:s,:e,cast(:cfg as jsonb),cast(:metrics as jsonb),:version)
          on conflict(id) do update set metrics=excluded.metrics
          where research_runs.run_type='BACKTEST_MATRIX' and research_runs.config->>'identity'=cast(:cfg as jsonb)->>'identity'"""),
            params,
        )
        if changed.rowcount != 1:
            raise ValueError("Research run identity conflict")


@contextmanager
def report_lock(path):
    lock_path = Path(str(Path(path).resolve()) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("Another matrix process owns this report checkpoint") from None
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
