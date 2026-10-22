from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from sqlalchemy import Engine, text
from packages.pms.matrix_data import consistent_inputs


@dataclass(frozen=True)
class DatasetManifest:
    manifest_id: str
    root_path: str
    start_date: date
    end_date: date
    universe: str
    tables: dict[str, dict]


EXPORTS = {
    "instruments": "select * from instruments order by id",
    "daily_prices": "select * from daily_prices where trade_date between cast(:start as date)-interval '1100 days' and :end order by instrument_id,trade_date,source",
    "financial_filings": "select * from financial_filings where filing_timestamp<=:stamp order by instrument_id,period_end,filing_timestamp,id",
    "financial_facts": "select ff.* from financial_facts ff join financial_filings f on f.id=ff.filing_id where f.filing_timestamp<=:stamp order by ff.filing_id,ff.fact_name",
    "shareholding_history": "select * from shareholding_history where filed_at<=:stamp order by instrument_id,period_end,filed_at",
    "shares_outstanding_history": "select * from shares_outstanding_history where effective_from<=:end and knowable_at<=:stamp order by instrument_id,effective_from,id",
    "sector_history": "select * from sector_history where effective_from<=:end order by instrument_id,effective_from,id",
    "surveillance_history": "select * from surveillance_history where effective_from<=:stamp order by instrument_id,effective_from,id",
    "index_constituents_history": "select * from index_constituents_history where effective_from<=:end order by index_code,instrument_id,effective_from",
    "corporate_actions": "select * from corporate_actions where ex_date between cast(:start as date)-interval '1100 days' and :end and announced_at<=:stamp order by instrument_id,ex_date,id",
    "earnings_calendar": "select * from earnings_calendar where announced_at<=:stamp order by instrument_id,event_date,id",
    "module_e_benchmark_bars": "select * from module_e_benchmark_bars where trade_date between cast(:start as date)-interval '1100 days' and :end order by benchmark_code,trade_date,source",
    "provider_snapshots": "select * from provider_snapshots where observed_at<=:stamp order by id",
    "data_quality_issues": "select * from data_quality_issues where resolved_at is null order by id",
}


def _jsonable(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, sort_keys=True)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _checksum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def export_research_dataset(
    engine: Engine,
    root: str | Path,
    start: date,
    end: date,
    *,
    universe: str = "NSE_EQUITY",
) -> DatasetManifest:
    if end < start or end >= date.today():
        raise ValueError("Ordered completed historical dates required")
    root = Path(root)
    manifest_id = str(uuid.uuid4())
    out = root / manifest_id
    out.mkdir(parents=True, exist_ok=True)
    table_meta: dict[str, dict] = {}

    from packages.pms.module_a_context import end_of_day

    with consistent_inputs(engine) as conn:
        for name, sql in EXPORTS.items():
            rows = conn.execute(text(sql), {"start": start, "end": end, "stamp": end_of_day(end)}).mappings().all()
            normalized = [{k: _jsonable(v) for k, v in dict(row).items()} for row in rows]
            frame = pl.DataFrame(normalized, infer_schema_length=None) if normalized else pl.DataFrame()
            path = out / f"{name}.parquet"
            frame.write_parquet(path)
            table_meta[name] = {
                "rows": len(normalized),
                "file": path.name,
                "sha256": _checksum(path),
            }

    manifest = DatasetManifest(manifest_id, str(out), start, end, universe, table_meta)
    (out / "manifest.json").write_text(json.dumps({
        "manifest_id": manifest_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "universe": universe,
        "tables": table_meta,
        "version": "ISOLATED_REPLAY_DATASET_v2",
        "snapshot_consistency": "REPEATABLE_READ_READ_ONLY",
        "dataset_complete": False,
        "acceptance": "NOT_CERTIFIED",
    }, indent=2), encoding="utf-8")
    return manifest
