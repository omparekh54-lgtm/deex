"""Conservative completion status and actionable inventory for historical datasets."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import json
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from packages.pms.backtest_matrix import fingerprint, plain
from packages.pms.integrity_checks import CHECKS, REVIEWS
from packages.pms.module_a_context import IST, end_of_day

PRIORITY = {"SCHEMA": 0, "UNIVERSE": 1, "COVERAGE": 2, "FUNDAMENTALS": 3,
            "POINT_IN_TIME": 3, "ACTIONS": 4, "REFERENCE": 4, "PROVENANCE": 5}


def completion_plan(report):
    tasks = []
    for row in report["checks"]:
        if row["status"] in {"PASS", "NOT_APPLICABLE"}:
            continue
        tasks.append(dict(
            check=row["code"], group=row["group"], status=row["status"],
            priority=PRIORITY.get(row["group"], 6),
            finding_count=row.get("finding_count"),
            samples=row.get("samples", []),
            action=row.get("remediation", "Rerun this check with verified source inputs."),
            source_evidence_required=True,
            automatic_source_write=False,
        ))
    tasks.sort(key=lambda r: (r["priority"], r["check"]))
    summary = report.get("summary", {})
    ready = bool(summary.get("complete")) and not tasks
    payload = dict(
        version="HISTORICAL_DATASET_v1", audit_run_id=report["run_id"],
        audit_identity=report.get("identity"), scope=report.get("requested_scope"),
        status="READY_FOR_REPLAY" if ready else "BLOCKED",
        dataset_complete=ready, full_data_certification=False,
        pending_tasks=len(tasks), tasks=tasks,
        note="Counts describe audited gaps, not repaired records. Unknown/error/unrun checks and independent reviews remain blocking. Reaudit original sources after every repair; never interpolate prices, invent timestamps or copy current reference values backwards.",
    )
    return {**payload, "fingerprint": fingerprint(payload)}


def write_completion_plan(report, path):
    path = Path(path)
    payload = completion_plan(report)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(plain(payload), indent=2, allow_nan=False))
    temporary.replace(path)
    return payload


def remote_audit_bundle(scope, cfg):
    """One read-only transaction for connector-only access; no source mutations.

    This diagnostic bundle omits the Python JSON decision-timestamp validator;
    its NOT_RUN marker prevents it being mistaken for a complete integrity scan.
    """
    params = dict(start=scope.start, end=scope.end,
        warmup=scope.start-timedelta(days=1100), stamp=end_of_day(scope.end),
        start_stamp=datetime.combine(scope.start, time.min, tzinfo=IST),
        audit_stamp=datetime.now(timezone.utc), sample=scope.sample_limit,
        adjusted=json.dumps([str(s).lower() for s in cfg["technical"].get("adjusted_price_sources", [])]))
    universe = """select id from pms.instruments where exchange='NSE'
        and (listed_from is null or listed_from<=:end)
        and (delisted_on is null or delisted_on>:start)"""
    ids = "(select coalesce(jsonb_agg(id),'[]'::jsonb)::text from ("+universe+") audit_universe)"
    def compile_sql(sql):
        sql = sql.replace(":ids", ids)
        statement = text(sql)
        statement = statement.bindparams(**{k: v for k, v in params.items() if k in statement._bindparams})
        return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    statements = ["SET TRANSACTION READ ONLY", "SET LOCAL search_path TO pms,public",
                  "SET LOCAL statement_timeout='"+str(scope.statement_timeout_ms)+"ms'"]
    statements.append(compile_sql("""select transaction_timestamp() snapshot_at,
        (select count(*) from ("""+universe+""") u) universe_count,
        (select count(*) from pms.instruments where exchange='NSE' and not active) inactive_master_rows,
        (select count(*) from pms.daily_prices where trade_date between :start and :end) price_rows,
        (select count(*) from pms.financial_filings where filing_timestamp<=:stamp) financial_filings,
        (select min(period_end) from pms.financial_filings) earliest_financial_period"""))
    codes = []
    for check in CHECKS:
        if check.code == "A_DECISION_KNOWABILITY":
            continue
        query = "select '"+check.code+"' code,count(*) total,coalesce((select jsonb_agg(t) from (select * from bad limit :sample) t),'[]'::jsonb) samples from bad"
        statements.append(compile_sql("with bad as ("+check.sql+") "+query))
        codes.append(check.code)
    return dict(version="REMOTE_HISTORICAL_DIAGNOSTIC_v1", scope=plain(scope.__dict__),
                statements=statements, check_codes=codes,
                not_run=["SCHEMA_CONTRACT", "A_DECISION_KNOWABILITY"],
                independent_reviews=[dict(code=c, group=g, action=a) for c,g,a in REVIEWS],
                dataset_complete=False)


def discover_financial_history(provider, symbol, start, end, *, page_size=200, max_pages=100):
    """Bounded complete page traversal; duplicate pages cannot become success."""
    if max_pages < 1 or page_size < 1 or end < start:
        raise ValueError("Valid discovery dates and page bounds required")
    records, seen = [], set()
    for page in range(1, max_pages + 1):
        batch = provider.list_filings(symbol=symbol, from_date=start, to_date=end,
                                      page=page, size=page_size, strict=True)
        identities = [(r.symbol, r.period_end, r.filed_at, r.statement_scope,
                       r.submission_type, r.xbrl_uri, r.external_id) for r in batch]
        if any(key in seen for key in identities) or len(set(identities)) != len(identities):
            raise ValueError("Financial discovery repeated a filing/page; source pagination needs review")
        seen.update(identities)
        records.extend(batch)
        if len(batch) < page_size:
            return records
    raise ValueError("Financial discovery page cap reached; narrow the date window and retry")


def v2_financial_parser():
    from packages.data_layer import xbrl, exchange_financials
    if getattr(xbrl, "EXTRACTION_VERSION", None) != "FINANCIAL_XBRL_v2":
        raise RuntimeError("Merge the financial ingestion V2 fix and apply migration 012 before reprocessing")
    return exchange_financials.filings_from_exchange_record


def require_financial_period_identity(engine):
    """Fail before writing if an old unique key would collapse annual companions."""
    with engine.connect() as conn:
        keys = conn.execute(text("""select array_agg(a.attname::text order by k.ordinality) columns
            from pg_index ix cross join lateral unnest(ix.indkey) with ordinality k(attnum,ordinality)
            join pg_attribute a on a.attrelid=ix.indrelid and a.attnum=k.attnum
            where ix.indrelid=coalesce(to_regclass('pms.financial_filings_local'),
                to_regclass('pms.financial_filings'),to_regclass('financial_filings'))
            and ix.indisunique and ix.indisvalid and not ix.indisprimary
            group by ix.indexrelid""")).scalars().all()
    old = ['instrument_id','period_end','filing_timestamp','source','consolidated','revision_number']
    if old in keys or old + ['filing_kind'] not in keys:
        raise RuntimeError("Financial period identity migration 012 is required before source writes")
