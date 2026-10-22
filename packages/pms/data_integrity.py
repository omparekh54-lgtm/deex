"""Read-only integrity evidence. A clean automated scan is not full certification."""

from __future__ import annotations
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from sqlalchemy import text
from packages.pms.backtest_matrix import Cell, Case, fingerprint, plain
from packages.pms.integrity_checks import CHECKS, REVIEWS
from packages.pms.matrix_data import input_manifest
from packages.pms.module_a_context import IST, end_of_day
from packages.pms.research import ResearchVariant
from packages.pms.walk_forward import ReplayPeriod

VERSION = "DATA_INTEGRITY_v1.1"
EXTRA_INPUTS = {
    "module_a_history": "as_of<=:stamp",
    "module_b_signals": "signal_date<=:end",
    "module_c_rankings": "trade_date<=:end",
    "provider_snapshots": "observed_at<=:audit_stamp",
}
SCHEMA_SQL = """select table_name,column_name,data_type,is_nullable from information_schema.columns where table_schema='pms' order by table_name,ordinal_position"""
REQUIRED = {
    "instruments": {
        "id",
        "exchange",
        "nse_symbol",
        "trading_symbol",
        "listed_from",
        "delisted_on",
        "active",
    },
    "daily_prices": {
        "instrument_id",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "source",
    },
    "module_e_benchmark_bars": {
        "benchmark_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "source",
    },
    "financial_filings": {
        "id",
        "instrument_id",
        "period_end",
        "filing_timestamp",
        "revision_number",
        "source",
        "consolidated",
        "filing_kind",
        "raw_payload",
        "xbrl_uri",
        "exchange_received_at",
        "exchange_disseminated_at",
    },
    "financial_facts": {"filing_id", "fact_name", "value", "unit"},
    "shareholding_history": {
        "id",
        "instrument_id",
        "period_end",
        "filed_at",
        "promoter_holding_pct",
        "promoter_pledge_pct",
        "total_shares",
    },
    "shares_outstanding_history": {
        "id",
        "instrument_id",
        "effective_from",
        "effective_to",
        "source",
        "knowable_at",
        "shares_outstanding",
    },
    "sector_history": {
        "id",
        "instrument_id",
        "effective_from",
        "effective_to",
        "sector",
    },
    "index_constituents_history": {
        "id",
        "instrument_id",
        "effective_from",
        "effective_to",
        "index_code",
    },
    "surveillance_history": {
        "id",
        "instrument_id",
        "effective_from",
        "effective_to",
        "surveillance_type",
    },
    "corporate_actions": {
        "id",
        "instrument_id",
        "action_type",
        "ex_date",
        "announced_at",
        "ratio_from",
        "ratio_to",
        "cash_amount",
    },
    "earnings_calendar": {"id", "instrument_id", "event_date", "announced_at"},
    "provider_snapshots": {
        "id",
        "provider",
        "dataset",
        "source_uri",
        "content_sha256",
        "metadata",
        "observed_at",
    },
    "data_quality_issues": {"id", "dataset", "code", "severity", "resolved_at"},
    "module_a_history": {"id", "instrument_id", "as_of", "snapshot", "eligible"},
    "module_b_signals": {"signal_date"},
    "module_c_rankings": {"trade_date"},
}


@dataclass(frozen=True)
class AuditScope:
    start: date
    end: date
    sample_limit: int = 25
    statement_timeout_ms: int = 60000

    def __post_init__(self):
        if self.end < self.start or self.end >= date.today():
            raise ValueError("Ordered completed historical dates required")
        if not 1 <= self.sample_limit <= 100:
            raise ValueError("sample_limit must be 1..100")
        if not 1000 <= self.statement_timeout_ms <= 600000:
            raise ValueError("statement timeout must be 1000..600000 ms")


def schema_findings(rows):
    existing = {}
    for r in rows:
        existing.setdefault(r["table_name"], set()).add(r["column_name"])
    return [
        dict(table=table, missing_columns=sorted(columns - existing.get(table, set())))
        for table, columns in REQUIRED.items()
        if columns - existing.get(table, set())
    ]


def decision_finding(row):
    raw = row.get("filing_timestamp")
    if raw is None:
        return (
            "eligible_decision_without_filing_timestamp"
            if row.get("eligible")
            else None
        )
    try:
        filing = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        stamp = (
            row["as_of"]
            if isinstance(row["as_of"], datetime)
            else datetime.fromisoformat(str(row["as_of"]).replace("Z", "+00:00"))
        )
        if filing.tzinfo is None or stamp.tzinfo is None:
            return "ambiguous_timestamp_without_timezone"
        return "filing_after_decision" if filing > stamp else None
    except (ValueError, TypeError):
        return "malformed_filing_timestamp"


def summarize(rows, *, expected, finished=False, scope="HISTORICAL_MASTER"):
    counts = {
        s: sum(r["status"] == s for r in rows)
        for s in (
            "PASS",
            "FAIL",
            "ERROR",
            "REVIEW_REQUIRED",
            "NOT_APPLICABLE",
            "NOT_RUN",
        )
    }
    complete = finished and not counts["NOT_RUN"] and len(rows) == expected
    status = (
        "INCOMPLETE"
        if not complete
        else (
            "FAILED"
            if counts["FAIL"] or counts["ERROR"]
            else (
                "REVIEW_REQUIRED"
                if counts["REVIEW_REQUIRED"]
                else "AUTOMATED_CHECKS_PASSED"
            )
        )
    )
    return dict(
        status=status,
        complete=complete,
        counts=counts,
        checks=len(rows),
        expected_checks=expected,
        scope=scope,
        automated_checks_passed=complete and not (counts["FAIL"] or counts["ERROR"]),
        full_data_certification=False,
        acceptance="NOT_CERTIFIED",
        note="Missing/error/unexecuted checks cannot pass. Independent source-history reviews remain separate. No data is repaired or quality issue resolved by this audit.",
    )


def write_report(report, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    report["report_hash"] = fingerprint(
        {k: v for k, v in report.items() if k != "report_hash"}
    )
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(plain(report), sort_keys=True, indent=2, allow_nan=False))
    tmp.replace(path)


def read_report(path):
    report = json.loads(Path(path).read_text())
    if report.get("report_hash") != fingerprint(
        {k: v for k, v in report.items() if k != "report_hash"}
    ):
        raise ValueError("Integrity report checksum mismatch")
    return report


def audit_manifest(snapshot, scope, symbols, stamp):
    cell = Cell(
        ReplayPeriod("INTEGRITY", scope.start, scope.end, "AUDIT"),
        Case("FULL", ResearchVariant("B_A_C_E")),
    )
    manifest = input_manifest(snapshot, (cell,), symbols, allow_ambiguous=True)
    params = dict(stamp=end_of_day(scope.end), end=scope.end, audit_stamp=stamp)
    extra = {}
    for table, where in EXTRA_INPUTS.items():
        rows = (
            snapshot.execute(
                text(
                    f"""with hashes as(select md5(row_to_json(t)::text) h from pms.{table} t where {where})
        select left(h,2) bucket,count(*) rows,md5(string_agg(h,'' order by h)) digest from hashes group by left(h,2) order by bucket"""
                ),
                params,
            )
            .mappings()
            .all()
        )
        extra[table] = {
            "rows": sum(int(r["rows"]) for r in rows),
            "sha256": fingerprint([dict(r) for r in rows]),
        }
    manifest["audit_additional_inputs"] = extra
    manifest["fingerprint"] = fingerprint(
        {k: v for k, v in manifest.items() if k != "fingerprint"}
    )
    return manifest


class IntegrityAuditor:
    def __init__(
        self, snapshot, scope, cfg, *, code_version, symbols=None, checks=None
    ):
        self.snapshot, self.scope, self.cfg = snapshot, scope, cfg
        self.checks = tuple(CHECKS if checks is None else checks)
        if not self.checks or len({c.code for c in self.checks}) != len(self.checks):
            raise ValueError("Unique nonempty checks required")
        if not set(c.code for c in self.checks) <= set(c.code for c in CHECKS):
            raise ValueError("Unknown audit checks")
        self.symbols, self.code_version = symbols, code_version

    def run(self, *, on_progress=None):
        now = datetime.now(timezone.utc)
        report = dict(
            run_id=str(uuid4()),
            version=VERSION,
            created_at=now.isoformat(),
            code_version=self.code_version,
            config_hash=fingerprint(self.cfg),
            requested_scope=plain(asdict(self.scope)),
            requested_symbols=self.symbols,
            checks=[],
            dataset=None,
            expected_checks=1 + len(CHECKS) + len(REVIEWS),
            finished=False,
        )

        def save():
            report["summary"] = summarize(
                report["checks"],
                expected=report["expected_checks"],
                finished=report["finished"],
                scope=(report.get("dataset") or {}).get("scope", "UNKNOWN"),
            )
            if on_progress:
                on_progress(report)

        schema_rows = self.snapshot.execute(text(SCHEMA_SQL)).mappings().all()
        report["schema_fingerprint"] = fingerprint([dict(r) for r in schema_rows])
        missing = schema_findings(schema_rows)
        report["checks"].append(
            dict(
                code="SCHEMA_CONTRACT",
                group="SCHEMA",
                severity="ERROR",
                status="FAIL" if missing else "PASS",
                finding_count=len(missing),
                samples=missing[: self.scope.sample_limit],
                remediation="Apply only outstanding migrations after inspecting/backup of the populated database; do not rebuild it.",
            )
        )
        if missing:
            report["checks"] += [
                self.pending(c, "required_schema_missing") for c in CHECKS
            ]
            report["checks"] += self.reviews()
            report["finished"] = True
            report["identity"] = fingerprint(
                dict(
                    version=VERSION,
                    scope=report["requested_scope"],
                    schema_failures=missing,
                    code=self.code_version,
                    schema=report["schema_fingerprint"],
                    config=report["config_hash"],
                )
            )
            save()
            return report
        save()
        report["dataset"] = audit_manifest(self.snapshot, self.scope, self.symbols, now)
        universe = report["dataset"]["universe"]
        params = dict(
            ids=json.dumps([r["id"] for r in universe]),
            start=self.scope.start,
            end=self.scope.end,
            warmup=self.scope.start - timedelta(days=1100),
            stamp=end_of_day(self.scope.end),
            start_stamp=datetime.combine(self.scope.start, time.min, tzinfo=IST),
            audit_stamp=now,
            sample=self.scope.sample_limit,
            adjusted=json.dumps(
                [
                    str(s).lower()
                    for s in self.cfg["technical"].get("adjusted_price_sources", [])
                ]
            ),
        )
        if not universe:
            report["checks"].append(
                dict(
                    code="EMPTY_UNIVERSE",
                    group="UNIVERSE",
                    severity="ERROR",
                    status="FAIL",
                    finding_count=1,
                    samples=[],
                    remediation="Load/resolve the requested historical instrument universe.",
                )
            )
            report["expected_checks"] += 1
        save()
        selected = {c.code for c in self.checks}
        for check in CHECKS:
            if check.code not in selected:
                report["checks"].append(self.pending(check, "not_requested"))
                save()
                continue
            row = dict(
                code=check.code,
                group=check.group,
                severity=check.severity,
                remediation=check.remediation,
            )
            try:
                # PostgreSQL errors/timeouts abort a transaction; savepoints let the audit continue read-only.
                with self.snapshot.connection.begin_nested():
                    self.snapshot.execute(
                        text("select set_config('statement_timeout',:timeout,true)"),
                        {"timeout": str(self.scope.statement_timeout_ms)},
                    )
                    if check.code == "A_DECISION_KNOWABILITY":
                        count, samples, observed = 0, [], 0
                        for item in self.snapshot.execute(
                            text(check.sql), params
                        ).mappings():
                            observed += 1
                            reason = decision_finding(item)
                            if reason:
                                count += 1
                                if len(samples) < self.scope.sample_limit:
                                    samples.append({**dict(item), "reason": reason})
                        row.update(
                            finding_count=count,
                            samples=plain(samples),
                            observed_decisions=observed,
                        )
                        if not observed:
                            row["status"] = "NOT_APPLICABLE"
                    else:
                        result = (
                            self.snapshot.execute(
                                text(
                                    "with bad as ("
                                    + check.sql
                                    + ") select count(*) total,coalesce((select jsonb_agg(t) from (select * from bad limit :sample) t),'[]'::jsonb) samples from bad"
                                ),
                                params,
                            )
                            .mappings()
                            .one()
                        )
                        row.update(
                            finding_count=int(result["total"]),
                            samples=plain(result["samples"]),
                        )
                    row.setdefault(
                        "status",
                        (
                            "FAIL"
                            if row["finding_count"] and check.severity == "ERROR"
                            else "REVIEW_REQUIRED" if row["finding_count"] else "PASS"
                        ),
                    )
            except Exception as exc:
                row.update(
                    status="ERROR",
                    finding_count=None,
                    samples=[],
                    error_type=type(exc).__name__,
                )
            report["checks"].append(row)
            save()
        report["checks"] += self.reviews()
        report["finished"] = True
        report["identity"] = fingerprint(
            dict(
                version=VERSION,
                scope=report["requested_scope"],
                dataset=report["dataset"]["fingerprint"],
                config=report["config_hash"],
                schema=report["schema_fingerprint"],
                code=self.code_version,
                checks=sorted(selected),
            )
        )
        save()
        return report

    @staticmethod
    def pending(check, reason):
        return dict(
            code=check.code,
            group=check.group,
            severity=check.severity,
            status="NOT_RUN",
            finding_count=None,
            samples=[],
            reason=reason,
            remediation=check.remediation,
        )

    @staticmethod
    def reviews():
        return [
            dict(
                code=code,
                group=group,
                severity="REVIEW",
                status="REVIEW_REQUIRED",
                finding_count=None,
                samples=[],
                remediation=remedy,
            )
            for code, group, remedy in REVIEWS
        ]


def persist_audit(engine, report):
    if not report.get("finished") or not report.get("identity"):
        raise ValueError("Only finished fingerprinted audit reports can be stored")
    cfg = json.dumps(
        plain(
            {
                k: report[k]
                for k in (
                    "identity",
                    "requested_scope",
                    "dataset",
                    "config_hash",
                    "code_version",
                    "schema_fingerprint",
                )
            }
        ),
        allow_nan=False,
    )
    payload = json.dumps(plain(report), allow_nan=False)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """insert into pms.research_runs(id,run_type,variant,start_date,end_date,config,metrics,strategy_version)
        values(:id,'DATA_INTEGRITY_AUDIT','READ_ONLY_SOURCE_SCAN',:s,:e,cast(:cfg as jsonb),cast(:metrics as jsonb),:version)
        on conflict(id) do update set metrics=excluded.metrics
        where research_runs.run_type='DATA_INTEGRITY_AUDIT' and research_runs.config->>'identity'=cast(:cfg as jsonb)->>'identity'"""
            ),
            dict(
                id=report["run_id"],
                s=report["requested_scope"]["start"],
                e=report["requested_scope"]["end"],
                cfg=cfg,
                metrics=payload,
                version=VERSION,
            ),
        )
        if result.rowcount != 1:
            raise ValueError("Audit identity conflict")
