"""Deferred local integrity scan. No repair, ingestion, issue resolution or safety unlock."""

from __future__ import annotations
import argparse
import csv
import json
import os
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.pms.backtest_matrix import plain
from packages.pms.config import load_strategy_config
from packages.pms.data_integrity import (
    AuditScope,
    IntegrityAuditor,
    write_report,
    persist_audit,
    VERSION,
)
from packages.pms.integrity_checks import CHECKS, REVIEWS
from packages.pms.historical_dataset import write_completion_plan


def export_csv(report, path):
    if Path(path).suffix.lower() != ".json":
        raise ValueError("Audit report must use a .json path")
    target = Path(path).with_suffix(".csv")
    tmp = target.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "code",
                "group",
                "status",
                "severity",
                "finding_count",
                "error_type",
                "remediation",
            ],
        )
        writer.writeheader()
        for row in report["checks"]:
            writer.writerow({k: row.get(k) for k in writer.fieldnames})
    tmp.replace(target)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only PMS source integrity audit; never certifies a strategy"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--plan",
        action="store_true",
        help="List automated checks and independent review requirements offline",
    )
    mode.add_argument(
        "--run",
        action="store_true",
        help="Read a consistent database snapshot and write a new report",
    )
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        help="Required for execution; explicit completed-date cutoff",
    )
    parser.add_argument(
        "--symbols",
        help="Optional canonical symbol sample, labelled separately from historical master",
    )
    parser.add_argument(
        "--checks",
        help="Optional comma-separated check codes; omitted checks stay NOT_RUN",
    )
    parser.add_argument("--sample-limit", type=int, default=25)
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--config")
    parser.add_argument(
        "--report",
        type=Path,
        help="New JSON report path; existing evidence is never overwritten",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Additionally save a research-only record after the scan",
    )
    args = parser.parse_args(argv)
    codes = (
        {c.strip() for c in args.checks.split(",") if c.strip()}
        if args.checks
        else {c.code for c in CHECKS}
    )
    if not codes or not codes <= {c.code for c in CHECKS}:
        parser.error("Unknown/empty check selection")
    if args.report and args.report.suffix.lower() != ".json":
        parser.error("--report must end in .json to preserve JSON and CSV evidence")
    if args.plan:
        print(
            json.dumps(
                dict(
                    version=VERSION,
                    checks=[
                        {k: v for k, v in asdict(c).items() if k != "sql"}
                        for c in CHECKS
                        if c.code in codes
                    ],
                    schema_check=True,
                    independent_reviews=[
                        dict(code=c, group=g, remediation=r) for c, g, r in REVIEWS
                    ],
                    source_writes=False,
                    full_certification=False,
                ),
                indent=2,
            )
        )
        return 0
    if not args.end:
        parser.error("--end is required for a dated audit")
    try:
        scope = AuditScope(args.start, args.end, args.sample_limit, args.timeout_ms)
    except ValueError as exc:
        parser.error(str(exc))
    path = args.report or Path("artifacts/data_integrity") / (str(uuid4()) + ".json")
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from apps.api.main import _render_safe_database_url
    from packages.pms.matrix_data import (
        consistent_inputs,
        report_lock,
        code_fingerprint,
    )

    load_dotenv()
    url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not url:
        parser.error(
            "DATABASE_URL or PMS_DATABASE_URL is required after consolidated setup"
        )
    cfg = load_strategy_config(args.config) if args.config else load_strategy_config()
    symbols = (
        sorted({s.strip().upper() for s in args.symbols.split(",") if s.strip()})
        if args.symbols
        else None
    )
    engine = create_engine(
        _render_safe_database_url(url),
        pool_pre_ping=True,
        connect_args={"options": "-c search_path=pms,public"},
    )
    try:
        with report_lock(path):
            if any(p.exists() for p in (path, path.with_suffix(".csv"), path.with_suffix(".repair.json"))):
                raise ValueError("Preserve existing evidence; select a new report path")
            with consistent_inputs(engine) as snapshot:
                auditor = IntegrityAuditor(
                    snapshot,
                    scope,
                    cfg,
                    code_version=code_fingerprint(),
                    symbols=symbols,
                    checks=[c for c in CHECKS if c.code in codes],
                )

                def progress(report):
                    write_report(report, path)

                report = auditor.run(on_progress=progress)
            export_csv(report, path)
            repair_plan = write_completion_plan(report, path.with_suffix(".repair.json"))
            if args.persist:
                persist_audit(engine, report)
            print(
                json.dumps(
                    dict(
                        run_id=report["run_id"],
                        summary=report["summary"],
                        report=str(path),
                        completion=repair_plan["status"],
                        pending_repairs=repair_plan["pending_tasks"],
                        repair_plan=str(path.with_suffix(".repair.json")),
                    ),
                    indent=2,
                )
            )
            return 0 if report["summary"]["status"] == "AUTOMATED_CHECKS_PASSED" else 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "Audit interrupted; any partial report is INCOMPLETE. Start a new audit report.",
            file=sys.stderr,
        )
        raise SystemExit(130)
    except Exception as exc:
        print(
            f"Audit stopped ({type(exc).__name__}); inspect schema/data and a new report path. Credentials are not printed.",
            file=sys.stderr,
        )
        raise SystemExit(2)
