"""Run only after the consolidated local setup/data checks have been completed."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from contextlib import nullcontext
from dataclasses import asdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from packages.pms.backtest_matrix import MatrixRunner, Period, matrix_plan, plain
from packages.pms.config import load_strategy_config


def export_csv(report, path):
    names = [
        "key",
        "period",
        "case",
        "family",
        "status",
        "blockers",
        "closed_trades",
        "trades_per_year",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "sharpe",
        "sortino",
        "calmar",
        "profit_factor",
        "expectancy_r",
        "total_costs",
        "ending_equity",
    ]
    destination = path.with_suffix(".csv")
    temporary = destination.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=names)
        writer.writeheader()
        for cell in report["cells"]:
            row = {k: v for k, v in cell.get("metrics", {}).items() if k in names}
            row.update(
                key=cell["key"],
                period=cell["period"]["name"],
                case=cell["case"]["name"],
                family=cell["case"]["family"],
                status=cell["status"],
                blockers=";".join(cell.get("blockers", [])),
            )
            writer.writerow(row)
    temporary.replace(destination)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Predeclared research matrix; never promotes execution or claims acceptance"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--plan",
        action="store_true",
        help="List comparisons without a database connection",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Read-only data and policy preflight; no backtests",
    )
    mode.add_argument(
        "--run", action="store_true", help="Run/resume isolated historical experiments"
    )
    parser.add_argument(
        "--config",
        help="Explicit research strategy YAML; production defaults are never rewritten",
    )
    parser.add_argument(
        "--symbols",
        help="Optional comma-separated canonical NSE IDs; results explicitly marked as a sample",
    )
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--capital", type=float)
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Core comparisons only; not the complete matrix",
    )
    parser.add_argument(
        "--max-cells",
        type=int,
        help="Bound work per invocation; interrupted/noncompleted cells retry",
    )
    parser.add_argument(
        "--report", type=Path, default=Path("artifacts/backtest_matrix/report.json")
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Keep local JSON/CSV only; skip research_runs storage",
    )
    args = parser.parse_args(argv)
    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be supplied together")
    if args.max_cells is not None and args.max_cells < 1:
        parser.error("--max-cells must be positive")
    cfg = load_strategy_config(args.config) if args.config else load_strategy_config()
    if args.capital is not None:
        import math

        if not math.isfinite(args.capital) or args.capital <= 0:
            parser.error("--capital must be finite and positive")
        cfg["backtest"]["initial_capital"] = args.capital
    cells = matrix_plan(
        cfg,
        [Period("CUSTOM", args.start, args.end)] if args.start else None,
        quick=args.quick,
    )
    if args.plan:
        print(
            json.dumps(
                {
                    "cells": len(cells),
                    "complete_design": [c.key for c in cells]
                    == [c.key for c in matrix_plan(cfg)],
                    "holdout": "2025 onward reserved for out-of-sample",
                    "plan": [{"key": c.key, **plain(asdict(c))} for c in cells],
                },
                indent=2,
                allow_nan=False,
            )
        )
        return 0

    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from apps.api.main import _render_safe_database_url
    from packages.pms.matrix_data import (
        consistent_inputs,
        input_manifest,
        MatrixGates,
        code_fingerprint,
        persist_report,
        report_lock,
    )
    from packages.pms.replay_service import execute_replay

    load_dotenv()
    url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not url:
        parser.error("PMS_DATABASE_URL or DATABASE_URL is required after local setup")
    engine = create_engine(
        _render_safe_database_url(url),
        pool_pre_ping=True,
        connect_args={"options": "-c search_path=pms,public"},
    )
    symbols = (
        sorted({s.strip().upper() for s in args.symbols.split(",") if s.strip()})
        if args.symbols
        else None
    )
    try:
        with report_lock(args.report) if args.run else nullcontext(), consistent_inputs(
            engine
        ) as snapshot:
            manifest = input_manifest(snapshot, cells, symbols)
            gate = MatrixGates(snapshot, manifest)
            if args.check:
                rows = [
                    {
                        "key": c.key,
                        "period": c.period.name,
                        "case": c.case.name,
                        "blockers": gate(c, c.case.config(cfg)),
                    }
                    for c in cells
                ]
                print(
                    json.dumps(
                        {
                            "mode": "CHECK_ONLY",
                            "scope": manifest["scope"],
                            "dataset_fingerprint": manifest["fingerprint"],
                            "ready_cells": sum(not c["blockers"] for c in rows),
                            "cells": rows,
                            "coverage_samples": gate.samples,
                            "acceptance": "NOT_CERTIFIED",
                        },
                        default=str,
                        indent=2,
                        allow_nan=False,
                    )
                )
                return 0 if all(not c["blockers"] for c in rows) else 2

            selected = [r["instrument_id"] for r in manifest["universe"]]

            def execute(cell, config):
                result, diagnostics = execute_replay(
                    snapshot,
                    selected,
                    config,
                    cell.case,
                    cell.period.start,
                    cell.period.end,
                )
                import hashlib

                folder = args.report.parent / (args.report.stem + "_cells")
                folder.mkdir(parents=True, exist_ok=True)
                evidence = folder / (cell.key + ".json")
                payload = json.dumps(
                    plain(asdict(result)), sort_keys=True, allow_nan=False
                ).encode()
                temporary = evidence.with_suffix(".json.tmp")
                temporary.write_bytes(payload)
                temporary.replace(evidence)
                diagnostics.update(
                    evidence_file=str(evidence.relative_to(args.report.parent)),
                    evidence_sha256=hashlib.sha256(payload).hexdigest(),
                )
                return result, diagnostics

            runner = MatrixRunner(
                cfg,
                cells,
                manifest,
                args.report,
                executor=execute,
                gate=gate,
                code_version=code_fingerprint(),
            )

            def progress(row):
                print(
                    f"{row['period']['name']} / {row['case']['name']}: {row['status']}",
                    flush=True,
                )
                if not args.no_persist:
                    persist_report(engine, runner.report)

            report = runner.run(max_cells=args.max_cells, on_progress=progress)
            export_csv(report, args.report)
            if not args.no_persist:
                persist_report(engine, report)
            print(
                json.dumps(
                    {
                        "run_id": report["run_id"],
                        "summary": report["summary"],
                        "report": str(args.report),
                    },
                    indent=2,
                )
            )
            return 0 if report["summary"]["status"] == "COMPLETED" else 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "Interrupted. Completed cells are checkpointed; resume with the same options.",
            file=sys.stderr,
        )
        raise SystemExit(130)
    except Exception as exc:
        print(
            f"Matrix stopped ({type(exc).__name__}). Check data, setup and the report path; database credentials are not printed.",
            file=sys.stderr,
        )
        raise SystemExit(2)
