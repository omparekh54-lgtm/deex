"""Deferred local walk-forward entry point: plan, preflight, or bounded resumable work."""

from __future__ import annotations
import argparse
import csv
import json
import math
import os
import sys
from contextlib import nullcontext
from dataclasses import asdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.pms.backtest_matrix import plain
from packages.pms.config import load_strategy_config
from packages.pms.walk_forward import (
    DEFAULT_WINDOWS,
    FINAL_WINDOW,
    WalkForwardWindow,
    WalkForwardRunner,
    SelectionPolicy,
    candidate_cases,
    validate_plan,
    phase_cells,
)


def export_csv(report, path):
    destination = path.with_suffix(".csv")
    temporary = destination.with_suffix(".csv.tmp")
    fields = [
        "window",
        "kind",
        "status",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "candidate",
        "training_score",
        "train_cagr_pct",
        "oos_return_pct",
        "oos_cagr_pct",
        "oos_drawdown_pct",
        "oos_closed_trades",
        "generalization_gap_pct",
        "blockers",
    ]
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in report["folds"]:
            w = row["window"]
            s = row.get("selection", {})
            t = row.get("test_metrics", {})
            writer.writerow(
                dict(
                    window=w["name"],
                    kind=w["kind"],
                    status=row["status"],
                    **{
                        k: w[k]
                        for k in ("train_start", "train_end", "test_start", "test_end")
                    },
                    candidate=s.get("candidate", {}).get("name"),
                    training_score=s.get("score"),
                    train_cagr_pct=row.get("train_metrics", {}).get("cagr_pct"),
                    oos_return_pct=t.get("total_return_pct"),
                    oos_cagr_pct=t.get("cagr_pct"),
                    oos_drawdown_pct=t.get("max_drawdown_pct"),
                    oos_closed_trades=t.get("closed_trades"),
                    generalization_gap_pct=row.get("generalization_gap_pct"),
                    blockers=";".join(row.get("blockers", [])),
                )
            )
    temporary.replace(destination)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Train-only frozen walk-forward research; no automatic strategy promotion"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    for name, help in [
        ("plan", "Print the frozen design offline"),
        ("check", "Read-only preflight without replay"),
        ("run", "Run/resume chronological folds"),
    ]:
        mode.add_argument("--" + name, action="store_true", help=help)
    parser.add_argument(
        "--include-holdout",
        action="store_true",
        help="Explicitly consume the separately labelled 2025 final holdout",
    )
    parser.add_argument(
        "--windows",
        type=Path,
        help="Explicit JSON list of dated windows, fixed before computation",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Four candidates rather than 27; explicitly reduced research",
    )
    parser.add_argument(
        "--symbols",
        help="Optional comma-separated canonical IDs; explicitly sampled research",
    )
    parser.add_argument("--capital", type=float)
    parser.add_argument(
        "--config", help="Explicit research YAML, never rewrites production settings"
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=24,
        help="Minimum closed training cycles (not performance acceptance)",
    )
    parser.add_argument(
        "--max-cells", type=int, help="Bound train and test attempts in this invocation"
    )
    parser.add_argument(
        "--report", type=Path, default=Path("artifacts/walk_forward/report.json")
    )
    parser.add_argument("--no-persist", action="store_true")
    args = parser.parse_args(argv)
    if args.max_cells is not None and args.max_cells < 1:
        parser.error("--max-cells must be positive")
    if args.capital is not None and (
        not math.isfinite(args.capital) or args.capital <= 0
    ):
        parser.error("--capital must be finite and positive")
    if args.min_trades < 1:
        parser.error("--min-trades must be positive")
    cfg = load_strategy_config(args.config) if args.config else load_strategy_config()
    if args.capital is not None:
        cfg["backtest"]["initial_capital"] = args.capital
    try:
        windows = (
            tuple(
                WalkForwardWindow(
                    **{
                        k: (
                            date.fromisoformat(v)
                            if k
                            in {"train_start", "train_end", "test_start", "test_end"}
                            else v
                        )
                        for k, v in row.items()
                    }
                )
                for row in json.loads(args.windows.read_text())
            )
            if args.windows
            else DEFAULT_WINDOWS
        )
        if args.include_holdout and not args.windows:
            windows = (*windows, FINAL_WINDOW)
        if any(w.kind == "FINAL_HOLDOUT" for w in windows) and not args.include_holdout:
            parser.error("Final holdout windows require --include-holdout")
        candidates = candidate_cases(cfg, quick=args.quick)
        validate_plan(windows, candidates)
    except (ValueError, TypeError, KeyError) as exc:
        parser.error(str(exc))
    policy = SelectionPolicy(args.min_trades)
    if args.plan:
        print(
            json.dumps(
                plain(
                    dict(
                        version="WALK_FORWARD_v2.0",
                        windows=[asdict(w) for w in windows],
                        candidates=[asdict(c) for c in candidates],
                        training_cells=len(windows) * len(candidates),
                        test_cells=len(windows),
                        policy=asdict(policy),
                        final_holdout_included=args.include_holdout,
                        note="Pre-2025 OOS is retrospective, not pristine unseen evidence. No production promotion.",
                    )
                ),
                indent=2,
            )
        )
        return 0
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from apps.api.main import _render_safe_database_url
    from packages.pms.matrix_data import (
        consistent_inputs,
        report_lock,
        code_fingerprint,
    )
    from packages.pms.walk_forward_data import (
        prepare_inputs,
        replay_executor,
        persist_walk_forward,
    )

    load_dotenv()
    url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not url:
        parser.error(
            "DATABASE_URL or PMS_DATABASE_URL is required after consolidated setup"
        )
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
            datasets, gates = prepare_inputs(snapshot, windows, candidates, symbols)

            def gate(w, phase, cell, config):
                return gates[w.name, phase](cell, config)

            if args.check:
                rows = [
                    dict(
                        window=w.name,
                        phase=phase,
                        blockers=gate(w, phase, c, c.case.config(cfg)),
                    )
                    for w in windows
                    for phase in ("TRAIN", "TEST")
                    for c in phase_cells(w, candidates[:1], phase)
                ]
                print(
                    json.dumps(
                        plain(
                            dict(
                                mode="CHECK_ONLY",
                                checks=rows,
                                datasets=datasets,
                                acceptance="NOT_CERTIFIED",
                            )
                        ),
                        indent=2,
                    )
                )
                return 0 if all(not r["blockers"] for r in rows) else 2
            runner = WalkForwardRunner(
                cfg,
                windows,
                candidates,
                datasets,
                args.report,
                executor=replay_executor(snapshot, datasets, args.report),
                gate=gate,
                code_version=code_fingerprint(),
                policy=policy,
            )

            def progress(report):
                export_csv(report, args.report)
                if not args.no_persist:
                    persist_walk_forward(engine, report)

            report = runner.run(max_cells=args.max_cells, on_progress=progress)
            progress(report)
            print(
                json.dumps(
                    dict(
                        run_id=report["run_id"],
                        summary=report["summary"],
                        report=str(args.report),
                    ),
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
            "Interrupted; resume with identical inputs and the same report path.",
            file=sys.stderr,
        )
        raise SystemExit(130)
    except Exception as exc:
        print(
            f"Walk-forward stopped ({type(exc).__name__}); inspect data/setup and checkpoint integrity. Credentials are not printed.",
            file=sys.stderr,
        )
        raise SystemExit(2)
