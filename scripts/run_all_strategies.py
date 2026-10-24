"""Backtest all eleven DEEX strategies on the local data folder.

    python scripts/run_all_strategies.py                         # 10 years, Rs 1 crore, all 11
    python scripts/run_all_strategies.py --only S1,S7,PMS_SETUP1
    python scripts/run_all_strategies.py --start 2019-01-01 --end 2026-09-30 --capital 5000000

Writes one JSON report per strategy plus summary.csv / summary.md under <root>/reports/.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from packages.strategies.catalog import STRATEGY_IDS  # noqa: E402
from packages.strategies.lake import data_root, load_panel  # noqa: E402
from packages.strategies.runner import WARMUP_DAYS, run_strategy  # noqa: E402

ALL = ["PMS_SETUP1", *STRATEGY_IDS]


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="deex_data")
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    ap.add_argument("--capital", type=float, default=1e7)
    ap.add_argument("--only", help="comma-separated: " + ",".join(ALL))
    ap.add_argument("--strict-surveillance", action="store_true",
                    help="PMS: fail Module A before 23 Jan 2024 (no historical ASM/GSM file)")
    a = ap.parse_args(argv)
    root = data_root(a.root)
    manifest = json.loads((root / "manifest.json").read_text()) if (root / "manifest.json").exists() else {}
    win = manifest.get("window", {})
    start = a.start or date.fromisoformat(win.get("start", str(date.today().replace(year=date.today().year - 10))))
    end = a.end or date.fromisoformat(win.get("end", str(date.today() - timedelta(days=1))))
    wanted = [s.strip() for s in a.only.split(",")] if a.only else ALL
    bad = set(wanted) - set(ALL)
    if bad:
        ap.error("unknown: " + ", ".join(sorted(bad)))
    out = root / "reports"
    out.mkdir(exist_ok=True)
    rows = []
    lib = [s for s in wanted if s != "PMS_SETUP1"]
    if lib:
        log(f"Loading prices {start - timedelta(days=WARMUP_DAYS)} -> {end} for S1-S10")
        panel = load_panel(root, start - timedelta(days=WARMUP_DAYS), end)
        for sid in lib:
            t0 = time.monotonic()
            log(f"Running {sid}")
            try:
                rep = run_strategy(sid, start, end, a.capital, panel=panel)
            except Exception as exc:  # noqa: BLE001
                log(f"  {sid} failed: {exc}")
                rows.append({"strategy": sid, "error": str(exc)[:200]})
                continue
            (out / f"{sid}.json").write_text(json.dumps(rep, indent=1, default=str))
            rows.append(_row(sid, rep["mode_label"], rep["metrics"], time.monotonic() - t0,
                             rep["diagnostics"]["audit"]["verdict"]))
            log(f"  {_fmt(rows[-1])}")
        del panel
    if "PMS_SETUP1" in wanted:
        if not (root / "financials.parquet").exists():
            log("PMS_SETUP1 skipped: no financials. Run scripts/fetch_all_data.py without --quick first.")
            rows.append({"strategy": "PMS_SETUP1", "error": "financials not downloaded"})
        else:
            from packages.strategies.pms_lake import LakePMS
            t0 = time.monotonic()
            log("Running PMS_SETUP1 (A + B + C + E). This one takes longer.")
            try:
                pms = LakePMS(root, start, end, a.capital, strict_surveillance=a.strict_surveillance, log=log)
                result = pms.run(progress=lambda f, m: log(f"  {100 * f:.0f}% {m}"))
                rep = pms.report(result)
                (out / "PMS_SETUP1.json").write_text(json.dumps(rep, indent=1, default=str))
                rows.append(_row("PMS_SETUP1", rep["mode_label"], rep["metrics"], time.monotonic() - t0, "Primary PMS"))
                log(f"  {_fmt(rows[-1])}")
            except Exception as exc:  # noqa: BLE001
                log(f"  PMS_SETUP1 failed: {type(exc).__name__}: {exc}")
                rows.append({"strategy": "PMS_SETUP1", "error": f"{type(exc).__name__}: {exc}"[:200]})
    _write_summary(out, rows, start, end, a.capital)
    log(f"Reports written to {out}")
    return 0 if all("error" not in r for r in rows) else 1


def _row(sid, label, m, seconds, verdict):
    pick = lambda *ks: next((m[k] for k in ks if k in m and m[k] is not None), None)  # noqa: E731
    return {"strategy": sid, "name": label, "cagr_pct": pick("cagr_pct"),
            "max_drawdown_pct": pick("max_drawdown_pct"), "sharpe": pick("sharpe", "sharpe_ratio"),
            "closed_trades": pick("closed_trades", "closed_trade_count", "trades"),
            "final_equity": pick("final_equity", "ending_equity"), "verdict": verdict, "seconds": round(seconds)}


def _fmt(r):
    f = lambda v, d=2: "n/a" if v is None else f"{v:.{d}f}"  # noqa: E731
    return (f"{r['strategy']}: CAGR {f(r['cagr_pct'])}%  max DD {f(r['max_drawdown_pct'])}%  "
            f"Sharpe {f(r['sharpe'])}  trades {r['closed_trades']}  ({r['seconds']}s)")


def _write_summary(out: Path, rows, start, end, capital):
    import csv
    keys = ["strategy", "name", "cagr_pct", "max_drawdown_pct", "sharpe", "closed_trades", "final_equity",
            "verdict", "seconds", "error"]
    with open(out / "summary.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in keys})
    lines = [f"# DEEX strategy backtests {start} to {end} (Rs {capital:,.0f})", "",
             "| Strategy | CAGR % | Max DD % | Sharpe | Trades | Verdict |", "|---|---|---|---|---|---|"]
    for r in rows:
        if "error" in r:
            lines.append(f"| {r['strategy']} | error: {r['error']} | | | | |")
            continue
        f = lambda v: "" if v is None else f"{v:.2f}"  # noqa: E731
        lines.append(f"| {r['name']} | {f(r['cagr_pct'])} | {f(r['max_drawdown_pct'])} | {f(r['sharpe'])} | "
                     f"{r['closed_trades']} | {r['verdict']} |")
    lines += ["", "Historical simulations on public NSE data. Not certified, not investment advice."]
    (out / "summary.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
