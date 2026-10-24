"""Run one strategy-library backtest from the command line.

    python scripts/run_strategy.py S1 2022-01-03 2024-12-31 --capital 1000000 --out report.json

Set DEEX_BHAVCOPY_DIR to a local copy of the bhavcopy archive (folders legacy/ and udiff/) to avoid
downloading. The PMS Setup 1 strategy is run through the web app or scripts/run_backtest.py instead.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.strategies.catalog import STRATEGY_IDS  # noqa: E402
from packages.strategies.runner import run_strategy  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("strategy", choices=STRATEGY_IDS)
    ap.add_argument("start", type=date.fromisoformat)
    ap.add_argument("end", type=date.fromisoformat)
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    report = run_strategy(a.strategy, a.start, a.end, a.capital,
                          progress=lambda p, m: print(f"[{int(p):3d}%] {m}", file=sys.stderr))
    m = report["metrics"]
    print(f"{report['mode_label']}: CAGR {m['cagr_pct']:.2f}%  max drawdown {m['max_drawdown_pct']:.2f}%  "
          f"closed trades {m['closed_trades']}  costs Rs {m['total_costs']:,.0f}")
    for k, v in report["benchmarks"].items():
        print(f"  {k} (price only): CAGR {v['cagr_pct']:.2f}%")
    print("  Audit verdict:", report["diagnostics"]["audit"]["verdict"])
    if a.out:
        a.out.write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
