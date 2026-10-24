"""Download everything the eleven DEEX strategies need, in one resumable command.

    python scripts/fetch_all_data.py                      # full 10-year run (all 11 strategies)
    python scripts/fetch_all_data.py --quick              # S1-S10 only (prices, ETFs, indices): ~5-10 min
    python scripts/fetch_all_data.py --root D:\\DEEX_Data  # choose the data folder

Safe to stop with Ctrl+C and run again: finished files are skipped. If NSE starts refusing
requests the run slows itself down, and stops cleanly if refusals continue; wait 15-30
minutes and run the same command again.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from packages.datapipe import events, financials, prices, reference  # noqa: E402
from packages.datapipe.net import HostBlocked, Net  # noqa: E402
from packages.datapipe.store import Store, Window  # noqa: E402

STEPS = ["prices", "indices", "reference", "surveillance", "events", "financials"]
QUICK = ["prices", "indices"]


def log(msg: str):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def main(argv=None) -> int:
    today = date.today()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="deex_data", help="data folder (default: ./deex_data)")
    ap.add_argument("--start", type=date.fromisoformat, default=today.replace(year=today.year - 10),
                    help="first backtest day (default: 10 years ago)")
    ap.add_argument("--end", type=date.fromisoformat, default=today - timedelta(days=1),
                    help="last backtest day (default: yesterday)")
    ap.add_argument("--quick", action="store_true", help="only what S1-S10 need (prices, ETFs, indices)")
    ap.add_argument("--only", help="comma-separated steps: " + ",".join(STEPS))
    ap.add_argument("--skip", default="", help="comma-separated steps to skip")
    ap.add_argument("--pace", type=float, default=1.0,
                    help="request pace multiplier 0.25-2 (lower = gentler on NSE; default 1)")
    ap.add_argument("--workers", type=int, default=8, help="parallel downloads for archive files (default 8)")
    a = ap.parse_args(argv)
    steps = QUICK if a.quick else STEPS
    if a.only:
        steps = [s.strip() for s in a.only.split(",") if s.strip()]
    steps = [s for s in steps if s not in {x.strip() for x in a.skip.split(",")}]
    bad = set(steps) - set(STEPS)
    if bad:
        ap.error("unknown step(s): " + ", ".join(sorted(bad)))
    window = Window(a.start, a.end)
    store = Store(a.root)
    store.set_window(window)
    net = Net(scale=a.pace, log=log)
    log(f"DEEX data pipeline -> {store.root}")
    log(f"Backtest window {window.start} -> {window.end}; prices from {window.price_start}; "
        f"financial filings from {window.financial_start}")
    log("Steps: " + ", ".join(steps))
    t0 = time.monotonic()
    timings, problems = {}, []
    for step in steps:
        s0 = time.monotonic()
        try:
            if step == "prices":
                prices.download(store, net, window, workers=a.workers, log=log)
                prices.build_table(store, window, log=log)
            elif step == "indices":
                reference.fetch_indices(store, net, window, workers=a.workers, log=log)
            elif step == "reference":
                reference.fetch_reference(store, net, log=log)
            elif step == "surveillance":
                reference.fetch_surveillance(store, net, window, workers=min(6, a.workers), log=log)
            elif step == "events":
                events.fetch_events(store, net, window, log=log)
            elif step == "financials":
                if not (store.root / "shareholding.parquet").exists():
                    reference.fetch_reference(store, net, log=log)
                financials.run(store, net, window, workers=a.workers, log=log)
        except HostBlocked as exc:
            problems.append(f"{step}: {exc}")
            log(f"STOPPED {step}: {exc}")
            break
        except KeyboardInterrupt:
            log("Interrupted. Run the same command again to resume.")
            return 130
        except Exception as exc:  # noqa: BLE001 - keep going with other steps, report at the end
            problems.append(f"{step}: {type(exc).__name__}: {exc}")
            log(f"ERROR in {step}: {type(exc).__name__}: {exc}")
        timings[step] = round((time.monotonic() - s0) / 60, 1)
        log(f"Finished {step} in {timings[step]} min")
    total = round((time.monotonic() - t0) / 60, 1)
    store.record("run", minutes=total, step_minutes=timings, network=net.stats(), problems=problems)
    log(f"Done in {total} min. Network: {net.stats()}")
    if problems:
        log("Some steps need a rerun (finished work is kept):")
        for p in problems:
            log("  - " + p)
        return 1
    log("Next: python scripts/run_all_strategies.py --root " + str(a.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
