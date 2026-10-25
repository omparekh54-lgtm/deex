"""One-command local collection: python fetch_data.py (Python 3.10+)."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def bootstrap():
    if sys.version_info < (3, 10):
        raise SystemExit('Python 3.10 or newer is required; Python 3.12 is recommended.')
    required = ('bs4', 'httpx', 'tzdata')
    if all(importlib.util.find_spec(name) for name in required):
        return
    env = ROOT / '.venv-data'
    python = env / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
    if Path(sys.prefix).resolve() == env.resolve():
        raise SystemExit('Dependencies are missing. Run: python -m pip install -r requirements-data.txt')
    if not python.exists():
        print('Creating an isolated local data environment...', flush=True)
        subprocess.run([sys.executable, '-m', 'venv', str(env)], check=True)
    probe = subprocess.run([str(python), '-c', 'import bs4, httpx, tzdata'], capture_output=True)
    if probe.returncode:
        subprocess.run([str(python), '-m', 'pip', 'install', '-r', str(ROOT/'requirements-data.txt')], check=True)
    raise SystemExit(subprocess.call([str(python), str(Path(__file__).resolve()), *sys.argv[1:]], cwd=ROOT))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT/'local_data')
    parser.add_argument('--start', type=date.fromisoformat, help='Study start; default ten years before end')
    parser.add_argument('--end', type=date.fromisoformat, help='Study end; default yesterday in India')
    parser.add_argument('--retry', action='store_true', help='Retry previous failed/missing/partial jobs once this run')
    parser.add_argument('--plan-only', action='store_true', help='Plan and report without network access')
    parser.add_argument('--interval', type=float, default=1.5, help='Minimum seconds per request; minimum 1.0')
    parser.add_argument('--reuse-cache', type=Path, action='append', default=[], help='Existing NSE XML cache directory')
    args = parser.parse_args(argv)
    from zoneinfo import ZoneInfo
    from packages.data_layer.free_pipeline import (FreeLake, PoliteHTTP, collector_lock,
                                                   run_collection, write_report)
    from packages.data_layer.local_lake import years_before
    from packages.data_layer.cloud_sources import SourceError
    end = args.end or datetime.now(ZoneInfo('Asia/Kolkata')).date()-timedelta(days=1)
    start = args.start or years_before(end, 10)
    args.root.mkdir(parents=True, exist_ok=True)
    try:
        with collector_lock(args.root):
            lake = FreeLake(args.root)
            reuse = args.reuse_cache+[ROOT/'data/_cache/nse']
            http = PoliteHTTP(args.root, interval=args.interval, reuse_dirs=reuse)
            outcome = 'INTERRUPTED'
            code = 130
            try:
                if args.plan_only:
                    lake.plan_free(start,end)
                    outcome, code = 'PLAN_ONLY', 0
                else:
                    print(f'Collecting free sources for {start} to {end}, plus warm-up history.', flush=True)
                    print('Progress measures processed tasks, including missing/failed tasks. Ctrl+C safely stops.', flush=True)
                    outcome = run_collection(lake,http,start=start,end=end,retry=args.retry)
                    code = 3 if outcome == 'PAUSED_BY_SOURCE' else 0
                report = write_report(lake,http,outcome)
                if code == 0 and not args.plan_only and (report['missing_datasets'] or
                        any(r['status'] in ('FAILED','MISSING','PARTIAL') for r in report['jobs'])):
                    code = 2
            except KeyboardInterrupt:
                print('\nStopped. Re-run the same command to resume.', flush=True)
            except (ValueError, SourceError):
                outcome, code = 'CONFIGURATION_OR_SOURCE_ERROR', 2
                print('Check dates and configuration; no data completeness was recorded.', file=sys.stderr)
            finally:
                write_report(lake,http,outcome)
                http.close(); lake.close()
            print(f'{outcome}. Report: {args.root.resolve()/"coverage_report.json"}', flush=True)
            if outcome == 'PAUSED_BY_SOURCE':
                print('Source requested a pause. Cooldown is saved; wait before rerunning. No block bypass is attempted.')
            return code
    except SourceError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    # --help works even before installation; all other commands bootstrap once.
    if '--help' not in sys.argv and '-h' not in sys.argv:
        bootstrap()
    raise SystemExit(main())
