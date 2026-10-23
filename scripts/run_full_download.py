"""Resume the entire dated acquisition queue, with bounded batches and honest gaps.

Run on a persistent local disk. No credentials or raw responses appear in logs.
Stopping the process leaves committed tasks intact; rerunning resumes PENDING work.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from datetime import date
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.data_layer.local_lake import Lake, ROOT, stamp
from packages.data_layer.finstack_collection import FinstackHTTP

PHASES = ('prices', 'indices', 'actions', 'earnings', 'financial_discovery', 'shareholding_discovery')
BLOCKS = {'source_http_401', 'source_http_403', 'source_http_429',
          'finstack_transport_unavailable', 'source_transport_unavailable'}


@contextmanager
def collector_lock(root):
    path = Path(root) / 'collector.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as f:
        try:
            if os.name == 'nt':
                import msvcrt
                if path.stat().st_size == 0:
                    f.write(b'0'); f.flush()
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('another_collector_is_running') from None
        try:
            yield
        finally:
            if os.name == 'nt':
                f.seek(0); msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8') as f:
        json.dump(value, f, indent=2)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    temp.replace(path)


def progress(lake, state, attempted, started, scope=None):
    return dict(state=state, backtest_ready=False, updated_at=stamp(),
        tasks_attempted_this_run=attempted, elapsed_seconds=round(time.monotonic()-started), acquisition_scope=scope,
        jobs=[dict(r) for r in lake.db.execute('select dataset,status,count(*) tasks,sum(rows) rows '
                                             'from jobs group by dataset,status order by dataset,status')],
        documents=[dict(r) for r in lake.db.execute('select dataset,status,count(*) tasks from documents '
                                                  'group by dataset,status')],
        known_candidates=lake.db.execute('select count(*) from company_targets').fetchone()[0],
        errors=[dict(r) for r in lake.db.execute('select dataset,error,count(*) tasks from jobs '
                "where status in ('FAILED','MISSING','SPLIT') group by dataset,error order by tasks desc limit 25")],
        note='Attempted/downloaded is not complete coverage. Missing dates, reference layers, original '
             'filing availability and identity/point-in-time audits remain explicit requirements.')


def run(lake, *, phases=PHASES, batch=10, max_tasks=0, max_seconds=0, documents=True,
        factory=None, emit=print, acquire_from=None, acquire_to=None):
    started, attempted, consecutive_blocks = time.monotonic(), 0, 0
    factory = factory or (lambda: FinstackHTTP(lake.root))
    state = 'RUNNING'
    last_report = 0

    def save():
        report = progress(lake, state, attempted, started,
                          {'from': str(acquire_from), 'to': str(acquire_to)} if acquire_from else None)
        atomic_json(lake.root / 'download_progress.json', report)
        return report

    save()
    try:
        while True:
            worked = False
            for phase in (*phases, *(('documents',) if documents else ())):
                for _ in range(batch):
                    if ((max_tasks and attempted >= max_tasks) or
                            (max_seconds and time.monotonic()-started >= max_seconds)):
                        state = 'CHECKPOINTED_LIMIT'
                        return save()
                    rows = (lake.documents(limit=1, http_factory=factory) if phase == 'documents'
                            else lake.fetch(datasets=(phase,), limit=1, http_factory=factory,
                                            start=acquire_from, end=acquire_to))
                    if not rows:
                        break
                    worked = True
                    attempted += 1
                    r = rows[0]
                    emit(json.dumps({'task': attempted, **r}), flush=True)
                    consecutive_blocks = consecutive_blocks+1 if r.get('error') in BLOCKS else 0
                    if consecutive_blocks >= 5:
                        state = 'SOURCE_BLOCKED'
                        return save()
                    if time.monotonic()-last_report >= 30:
                        save(); last_report = time.monotonic()
            if not worked:
                # No remaining queued work does not imply the data is complete.
                state = 'ACQUISITION_PASS_FINISHED_AUDIT_REQUIRED'
                return save()
    except KeyboardInterrupt:
        state = 'CHECKPOINTED_INTERRUPTED'
        save()
        raise
    except Exception:
        state = 'CHECKPOINTED_ERROR'
        save()
        raise


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', default=os.environ.get('DEEX_DATA_ROOT', 'local_data'))
    p.add_argument('--start', type=date.fromisoformat, default=date(2016, 10, 24))
    p.add_argument('--end', type=date.fromisoformat, default=date(2026, 10, 23))
    p.add_argument('--phases', default=','.join(PHASES))
    p.add_argument('--batch', type=int, default=10)
    p.add_argument('--max-tasks', type=int, default=0, help='0: drain the whole pending queue')
    p.add_argument('--max-seconds', type=int, default=0, help='0: no run time limit')
    p.add_argument('--skip-documents', action='store_true')
    p.add_argument('--acquire-year', type=int, help='Limit acquisition to this calendar year; retain the full plan')
    args = p.parse_args(argv)
    phases = tuple(args.phases.split(','))
    if (not phases or not set(phases) <= set(PHASES) or not 1 <= args.batch <= 100
            or args.max_tasks < 0 or args.max_seconds < 0):
        p.error('Invalid phases or bounds')
    try:
        with collector_lock(args.root):
            lake = Lake(args.root)
            try:
                lake.seed_targets(ROOT / 'config/company_universe_seed.csv')
                lake.plan(args.start, args.end)
                report = run(lake, phases=phases, batch=args.batch, max_tasks=args.max_tasks,
                             max_seconds=args.max_seconds, documents=not args.skip_documents,
                             acquire_from=date(args.acquire_year,1,1) if args.acquire_year else None,
                             acquire_to=date(args.acquire_year,12,31) if args.acquire_year else None)
                print(json.dumps(report, indent=2))
                return 2 if report['state'] == 'SOURCE_BLOCKED' else 0
            finally:
                lake.close()
    except KeyboardInterrupt:
        print('Stopped safely; rerun the same command to resume.', file=sys.stderr)
        return 130
    except Exception as exc:
        # Exception type is safe and useful; arbitrary driver/source text is not.
        print('Collection stopped safely: ' + type(exc).__name__ + '. See download_progress.json.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
