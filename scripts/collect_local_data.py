"""Local-only data CLI; no web endpoint and no broker credentials required."""
from __future__ import annotations
import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.data_layer.local_lake import Lake, REQUIREMENTS


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', default=os.environ.get('DEEX_DATA_ROOT', 'local_data'))
    sub = p.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init', help='Create resumable tasks including warm-up history')
    init.add_argument('--start', type=date.fromisoformat, required=True)
    init.add_argument('--end', type=date.fromisoformat, required=True)
    fetch = sub.add_parser('fetch', help='Fetch a bounded batch; re-run to resume')
    fetch.add_argument('--datasets', default='prices,indices')
    fetch.add_argument('--limit', type=int, default=100)
    fetch.add_argument('--retry', action='store_true', help='Also retry failed/missing jobs')
    fetch.add_argument('--from', dest='start', type=date.fromisoformat)
    fetch.add_argument('--to', dest='end', type=date.fromisoformat)
    fetch.add_argument('--provider', choices=['finstack', 'official'], default='finstack')
    sub.add_parser('status', help='Counts and blockers for every strategy')
    sub.add_parser('requirements', help='Exact dataset contract for the 11 strategies')
    ingest = sub.add_parser('import', help='Stage historical reference/fact JSONL and source evidence')
    ingest.add_argument('path', type=Path)
    docs = sub.add_parser('documents', help='Download/parse queued financial and shareholding XBRL')
    docs.add_argument('--limit', type=int, default=100)
    docs.add_argument('--provider', choices=['finstack', 'official'], default='finstack')
    master = sub.add_parser('universe', help='Build/expand the all-company target register')
    master.add_argument('--refresh-master', action='store_true', help='Fetch current NSE metadata through Finstack')
    master.add_argument('--out', type=Path, help='Export the candidate identities and required datasets as CSV')
    reference = sub.add_parser('finstack-reference', help='Stage Finstack reconstructed reference data for audit')
    reference.add_argument('--dataset', choices=['shareholding', 'membership'], required=True)
    backup = sub.add_parser('backup', help='Consistent SQLite backup plus immutable raw files')
    backup.add_argument('destination', type=Path)
    verify = sub.add_parser('verify', help='Check SQLite integrity and every raw-source SHA256')
    args = p.parse_args(argv)
    lake = Lake(args.root)
    try:
        if args.command == 'init':
            seed = Path(__file__).resolve().parents[1] / 'config/company_universe_seed.csv'
            seeded = lake.seed_targets(seed)
            output = lake.plan(args.start, args.end)
            output['candidate_seed_rows_read'] = seeded
        elif args.command == 'fetch':
            from packages.data_layer.finstack_collection import FinstackHTTP
            output = lake.fetch(datasets=tuple(args.datasets.split(',')), limit=args.limit,
                                retry=args.retry, start=args.start, end=args.end,
                                http_factory=(lambda: FinstackHTTP(lake.root)) if args.provider == 'finstack' else None)
            print(json.dumps(output, indent=2, default=str))
            return 2 if any(r['status'] in ('FAILED', 'MISSING', 'PARTIAL') for r in output) else 0
        elif args.command == 'status':
            output = lake.status()
        elif args.command == 'requirements':
            output = REQUIREMENTS
        elif args.command == 'import':
            output = {'staged_rows': lake.import_evidence(args.path), 'status': 'AUDIT_REQUIRED'}
        elif args.command == 'documents':
            from packages.data_layer.finstack_collection import FinstackHTTP
            output = lake.documents(limit=args.limit,
                                    http_factory=(lambda: FinstackHTTP(lake.root)) if args.provider == 'finstack' else None)
            print(json.dumps(output, indent=2, default=str))
            return 2 if any(r['status'] == 'FAILED' for r in output) else 0
        elif args.command == 'universe':
            if args.refresh_master:
                from packages.data_layer.finstack_collection import FinstackHTTP
                output = lake.refresh_current_master(FinstackHTTP(lake.root))
            else:
                output = lake.target_summary()
            if args.out:
                output['export'] = lake.export_targets(args.out)
        elif args.command == 'finstack-reference':
            from packages.data_layer.finstack_collection import stage_reference
            output = stage_reference(lake, args.dataset)
        elif args.command == 'backup':
            output = {'backup': lake.backup(args.destination)}
        else:
            output = lake.verify()
            print(json.dumps(output, indent=2))
            return 0 if output['ok'] else 2
        print(json.dumps(output, indent=2, default=str))
        return 0
    except (ValueError, KeyError):
        print('Invalid dates, configuration, or evidence schema.', file=sys.stderr)
        return 2
    except Exception:
        print('Source unavailable or configuration incomplete; no completion was recorded.', file=sys.stderr)
        return 2
    finally:
        lake.close()


if __name__ == '__main__':
    raise SystemExit(main())
