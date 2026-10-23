"""Export an authorised Design Experience DB for DEEX's existing local runtime.

Credentials are read only from environment and never put in process arguments.
Requires PostgreSQL 17 client tools. Writes a private dump, never a Git commit.
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, parse_qs, unquote


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, default=Path('snapshot/pms.dump'))
    p.add_argument('--pg-bin', type=Path)
    args = p.parse_args()
    raw = os.environ.get('SNAPSHOT_SOURCE_URL', '')
    url = urlsplit(raw)
    if url.scheme not in ('postgres', 'postgresql') or not url.hostname or not url.username:
        p.error('Set SNAPSHOT_SOURCE_URL in this terminal; do not put it in the command.')
    query = parse_qs(url.query)
    env = dict(os.environ)
    env.update(PGHOST=url.hostname, PGPORT=str(url.port or 5432),
               PGUSER=unquote(url.username), PGPASSWORD=unquote(url.password or ''),
               PGDATABASE=unquote(url.path.lstrip('/')), PGCONNECT_TIMEOUT='20',
               PGSSLMODE=query.get('sslmode', ['require'])[0],
               PGOPTIONS='-c default_transaction_read_only=on -c timezone=UTC')
    if env['PGSSLMODE'] not in ('require', 'verify-ca', 'verify-full'):
        p.error('Remote snapshot source requires TLS.')
    env.pop('SNAPSHOT_SOURCE_URL', None)
    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = out.with_suffix('.dump.tmp')
    command = str(args.pg_bin / 'pg_dump') if args.pg_bin else 'pg_dump'
    # Whole DB preserves public dependencies of the pms schema. Treat this dump as
    # private: it may contain other source application data and must not be served.
    try:
        proc = subprocess.run([command, '--format=custom', '--compress=6', '--no-owner',
                               '--no-privileges', '--file', str(tmp)], env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=7200)
        if proc.returncode != 0:
            print('Snapshot export failed. Check network, source permissions and PostgreSQL client version.', file=sys.stderr)
            return 2
        os.chmod(tmp, 0o600)
        tmp.replace(out)
        print('Private snapshot exported. Data completeness still requires the PMS historical audit.')
        return 0
    except (OSError, subprocess.TimeoutExpired):
        print('Snapshot export unavailable or timed out.', file=sys.stderr)
        return 2
    finally:
        if tmp.exists():
            tmp.unlink()


if __name__ == '__main__':
    raise SystemExit(main())
