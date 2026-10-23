"""Merge extracted year checkpoints into one resumable local acquisition store."""
from datetime import date
import argparse
import hashlib
from pathlib import Path
import re
import shutil
import sqlite3
import sys
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.data_layer.local_lake import Lake


def merge(root, sources):
    target = Lake(root)
    priority = {'PENDING': 0, 'FAILED': 1, 'MISSING': 1, 'SPLIT': 2,
                'PARTIAL': 3, 'STAGED': 3, 'DONE': 4}
    try:
        for source in sources:
            source = Path(source).resolve()
            if source == target.root or not (source / 'catalog.sqlite').is_file():
                raise ValueError('Expected a separate extracted checkpoint directory')
            db = sqlite3.connect('file:' + quote(str(source / 'catalog.sqlite')) + '?mode=ro', uri=True)
            db.row_factory = sqlite3.Row
            try:
                if db.execute('pragma integrity_check').fetchone()[0] != 'ok':
                    raise ValueError('Source database is corrupt')
                # Verify original bytes before importing any source rows.
                for r in db.execute('select * from receipts'):
                    if not re.fullmatch(r'[a-f0-9]{64}\.bin', r['path']):
                        raise ValueError('Invalid raw evidence filename')
                    path = source / 'raw' / r['path']
                    if path.stat().st_size != r['size'] or hashlib.sha256(path.read_bytes()).hexdigest() != r['sha256']:
                        raise ValueError('Source evidence checksum mismatch')
                    dest = target.root / 'raw' / r['path']
                    if not dest.exists():
                        shutil.copyfile(path, dest)
                for p in db.execute('select * from plans'):
                    if not target.db.execute('select 1 from plans where id=?', (p['id'],)).fetchone():
                        target.plan(date.fromisoformat(p['start']), date.fromisoformat(p['end']))
                target.db.execute('attach database ? as incoming', ('file:' + quote(str(source / 'catalog.sqlite')) + '?mode=ro',))
                try:
                    with target.db:
                        for table in ('receipts', 'job_receipts', 'prices', 'indices', 'records', 'company_targets'):
                            # Fixed table identifiers only. Receipt versions remain separate.
                            if table in ('prices', 'indices'):
                                keys = ('trade_date','symbol','isin','series') if table == 'prices' else ('trade_date','code')
                                facts = ('open','high','low','close','volume','turnover') if table == 'prices' else ('open','high','low','close')
                                join = ' and '.join('a.'+k+'=b.'+k for k in keys)
                                diff = ' or '.join('a.'+k+' is not b.'+k for k in facts)
                                if target.db.execute(f'select 1 from main.{table} a join incoming.{table} b on {join} where {diff} limit 1').fetchone():
                                    raise ValueError('Conflicting market observations require review')
                            cols = [r[1] for r in target.db.execute('pragma table_info('+table+')')]
                            names = ','.join(cols)
                            target.db.execute(f'insert or ignore into main.{table}({names}) select {names} from incoming.{table}')
                        for table, keys in (('jobs', ('dataset','start','end')), ('documents', ('dataset','key'))):
                            cols = [r[1] for r in target.db.execute('pragma table_info('+table+')')]
                            for row in db.execute('select * from '+table):
                                if table == 'jobs' and row['status'] == 'PENDING':
                                    continue
                                where = ' and '.join(k+'=?' for k in keys)
                                values = tuple(row[k] for k in keys)
                                old = target.db.execute('select status from '+table+' where '+where, values).fetchone()
                                if old and priority.get(old[0],0) > priority.get(row['status'],0):
                                    continue
                                data = {k: row[k] if k in row.keys() else 0 for k in cols}
                                target.db.execute('insert or replace into '+table+'('+','.join(cols)+') values('+','.join('?' for _ in cols)+')', tuple(data[k] for k in cols))
                finally:
                    target.db.execute('detach database incoming')
            finally:
                db.close()
        with target.db:
            target.refresh_targets()
        return target.verify()
    finally:
        target.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', required=True)
    p.add_argument('--source', nargs='+', required=True)
    args = p.parse_args()
    print(merge(args.root, args.source))
