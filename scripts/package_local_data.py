"""Make a verified, portable local acquisition checkpoint (never credentials/cache)."""
import argparse
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import zipfile
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packages.data_layer.local_lake import Lake


def package(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    if root == output or root in output.parents:
        raise ValueError('Output must be outside the data directory')
    output.parent.mkdir(parents=True, exist_ok=True)
    lake = Lake(root)
    try:
        check = lake.verify()
        if not check['ok']:
            raise ValueError('Cannot package corrupt source evidence')
        with tempfile.TemporaryDirectory(prefix='deex-package-') as temp:
            target = sqlite3.connect(Path(temp) / 'catalog.sqlite')
            try:
                lake.db.backup(target)
            finally:
                target.close()
            with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=3, allowZip64=True) as z:
                z.write(Path(temp) / 'catalog.sqlite', 'deex_data/catalog.sqlite')
                for r in lake.db.execute('select path from receipts order by sha256'):
                    z.write(root / 'raw' / r['path'], 'deex_data/raw/' + r['path'])
                for name in ('download_progress.json', 'company_targets.csv'):
                    if (root / name).is_file():
                        z.write(root / name, 'deex_data/' + name)
                z.writestr('deex_data/README.txt',
                    'DEEX acquisition checkpoint. Extract to a local drive.\n'
                    'Resume with scripts/run_full_download.py --root PATH_TO_EXTRACTED_deex_data\n'
                    'This archive is not a certification of complete history or backtest readiness.\n')
    finally:
        lake.close()
    return {'path': str(output), 'bytes': output.stat().st_size, 'integrity': check}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', required=True)
    p.add_argument('--out', required=True)
    a = p.parse_args()
    print(json.dumps(package(a.root, a.out)))
