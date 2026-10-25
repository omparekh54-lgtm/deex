"""Import the owner's CSV/ZIP observations without inventing source provenance.

Uploaded price adjustment basis is unknown. Keep those values separate from raw
exchange bars; supplement turnover/identities separately rather than overwrite.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
from pathlib import Path, PurePosixPath
import zipfile

from .cloud_sources import Document, MAX_BYTES, SourceError
from .local_lake import dumps, stamp


SCHEMA = '''
create table if not exists import_sources(path text primary key, kind text, updated_at text);
create table if not exists imported_files(source text, member text, sha text, kind text,
  rows integer, issues integer, primary key(source,member));
create table if not exists local_prices(trade_date text, symbol text, open text, high text,
  low text, close text, volume integer, receipt text references receipts(sha256),
  primary key(trade_date,symbol));
create index if not exists local_price_symbol on local_prices(symbol,trade_date);
create table if not exists local_financials(symbol text, period_end text, filed_ts text,
  scope text, payload text, receipt text references receipts(sha256),
  primary key(symbol,period_end,filed_ts,scope));
create table if not exists local_reference(symbol text primary key,payload text,
  receipt text references receipts(sha256));
create table if not exists local_xml_cache(name text, source text, member text,
  primary key(name,source,member));
create table if not exists import_issues(source text,member text,row_number integer,error text,
  primary key(source,member,row_number));
create table if not exists price_metadata(trade_date text,symbol text,isin text,series text,
  turnover text,volume integer,receipt text references receipts(sha256),
  primary key(trade_date,symbol,isin,series));
'''


class DatasetSource:
    def __init__(self, path):
        self.path = Path(path).expanduser().resolve()
        self.zip = None
        if self.path.is_file() and zipfile.is_zipfile(self.path):
            self.zip = zipfile.ZipFile(self.path)
            self.members = {}
            for info in self.zip.infolist():
                parts = PurePosixPath(info.filename).parts
                if info.is_dir():
                    continue
                if '..' in parts or info.filename.startswith('/') or '\\' in info.filename:
                    raise ValueError('Unsafe ZIP member name')
                if info.filename in self.members:
                    raise ValueError('Duplicate ZIP member name')
                self.members[info.filename] = info
        elif self.path.is_dir():
            self.members = {p.relative_to(self.path).as_posix(): p for p in self.path.rglob('*') if p.is_file()}
        else:
            raise ValueError('Existing dataset must be a ZIP or extracted directory')

    def close(self):
        if self.zip:
            self.zip.close()

    def read(self, name):
        item = self.members[name]
        size = item.file_size if self.zip else item.stat().st_size
        if not 0 < size <= MAX_BYTES:
            raise ValueError('Dataset member exceeds size limit')
        return self.zip.read(item) if self.zip else item.read_bytes()

    def classify(self, name):
        parts = PurePosixPath(name).parts
        if len(parts) >= 2 and parts[-2] in ('prices','indices') and name.lower().endswith('.csv'):
            return parts[-2]
        if len(parts) >= 2 and parts[-2] == 'fundamentals':
            return {'quarterly.csv':'financials','reference.csv':'reference'}.get(parts[-1])
        if len(parts) >= 3 and parts[-2] == 'nse' and parts[-3] == '_cache' and name.lower().endswith('.xml'):
            return 'xml'
        return None


def find_sources(lake, project, explicit=(), *, fresh=False):
    lake.db.executescript(SCHEMA)
    if explicit:
        return [Path(p).expanduser().resolve() for p in explicit]
    remembered = [Path(r[0]) for r in lake.db.execute('select path from import_sources order by path')]
    if remembered:
        available = [p for p in remembered if p.exists()]
        if not available:
            raise ValueError('Remembered dataset is missing; restore it or pass --existing with its new path')
        return available
    if fresh:
        return []
    project = Path(project)
    extracted = project/'data'
    if (extracted/'prices').is_dir():
        return [extracted]
    candidates = sorted(project.glob('data*.zip'))
    if len(candidates) == 1:
        return candidates
    if len(candidates) > 1:
        raise ValueError('Several data ZIPs found; choose one with --existing "path"')
    raise ValueError('Existing data not found. Put data(2).zip in the project, use --existing "path", or --fresh for a new download')


def _numeric(value, *, integer=False):
    n = Decimal(value)
    if not n.is_finite() or n < 0 or n > Decimal('1e24') or (integer and n != n.to_integral_value()):
        raise ValueError('Invalid number')
    return int(n) if integer else n


def import_source(lake, path, progress_fn=lambda *a: None):
    lake.db.executescript(SCHEMA)
    source = DatasetSource(path)
    key = str(source.path)
    try:
        members = [(n,source.classify(n)) for n in source.members if source.classify(n)]
        if not any(kind == 'prices' for _,kind in members):
            raise ValueError('Dataset has no prices/*.csv files')
        csvs = [(n,k) for n,k in members if k != 'xml']
        with lake.db:
            lake.db.execute('insert or replace into import_sources values(?,?,?)',
                            (key,'zip' if source.zip else 'directory',stamp()))
            lake.db.executemany('insert or ignore into local_xml_cache values(?,?,?)',
                               [(PurePosixPath(n).name,key,n) for n,k in members if k=='xml'])
        for i,(name,kind) in enumerate(csvs,1):
            body = source.read(name)
            sha = hashlib.sha256(body).hexdigest()
            old = lake.db.execute('select sha from imported_files where source=? and member=?',(key,name)).fetchone()
            if old:
                if old[0] != sha:
                    # Never keep stale rows removed from an amended file. A new
                    # import root makes the changed dataset/version explicit.
                    raise ValueError('Previously imported file changed: '+name+'; use a new --root to import the revised dataset')
                progress_fn('Import existing CSVs',i,len(csvs),'REUSED '+PurePosixPath(name).name)
                continue
            reader = csv.DictReader(io.StringIO(body.decode('utf-8-sig')))
            fields = set(reader.fieldnames or [])
            required = {'date','open','high','low','close','volume'} if kind in ('prices','indices') else (
                {'symbol','period_end','filed_ts','sales','op_profit','pat','eps_diluted','consolidated'}
                if kind=='financials' else {'symbol','industry','is_financial','is_cyclical'})
            if not required <= fields:
                raise ValueError('Unsupported CSV schema: '+name)
            rows = issues = 0
            symbol = PurePosixPath(name).stem.upper()
            with lake.db:
                digest = lake.archive(Document('local-import://'+sha,body,sha,datetime.now(timezone.utc),len(body)))
                batch = []
                for lineno,r in enumerate(reader,2):
                    try:
                        if kind in ('prices','indices'):
                            day = date.fromisoformat(r['date']).isoformat()
                            o,h,l,c = [_numeric(r[k]) for k in ('open','high','low','close')]
                            v = _numeric(r['volume'],integer=True)
                            if min(o,h,l,c)<=0 or l>min(o,c) or h<max(o,c) or h<l:
                                raise ValueError('invalid_ohlc')
                            if kind=='prices':
                                batch.append((day,symbol,*(str(x) for x in (o,h,l,c)),v,digest))
                                if len(batch)>=5000:
                                    lake.db.executemany('insert or ignore into local_prices values(?,?,?,?,?,?,?,?)',batch);batch=[]
                            else:
                                code = {'INDIAVIX':'INDIAVIX','MIDCAP100':'INDEX:NIFTY MIDCAP 100',
                                        'NIFTY50':'NIFTY50','NIFTY500':'NIFTY500',
                                        'NIFTYMIDSMALLCAP400':'NIFTYMIDSMALLCAP400'}.get(symbol,'IMPORTED:'+symbol)
                                lake.db.execute('insert or ignore into indices values(?,?,?,?,?,?,?)',
                                                (day,code,*(str(x) for x in (o,h,l,c)),digest))
                        elif kind=='financials':
                            date.fromisoformat(r['period_end'])
                            datetime.fromisoformat(r['filed_ts'])
                            if not r['symbol']:
                                raise ValueError('missing_symbol')
                            lake.db.execute('insert or ignore into local_financials values(?,?,?,?,?,?)',
                                            (r['symbol'],r['period_end'],r['filed_ts'],r['consolidated'],dumps(r),digest))
                        else:
                            if not r['symbol']:
                                raise ValueError('missing_symbol')
                            lake.db.execute('insert or ignore into local_reference values(?,?,?)',
                                            (r['symbol'],dumps(r),digest))
                        rows += 1
                    except (ValueError,InvalidOperation,TypeError,KeyError):
                        issues += 1
                        lake.db.execute('insert or ignore into import_issues values(?,?,?,?)',
                                        (key,name,lineno,'invalid_or_incomplete_row'))
                if batch:
                    lake.db.executemany('insert or ignore into local_prices values(?,?,?,?,?,?,?,?)',batch)
                lake.db.execute('insert into imported_files values(?,?,?,?,?,?)',(key,name,sha,kind,rows,issues))
            progress_fn('Import existing CSVs',i,len(csvs),f'{PurePosixPath(name).name}: {rows} rows, {issues} issues')
        return {'source':key,'csv_files':len(csvs),'xml_cache_entries':sum(k=='xml' for _,k in members)}
    finally:
        source.close()


def read_cached_xml(lake, name, archives=None):
    matches = lake.db.execute('select source,member from local_xml_cache where name=?',(name,)).fetchall()
    if not matches:
        return None
    bodies = []
    archives = archives if archives is not None else {}
    for path,member in matches:
        if Path(path).is_file():
            if path not in archives:
                archives[path] = zipfile.ZipFile(path)
            archive = archives[path]
            item = archive.getinfo(member)
            if not 0 < item.file_size <= MAX_BYTES:
                continue
            body = archive.read(item)
        else:
            file = Path(path)/member
            if not file.is_file() or not 0 < file.stat().st_size <= MAX_BYTES:
                continue
            body = file.read_bytes()
        bodies.append(body)
    # Ambiguous files must be reviewed, not chosen arbitrarily.
    if not bodies or len({hashlib.sha256(b).hexdigest() for b in bodies}) != 1:
        return None
    return bodies[0]


def apply_missing_plan(lake):
    """Local rows satisfy OHLCV only. Bulk metadata is a separate missing field."""
    lake.db.executescript(SCHEMA)
    with lake.db:
        # Changing a plan must also cover previously failed or downloaded jobs;
        # completed official jobs are retained and never downgraded.
        candidates = lake.db.execute("select start,end,status from jobs where dataset='prices' and status not in ('DONE','REUSED_LOCAL') and exists(select 1 from local_prices p where p.trade_date=jobs.start)").fetchall()
        for start,end,status in candidates:
            lake.db.execute("update jobs set status='REUSED_LOCAL',error=NULL,updated_at=? where dataset='prices' and start=? and end=?",
                            (stamp(),start,end))
            lake.db.execute('insert or ignore into jobs(dataset,start,end) values(?,?,?)',('price_supplements',start,end))
        # Full downloaded jobs already contain turnover and identity; no need to
        # queue a supplement for those dates merely because local data was added.
        for r in lake.db.execute("select start,end from jobs where dataset='indices' and status!='DONE'").fetchall():
            codes = {x[0] for x in lake.db.execute('select code from indices where trade_date=?',(r['start'],))}
            # The runner also collects sector rows for C; two uploaded indices
            # cannot make that daily index discovery redundant.
            need = {'NIFTY50','NIFTY500','NIFTYMIDSMALLCAP400','INDIAVIX',
                    'INDEX:NIFTY BANK','INDEX:NIFTY IT','INDEX:NIFTY PHARMA',
                    'INDEX:NIFTY AUTO','INDEX:NIFTY FMCG','INDEX:NIFTY METAL',
                    'INDEX:NIFTY REALTY','INDEX:NIFTY ENERGY'}
            if need <= codes:
                lake.db.execute("update jobs set status='REUSED_LOCAL',error=NULL where dataset='indices' and start=? and end=?",tuple(r))
    summary = import_summary(lake)
    summary['pending_by_dataset'] = [dict(r) for r in lake.db.execute("select dataset,count(*) tasks from jobs where status='PENDING' group by dataset")]
    summary['price_job_reasons'] = {
        'REUSED_LOCAL':'Existing valid OHLCV is retained without a price-history download.',
        'price_supplements':'Fetch bulk exchange file for missing actual turnover/ISIN/series and absent/invalid symbol-date rows; overlapping uploaded OHLCV is not stored again.',
        'prices':'No valid local OHLCV for this date; obtain missing daily raw bars.'}
    from .free_pipeline import atomic_json
    atomic_json(lake.root/'missing_data_plan.json',summary)
    return summary


def import_summary(lake):
    lake.db.executescript(SCHEMA)
    return {
        'sources':[dict(r) for r in lake.db.execute('select * from import_sources')],
        'local_price_rows':lake.db.execute('select count(*) from local_prices').fetchone()[0],
        'local_price_symbols':lake.db.execute('select count(distinct symbol) from local_prices').fetchone()[0],
        'local_financial_rows':lake.db.execute('select count(*) from local_financials').fetchone()[0],
        'local_reference_rows':lake.db.execute('select count(*) from local_reference').fetchone()[0],
        'xml_cache_entries':lake.db.execute('select count(*) from local_xml_cache').fetchone()[0],
        'invalid_import_rows':lake.db.execute('select count(*) from import_issues').fetchone()[0],
        'reused_download_tasks':lake.db.execute("select count(*) from jobs where status='REUSED_LOCAL'").fetchone()[0],
        'price_basis':'UPLOADED_UNVERIFIED; kept separate from raw exchange bars',
        'financial_status':'UPLOADED_UNVERIFIED; original units, timestamp timezone and completeness require audit'}
