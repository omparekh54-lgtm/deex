"""Offline, bounded collection diagnostics. Never export response bodies/secrets."""
from collections import Counter
from pathlib import Path
import re
import traceback
from urllib.parse import urlsplit, urlunsplit

from .cloud_sources import SourceError
from .local_lake import stamp, dumps

ERROR_SCHEMA = '''create table if not exists document_errors(
 dataset text,key text,error_code text,error_type text,failed_at text,frames text,
 primary key(dataset,key));'''


def document_error(exc):
    if isinstance(exc,SourceError) and re.fullmatch('[a-z0-9_]{1,100}',str(exc)):
        code=str(exc)
    else:
        code='document_'+re.sub('[^a-z0-9_]','',type(exc).__name__.lower())
    frames=[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
            for f in traceback.extract_tb(exc.__traceback__)[-4:]]
    return code,type(exc).__name__,frames


def diagnose(lake):
    """Read only catalogue metadata; no source downloads and no work requeued."""
    import json
    lake.db.executescript(ERROR_SCHEMA)
    groups=[dict(r) for r in lake.db.execute(
        'select dataset,status,error,count(*) jobs from documents group by dataset,status,error')]
    # Prefer known failures, then pending rows if this is the old generic-error format.
    rows=lake.db.execute("select d.*,e.error_type,e.frames from documents d left join document_errors e on e.dataset=d.dataset and e.key=d.key where d.status='FAILED' order by d.key limit 10").fetchall()
    samples=[]
    fields=('symbol','sm_symbol','quarterEndDate','periodEnded','period_end','qe_Date',
            'broadcastDateTime','broadcast_date_time','broadcast_Date','creation_Date','revised_Date',
            'filingDateTime','exchdisstime','sort_date','period','periodType','from_date','to_date',
            'consolidated','consolidatedStandalone','statementType','scope','id','seqId')
    for r in rows:
        try:
            payload=json.loads(r['payload'])
        except ValueError:
            payload={}
        url=urlsplit(r['url'])
        samples.append(dict(dataset=r['dataset'],key=r['key'],error=r['error'],
                            error_type=r['error_type'],frames=json.loads(r['frames']) if r['frames'] else [],
                            source_url=urlunsplit((url.scheme,url.netloc,url.path,'','')),
                            metadata_fields=sorted(payload) if isinstance(payload,dict) else [],
                            filing_metadata={k:payload[k] for k in fields if isinstance(payload,dict) and k in payload}))
    return dict(generated_at=stamp(),document_groups=groups,failed_samples=samples,
                document_count=lake.db.execute('select count(*) from documents').fetchone()[0],
                distinct_source_urls=lake.db.execute('select count(distinct url) from documents').fetchone()[0],
                note='Generic historical failures cannot be diagnosed exactly until sampled or retried with detailed logging. No documents were fetched or retried by this command.')
