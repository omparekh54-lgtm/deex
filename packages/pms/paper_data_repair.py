"""Targeted, resumable source repairs. Never changes PAPER permissions."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime,timedelta
import hashlib
import re

from sqlalchemy import text
from packages.data_layer.cloud_sources import Document,IST,MAX_BYTES,SourceError
from packages.data_layer.cloud_storage import CloudStore
from packages.data_layer.exchange_financials import ExchangeFilingRecord,FINANCIAL_ACK_DATASET,filings_from_exchange_record
from packages.data_layer.private_archive import BUCKET
from packages.pms.cloud_collection import record_key
from packages.pms.historical_dataset import require_financial_period_identity

PENDING_SQL="""select f.*,i.nse_symbol symbol,p.content_sha256 archived_sha,p.metadata->>'archive_key' archive_key
from pms.financial_filings f join pms.instruments i on i.id=f.instrument_id
left join lateral (select content_sha256,metadata from pms.provider_snapshots
    where source_uri=f.xbrl_uri and metadata->>'archive_key' is not null
    order by observed_at,id limit 1) p on true
where f.xbrl_uri is not null and f.source='NSE'
and coalesce(f.raw_payload->'financial_extraction'->>'version','')<>'FINANCIAL_XBRL_v2'
and not(f.id=any(cast(:attempted as bigint[])))
order by f.id limit :limit"""
COUNTS_SQL="""select
 (select count(*) from pms.financial_filings where xbrl_uri is not null and
 coalesce(raw_payload->'financial_extraction'->>'version','')<>'FINANCIAL_XBRL_v2') financial_pending,
 (select count(*) from pms.instruments i where exchange='NSE' and active and not exists
 (select 1 from pms.sector_history h where h.instrument_id=i.id and h.effective_from<=:effective
 and (h.effective_to is null or h.effective_to>:effective) and btrim(h.sector)<>'')) sector_pending,
 (select count(*) from pms.financial_filings) financial_rows"""


def safe_error(exc):
    return str(exc) if isinstance(exc,SourceError) and re.fullmatch('[a-z0-9_]+',str(exc)) else type(exc).__name__


def stored_record(row):
    if row['source']!='NSE' or not row['symbol'] or not row.get('xbrl_uri'):
        raise SourceError('stored_filing_identity_invalid')
    return ExchangeFilingRecord(exchange='NSE',symbol=row['symbol'],company_name=None,
        period_end=row['period_end'],filed_at=row['filing_timestamp'],
        statement_scope='consolidated' if row['consolidated'] else 'standalone',
        submission_type=row.get('submission_type') or 'Original',
        audit_status=row.get('audit_status'),revised_at=row.get('revised_at'),
        revision_remarks=row.get('revision_remarks'),exchange_received_at=row.get('exchange_received_at'),
        exchange_disseminated_at=row.get('exchange_disseminated_at'),filing_kind=row['filing_kind'],
        external_id=row.get('external_id'),details_uri=row.get('details_uri'),
        xbrl_uri=row['xbrl_uri'],raw_payload=dict(row.get('raw_payload') or {}))


def archived_document(archive,row):
    key,sha=row['archive_key'],row['archived_sha']
    if not key.startswith('raw-sources/') or not re.fullmatch('[0-9a-f]{64}',sha or ''):
        raise SourceError('stored_source_provenance_invalid')
    response=archive.client.get_object(Bucket=BUCKET,Key=key)
    stream=response['Body']
    try:body=stream.read(MAX_BYTES+1)
    finally:stream.close()
    if not body or len(body)>MAX_BYTES or hashlib.sha256(body).hexdigest()!=sha:
        raise SourceError('stored_source_checksum_mismatch')
    return Document(row['xbrl_uri'],body,sha,datetime.now(IST),len(body),key)


def verify_reparse(row,doc):
    if b'<!DOCTYPE' in doc.body.upper() or b'<!ENTITY' in doc.body.upper():
        raise SourceError('xbrl_entities_forbidden')
    record=stored_record(row)
    filings=[replace(f,version=int(row['revision_number'])) for f in filings_from_exchange_record(record,doc.body)]
    primary=filings[0]
    if (primary.instrument_id,primary.period_end,primary.filed_at,primary.filing_kind,primary.statement_scope)!=(
        record.instrument_id,row['period_end'],row['filing_timestamp'],row['filing_kind'],record.statement_scope):
        raise SourceError('reparse_changed_filing_identity')
    for f in filings:
        if (f.raw_payload.get('financial_extraction') or {}).get('version')!='FINANCIAL_XBRL_v2':
            raise SourceError('financial_v2_evidence_missing')
        f.raw_payload['source_reprocessing']=dict(sha256=doc.sha256,archive_key=doc.archive_key,
            processed_at=datetime.now(IST).isoformat(),original_filing_id=str(row['id']))
    return filings


class PaperDataRepair:
    def __init__(self,engine,http,archive):
        self.engine,self.http,self.archive=engine,http,archive
        self.store=CloudStore(engine)

    def locked(self):
        with self.engine.connect() as c:
            state=c.execute(text('select execution_mode,new_orders_enabled from pms.safety_state where singleton=true')).mappings().one()
        if state['execution_mode']!='PAPER' or state['new_orders_enabled']:
            raise SourceError('repair_requires_paper_entries_disabled')

    def counts(self):
        effective=datetime.now(IST).date()+timedelta(days=1)
        with self.engine.connect() as c:
            data=dict(c.execute(text(COUNTS_SQL),{'effective':effective}).mappings().one())
        return {**data,'sector_effective_through':str(effective)}

    def financials(self,*,max_filings=2500,on_progress=None):
        if not 1<=max_filings<=5000:raise ValueError('max_filings must be 1..5000')
        self.locked()
        require_financial_period_identity(self.engine)
        # Resolve only existing identities, retaining validated symbol aliases.
        known=self.store.instruments()
        self.store._known={**known,**{r['canonical_symbol']:r for r in known.values()}}
        attempted,written,errors=[],0,[]
        before=self.counts()
        while len(attempted)<max_filings:
            self.locked()
            with self.engine.connect() as c:
                rows=c.execute(text(PENDING_SQL),{'attempted':attempted,'limit':min(50,max_filings-len(attempted))}).mappings().all()
            if not rows:break
            for row in rows:
                attempted.append(int(row['id']))
                try:
                    doc=archived_document(self.archive,row) if row['archive_key'] else self.http.fetch(row['xbrl_uri'])
                    filings=verify_reparse(row,doc)
                    self.store.financial(filings)
                    with self.engine.begin() as c:
                        self.store.receipt(c,'FINANCIAL_V2_REPROCESSING',[doc],
                            {'original_filing_id':str(row['id']),'complete':True,'filing_rows':len(filings)})
                    self.store.ack(FINANCIAL_ACK_DATASET,doc,record_key(row['raw_payload']))
                    written+=1
                except Exception as exc:
                    errors.append(dict(filing_id=str(row['id']),symbol=row['symbol'],code=safe_error(exc)))
            if on_progress:on_progress(dict(attempted=len(attempted),written=written,errors=len(errors),counts=self.counts()))
        after=self.counts()
        return dict(status='SUCCESS' if after['financial_pending']==0 and not errors else 'PARTIAL',before=before,after=after,
                    attempted=len(attempted),written=written,errors=errors)

    def identities(self, *, on_progress=None):
        from packages.data_layer.dated_identity_repair import reconcile_dated_identity
        self.locked()
        return reconcile_dated_identity(self.store, self.http)

    def sectors(self,*,max_passes=16,on_progress=None):
        if not 1<=max_passes<=24:raise ValueError('max_passes must be 1..24')
        self.locked()
        before=self.counts();passes=[]
        for n in range(max_passes):
            self.locked()
            # A fresh store sees the immediately committed classifications.
            store=CloudStore(self.engine)
            result=store.classifications(self.http,datetime.now(IST).date(),limit=100)
            passes.append(result)
            counts=self.counts()
            if on_progress:on_progress(dict(pass_number=n+1,result=result,counts=counts))
            if (counts['sector_pending']==0 and result['remaining']==0) or result['due']==0:break
        after=self.counts()
        return dict(status='SUCCESS' if after['sector_pending']==0 and passes[-1]['remaining']==0 else 'PARTIAL',before=before,after=after,passes=passes)
