"""Strict official rendered statements; period columns and units remain explicit."""
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from packages.data_layer.xbrl import XbrlFact
from packages.data_layer.cloud_sources import SourceError
from dataclasses import replace


def norm(value):
    return re.sub('[^a-z0-9]', '', value.lower())


LABELS = {
    'revenuefromoperations': 'RevenueFromOperations',
    'otherincome': 'OtherIncome',
    'totalprofitbeforetax': 'ProfitLossBeforeTax',
    'exceptionalitems': 'ExceptionalItems',
    'totalprofitlossforperiod': 'ProfitLossForPeriod',
    'financecosts': 'FinanceCosts',
    'dilutedearningslosspersharefromcontinuinganddiscontinuedoperations': 'DilutedEarningsLossPerShare',
}


def financial_document(http, record, *, expected_isin=None):
    try:
        return record,http.fetch(record.xbrl_uri)
    except SourceError as exc:
        if str(exc)!='source_http_404':raise
    uri=record.raw_payload.get('ixbrl')
    parsed=urlsplit(uri or '')
    seq=str(record.raw_payload.get('seq_Id') or '')
    if (record.exchange!='NSE' or not seq.isdigit() or parsed.scheme!='https'
            or parsed.hostname!='nsearchives.nseindia.com' or parsed.query or parsed.fragment
            or not re.fullmatch(r'/corporate/ixbrl/INTEGRATED_FILING_INDAS_'+re.escape(seq)+r'_[0-9]+_iXBRL_WEB\.html',parsed.path)):
        raise SourceError('financial_render_pair_unavailable')
    doc=http.fetch(uri,financial_render=True)
    if not expected_isin:
        raise SourceError('financial_render_expected_identity_required')
    source=dict(format='NSE_RENDERED_TABLES',source_uri=doc.url,sha256=doc.sha256,
        archive_key=doc.archive_key,original_xbrl_uri=record.xbrl_uri,fallback_reason='source_http_404',expected_isin=expected_isin)
    return replace(record,raw_payload={**record.raw_payload,'financial_source':source}),doc


def parse_financial_render(body, record):
    source = (record.raw_payload or {}).get('financial_source') or {}
    uri = urlsplit(source.get('source_uri',''))
    if (record.exchange!='NSE' or uri.hostname!='nsearchives.nseindia.com'
            or not re.fullmatch(r'/corporate/ixbrl/INTEGRATED_FILING_INDAS_[0-9]+_[0-9]+_iXBRL_WEB\.html',uri.path)
            or b'<!ENTITY' in body.upper()):
        raise ValueError('financial_render_source_invalid')
    soup=BeautifulSoup(body,'lxml')
    tables=[t for t in soup.find_all('table') if t.find_parent('table') is None]
    if len(tables)<2:raise ValueError('financial_render_tables_missing')
    def rows(table):
        return [[c.get_text(' ',strip=True) for c in tr.find_all(['th','td'],recursive=False)]
                for tr in table.find_all('tr') if tr.find_parent('table') is table]
    pairs=rows(tables[0]); header={}
    for cells in pairs:
        if len(cells)==2:
            key=norm(cells[0])
            if key in header:raise ValueError('financial_render_duplicate_header')
            header[key]=cells[1]
    if (header.get('nsesymbol')!=record.symbol
            or norm(header.get('nameofcompany',''))!=norm(record.company_name or '')
            or norm(header.get('natureofreportstandaloneorconsolidated',''))!=record.statement_scope
            or header.get('descriptionofpresentationcurrency')!='INR'
            or header.get('isin')!=source.get('expected_isin')
            or not re.fullmatch(r'IN[A-Z0-9]{10}',header.get('isin',''))):
        raise ValueError('financial_render_identity_or_currency_mismatch')
    scale={'actuals':Decimal(1),'lakhs':Decimal(100000),'crores':Decimal(10000000)}.get(
        norm(header.get('levelofroundingusedinfinancialresults','')))
    if scale is None:raise ValueError('financial_render_unknown_rounding')
    data=rows(tables[1]); fields={}
    for cells in data:
        if len(cells)==4:
            key=norm(cells[1])
            if key in fields:raise ValueError('financial_render_duplicate_label')
            fields[key]=cells[2:]
    starts=fields.get('dateofstartofreportingperiod',[])
    ends=fields.get('dateofendofreportingperiod',[])
    scopes=fields.get('natureofreportstandaloneorconsolidated',[])
    if len(starts)!=2 or len(ends)!=2 or len(scopes)!=2:
        raise ValueError('financial_render_period_headers_missing')
    facts=[]
    for column in range(2):
        start=datetime.strptime(starts[column],'%d-%m-%Y').date()
        end=datetime.strptime(ends[column],'%d-%m-%Y').date()
        if end!=record.period_end or start>end or norm(scopes[column])!=record.statement_scope:
            raise ValueError('financial_render_period_or_scope_mismatch')
        for label,name in LABELS.items():
            value=fields.get(label,['',''])[column].strip()
            if value in {'','-','--','NA','N/A'}:continue
            value=value.replace(',','')
            if value.startswith('(') and value.endswith(')'):value='-'+value[1:-1]
            try:value=Decimal(value)
            except InvalidOperation:raise ValueError('financial_render_number_invalid') from None
            if not value.is_finite():raise ValueError('financial_render_number_invalid')
            eps=name=='DilutedEarningsLossPerShare'
            facts.append(XbrlFact(name,value if eps else value*scale,
                'INR/shares' if eps else 'INR',f'NSE_RENDERED_{column}_{start}_{end}',
                start,end,None,entity_identifier=header['isin']))
    return facts
