"""Account for absent exchange bars without inventing prices or trading status."""
from datetime import date, datetime

from packages.data_layer.cloud_sources import ARCHIVE, SourceError, rows_csv, number


# Reviewed original NSE circular bytes. This is a bounded historical correction,
# not a general rule that absence from REG_IND means suspension.
SUSPENSION_DOCS = {
    'CML76849': '17392db2008c37e11798eb17d4e08936fc67d3a44876a1093bdb066809ce141f',
    'CML76916': '79601d2e2d08e1f7b6ee362aa3b1191f1076aa756aa54cb9db4d45c68ea27a73',
}


def reviewed_suspensions(http, day, known):
    if not date(2026, 10, 13) <= day < date(2026, 10, 19):
        return {}, []
    instrument = known.get('CCAVENUE')
    if not instrument or instrument['isin'] != 'INE483S01038':
        raise SourceError('reviewed_suspension_identity_mismatch')
    docs = []
    for name, sha in SUSPENSION_DOCS.items():
        doc = http.fetch(ARCHIVE + '/content/circulars/' + name + '.pdf')
        if doc.sha256 != sha:
            raise SourceError('reviewed_suspension_source_changed')
        docs.append(doc)
    return {'CCAVENUE': {'status': 'S', 'effective_from': '2026-10-13',
            'resume_on': '2026-10-19', 'source': 'NSE/CML/76849+76916'}}, docs


def security_report_url(day):
    return ARCHIVE + '/products/content/sec_bhavdata_full_' + day.strftime('%d%m%Y') + '.csv'


def parse_security_report(body, day):
    rows = rows_csv(body, ['SYMBOL', 'SERIES', 'DATE1', 'OPEN_PRICE',
                          'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'TTL_TRD_QNTY'])
    report = {}
    for r in rows:
        try:
            stamp = datetime.strptime(r['DATE1'], '%d-%b-%Y').date()
        except ValueError:
            raise SourceError('security_report_date_invalid') from None
        if stamp != day:
            raise SourceError('security_report_wrong_session')
        key = (r['SYMBOL'], r['SERIES'])
        if not all(key) or key in report:
            raise SourceError('security_report_identity_ambiguous')
        report[key] = {k: number(r[src]) for k, src in (
            ('open','OPEN_PRICE'), ('high','HIGH_PRICE'), ('low','LOW_PRICE'),
            ('close','CLOSE_PRICE'), ('volume','TTL_TRD_QNTY'))}
    return report


def verify_absences(prices, secondary, missing, suspensions):
    """Require agreement on all reported eligible bars before negative evidence.

    This establishes NO_REPORTED_BAR in two complete dated reports. It does not
    establish a zero price, zero volume, suspension, fill or investable stock.
    """
    if not prices or not secondary:
        raise SourceError('price_reports_empty')
    for r in prices:
        other = secondary.get((r['symbol'], r['series']))
        if other is None or any(other[k] != r[k] for k in other):
            raise SourceError('official_price_reports_disagree')
    reported_symbols = {s for s, series in secondary}
    if any(s in reported_symbols for s in missing | set(suspensions)):
        raise SourceError('price_absence_conflicts_with_report')
    return sorted(missing - reported_symbols - set(suspensions))
