"""Narrow, document-bound reviews of exchange filing identity defects."""
from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path


@lru_cache(maxsize=1)
def reviews():
    return json.loads(Path(__file__).with_name('reviewed_shareholding_identities.json').read_text())


def reviewed_identity(xml, record, expected_isin):
    digest = sha256(xml).hexdigest()
    for item in reviews():
        if (item['xml_sha256'] == digest and item['symbol'] == record.get('symbol')
                and item['current_isin'] == expected_isin
                and item['summary_isin'] == record.get('isin')
                and item['source_url'] == record.get('xbrl')
                and item['period'] == record.get('date')):
            return ({**record, 'symbol': item['xml_symbol'], 'isin': item['xml_isin']},
                    item['xml_isin'], {
                        'reason': item['reason'], 'xml_sha256': digest,
                        'current_symbol': item['symbol'], 'current_isin': expected_isin,
                        'xml_symbol': item['xml_symbol'], 'xml_isin': item['xml_isin'],
                        'summary_period': item['period'], 'xml_period': item.get('xml_period'),
                        'master_sha256': item['master_sha256'],
                        'quote_sha256': item['quote_sha256']})
    return record, expected_isin, None
