"""Reviewed stale quote metadata, verified against the live equity master.

These links authorize current classifications only, never prices, filings,
corporate actions, instrument changes or historical sector backfills.
"""
import json
from pathlib import Path


def reviews():
    return json.loads(Path(__file__).with_name('reviewed_classification_identities.json').read_text())


def reviewed_classification_identity(http, metadata_doc, metadata, symbol, isin, series):
    from packages.data_layer.cloud_sources import SourceError, parse_master

    matches = [r for r in reviews() if
               r['symbol'] == symbol and r['current_isin'] == isin
               and r['metadata_isin'] == metadata.get('isin')
               and r['metadata_sha256'] == metadata_doc.sha256
               and r['series'] == series and r['company'] == metadata.get('companyName')
               and metadata.get('symbol') == symbol]
    if len(matches) != 1:
        raise SourceError('classification_identity_mismatch')
    review = matches[0]
    master_doc = http.fetch('https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv')
    current = parse_master(master_doc.body).get(symbol)
    if (current is None or current['isin'] != isin
            or series not in current.get('series_list', [current['series']])
            or current['name'] != review['company']):
        raise SourceError('classification_identity_mismatch')
    return {**review, 'verified_master_sha256': master_doc.sha256,
            'master_observed_at': master_doc.observed_at.isoformat()}
