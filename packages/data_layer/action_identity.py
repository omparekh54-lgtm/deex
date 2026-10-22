"""Explicitly reviewed source-record identity links; never a general ISIN alias."""
import hashlib
import json
from pathlib import Path

from packages.data_layer.cloud_sources import SourceError


def validate_action_identities(actions, known):
    reviews = json.loads(Path(__file__).with_name('reviewed_action_identities.json').read_text())
    applied = []
    for action in actions:
        instrument = known.get(action['symbol'])
        if instrument is None or action['isin'] == instrument['isin']:
            continue
        digest = hashlib.sha256(json.dumps(action['raw'], sort_keys=True, default=str).encode()).hexdigest()
        matches = [r for r in reviews if
                   r['symbol'] == action['symbol'] and
                   r['previous_isin'] == action['isin'] and
                   r['current_isin'] == instrument['isin'] and
                   r['ex_date'] == str(action['ex_date']) and
                   r['action_type'] == action['action_type'] and
                   r['raw_record_sha256'] == digest]
        if len(matches) != 1:
            raise SourceError('action_isin_mismatch')
        applied.append({k: matches[0][k] for k in (
            'symbol', 'previous_isin', 'current_isin', 'ex_date',
            'raw_record_sha256', 'master_sha256', 'quote_sha256', 'reviewed_on')})
    return applied
