"""Finstack-backed official transport and explicitly labelled reference snapshots.

The transport reuses Finstack's NSE client and network governor while retaining
the *original* exchange response in DEEX's raw evidence store. No broker APIs.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time

from .cloud_sources import Document, MAX_BYTES, OfficialHTTP, SourceError, allowed_url

FINSTACK_COMMIT = '4b336cbdf3750834f67787c8eaf7d311e8b4e9d7'
_configured_root = None


def load_finstack(root):
    global _configured_root
    try:
        import finstack as fs
    except ImportError:
        raise SourceError('finstack_not_installed') from None
    home = str(Path(root) / 'finstack_cache')
    if _configured_root != home:
        fs.configure(FINSTACK_HOME=home, budgets={'nse': 1.0})
        _configured_root = home
    return fs


class FinstackHTTP(OfficialHTTP):
    """Compatible with Lake.fetch(http_factory=...), including official API calls."""
    def __init__(self, root, *, seconds=90):
        super().__init__(seconds=seconds)
        self.fs = load_finstack(root)

    def fetch(self, url, *, financial_render=False):
        allowed_url(url)
        from .source_pacing import PUBLIC_SOURCE_PACER
        for attempt in range(3):
            if time.monotonic() >= self.deadline:
                raise SourceError('collection_deadline_exceeded')
            try:
                PUBLIC_SOURCE_PACER.wait(url, self.deadline)
                # This is the pinned Finstack 0.8.0 NSE transport, not an arbitrary
                # URL endpoint exposed to the website. URLs are allowlisted above.
                response = self.fs.nse_client()._transport.request(url)
                code = response.status_code
                if code != 200:
                    raise SourceError('source_http_' + str(code))
                allowed_url(str(response.url))
                body = response.content
                if not body or len(body) > MAX_BYTES:
                    raise SourceError('source_empty_or_too_large')
                if not financial_render and body.lstrip().lower().startswith((b'<html', b'<!doctype html')):
                    raise SourceError('empty_or_html_source')
                return Document(url, body, hashlib.sha256(body).hexdigest(),
                                datetime.now(timezone.utc), len(body))
            except SourceError:
                raise
            except Exception as exc:
                code = getattr(getattr(exc, 'response', None), 'status_code', None)
                if code and code not in (429, 500, 502, 503, 504):
                    raise SourceError('source_http_' + str(code)) from None
                if attempt == 2:
                    raise SourceError('finstack_transport_unavailable') from None
                time.sleep(2**attempt)
        raise SourceError('finstack_transport_unavailable')


def stage_reference(lake, dataset):
    """Reconstructed history is acquisition evidence, not certified source truth."""
    fs = load_finstack(lake.root)
    if dataset == 'shareholding':
        frame = fs.nse_shareholding_history()
        api = 'nse_shareholding_history'
    elif dataset == 'membership':
        frame = fs.nse_index_membership_history()
        api = 'nse_index_membership_history'
    else:
        raise ValueError('Only shareholding/membership are bulk reference snapshots')
    if frame is None or frame.empty:
        raise SourceError('finstack_reference_empty')
    source = 'https://github.com/omparekh54-lgtm/Finstack/blob/' + FINSTACK_COMMIT + '/src/finstack/reference_plus.py'
    from .local_lake import dumps
    for start in range(0, len(frame), 5000):
        block = frame.iloc[start:start+5000]
        body = block.to_json(orient='table', date_format='iso', index=False).encode()
        if len(body) > MAX_BYTES:
            raise SourceError('finstack_reference_snapshot_too_large')
        doc = Document(source, body, hashlib.sha256(body).hexdigest(), datetime.now(timezone.utc), len(body))
        with lake.db:
            digest = lake.archive(doc)
            lake.db.execute('insert or ignore into records values(?,?,?,?,?,?)',
                            ('finstack_' + dataset + '_staged', digest, None, None,
                             dumps({'api': api, 'rows': len(block), 'total_rows': len(frame), 'block_start': start,
                                    'finstack_version': fs.__version__,
                                    'evidence_kind': 'NORMALIZED_THIRD_PARTY_RECONSTRUCTION',
                                    'coverage': 'UNVERIFIED', 'source_code_commit': FINSTACK_COMMIT}), digest))
    return {'dataset': dataset, 'rows': len(frame), 'status': 'STAGED_UNVERIFIED',
            'note': 'Compare with original exchange filings/releases before accepting historical inputs.'}
