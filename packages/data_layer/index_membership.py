"""Project the official index snapshot onto the PMS cash-equity universe.

Keep every source row in the receipt. REIT units are outside this collector's
equity master; an exact reviewed dummy is an index adjustment, not an issuer.
Unknown equity constituents and changed identities still block ingestion.
"""
from packages.data_layer.cloud_sources import SourceError


REVIEWED_DUMMY = {
    "Company Name": "Dummy HEG Ltd.", "Industry": "Capital Goods",
    "Symbol": "DUMMYHEG", "Series": "EQ", "ISIN Code": "DUM545A01024",
}


def equity_membership(members, known):
    included, excluded, seen = [], [], set()
    for row in members:
        symbol = row['Symbol']
        if symbol in seen:
            raise SourceError('index_membership_duplicate')
        seen.add(symbol)
        if row['Series'] == 'RR':
            # RR is NSE's REIT series. No membership or price is fabricated for
            # instruments absent from the cash-equity master.
            excluded.append({**row, 'reason': 'REIT_OUTSIDE_CASH_EQUITY_UNIVERSE'})
        elif row == REVIEWED_DUMMY:
            excluded.append({**row, 'reason': 'REVIEWED_NON_TRADABLE_INDEX_ADJUSTMENT'})
        elif (symbol not in known or known[symbol]['isin'] != row['ISIN Code']
              or row['Series'] not in {'EQ', 'BE', 'BZ', 'SM', 'ST', 'SZ'}):
            raise SourceError('index_membership_identity_unresolved')
        else:
            included.append(row)
    if not included:
        raise SourceError('index_membership_empty_equity_universe')
    return included, excluded
