"""Versioned research assumptions, normalized sector matching and decision provenance."""
from __future__ import annotations

import hashlib
import json
import math
import re

BENCHMARK_CODE = 'NIFTYMIDSMALLCAP400'


def sector_key(value: str | None) -> str:
    return re.sub(r'[^a-z0-9]', '', (value or '').casefold().replace('&', 'and'))


def financial_sector(value: str | None) -> bool:
    return sector_key(value) in {
        'financialservices', 'financials', 'banks', 'banking', 'nbfc', 'insurance',
    }


def resolve_leverage_policy(cfg: dict, sector: str | None) -> dict:
    f = cfg['fundamental']
    limits = f.get('leverage_limits', {})
    policies = {sector_key(k): (k, v) for k, v in limits.get('sectors', {}).items()}
    aliases = {sector_key(k): v for k, v in limits.get('sector_aliases', {}).items()}
    canonical = aliases.get(sector_key(sector), sector or '')
    match = policies.get(sector_key(canonical))
    return dict(
        matched_sector=match[0] if match else None,
        match_type='sector' if match else 'default',
        limits={**limits.get('default', {}), **(match[1] if match else {})},
    )


def regime_benchmark(cfg: dict) -> str:
    code = cfg.get('regime', {}).get('benchmark_code', BENCHMARK_CODE)
    if code != BENCHMARK_CODE:
        raise ValueError('Unsupported regime benchmark: v1 requires NIFTYMIDSMALLCAP400')
    return code


def is_cyclical(sector: str | None, industry: str | None) -> bool:
    # Normalize punctuation so the actual NSE label Oil Gas & Consumable Fuels
    # and historical aliases follow the same valuation policy.
    key = sector_key((sector or '') + ' ' + (industry or ''))
    return any(word in key for word in ('metal', 'commodity', 'commodities', 'sugar', 'oilgas', 'oilandgas', 'mining'))


def policy_summary(cfg: dict, sector: str | None = None) -> dict:
    f = cfg['fundamental']
    payload = dict(
        version=f.get('policy_version', 'UNVERSIONED'),
        status=f.get('policy_status', 'UNSPECIFIED'),
        leverage_limits=f.get('leverage_limits', {}),
        cyclical_high_pe_threshold=f.get('cyclical_high_pe_threshold'),
        cyclical_valuation_penalty_points=f.get('cyclical_valuation_penalty_points', 5),
        benchmark_code=regime_benchmark(cfg),
    )
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return dict(
        version=payload['version'], status=payload['status'], fingerprint=digest,
        sector=sector, leverage=resolve_leverage_policy(cfg, sector),
        cyclical_high_pe_threshold=payload['cyclical_high_pe_threshold'],
        cyclical_valuation_penalty_points=payload['cyclical_valuation_penalty_points'],
        benchmark_code=payload['benchmark_code'],
    )


def validate_policy_config(cfg: dict):
    f = cfg['fundamental']
    limits = f.get('leverage_limits', {})
    seen = set()
    for name, policy in [('default', limits.get('default', {})), *limits.get('sectors', {}).items()]:
        key = sector_key(name)
        if not key or key in seen:
            raise ValueError('Duplicate or empty leverage sector: ' + str(name))
        seen.add(key)
        for field in ('max_debt_to_equity', 'min_interest_coverage'):
            value = policy.get(field)
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value < 0):
                raise ValueError(f'Invalid leverage limit {name}.{field}')
    alias_keys = set()
    for alias, target in limits.get('sector_aliases', {}).items():
        key = sector_key(alias)
        if not key or key in alias_keys or not isinstance(target, str) or sector_key(target) not in seen:
            raise ValueError('Invalid or duplicate leverage sector alias: ' + str(alias))
        alias_keys.add(key)
    pe = f.get('cyclical_high_pe_threshold')
    if pe is not None and (isinstance(pe, bool) or not isinstance(pe, (int, float)) or not math.isfinite(pe) or pe <= 0):
        raise ValueError('cyclical_high_pe_threshold must be positive or null')
    penalty = f.get('cyclical_valuation_penalty_points', 5)
    if isinstance(penalty, bool) or not isinstance(penalty, (int, float)) or not math.isfinite(penalty) or not 0 <= penalty <= 100:
        raise ValueError('cyclical_valuation_penalty_points must be between 0 and 100')
    if 'policy_version' in f and (not isinstance(f['policy_version'], str) or not f['policy_version'].strip()):
        raise ValueError('policy_version must be nonempty')
    if 'policy_status' in f and f['policy_status'] not in {'PROVISIONAL_BACKTEST', 'VALIDATED'}:
        raise ValueError('Unknown fundamental policy_status')
    regime_benchmark(cfg)
