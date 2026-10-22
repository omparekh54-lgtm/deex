"""Time and reference-data contracts shared by Module A entry points."""
from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
import math

IST = ZoneInfo('Asia/Kolkata')


def as_ist(value: datetime) -> datetime:
    # Existing APIs supplied naive India-market timestamps. Interpret those as
    # IST explicitly; never let the server's timezone change an as-of decision.
    return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)


def end_of_day(value) -> datetime:
    return datetime.combine(value, time(23, 59, 59), tzinfo=IST)


def reference_failures(context: dict, as_of: datetime, cfg: dict) -> list[str]:
    f = cfg['fundamental']
    failures = []
    if not context.get('sector'):
        failures.append('sector_unavailable_as_of')
    if context.get('suspended') is True:
        failures.append('security_suspended')
    if not context.get('listing_known', False):
        failures.append('listing_unavailable_as_of')
    if context.get('price_date') is None:
        failures.append('market_price_unavailable')
    elif (as_of.date() - context['price_date']).days > f.get('max_price_age_days', 7):
        failures.append('market_price_stale')
    if context.get('liquidity_sessions', 0) < f.get('liquidity_sessions', 63):
        failures.append('liquidity_history_incomplete')
    if context.get('shareholding_period') is None:
        failures.append('shareholding_unavailable')
    elif (as_of.date() - context['shareholding_period']).days > f.get('max_shareholding_age_days', 200):
        failures.append('shareholding_stale')
    if not context.get('surveillance_known', False):
        failures.append('surveillance_coverage_unverified')
    if context.get('open_data_errors', 0):
        failures.append('unresolved_data_quality_errors')
    return failures


def validate_module_a_config(cfg: dict):
    f = cfg['fundamental']
    for key in ('max_result_age_days','liquidity_sessions','max_price_age_days',
                'max_shareholding_age_days','max_cash_flow_age_days','max_surveillance_age_days'):
        value = f.get(key)
        if value is not None and (not math.isfinite(float(value)) or float(value)<=0 or float(value)!=int(float(value))):
            raise ValueError(f'fundamental.{key} must be a positive integer')
    from packages.pms.policies import validate_policy_config
    validate_policy_config(cfg)
