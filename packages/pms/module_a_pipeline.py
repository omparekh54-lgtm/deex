from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta
from collections import Counter
from sqlalchemy import Engine, text

from packages.data_layer.queries import PointInTimeQueries
from packages.pms.config import load_strategy_config
from packages.pms.fundamental_history import calculate_financial_history
from packages.pms.module_a import FundamentalDecision, FundamentalSnapshot, evaluate_fundamentals
from packages.pms.module_a_context import as_ist, reference_failures, validate_module_a_config
from packages.pms.module_a_data import ModuleAReferenceInputs
from packages.pms.policies import policy_summary, is_cyclical


def evaluate_inputs(history: list[dict], context: dict, as_of: datetime, cfg: dict):
    """Single deterministic evaluation path for API, workers, CLI and replay."""
    as_of = as_ist(as_of)
    validate_module_a_config(cfg)
    f = cfg['fundamental']
    data_failures = reference_failures(context, as_of, cfg)
    flags = []
    if context.get('market_cap_cr') is None:
        data_failures.append('market_cap_unavailable')
    if context.get('promoter_pledge_pct') is None:
        data_failures.append('promoter_pledge_unavailable')
    try:
        metrics = calculate_financial_history(history, as_of=as_of,
            consistency_metrics=tuple(f.get('consistency_growth_metrics',['sales','pat'])),
            min_ocf_to_pat=float(f['min_ocf_to_pat']),
            max_cash_flow_age_days=int(f.get('max_cash_flow_age_days',550)),
            min_core_profit_growth=float(f['min_pat_yoy_pct']))
    except ValueError:
        metrics = None
        data_failures.append('quarterly_financial_history_unavailable')
    quality_failures = list(metrics.quality_failures) if metrics else ['financial_quality_unavailable']
    values = {}
    filed_at = expires = None
    if metrics:
        flags.extend(metrics.flags)
        filed_at = as_ist(metrics.latest_filing_timestamp)
        expires = filed_at + timedelta(days=int(f.get('max_result_age_days',100)))
        if as_of >= expires:
            data_failures.append('eligibility_expired_100_days')
        values = {key:getattr(metrics,key) for key in (
            'quarterly_sales_cr','pat_cr','sales_yoy_pct','operating_profit_yoy_pct','pat_yoy_pct',
            'eps_yoy_pct','positive_growth_quarters_last_4','ttm_sales_rising','ttm_profit_rising',
            'ocf_to_pat','growth_acceleration_pp','margin_expansion_bps','debt_to_equity','interest_coverage',
            'record_sales_8q','record_pat_8q','turnaround','pbt_yoy_pct','core_pbt_yoy_pct')}
        flags.append('quality_growth_uses_normalized_pre_tax_profit')
    else:
        values = dict(quarterly_sales_cr=0,pat_cr=0,sales_yoy_pct=None,operating_profit_yoy_pct=None,
                      pat_yoy_pct=None,eps_yoy_pct=None,positive_growth_quarters_last_4=0,
                      ttm_sales_rising=False,ttm_profit_rising=False,ocf_to_pat=None)
    sector = context.get('sector')
    industry = context.get('industry')
    applied_policy = policy_summary(cfg, sector)
    if applied_policy['status'] == 'PROVISIONAL_BACKTEST':
        flags.append('provisional_policy')
    policy = applied_policy['leverage']['limits']
    max_debt, min_cover = policy.get('max_debt_to_equity'),policy.get('min_interest_coverage')
    if max_debt is None or min_cover is None:
        quality_failures.append('sector_leverage_policy_unconfigured')
    if not metrics or metrics.debt_to_equity is None:
        quality_failures.append('debt_to_equity_unavailable')
    elif metrics.debt_to_equity < 0 or (max_debt is not None and metrics.debt_to_equity>float(max_debt)):
        quality_failures.append('debt_to_equity')
    if not metrics or (metrics.interest_coverage is None and not metrics.interest_free):
        quality_failures.append('interest_coverage_unavailable')
    elif not metrics.interest_free and min_cover is not None and metrics.interest_coverage<float(min_cover):
        quality_failures.append('interest_coverage')
    cyclical = is_cyclical(sector, industry)
    pe = (context['market_cap_cr']/metrics.ttm_pat_cr if metrics and metrics.ttm_pat_cr is not None
          and metrics.ttm_pat_cr>0 and context.get('market_cap_cr') is not None else None)
    snapshot = FundamentalSnapshot(
        market_cap_cr=float(context.get('market_cap_cr') or 0),
        avg_traded_value_cr=float(context.get('avg_traded_value_cr') or 0),
        asm_gsm=bool(context.get('asm_gsm')),
        promoter_pledge_pct=float(context['promoter_pledge_pct']) if context.get('promoter_pledge_pct') is not None else 100,
        **values, quality_pass=not quality_failures,
        sector=sector,industry=industry,filing_timestamp=filed_at,eligible_until=expires,
        flags=tuple(dict.fromkeys(flags)),triggers=metrics.triggers if metrics else (),
        data_failures=tuple(dict.fromkeys(data_failures)),quality_failures=tuple(dict.fromkeys(quality_failures)),
        cyclical=cyclical,valuation_pe=pe,
        reference_context={**{k:v for k,v in context.items() if k not in {'metadata','current_sector'}},
                           'applied_policy': applied_policy},
    )
    return snapshot,evaluate_fundamentals(snapshot,cfg)


class ModuleAPipeline:
    """Point-in-time eligibility with explicit missing-data decisions and persistence."""
    def __init__(self, engine: Engine, cfg: dict | None=None):
        self.engine = engine
        self.cfg = load_strategy_config() if cfg is None else cfg
        validate_module_a_config(self.cfg)
        self.financials = PointInTimeQueries(engine)
        self.inputs = ModuleAReferenceInputs(engine,self.cfg)

    def evaluate(self, instrument_id: str, as_of: datetime, *, persist: bool=False):
        stamp = as_ist(as_of)
        with self.engine.connect() as conn:
            pk = self.financials._db_instrument_id(conn,instrument_id)
        contexts = self.inputs.contexts(stamp,pk)
        if not contexts:
            raise KeyError(instrument_id)
        history = self.financials.financial_history_as_of(instrument_id,stamp,limit_periods=24)
        snapshot,decision = evaluate_inputs(history,contexts[0],stamp,self.cfg)
        if persist:
            self.persist(pk,stamp,snapshot,decision)
        return snapshot,decision

    def persist(self, instrument_pk: int, as_of: datetime, snapshot, decision):
        payload = self.serialize(snapshot,decision)
        with self.engine.begin() as conn:
            conn.execute(text('''
                insert into module_a_history(instrument_id,as_of,eligible,score,failed_rules,triggers,flags,snapshot,strategy_version)
                values(:id,:as_of,:eligible,:score,cast(:failed as jsonb),cast(:triggers as jsonb),cast(:flags as jsonb),cast(:snapshot as jsonb),:version)
                on conflict(instrument_id,as_of,strategy_version) do update
                set eligible=excluded.eligible,score=excluded.score,failed_rules=excluded.failed_rules,
                    triggers=excluded.triggers,flags=excluded.flags,snapshot=excluded.snapshot
            '''),{'id':instrument_pk,'as_of':as_ist(as_of),'eligible':decision.eligible,'score':decision.score,
                  'failed':json.dumps(decision.failed_rules),'triggers':json.dumps(decision.triggers),
                  'flags':json.dumps(decision.flags),'snapshot':json.dumps(payload,default=str,allow_nan=False),
                  'version':self.cfg['version']})

    def scan(self, as_of: datetime, *, persist: bool=True, symbols: list[str] | None=None):
        stamp = as_ist(as_of)
        results = []
        counts = Counter()
        histories = self.financials.financial_histories_as_of(stamp,limit_periods=24)
        for context in self.inputs.contexts(stamp):
            instrument = context['instrument_id']
            if symbols is not None and instrument not in symbols:
                continue
            history = histories.get(instrument,[])
            snapshot,decision = evaluate_inputs(history,context,stamp,self.cfg)
            if persist:
                self.persist(context['id'],stamp,snapshot,decision)
            counts.update(decision.failed_rules)
            results.append({'instrument_id':instrument,**self.serialize(snapshot,decision)})
        return {'as_of':stamp.isoformat(),'strategy_version':self.cfg['version'],'evaluated':len(results),
                'eligible':sum(r['decision']['eligible'] for r in results),
                'configuration_ready':counts.get('sector_leverage_policy_unconfigured',0)==0,
                'policy':policy_summary(self.cfg),
                'stocks_with_financial_history':sum(r['snapshot']['filing_timestamp'] is not None for r in results),
                'rejection_counts':dict(counts),'results':results,'persisted':persist}

    def eligible_list(self, as_of: datetime) -> dict:
        """Revalidate saved eligible candidates; never expose expired cached membership."""
        stamp = as_ist(as_of)
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                with latest as (
                    select distinct on (instrument_id) instrument_id,eligible,as_of
                    from module_a_history where as_of<=:as_of and strategy_version=:version
                    order by instrument_id,as_of desc
                )
                select i.exchange||':'||i.nse_symbol instrument_id from latest a
                join instruments i on i.id=a.instrument_id where a.eligible and i.active
            """),{'as_of':stamp,'version':self.cfg['version']}).scalars().all()
        results = []
        for instrument in rows:
            snapshot,decision = self.evaluate(instrument,stamp)
            if decision.eligible:
                results.append({'instrument_id':instrument,**self.serialize(snapshot,decision)})
        return {'as_of':stamp.isoformat(),'strategy_version':self.cfg['version'],
                'source':'stored_candidates_revalidated_against_current_asof_inputs',
                'candidate_count':len(rows),'eligible':len(results),'results':results}

    @staticmethod
    def serialize(snapshot: FundamentalSnapshot, decision: FundamentalDecision) -> dict:
        return {'snapshot':asdict(snapshot),'decision':asdict(decision)}
