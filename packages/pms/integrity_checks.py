"""Fixed, bound-parameter PostgreSQL audit rules. Findings never mutate source data."""

from dataclasses import dataclass
from packages.pms.matrix_data import COVERAGE


@dataclass(frozen=True)
class IntegrityCheck:
    code: str
    group: str
    severity: str
    sql: str
    remediation: str


# Every instrument-scoped rule uses the dated audit universe, including inactive stocks.
IDS = "in (select value::bigint from jsonb_array_elements_text(cast(:ids as jsonb)))"
CAL = "select distinct trade_date from pms.module_e_benchmark_bars where benchmark_code='NIFTY50' and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date between :start and :end"
NUM = "('NaN','Infinity','-Infinity')"
CHECKS = []


def add(code, group, sql, remediation, severity="ERROR"):
    CHECKS.append(IntegrityCheck(code, group, severity, sql, remediation))


add(
    "MASTER_IDENTITY",
    "UNIVERSE",
    f"""select exchange||':'||coalesce(nse_symbol,replace(trading_symbol,'-EQ','')) instrument_id,count(*) records
from pms.instruments where id {IDS} group by 1 having count(*)>1 or exchange||':'||coalesce(nse_symbol,replace(trading_symbol,'-EQ','')) is null or btrim(split_part(exchange||':'||coalesce(nse_symbol,replace(trading_symbol,'-EQ','')),':',2))=''""",
    "Correct canonical identities using exchange evidence; retain former listings.",
)
add(
    "LISTING_DATES",
    "UNIVERSE",
    f"""select id,listed_from,delisted_on from pms.instruments where id {IDS}
and (listed_from is null or delisted_on<=listed_from or (active and delisted_on<=:end))""",
    "Verify listing/delisting dates and current active status against exchange records.",
)
add(
    "PRICE_VALUES",
    "PRICES",
    f"""select instrument_id,trade_date,source from pms.daily_prices where instrument_id {IDS}
and trade_date between :warmup and :end and (open is null or high is null or low is null or close is null or volume is null
or open<=0 or high<=0 or low<=0 or close<=0 or high<greatest(open,close,low) or low>least(open,close,high)
or volume<0 or turnover<0 or open::text in {NUM} or high::text in {NUM} or low::text in {NUM}
or close::text in {NUM} or volume::text in {NUM} or turnover::text in {NUM})""",
    "Re-fetch invalid raw bars; never silently interpolate or clamp prices.",
)
add(
    "PRICE_DUPLICATES",
    "PRICES",
    f"""select instrument_id,trade_date,source,count(*) records from pms.daily_prices
where instrument_id {IDS} and trade_date between :warmup and :end group by 1,2,3 having count(*)>1""",
    "Reconcile duplicate provider identities before research.",
)
add(
    "PRICE_SOURCE_CONFLICTS",
    "PRICES",
    f"""select instrument_id,trade_date,count(distinct (open,high,low,close,volume)) versions
from pms.daily_prices where instrument_id {IDS} and trade_date between :start and :end
and (lower(source) like 'nse%' or lower(source)='angel_one') group by 1,2 having count(distinct (open,high,low,close,volume))>1""",
    "Compare conflicting raw providers; document the authoritative bar and adjustment basis. Differences are not automatically corruption.",
    "REVIEW",
)
add(
    "PRICE_SESSION_COVERAGE",
    "COVERAGE",
    f"select * from ({COVERAGE}) coverage where expected<>observed or invalid>0 or unverified_raw_source>0",
    "Repair missing or invalid sessions and verify raw/adjusted sources, including 257 prior sessions.",
)
add(
    "PRICE_NON_SESSION",
    "COVERAGE",
    f"""select instrument_id,trade_date,source from pms.daily_prices where instrument_id {IDS}
and trade_date between :start and :end and trade_date not in ({CAL})""",
    "Check the independent exchange calendar, special sessions and bar dates before removing any bar.",
    "REVIEW",
)
add(
    "PRICE_LISTING_BOUNDS",
    "UNIVERSE",
    f"""select p.instrument_id,p.trade_date,i.listed_from,i.delisted_on from pms.daily_prices p join pms.instruments i on i.id=p.instrument_id
where p.instrument_id {IDS} and p.trade_date between :start and :end and (p.trade_date<i.listed_from or p.trade_date>=i.delisted_on)""",
    "Reconcile symbol histories and inclusive/exclusive listing interval conventions.",
)
add(
    "CALENDAR_WINDOW",
    "COVERAGE",
    f"""select min(trade_date) first_session,max(trade_date) last_session,count(*) sessions from ({CAL}) dates
having count(*)=0 or min(trade_date)>cast(:start as date)+7 or max(trade_date)<cast(:end as date)-7""",
    "Load official Nifty 50 sessions covering the requested period; independent calendar review is still required.",
)
add(
    "CALENDAR_WARMUP",
    "COVERAGE",
    """select count(distinct trade_date) sessions from pms.module_e_benchmark_bars where benchmark_code='NIFTY50'
and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX') and trade_date<:start having count(distinct trade_date)<257""",
    "Load at least 257 official sessions before the audit/research start.",
)
add(
    "BENCHMARK_SESSIONS",
    "COVERAGE",
    f"""select codes.code,c.trade_date from ({CAL}) c cross join
(values ('NIFTYMIDSMALLCAP400'),('INDIAVIX'),('NIFTY500')) codes(code)
where not exists(select 1 from pms.module_e_benchmark_bars b where b.benchmark_code=codes.code and b.trade_date=c.trade_date and b.source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX'))""",
    "Repair exact missing official benchmark/VIX dates; row totals are not a coverage denominator.",
)
add(
    "BENCHMARK_VALUES",
    "PRICES",
    f"""select benchmark_code,trade_date,source from pms.module_e_benchmark_bars where trade_date between :warmup and :end
and benchmark_code in ('NIFTY50','NIFTY500','NIFTYMIDSMALLCAP400','INDIAVIX') and
(close is null or close<=0 or close::text in {NUM} or open<=0 or low<=0 or high<=0
or high<greatest(open,close,low) or low>least(open,close,high) or open::text in {NUM} or high::text in {NUM} or low::text in {NUM})""",
    "Re-fetch invalid official observations; close-only rows are allowed where the provider supplies no OHLC.",
)
add(
    "FILINGS_CHRONOLOGY",
    "POINT_IN_TIME",
    f"""select id,instrument_id,period_end,filing_timestamp,exchange_received_at,exchange_disseminated_at
from pms.financial_filings where instrument_id {IDS} and period_end<=:end and
(filing_timestamp is null or filing_timestamp>:audit_stamp or filing_timestamp<cast(period_end as timestamp) at time zone 'Asia/Kolkata'
or filing_timestamp<exchange_disseminated_at or filing_timestamp<exchange_received_at
or revision_number is null or revision_number<1 or consolidated is null)""",
    "Use the latest relevant exchange publication timestamp and explicit filing scope/revision; never date back a filing to its reporting period.",
)
add(
    "FILINGS_DUPLICATES",
    "POINT_IN_TIME",
    f"""select instrument_id,period_end,filing_timestamp,source,consolidated,revision_number,filing_kind,count(*) records
from pms.financial_filings where instrument_id {IDS} and filing_timestamp<=:stamp group by 1,2,3,4,5,6,7 having count(*)>1""",
    "Reconcile duplicate filing/revision/scope identities; do not overwrite older vintages.",
)
add(
    "FACTS_VALUES_UNITS",
    "FUNDAMENTALS",
    f"""select f.id filing_id,f.instrument_id,v.fact_name,v.unit from pms.financial_filings f join pms.financial_facts v on v.filing_id=f.id
where f.instrument_id {IDS} and f.filing_timestamp<=:stamp and (v.value is null or v.value::text in {NUM} or v.unit is null or btrim(v.unit)='')""",
    "Validate missing/nonfinite facts and explicit units. Negative profits are permitted; blanks cannot be converted to zero.",
)
add(
    "FACTS_ORPHANS",
    "FUNDAMENTALS",
    """select v.filing_id,v.fact_name from pms.financial_facts v left join pms.financial_filings f on f.id=v.filing_id where f.id is null""",
    "Repair orphan facts against their original filing, not a guessed replacement.",
)
add(
    "FUNDAMENTAL_PRESENCE",
    "FUNDAMENTALS",
    f"""select i.id,i.nse_symbol from pms.instruments i where i.id {IDS} and
not exists(select 1 from pms.financial_filings f join pms.financial_facts v on v.filing_id=f.id where f.instrument_id=i.id and f.filing_timestamp<=:stamp)""",
    "Investigate absent financial history per historical company; missing data must remain ineligible.",
    "REVIEW",
)
add(
    "SHAREHOLDING_VALUES",
    "POINT_IN_TIME",
    f"""select id,instrument_id,period_end,filed_at from pms.shareholding_history where instrument_id {IDS} and period_end<=:end and
(filed_at is null or filed_at>:audit_stamp or filed_at<cast(period_end as timestamp) at time zone 'Asia/Kolkata' or promoter_holding_pct not between 0 and 100
or promoter_pledge_pct not between 0 and 100 or promoter_holding_pct is null or promoter_pledge_pct is null
or promoter_holding_pct::text in {NUM} or promoter_pledge_pct::text in {NUM} or total_shares<=0 or total_shares::text in {NUM})""",
    "Validate publication timestamps, percentage denominators and share totals from original filings.",
)
add(
    "CORPORATE_ACTION_VALUES",
    "ACTIONS",
    f"""select id,instrument_id,action_type,ex_date from pms.corporate_actions where instrument_id {IDS} and ex_date between :warmup and :end and
(announced_at is null or announced_at>(cast(ex_date as timestamp) at time zone 'Asia/Kolkata')+interval '9 hours 15 minutes'
or (upper(action_type) in ('SPLIT','BONUS') and (ratio_from is null or ratio_to is null or ratio_from<=0 or ratio_to<=0 or ratio_from::text in {NUM} or ratio_to::text in {NUM}))
or (upper(action_type)='DIVIDEND' and (cash_amount is null or cash_amount<0 or cash_amount::text in {NUM})))""",
    "Verify ex-date opening knowability, positive ratios and dividend amounts before applying adjustments.",
)
add(
    "CORPORATE_ACTION_CONFLICTS",
    "ACTIONS",
    f"""select instrument_id,upper(action_type) action_type,ex_date,count(distinct (ratio_from,ratio_to,cash_amount)) versions
from pms.corporate_actions where instrument_id {IDS} and ex_date between :warmup and :end group by 1,2,3
having count(distinct (ratio_from,ratio_to,cash_amount))>1""",
    "Reconcile conflicting action ratios/amounts using original exchange announcements.",
)
add(
    "UNSUPPORTED_ACTIONS",
    "ACTIONS",
    f"""select id,instrument_id,action_type,ex_date from pms.corporate_actions where instrument_id {IDS}
and ex_date between :warmup and :end and upper(action_type) not in ('SPLIT','BONUS','DIVIDEND')""",
    "Review rights, mergers, delistings and other unmodelled entitlements before any affected held-position replay.",
    "REVIEW",
)
add(
    "EARNINGS_KNOWABILITY",
    "POINT_IN_TIME",
    f"""select id,instrument_id,event_date,announced_at from pms.earnings_calendar where instrument_id {IDS}
and event_date between :warmup and :end and (announced_at is null or announced_at>(cast(event_date as timestamp) at time zone 'Asia/Kolkata')+interval '23 hours 59 minutes 59 seconds')""",
    "Preserve the timestamp when the event date became public; retrospective unknown dates cannot prove an advance blackout.",
    "REVIEW",
)
add(
    "A_DECISION_KNOWABILITY",
    "DECISIONS",
    f"""select id,instrument_id,as_of,eligible,snapshot->>'filing_timestamp' filing_timestamp from pms.module_a_history
where instrument_id {IDS} and as_of between :start_stamp and :stamp""",
    "Inspect the referenced filing timestamp: decisions must not use a filing published after their as-of time.",
)
# A timestamp in JSON is validated in Python, avoiding unsafe casts on malformed data.
for table, partition in [
    ("sector_history", "a.instrument_id=b.instrument_id"),
    (
        "index_constituents_history",
        "a.instrument_id=b.instrument_id and a.index_code=b.index_code",
    ),
    (
        "shares_outstanding_history",
        "a.instrument_id=b.instrument_id and a.source=b.source",
    ),
    (
        "surveillance_history",
        "a.instrument_id=b.instrument_id and a.surveillance_type=b.surveillance_type",
    ),
]:
    upper = table.upper()
    add(
        upper + "_INTERVALS",
        "REFERENCE",
        f"""select id,instrument_id,effective_from,effective_to from pms.{table} where instrument_id {IDS}
    and effective_from<=:stamp and (effective_to<=effective_from or effective_from is null)""",
        "Repair empty/inverted effective intervals; retain date provenance and history.",
    )
    add(
        upper + "_OVERLAPS",
        "REFERENCE",
        f"""select a.id,b.id other_id,a.instrument_id from pms.{table} a join pms.{table} b
    on {partition} and a.id<b.id and a.effective_from<coalesce(b.effective_to,'infinity')
    and b.effective_from<coalesce(a.effective_to,'infinity') where a.instrument_id {IDS} and a.effective_from<=:stamp and b.effective_from<=:stamp""",
        "Review overlapping effective intervals and provider precedence; cross-provider agreement may be legitimate.",
        "REVIEW",
    )
add(
    "SHARES_VALUES",
    "REFERENCE",
    f"""select id,instrument_id,knowable_at from pms.shares_outstanding_history where instrument_id {IDS} and effective_from<=:end
and (shares_outstanding<=0 or shares_outstanding::text in {NUM} or knowable_at is null)""",
    "Use positive shares with an explicit historical knowability timestamp.",
)
add(
    "SECTOR_PRESENCE",
    "REFERENCE",
    f"""select i.id,i.nse_symbol from pms.instruments i where i.id {IDS} and not exists
(select 1 from pms.sector_history h where h.instrument_id=i.id and h.effective_from<=:end and (h.effective_to is null or h.effective_to>:start) and btrim(h.sector)<>'')""",
    "Obtain dated sector history; current master classifications alone do not prove historical coverage.",
)
add(
    "SURVEILLANCE_PRESENCE",
    "REFERENCE",
    """select types.kind from (values ('ASM'),('GSM')) types(kind) where not exists
(select 1 from pms.surveillance_history h where h.surveillance_type=types.kind and h.effective_from<=:stamp and (h.effective_to is null or h.effective_to>=:start_stamp))""",
    "Load source-backed ASM/GSM histories. Presence does not establish completeness of the archive.",
)
add(
    "SOURCE_PROVENANCE",
    "PROVENANCE",
    """select id,provider,dataset from pms.provider_snapshots where observed_at<=:audit_stamp and
(source_uri is null or btrim(source_uri)='' or content_sha256 is null or content_sha256 !~ '^[0-9a-fA-F]{64}$')""",
    "Locate the original source artifact and SHA256. Missing hashes require review, not fabricated digests.",
    "REVIEW",
)
add(
    "OPEN_CRITICAL_DQ",
    "QUALITY",
    """select id,dataset,code from pms.data_quality_issues where severity='ERROR' and resolved_at is null""",
    "Investigate each condition; this audit never resolves or suppresses quality issues automatically.",
)
add(
    "REFERENCE_SESSION_GAPS",
    "REFERENCE",
    f"""select i.id,count(*) missing_sector_sessions from pms.instruments i join ({CAL}) c
on c.trade_date>=i.listed_from and (i.delisted_on is null or c.trade_date<i.delisted_on) where i.id {IDS} and not exists
(select 1 from pms.sector_history h where h.instrument_id=i.id and h.effective_from<=c.trade_date and (h.effective_to is null or h.effective_to>c.trade_date) and btrim(h.sector)<>'') group by i.id""",
    "Reconstruct dated sector intervals for missing sessions; a present current sector is insufficient.",
)
add(
    "FACTS_DUPLICATES",
    "FUNDAMENTALS",
    f"""select v.filing_id,v.fact_name,count(*) records from pms.financial_facts v join pms.financial_filings f on f.id=v.filing_id
where f.instrument_id {IDS} and f.filing_timestamp<=:stamp group by 1,2 having count(*)>1""",
    "Reconcile duplicated facts within the same filing before calculating financial ratios.",
)
add(
    "FINANCIAL_UNIT_CONVENTIONS",
    "FUNDAMENTALS",
    f"""select f.id filing_id,f.instrument_id,v.fact_name,v.unit
from pms.financial_filings f join pms.financial_facts v on v.filing_id=f.id where f.instrument_id {IDS} and f.filing_timestamp<=:stamp and
((v.fact_name='diluted_eps' and lower(btrim(coalesce(v.unit,'')))<>'inr/share')
or (v.fact_name in ('sales','operating_profit','pat','pbt','other_income','exceptional_items','operating_cash_flow','debt','equity','interest_expense') and lower(btrim(coalesce(v.unit,'')))<>'inr crore'))""",
    "Reconcile source units and scale against the normalized ingestion contract (INR crore; INR/share for EPS). Generic INR or other labels require source review; do not guess a conversion.",
    "REVIEW",
)
# Coverage inventory, not an assumed statutory deadline: omit the newest 120
# days so an unpublished recent period is not silently labelled corrupt.
# Fiscal quarters are compared by calendar-quarter key, not exact month-end.
EXPECTED_QUARTERS = f"""select i.id instrument_id,q.period_start::date,
    (q.period_start+interval '3 months'-interval '1 day')::date period_end
from pms.instruments i cross join lateral generate_series(
    date_trunc('quarter',greatest(cast(:start as date)-interval '2 years',
        coalesce(i.listed_from,cast(:start as date)))),
    date_trunc('quarter',least(cast(:end as date)-interval '120 days',
        coalesce(i.delisted_on,cast(:end as date)))),interval '3 months') q(period_start)
where i.id {IDS} and i.listed_from is not null and q.period_start>=i.listed_from
    and q.period_start+interval '3 months'<=cast(:end as date)-interval '120 days'
    and (i.delisted_on is null or q.period_start+interval '3 months'<=i.delisted_on)"""
add(
    "FINANCIAL_QUARTER_COVERAGE", "COVERAGE",
    f"""select e.instrument_id,e.period_end from ({EXPECTED_QUARTERS}) e where not exists
    (select 1 from pms.financial_filings f where f.instrument_id=e.instrument_id
    and date_trunc('quarter',f.period_end)=e.period_start
    and lower(f.filing_kind) in ('quarter','quarterly') and f.filing_timestamp<=:stamp)""",
    "Backfill missing quarterly reporting vintages from original exchange artifacts, including two years of financial warmup. The 120-day inventory lag is not a legal filing deadline; investigate fiscal-calendar exceptions.",
)
add(
    "QUARTER_CORE_FACTS", "FUNDAMENTALS",
    f"""select f.id filing_id,f.instrument_id,required.fact_name from pms.financial_filings f
    cross join (values ('sales'),('operating_profit'),('pat'),('diluted_eps'),
    ('pbt'),('other_income'),('exceptional_items'),('interest_expense')) required(fact_name)
    where f.instrument_id {IDS} and f.period_end>=cast(:start as date)-interval '2 years'
    and f.filing_timestamp<=:stamp and lower(f.filing_kind) in ('quarter','quarterly')
    and not exists(select 1 from pms.financial_facts v where v.filing_id=f.id
    and v.fact_name=required.fact_name and v.value is not null and v.value::text not in {NUM})""",
    "Reparse missing duration/scope-aware facts using ingestion V2. Missing facts remain unknown, not zero; preserve all filing vintages.",
)
add(
    "ANNUAL_CASH_FLOW_COVERAGE", "FUNDAMENTALS",
    f"""select distinct e.instrument_id,extract(year from e.period_end)::int audit_year
    from ({EXPECTED_QUARTERS}) e where not exists
    (select 1 from pms.financial_filings f where f.instrument_id=e.instrument_id
    and lower(f.filing_kind) in ('annual','year','yearly')
    and f.period_end between e.period_end-interval '2 years' and e.period_end
    and f.filing_timestamp<=:stamp and exists(select 1 from pms.financial_facts v
    where v.filing_id=f.id and v.fact_name='operating_cash_flow' and v.value is not null
    and v.value::text not in {NUM}) and exists(select 1 from pms.financial_facts v
    where v.filing_id=f.id and v.fact_name='pat' and v.value is not null and v.value::text not in {NUM}))""",
    "Backfill annual OCF/PAT companions for each historical year; quarterly PAT cannot substitute for annual cash flow. This inventories coverage, not historical publication readiness.",
)
add(
    "SHAREHOLDING_QUARTER_COVERAGE", "COVERAGE",
    f"""select e.instrument_id,e.period_end from ({EXPECTED_QUARTERS}) e where not exists
    (select 1 from pms.shareholding_history h where h.instrument_id=e.instrument_id
    and date_trunc('quarter',h.period_end)=e.period_start and h.filed_at<=:stamp)""",
    "Obtain missing historical shareholding/pledge filings and publication timestamps; do not copy current percentages backwards.",
)
add(
    "FINANCIAL_EXTRACTION_VERSION", "PROVENANCE",
    f"""select f.id filing_id,f.instrument_id,f.period_end from pms.financial_filings f
    where f.instrument_id {IDS} and f.filing_timestamp<=:stamp and f.xbrl_uri is not null
    and coalesce(f.raw_payload->'financial_extraction'->>'version','')<>'FINANCIAL_XBRL_v2'""",
    "Reprocess the original stored XBRL through the reviewed ingestion V2 parser. Verify migration 012 first; do not treat old receipt success as V2 evidence.",
)
add(
    "EARNINGS_ARCHIVE_PRESENCE", "COVERAGE",
    f"""select distinct f.instrument_id from pms.financial_filings f where f.instrument_id {IDS}
    and f.period_end between :start and :end and f.filing_timestamp<=:stamp and not exists
    (select 1 from pms.earnings_calendar e where e.instrument_id=f.instrument_id
    and e.event_date between :start and :end and e.announced_at<=:stamp)""",
    "Reconcile the original board-meeting/earnings-announcement inventory. A financial result publication does not establish advance earnings blackout knowability.",
    "REVIEW",
)
CHECKS = tuple(CHECKS)

REVIEWS = (
    (
        "INDEPENDENT_CALENDAR",
        "COVERAGE",
        "Reconcile an independent exchange session/holiday inventory, including special sessions. Comparing benchmarks to the same Nifty50 rows cannot detect dates missing from all series.",
    ),
    (
        "SURVIVORSHIP_INVENTORY",
        "UNIVERSE",
        "Reconcile the dated exchange listing/delisting and symbol-change inventory. Including inactive rows cannot prove absent companies were captured.",
    ),
    (
        "GSM_ARCHIVE_COMPLETENESS",
        "REFERENCE",
        "Reconcile the full source circular inventory and attachments against reconstructed ASM/GSM intervals. Row count and the absence of interval overlaps cannot certify archive completeness.",
    ),
    (
        "REFERENCE_VINTAGES",
        "POINT_IN_TIME",
        "Review original sector/index/shareholding/financial source vintages and publication dates. Effective dates and currently loaded values alone cannot establish historical knowability.",
    ),
    (
        "PRICE_ADJUSTMENT_RECONCILIATION",
        "ACTIONS",
        "Independently reconcile action-day prices/share counts and source adjustment conventions, especially fractional entitlements, rights and delistings.",
    ),
)
