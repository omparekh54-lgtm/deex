"""Shared conservative ordering rule for daily-bar entry execution."""
from datetime import datetime, time

from sqlalchemy import text

from packages.pms.module_a_context import IST


def intraday_trigger(open_price, high, trigger):
    return open_price < trigger <= high


def intraday_information_changed(conn, instrument_id, day):
    return bool(conn.execute(text("""select
        (select count(*) from financial_filings where instrument_id=:id and filing_timestamp>:opened and filing_timestamp<=:closed) +
        (select count(*) from shareholding_history where instrument_id=:id and filed_at>:opened and filed_at<=:closed) +
        (select count(*) from earnings_calendar where instrument_id=:id and announced_at>:opened and announced_at<=:closed)"""),
        {"id": instrument_id,
         "opened": datetime.combine(day, time(9, 15), tzinfo=IST),
         "closed": datetime.combine(day, time(15, 30), tzinfo=IST)},
    ).scalar_one())
