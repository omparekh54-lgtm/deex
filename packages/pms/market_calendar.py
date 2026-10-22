"""Observed historical sessions plus explicitly sourced future NSE holiday years."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from sqlalchemy import text
from packages.pms.module_a_context import as_ist

DATASET = "NSE_TRADING_CALENDAR"


class MarketCalendar:
    def __init__(
        self,
        sessions=(),
        holiday_years=None,
        special_sessions=(),
        unverified_sessions=(),
    ):
        self.sessions = set(sessions)
        self.holiday_years = holiday_years or {}
        self.special_sessions = set(special_sessions)
        self.unverified_sessions = set(unverified_sessions) - self.special_sessions

    def forward(self, start: date, count: int) -> list[date]:
        if count <= 0:
            return []
        result = []
        cursor = start
        observed_last = max(self.sessions) if self.sessions else None
        for _ in range(max(30, count * 5)):
            if observed_last is not None and cursor <= observed_last:
                trading = cursor in self.sessions
            elif cursor.year in self.holiday_years:
                if cursor in self.unverified_sessions:
                    return []
                trading = cursor in self.special_sessions or (
                    cursor.weekday() < 5
                    and cursor not in self.holiday_years[cursor.year]
                )
            else:
                return []
            if trading:
                result.append(cursor)
                if len(result) == count:
                    return result
            cursor += timedelta(days=1)
        return []

    def age(self, signal_date: date, as_of: date) -> int:
        return sum(signal_date < d <= as_of for d in self.sessions)

    @classmethod
    def from_database(cls, engine, as_of: datetime):
        cutoff = as_ist(as_of)
        with engine.connect() as conn:
            # Historical callers must never see observed sessions after their
            # as-of date. Future OOS sessions can otherwise leak merely because
            # they coexist in the same database.
            sessions = (
                conn.execute(
                    text(
                        """select distinct trade_date from module_e_benchmark_bars
                        where benchmark_code='NIFTY50'
                          and source in ('NSE_HISTORICAL_OR','NSE_DAILY_INDEX')
                          and trade_date<=cast(:as_of as date)
                        order by trade_date"""
                    ),
                    {"as_of": cutoff},
                )
                .scalars()
                .all()
            )
            snapshots = (
                conn.execute(
                    text(
                        """select metadata from provider_snapshots where provider='NSE'
                and dataset=:dataset and observed_at<=:as_of order by observed_at desc"""
                    ),
                    {"dataset": DATASET, "as_of": cutoff},
                )
                .scalars()
                .all()
            )
        years = {}
        special = set()
        unverified = set()
        for metadata in snapshots:
            for year, holidays in (metadata.get("holiday_years") or {}).items():
                years.setdefault(int(year), {date.fromisoformat(d) for d in holidays})
            special.update(
                date.fromisoformat(d) for d in metadata.get("special_sessions", [])
            )
            unverified.update(
                date.fromisoformat(d)
                for d in metadata.get("unverified_special_session_dates", [])
            )
        return cls(sessions, years, special, unverified)


def refresh_holiday_calendar(engine) -> dict:
    from nse import NSE

    cache = Path(".cache_nse")
    cache.mkdir(exist_ok=True)
    with NSE(download_folder=cache) as nse:
        payload = nse.holidays(type="trading")
    metadata = parse_holiday_calendar(payload)
    with engine.begin() as conn:
        conn.execute(
            text("""insert into provider_snapshots(provider,dataset,source_uri,metadata)
            values('NSE',:dataset,:uri,cast(:metadata as jsonb))"""),
            {
                "dataset": DATASET,
                "uri": metadata["source_url"],
                "metadata": json.dumps(metadata),
            },
        )
    return metadata


def parse_holiday_calendar(payload):
    rows = payload.get("CM")
    if not isinstance(rows, list) or len(rows) < 8:
        raise ValueError("NSE did not supply a complete equity holiday calendar")
    years = {}
    unverified = []
    for row in rows:
        raw = row.get("tradingDate") or row.get("date")
        parsed = None
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(str(raw), fmt).date()
                break
            except ValueError:
                pass
        if parsed is None:
            raise ValueError(f"Invalid NSE holiday date: {raw}")
        years.setdefault(str(parsed.year), []).append(parsed.isoformat())
        description = str(row.get("description", "")).casefold()
        if "*" in description or "muhurat" in description:
            unverified.append(parsed.isoformat())
    if any(len(set(dates)) < 8 for dates in years.values()):
        raise ValueError("Incomplete year in NSE holiday response")
    metadata = {
        "holiday_years": {year: sorted(set(dates)) for year, dates in years.items()},
        "special_sessions": [],
        "unverified_special_session_dates": sorted(set(unverified)),
        "source_url": "https://www.nseindia.com/api/holiday-master?type=trading",
        "scope": "Regular weekday sessions; special-session circulars require explicit ingestion.",
    }
    return metadata
