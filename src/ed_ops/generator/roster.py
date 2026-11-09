"""Attending physician roster: three 8-hour local shifts per day, converted to naive UTC."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

ROSTER_COLUMNS = ("facility_id", "shift_start", "shift_end", "shift_name", "attendings_on_duty")
SHIFTS = (("day", 7, 15, 1.0), ("evening", 15, 23, 1.1), ("night", 23, 7, 0.6))
VISITS_PER_ATTENDING_SHIFT = 45.0


def attendings_for(baseline_daily_visits: int, profile: str, shift_name: str, factor: float) -> int:
    n = round(baseline_daily_visits / VISITS_PER_ATTENDING_SHIFT * factor)
    if profile == "understaffed" and shift_name in ("evening", "night"):
        n -= 1
    if profile == "high_performer":
        n += 1
    return max(1, n)


def _utc(aware: datetime) -> datetime:
    return aware.astimezone(UTC).replace(tzinfo=None)


def build_roster(facilities: pd.DataFrame, start: date, days: int, tz_name: str) -> pd.DataFrame:
    tz = ZoneInfo(tz_name)
    rows = []
    for fac in facilities.itertuples(index=False):
        for d in range(days):
            day = start + timedelta(days=d)
            for name, start_hour, end_hour, factor in SHIFTS:
                end_day = day + timedelta(days=1) if end_hour <= start_hour else day
                rows.append(
                    (
                        fac.facility_id,
                        _utc(datetime.combine(day, time(start_hour), tzinfo=tz)),
                        _utc(datetime.combine(end_day, time(end_hour), tzinfo=tz)),
                        name,
                        attendings_for(int(fac.baseline_daily_visits), fac.scenario_profile, name, factor),
                    )
                )
    return pd.DataFrame(rows, columns=list(ROSTER_COLUMNS))
