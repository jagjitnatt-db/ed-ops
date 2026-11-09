"""Time helpers. Project convention: every Python datetime is naive UTC."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def epoch_seconds(dt: datetime) -> int:
    return int(dt.replace(tzinfo=UTC).timestamp())


def floor_to(dt: datetime, seconds: int) -> datetime:
    epoch = epoch_seconds(dt)
    return datetime.fromtimestamp(epoch - epoch % seconds, UTC).replace(tzinfo=None)


def five_minute_marks(start_exclusive: datetime, end_inclusive: datetime) -> list[datetime]:
    marks: list[datetime] = []
    t = floor_to(end_inclusive, 300)
    while t > start_exclusive:
        marks.append(t)
        t -= timedelta(minutes=5)
    return sorted(marks)


def to_local(dt: datetime, tz_name: str) -> datetime:
    return dt.replace(tzinfo=UTC).astimezone(ZoneInfo(tz_name))
