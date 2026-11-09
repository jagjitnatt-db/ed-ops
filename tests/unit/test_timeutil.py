from datetime import datetime

from ed_ops import timeutil


def test_floor_to_minute_and_hour():
    t = datetime(2026, 11, 9, 12, 34, 56)
    assert timeutil.floor_to(t, 60) == datetime(2026, 11, 9, 12, 34)
    assert timeutil.floor_to(t, 3600) == datetime(2026, 11, 9, 12, 0)


def test_five_minute_marks_are_exclusive_start_inclusive_end():
    marks = timeutil.five_minute_marks(datetime(2026, 11, 9, 12, 0), datetime(2026, 11, 9, 12, 17))
    assert marks == [datetime(2026, 11, 9, 12, 5), datetime(2026, 11, 9, 12, 10), datetime(2026, 11, 9, 12, 15)]


def test_to_local_chicago_standard_time():
    local = timeutil.to_local(datetime(2026, 11, 9, 18, 0), "America/Chicago")
    assert (local.hour, local.utcoffset().total_seconds()) == (12, -6 * 3600)


def test_utcnow_is_naive():
    assert timeutil.utcnow().tzinfo is None


def test_epoch_seconds():
    assert timeutil.epoch_seconds(datetime(1970, 1, 1, 0, 1)) == 60
