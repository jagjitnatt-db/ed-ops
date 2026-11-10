from datetime import datetime, timedelta

from ed_ops.setup import generator_live, streaming_active

NOW = datetime(2026, 11, 10, 12)


def test_generator_live_once_started():
    assert generator_live(None) is False
    assert generator_live(NOW - timedelta(days=1)) is True


def test_streaming_active_requires_recent_state_updates():
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(minutes=1), NOW) is True
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(hours=2), NOW) is False  # paused
    assert streaming_active(None, NOW, NOW) is False  # fresh setup: backfill just wrote state but never went live
