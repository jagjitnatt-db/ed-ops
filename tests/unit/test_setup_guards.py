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


def test_heartbeat_window_scales_with_tick():
    # a 5-minute tick means the generator heartbeat is routinely > 5 minutes old while streaming runs
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(minutes=12), NOW, tick_seconds=300) is True
    assert streaming_active(NOW - timedelta(hours=3), NOW - timedelta(minutes=16), NOW, tick_seconds=300) is False


def test_refusal_prefers_the_jobs_api_over_the_heartbeat():
    from ed_ops.setup import refusal_reason

    live, recent, stale = NOW - timedelta(hours=3), NOW - timedelta(minutes=1), NOW - timedelta(hours=1)
    # Jobs API says a run is active (for example generator restarting while notes_ai/gold keep running): refuse
    assert refusal_reason(True, live, stale, NOW, 60) is not None
    # Jobs API says nothing is running (paused + cancelled): allow even inside the heartbeat window
    assert refusal_reason(False, live, recent, NOW, 60) is None
    # Jobs API unknown (no job id configured): fall back to the heartbeat
    assert refusal_reason(None, live, recent, NOW, 60) is not None
    assert refusal_reason(None, live, stale, NOW, 60) is None
    assert refusal_reason(None, None, None, NOW, 60) is None
