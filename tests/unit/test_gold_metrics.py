from datetime import datetime, timedelta

from ed_ops.delta import conform, table_def
from ed_ops.metrics.drivers import compute_baselines
from ed_ops.pipeline.gold_metrics import compute_tick_frames, day_chunks
from tests.factories import SCENARIO_VISITS, T, facilities_df, roster_df, visits_df

BASELINE_INPUT_DDL = (
    "facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, "
    "patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE"
)


def test_day_chunks_cover_range_exactly():
    start, end = datetime(2026, 11, 1, 6), datetime(2026, 11, 3, 13, 25)
    chunks = day_chunks(start, end)
    assert chunks[0][0] == start and chunks[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))
    assert all(e - s <= timedelta(days=1) for s, e in chunks)
    assert day_chunks(end, end) == []


def test_tick_frames_shapes_match_gold_tables(spark):
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    baselines = compute_baselines(spark.createDataFrame([], BASELINE_INPUT_DDL), "America/Chicago")
    frames = compute_tick_frames(visits_df(spark, SCENARIO_VISITS), roster, facilities, baselines, T, "America/Chicago")

    current = conform(frames.current, table_def("gold", "facility_current")).collect()
    assert len(current) == 2 and {r.facility_name for r in current} == {"FAC001 Hospital", "FAC002 Hospital"}
    assert all(r.snapshot_ts == T for r in current)

    history = conform(frames.history, table_def("gold", "facility_snapshot_5min")).collect()
    assert sorted({r.snapshot_ts for r in history}) == [T - timedelta(minutes=m) for m in (25, 20, 15, 10, 5, 0)]
    assert len(history) == 12

    board = conform(frames.board, table_def("gold", "ed_board_current")).collect()
    assert {r.visit_id for r in board} == {"v1", "v2", "v3", "v4", "v5", "v9"}


def test_pending_gap_covers_downtime_plus_late_event_margin():
    from ed_ops.pipeline.gold_metrics import pending_gap

    now = datetime(2026, 11, 9, 12, 2)
    assert pending_gap(None, now) is None
    assert pending_gap(datetime(2026, 11, 9, 11, 35), now) is None  # ticker's own 30-min recompute covers it
    assert pending_gap(datetime(2026, 11, 9, 9, 0), now) == (datetime(2026, 11, 9, 8, 30), datetime(2026, 11, 9, 11, 30))


def test_gap_fill_waits_until_generator_and_silver_caught_up():
    from ed_ops.pipeline.gold_metrics import caught_up

    now = datetime(2026, 11, 9, 12, 0)
    assert caught_up(now - timedelta(minutes=1), now - timedelta(minutes=2), now, 60)
    assert not caught_up(now - timedelta(hours=2), now - timedelta(hours=2), now, 60)  # generator still catching up
    assert not caught_up(now - timedelta(minutes=1), now - timedelta(hours=1), now, 60)  # silver behind the generator
    assert not caught_up(None, now, now, 60)
