import json
from collections import Counter
from datetime import datetime, timedelta

import pytest

from ed_ops.config import Config
from ed_ops.generator.stores import InMemoryStore
from ed_ops.generator.tick import advance, initial_state
from tests.gen_factories import make_reference

T0 = datetime(2026, 11, 2, 12)
CLEAN = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)


class MemoryWriter:
    def __init__(self):
        self.files: list[tuple[datetime, list[dict]]] = []

    def __call__(self, events, window_end):
        self.files.append((window_end, events))
        return f"mem://{window_end.isoformat()}"


def _store():
    return InMemoryStore(initial_state(T0 + timedelta(days=1), 1))  # backfill_start = T0


def test_initial_state_is_hour_aligned():
    s = initial_state(datetime(2026, 11, 9, 12, 34), 14)
    assert s.backfill_start_ts == s.last_planned_ts == s.last_emitted_ts == datetime(2026, 10, 26, 12)
    assert s.live_started_ts is None


def test_backfill_writes_one_file_per_hour_and_persists_once():
    store, writer = _store(), MemoryWriter()
    paths = advance(store, make_reference(), CLEAN, T0 + timedelta(hours=6), live=False, writer=writer, persist_each_chunk=False)
    assert len(paths) == 6 and [w for w, _ in writer.files] == [T0 + timedelta(hours=h) for h in range(1, 7)]
    state = store.load_state()
    assert state.last_planned_ts == state.last_emitted_ts == T0 + timedelta(hours=6)
    assert store.plan.visit_id.is_unique and len(store.plan) > 50


def test_replay_to_same_time_is_a_noop():
    store, writer = _store(), MemoryWriter()
    advance(store, make_reference(), CLEAN, T0 + timedelta(hours=2), live=False, writer=writer, persist_each_chunk=False)
    assert advance(store, make_reference(), CLEAN, T0 + timedelta(hours=2), live=False, writer=writer, persist_each_chunk=False) == []


def test_catch_up_after_downtime_chunks_hourly_without_duplicates():
    store, writer = _store(), MemoryWriter()
    ref = make_reference()
    advance(store, ref, CLEAN, T0 + timedelta(minutes=60), live=True, writer=writer, persist_each_chunk=True)
    advance(store, ref, CLEAN, T0 + timedelta(hours=6, minutes=7), live=True, writer=writer, persist_each_chunk=True)
    assert [w for w, _ in writer.files][-1] == T0 + timedelta(hours=6, minutes=7)
    assert len(writer.files) == 7  # 1 + five full hours + one 7-minute chunk
    assert store.plan.visit_id.is_unique
    ids = Counter(e["event_id"] for _, evs in writer.files for e in evs)
    assert max(ids.values()) == 1


def test_live_sets_live_started_once():
    store = _store()
    ref = make_reference()
    advance(store, ref, CLEAN, T0 + timedelta(minutes=1), live=True, writer=MemoryWriter(), persist_each_chunk=True)
    first = store.load_state().live_started_ts
    advance(store, ref, CLEAN, T0 + timedelta(minutes=2), live=True, writer=MemoryWriter(), persist_each_chunk=True)
    assert first == T0 + timedelta(minutes=1) == store.load_state().live_started_ts


def test_deterministic_across_fresh_stores():
    def run():
        w = MemoryWriter()
        advance(_store(), make_reference(), Config(), T0 + timedelta(hours=3), live=False, writer=w, persist_each_chunk=False)
        return json.dumps(w.files, default=str)

    assert run() == run()


def test_missing_state_raises():
    with pytest.raises(RuntimeError, match="generator state missing"):
        advance(InMemoryStore(None), make_reference(), CLEAN, T0, live=True, writer=MemoryWriter(), persist_each_chunk=True)
