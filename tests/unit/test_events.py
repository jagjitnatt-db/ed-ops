import json
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.events_contract import EVENT_SCHEMA_DDL
from ed_ops.generator.events import LandingWriter, events_in_window
from tests.gen_factories import make_reference, plan_hours

START = datetime(2026, 11, 2, 18)
CLEAN = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)


@pytest.fixture(scope="module")
def world():
    ref = make_reference()
    return ref, plan_hours(ref, START, 3)


def _sweep(plan, ref, cfg, hours=72):
    out, t = [], START
    while t < START + timedelta(hours=hours):
        out += events_in_window(plan, ref, t, t + timedelta(minutes=60), cfg)
        t += timedelta(minutes=60)
    return out


def test_every_stage_emitted_exactly_once(world):
    ref, plan = world
    events = _sweep(plan, ref, CLEAN)
    ids = Counter(e["event_id"] for e in events)
    assert max(ids.values()) == 1
    by_visit = Counter(e["visit_id"] for e in events)
    optional = ("bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition_ts", "lwbs_ts")
    for r in plan.itertuples(index=False):
        expected = 2  # ARRIVAL + TRIAGE
        expected += sum(0 if pd.isna(getattr(r, c)) else 1 for c in optional)
        expected += 1 if pd.isna(r.lwbs_ts) else 0  # DEPARTURE unless the patient left without being seen
        assert by_visit[r.visit_id] == expected, r.visit_id


def test_emitted_ts_is_window_end_and_not_before_event(world):
    ref, plan = world
    end = START + timedelta(minutes=60)
    for e in events_in_window(plan, ref, START, end, CLEAN):
        assert e["emitted_ts"] == "2026-11-02T19:00:00Z"
        assert e["event_ts"] <= e["emitted_ts"]


def test_arrival_payload_has_phi_and_triage_has_esi(world):
    ref, plan = world
    events = _sweep(plan, ref, CLEAN, hours=4)
    arrival = next(e for e in events if e["event_type"] == "ARRIVAL")
    p = arrival["payload"]
    assert p["patient_name"] and p["mrn"].startswith("MRN-") and p["address"]["city"]
    triage = next(e for e in events if e["event_type"] == "TRIAGE")
    assert 1 <= triage["payload"]["esi_level"] <= 5 and triage["payload"]["triage_note"]


def test_late_events_are_held_back(world):
    ref, plan = world
    events = _sweep(plan, ref, Config(dup_rate=0.0, late_rate=1.0, malformed_rate=0.0), hours=6)
    for e in events:
        delay = datetime.fromisoformat(e["emitted_ts"][:-1]) - datetime.fromisoformat(e["event_ts"][:-1])
        assert delay >= timedelta(minutes=5)


def test_duplicates_reuse_event_id(world):
    ref, plan = world
    events = _sweep(plan, ref, Config(dup_rate=1.0, late_rate=0.0, malformed_rate=0.0))
    counts = Counter(e["event_id"] for e in events)
    assert set(counts.values()) == {2}


def test_malformed_events_are_extra_and_corrupted(world):
    ref, plan = world
    clean = _sweep(plan, ref, CLEAN, hours=6)
    dirty = _sweep(plan, ref, Config(dup_rate=0.0, late_rate=0.0, malformed_rate=1.0), hours=6)
    assert len(dirty) == 2 * len(clean)
    bad = [e for e in dirty if e["event_id"] not in {c["event_id"] for c in clean}]
    for e in bad:
        assert e["visit_id"] is None or e["facility_id"] == "FAC999" or e["payload"].get("esi_level") == 9


def test_deterministic(world):
    ref, plan = world
    cfg = Config()
    assert _sweep(plan, ref, cfg, hours=6) == _sweep(plan, ref, cfg, hours=6)


def test_landing_file_parses_with_bronze_contract(spark, world, tmp_path):
    ref, plan = world
    end = START + timedelta(minutes=60)
    events = events_in_window(plan, ref, START, end, CLEAN)
    path = LandingWriter(str(tmp_path))(events, end)
    assert path == f"{tmp_path}/2026/11/02/19/tick_{int((end - datetime(1970, 1, 1)).total_seconds())}.json"
    assert len(open(path).read().splitlines()) == len(events)
    df = spark.read.schema(EVENT_SCHEMA_DDL).json(path)
    assert df.count() == len(events)
    rows = df.filter("event_type = 'ARRIVAL'").select("event_ts", "payload.address.city", "payload.dob").collect()
    assert rows and all(r["city"] and r["dob"] for r in rows)
    assert df.filter("event_ts IS NULL").count() == 0
    first = json.loads(open(path).readline())
    assert first["event_ts"].endswith("Z")
