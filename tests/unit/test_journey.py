import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from ed_ops.generator.journey import (
    PLAN_COLUMNS, census_at, load_factor, plan_arrivals, plan_visit, staffing_factor,
)
from tests.gen_factories import FACILITIES, make_reference, plan_hours

START = datetime(2026, 11, 2, 6)  # Monday 00:00 Chicago


@pytest.fixture(scope="module")
def day_plan():
    return plan_hours(make_reference(), START, 24)


def _le(a, b):
    return pd.isna(a) or pd.isna(b) or a <= b


def test_plan_columns_and_types(day_plan):
    from ed_ops.ddl.metadata import load_catalog_def

    assert list(PLAN_COLUMNS) == load_catalog_def().table("sim", "visit_plan").column_names
    assert list(day_plan.columns) == list(PLAN_COLUMNS)
    assert str(day_plan["arrival_ts"].dtype).startswith("datetime64")
    assert len(day_plan) > 100


def test_journey_ordering_invariants(day_plan):
    for r in day_plan.itertuples(index=False):
        assert r.arrival_ts < r.triage_ts
        if not pd.isna(r.lwbs_ts):
            assert pd.isna(r.greet_ts) and pd.isna(r.bed_assigned_ts) and pd.isna(r.disposition_ts)
            assert r.lwbs_ts > r.triage_ts and r.departure_ts == r.lwbs_ts
            assert r.nurse_esi >= 3
            continue
        assert r.triage_ts < r.greet_ts and r.triage_ts <= r.bed_assigned_ts
        assert max(r.greet_ts, r.bed_assigned_ts) <= r.disposition_ts < r.departure_ts
        assert r.disposition in {"DISCHARGE", "ADMIT", "TRANSFER"}
        if r.disposition == "ADMIT":
            assert r.departure_ts - r.disposition_ts <= timedelta(hours=48, minutes=1)
        if not pd.isna(r.vent_start_ts):
            assert r.true_esi <= 2
            assert r.bed_assigned_ts <= r.vent_start_ts <= r.vent_end_ts <= r.departure_ts


def test_visit_ids_unique_and_match_arrival_minute(day_plan):
    assert day_plan.visit_id.is_unique
    for r in day_plan.itertuples(index=False):
        assert re.fullmatch(r"FAC\d{3}-\d{12}-\d{3}", r.visit_id)
        assert r.visit_id.split("-")[1] == r.arrival_ts.strftime("%Y%m%d%H%M")


def test_arrivals_strictly_inside_window():
    ref = make_reference()
    s, e = datetime(2026, 11, 2, 18), datetime(2026, 11, 2, 18, 1)
    total = 0
    for i in range(200):
        new = plan_arrivals(np.random.default_rng([1, i]), ref, pd.DataFrame(columns=PLAN_COLUMNS), s, e, [], "America/Chicago")
        assert ((new.arrival_ts > s) & (new.arrival_ts < e)).all()
        total += len(new)
    assert total > 0


def test_arrival_volume_matches_baseline_over_a_week():
    ref = make_reference(FACILITIES.head(1))  # 72 visits/day
    week = plan_hours(ref, START, 24 * 7, seed=7)
    assert abs(len(week) - 72 * 7) / (72 * 7) < 0.08


def test_load_feedback_increases_door_to_provider():
    ref = make_reference()
    fac = ref.facility("FAC001")
    patient = {**ref.patient(0), "city": fac.city, "state": fac.state}

    def median_d2p(occupancy):
        rng = np.random.default_rng(11)
        waits = []
        for i in range(400):
            v = plan_visit(rng, fac, f"FAC001-202611021200-{i:03d}", datetime(2026, 11, 2, 18), occupancy, 4.0, False, patient, 0, datetime(2026, 11, 2, 19))
            if v["greet_ts"] is not None:
                waits.append((v["greet_ts"] - v["arrival_ts"]).total_seconds() / 60)
        return float(np.median(waits))

    assert median_d2p(1.3) > 1.5 * median_d2p(0.4)
    assert load_factor(0.5) == 1.0 and load_factor(1.2) > load_factor(0.9) > 1.0
    assert staffing_factor(3.0) == 1.0 and staffing_factor(12.0) > 1.0


def test_undertriage_share_about_three_percent(day_plan):
    week = plan_hours(make_reference(), START, 72, seed=3)
    share = ((week.nurse_esi - week.true_esi) >= 2).mean()
    assert 0.015 < share < 0.05


def test_census_at(day_plan):
    t = START + timedelta(hours=12)
    census = census_at(day_plan, t)
    expected = day_plan[(day_plan.arrival_ts <= t) & (day_plan.departure_ts > t)].groupby("facility_id").size().to_dict()
    assert census == expected and census
    assert census_at(pd.DataFrame(columns=PLAN_COLUMNS), t) == {}


def test_plan_is_deterministic():
    a = plan_hours(make_reference(), START, 3, seed=5)
    b = plan_hours(make_reference(), START, 3, seed=5)
    pd.testing.assert_frame_equal(a, b)
