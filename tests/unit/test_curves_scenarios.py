from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.generator import curves
from ed_ops.generator.scenarios import acuity_shift_active, arrival_multiplier, build_scenarios
from ed_ops.generator.state import GeneratorState


def test_curves_are_normalised():
    assert len(curves.HOURLY) == 24 and np.mean(curves.HOURLY) == pytest.approx(1.0)
    assert len(curves.DOW) == 7 and np.mean(curves.DOW) == pytest.approx(1.0)
    assert curves.HOURLY[13] > curves.HOURLY[4]  # afternoon busier than 4am


def test_week_of_arrivals_matches_baseline():
    per_week = sum(curves.arrivals_per_minute(240, h, d) * 60 for d in range(7) for h in range(24))
    assert per_week == pytest.approx(240 * 7)


@pytest.mark.parametrize("ftype", ["level_1_trauma", "community", "rural_critical_access", "pediatric"])
def test_esi_mix_is_a_distribution(ftype):
    for shift in (False, True):
        p = curves.esi_mix(ftype, shift)
        assert p.shape == (5,) and p.sum() == pytest.approx(1.0) and (p > 0).all()


def test_flu_shift_moves_mass_to_esi_2_and_3():
    base, flu = curves.esi_mix("community", False), curves.esi_mix("community", True)
    assert flu[1] + flu[2] > base[1] + base[2]


def _facilities():
    return pd.DataFrame(
        {
            "facility_id": ["FAC001", "FAC002", "FAC003", "FAC004"],
            "region": ["North", "North", "South", "South"],
            "scenario_profile": ["normal", "surge_prone", "surge_prone", "normal"],
        }
    )


def test_flu_window_covers_surge_prone_and_their_region():
    start = datetime(2026, 10, 26)
    state = GeneratorState("main", start, start, start)
    windows = build_scenarios(_facilities(), state, Config())
    flu = [w for w in windows if w.name == "flu_surge"][0]
    assert flu.facility_ids == frozenset({"FAC001", "FAC002", "FAC003"})  # region of FAC002 = North
    assert flu.start == start + timedelta(days=8) and flu.end == start + timedelta(days=11)
    assert all(w.name != "live_surge" for w in windows)
    assert arrival_multiplier(windows, "FAC001", start + timedelta(days=9)) == pytest.approx(1.4)
    assert arrival_multiplier(windows, "FAC004", start + timedelta(days=9)) == pytest.approx(1.0)
    assert acuity_shift_active(windows, "FAC002", start + timedelta(days=9))
    assert not acuity_shift_active(windows, "FAC002", start + timedelta(days=12))


def test_live_surge_starts_after_offset_for_two_surge_prone_facilities():
    start = datetime(2026, 10, 26)
    live = datetime(2026, 11, 9, 12)
    windows = build_scenarios(_facilities(), GeneratorState("main", start, live, live, live), Config(live_surge_offset_min=30, live_surge_duration_min=90))
    surge = [w for w in windows if w.name == "live_surge"][0]
    assert surge.facility_ids == frozenset({"FAC002", "FAC003"})
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=29)) == 1.0
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=30)) == pytest.approx(2.0)
    assert arrival_multiplier(windows, "FAC002", live + timedelta(minutes=120)) == 1.0


def test_no_surge_prone_means_no_scenarios():
    f = _facilities().assign(scenario_profile="normal")
    s = datetime(2026, 10, 26)
    assert build_scenarios(f, GeneratorState("main", s, s, s), Config()) == []
